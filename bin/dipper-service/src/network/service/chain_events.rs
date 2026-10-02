//! Chain event source abstraction
//!
//! This module provides a trait-based abstraction for fetching the current
//! state of indexing agreements from a subgraph. `chain_listener` polls
//! agreements whose `lastStateChangeBlock` has advanced since the last poll
//! and reconciles the returned state against dipper's local DB.
//!
//! ## Expected Subgraph Schema
//!
//! The subgraph must expose the aggregated `IndexingAgreement` entity with
//! at least the fields dipper's state-diff logic reads:
//!
//! ```graphql
//! type IndexingAgreement @entity {
//!   id: Bytes!                       # bytes16 agreement ID
//!   payer: Bytes!                    # address
//!   indexer: Bytes!                  # address
//!   allocationId: Bytes!             # address
//!   state: AgreementState!
//!   canceledBy: Bytes!               # address (zero if not canceled)
//!   lastStateChangeBlock: BigInt!    # block of the latest state change
//! }
//!
//! enum AgreementState {
//!   NotAccepted
//!   Accepted
//!   CanceledByServiceProvider
//!   CanceledByPayer
//! }
//! ```
//!
//! `canceledBy` is `Bytes.empty()` for agreements that have not been
//! canceled, which serialises as `"0x"` over GraphQL. The parser here maps
//! that to `Address::ZERO` so consumers can compare against a real canceler
//! address without optional-unwrapping.

use std::time::Duration;

use async_trait::async_trait;
use dipper_core::{ids::IndexingAgreementId, time::now_secs};
use rand::Rng;
use serde::Deserialize;
use thegraph_core::alloy::primitives::Address;
use url::Url;

/// State of an agreement as recorded by the subgraph. Mirrors the
/// `AgreementState` enum in the indexing-payments-subgraph schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgreementState {
    NotAccepted,
    Accepted,
    CanceledByServiceProvider,
    CanceledByPayer,
}

impl AgreementState {
    /// Whether this state represents a canceled agreement.
    pub fn is_canceled(self) -> bool {
        matches!(
            self,
            AgreementState::CanceledByServiceProvider | AgreementState::CanceledByPayer,
        )
    }

    /// Whether the agreement ever reached `Accepted` (including post-accept
    /// cancellation states). `NotAccepted` is the only case where it hasn't.
    pub fn reached_accepted(self) -> bool {
        !matches!(self, AgreementState::NotAccepted)
    }
}

/// Cursor identifying a position in the `(lastStateChangeBlock, id)` total
/// order used to paginate `IndexingAgreement` snapshots.
///
/// graph-node implicitly tiebreaks `orderBy: lastStateChangeBlock` by `id`
/// ascending, so every entity has a unique position. `Ord` falls out of
/// the derive: `block` first, then `Option<IndexingAgreementId>` (where
/// `None < Some(_)` matches "block boundary < any row inside the block").
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Cursor {
    pub(crate) block: u64,
    pub(crate) id: Option<IndexingAgreementId>,
}

impl Cursor {
    pub(super) fn genesis() -> Self {
        Self::default()
    }

    pub(super) fn at_block(block: u64) -> Self {
        Self { block, id: None }
    }

    /// Hex string for the keyset query's `id_gt` clause; falls back to
    /// an all-zero sentinel below any real id when the cursor sits at a
    /// block boundary.
    fn id_hex(&self) -> String {
        self.id
            .unwrap_or_else(|| IndexingAgreementId::from_bytes([0u8; 16]))
            .to_string()
    }
}

/// Snapshot of an agreement's current state, pulled from the subgraph. One
/// snapshot per agreement whose `(lastStateChangeBlock, id)` is strictly past
/// the supplied cursor.
#[derive(Debug, Clone)]
pub struct AgreementStateSnapshot {
    /// The agreement ID (bytes16 from contract)
    pub agreement_id: IndexingAgreementId,
    /// The indexer address
    pub indexer: Address,
    /// Current lifecycle state
    pub state: AgreementState,
    /// Address that initiated the cancel. `Address::ZERO` when the
    /// agreement is not canceled.
    pub canceled_by: Address,
    /// Block number of the latest state change. Used as the pagination
    /// cursor for subsequent polls.
    pub last_state_change_block: u64,
    /// Timestamp of when the agreement was accepted on-chain (`0` if never
    /// accepted). Sourced from the subgraph's `acceptedAt`.
    pub accepted_at: u64,
    /// Transaction hash (0x hex) of the on-chain accept. 32-byte zero sentinel
    /// if never accepted. Sourced from the subgraph's `acceptedAtTx`.
    pub accepted_tx: String,
    /// Timestamp of when the agreement was canceled (`0` if not canceled).
    /// Sourced from the subgraph's `canceledAt`.
    pub canceled_at: u64,
    /// Transaction hash (0x hex) of the on-chain cancel. 32-byte zero sentinel
    /// if not canceled. Sourced from the subgraph's `canceledAtTx`.
    pub canceled_tx: String,
}

/// Errors that can occur when fetching snapshots.
#[derive(Debug, thiserror::Error)]
pub enum ChainEventError {
    /// Transient error - can be retried
    #[error("transient error: {0}")]
    Transient(String),

    /// Permanent error - should not be retried
    #[error("permanent error: {0}")]
    Permanent(String),
}

impl ChainEventError {
    /// Returns true if this error is transient and the operation can be retried.
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Transient(_))
    }
}

/// Result of fetching changed agreement snapshots.
pub struct ChangedAgreementsResult {
    /// Snapshots of agreements whose state changed since the last poll,
    /// ordered ascending by `(last_state_change_block, id)`.
    pub snapshots: Vec<AgreementStateSnapshot>,
    /// The latest block the subgraph has indexed. Used for stall detection
    /// and idle-heartbeat checks, not for cursor advance.
    pub latest_block: u64,
    /// Timestamp of the latest indexed block (seconds since epoch), if
    /// available. Used by the expiration service for chain-time comparisons.
    pub latest_block_timestamp: Option<u64>,
    /// Safe cursor for the caller to advance to. Holds back before the
    /// earliest parse failure so dropped entities are re-read next poll.
    /// Equal to the input cursor when a failure had no parseable block
    /// (caller's `cursor > previous` guard turns that into "don't advance"
    /// rather than "reset to genesis").
    pub cursor: Cursor,
}

/// Trait for fetching agreement state snapshots from a subgraph.
///
/// Implementations may use different data sources (subgraph, RPC, etc.)
/// but must provide the same interface for the chain listener service.
#[async_trait]
pub trait ChainEventSource: Send + Sync {
    /// Fetch all agreements strictly past the supplied keyset cursor.
    ///
    /// Results are filtered to agreements where the payer matches the
    /// configured signer address, and ordered by
    /// `(last_state_change_block, id)` ascending so the consumer processes
    /// transitions in the order they happened on-chain (with `id` breaking
    /// ties at the same block).
    ///
    /// # Arguments
    /// * `since` - Cursor identifying the last consumed `(block, id)` pair.
    ///   The query returns rows whose `(last_state_change_block, id)` is
    ///   strictly greater than this cursor in lexicographic order.
    /// * `pinned_block` - When `Some`, query the subgraph at exactly that
    ///   block (graph-node's `block: { number: $pinned }` argument). The
    ///   chain listener uses this to keep every page within a single drain
    ///   reading from the same snapshot, so a row's
    ///   `lastStateChangeBlock` cannot shift mid-drain. When `None`, query
    ///   at the subgraph's latest indexed block.
    ///
    /// # Returns
    /// * `Ok(ChangedAgreementsResult)` - Snapshots and the new safe cursor
    /// * `Err(ChainEventError::Transient(_))` - Temporary failure, can retry
    /// * `Err(ChainEventError::Permanent(_))` - Permanent failure, should not retry
    async fn get_changed_agreements(
        &self,
        since: &Cursor,
        pinned_block: Option<u64>,
    ) -> Result<ChangedAgreementsResult, ChainEventError>;
}

/// Configuration for the subgraph event source.
#[derive(Debug, Clone)]
pub struct SubgraphEventSourceConfig {
    /// The subgraph endpoint URL
    pub endpoint: Url,
    /// API key for authentication (optional for local/test subgraphs)
    pub api_key: Option<String>,
    /// The payer address to filter events by
    pub payer_address: Address,
    /// Request timeout
    pub request_timeout: Duration,
    /// Maximum retry attempts for transient failures
    pub max_retries: u32,
    /// How far ahead of the host's wall clock the subgraph's reported
    /// chain timestamp may sit before the response is rejected as
    /// corrupt. Without this, a single poisoned value would ratchet the
    /// persisted timestamp into the future and never recover.
    pub wall_clock_skew_tolerance_secs: u64,
    /// Bypass the wall-clock skew check. See the doc comment on
    /// `ChainListenerConfig::bypass_chain_clock_defenses` for the
    /// threat-model implications; must be false in production.
    pub bypass_chain_clock_defenses: bool,
}

/// Subgraph-based implementation of chain event source.
///
/// Queries a subgraph that indexes the aggregated `IndexingAgreement` entity.
pub struct SubgraphEventSource {
    client: reqwest::Client,
    config: SubgraphEventSourceConfig,
}

/// GraphQL response wrapping the `indexingAgreements` list and subgraph meta.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChangedAgreementsResponse {
    indexing_agreements: Vec<IndexingAgreementEntity>,
    #[serde(rename = "_meta")]
    meta: SubgraphMeta,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IndexingAgreementEntity {
    /// Hex-encoded bytes16 agreement ID
    id: String,
    /// Hex-encoded indexer address
    indexer: String,
    /// Agreement state enum (as string)
    state: String,
    /// Hex-encoded canceler address (`"0x"` when not canceled)
    canceled_by: String,
    /// Block number as string (BigInt in GraphQL)
    last_state_change_block: String,
    /// Accept timestamp as string (BigInt in GraphQL); `"0"` if never accepted.
    #[serde(default)]
    accepted_at: String,
    /// Hex-encoded tx hash of the on-chain accept (32-byte zero if never accepted).
    #[serde(default)]
    accepted_at_tx: String,
    /// Cancel timestamp as string (BigInt in GraphQL); `"0"` if not canceled.
    #[serde(default)]
    canceled_at: String,
    /// Hex-encoded tx hash of the on-chain cancel (32-byte zero if not canceled).
    #[serde(default)]
    canceled_at_tx: String,
}

#[derive(Debug, Deserialize)]
struct SubgraphMeta {
    block: SubgraphBlock,
}

#[derive(Debug, Deserialize)]
struct SubgraphBlock {
    number: u64,
    timestamp: Option<u64>,
}

impl SubgraphEventSource {
    /// Create a new subgraph event source.
    #[expect(
        clippy::expect_used,
        reason = "predates this lint; fix when next touched"
    )]
    pub fn new(config: SubgraphEventSourceConfig) -> Self {
        let client = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .build()
            .expect("Failed to build HTTP client");

        Self { client, config }
    }

    /// Execute a query with exponential backoff retry.
    async fn query_with_retry<T: for<'de> Deserialize<'de>>(
        &self,
        query: &str,
        variables: serde_json::Value,
    ) -> Result<T, ChainEventError> {
        let mut last_error = None;

        for attempt in 0..=self.config.max_retries {
            if attempt > 0 {
                let delay = calculate_retry_delay(attempt);
                tracing::debug!(
                    attempt = attempt,
                    delay_ms = delay.as_millis(),
                    "Retrying subgraph query after delay"
                );
                tokio::time::sleep(delay).await;
            }

            match self.execute_query(query, &variables).await {
                Ok(result) => return Ok(result),
                Err(e) if e.is_transient() => {
                    tracing::debug!(
                        attempt = attempt,
                        error = %e,
                        "Transient error querying subgraph"
                    );
                    last_error = Some(e);
                }
                Err(e) => return Err(e),
            }
        }

        Err(last_error
            .unwrap_or_else(|| ChainEventError::Permanent("No attempts made".to_string())))
    }

    /// Execute a single GraphQL query.
    async fn execute_query<T: for<'de> Deserialize<'de>>(
        &self,
        query: &str,
        variables: &serde_json::Value,
    ) -> Result<T, ChainEventError> {
        let body = serde_json::json!({
            "query": query,
            "variables": variables,
        });

        let mut request = self.client.post(self.config.endpoint.as_str()).json(&body);

        if let Some(ref api_key) = self.config.api_key {
            request = request.bearer_auth(api_key);
        }

        let response = request.send().await.map_err(|e| {
            if e.is_timeout() || e.is_connect() {
                ChainEventError::Transient(format!("Network error: {}", e))
            } else {
                ChainEventError::Permanent(format!("Request error: {}", e))
            }
        })?;

        if response.status().is_server_error() {
            return Err(ChainEventError::Transient(format!(
                "Server error: {}",
                response.status()
            )));
        }

        if !response.status().is_success() {
            return Err(ChainEventError::Permanent(format!(
                "HTTP error: {}",
                response.status()
            )));
        }

        #[derive(Deserialize)]
        struct GraphQLResponse<T> {
            data: Option<T>,
            errors: Option<Vec<GraphQLError>>,
        }

        #[derive(Deserialize)]
        struct GraphQLError {
            message: String,
        }

        let graphql_response: GraphQLResponse<T> = response
            .json()
            .await
            .map_err(|e| ChainEventError::Permanent(format!("Failed to parse response: {}", e)))?;

        if let Some(errors) = graphql_response.errors {
            let messages: Vec<_> = errors.iter().map(|e| e.message.as_str()).collect();
            // Check for transient-looking errors
            if messages.iter().any(|m| {
                m.contains("timeout")
                    || m.contains("rate limit")
                    || m.contains("temporarily unavailable")
            }) {
                return Err(ChainEventError::Transient(format!(
                    "GraphQL errors: {:?}",
                    messages
                )));
            }
            return Err(ChainEventError::Permanent(format!(
                "GraphQL errors: {:?}",
                messages
            )));
        }

        graphql_response
            .data
            .ok_or_else(|| ChainEventError::Permanent("No data in response".to_string()))
    }
}

/// Substituted into the GraphQL `first:` clause below so the cursor
/// logic and the query can never drift apart. Used by the chain_listener
/// to decide when the page cap was hit.
pub(super) const SUBGRAPH_PAGE_SIZE: usize = 1000;

#[async_trait]
impl ChainEventSource for SubgraphEventSource {
    #[expect(
        clippy::too_many_lines,
        reason = "predates this lint; fix when next touched"
    )]
    async fn get_changed_agreements(
        &self,
        since: &Cursor,
        pinned_block: Option<u64>,
    ) -> Result<ChangedAgreementsResult, ChainEventError> {
        // Composite-key (keyset) cursor: rows past `(cursorBlock, cursorId)`
        // in lexicographic order. We rely on graph-node's documented
        // tiebreak — when `orderBy` matches across rows, results are sorted
        // by `id` ascending — to give this query a total order over
        // `(lastStateChangeBlock, id)`. The response is validated against
        // that ordering before any cursor advance: a hostile or buggy
        // backend that returns rows out of order is rejected as transient,
        // not silently trusted. The optional `block: { number: $pinnedBlock }`
        // argument pins every page in a multi-page drain to the same
        // subgraph snapshot.
        let (block_clause, meta_clause) = match pinned_block {
            Some(_) => (
                "block: { number: $pinnedBlock }",
                "_meta(block: { number: $pinnedBlock })",
            ),
            None => ("", "_meta"),
        };
        let pinned_decl = if pinned_block.is_some() {
            ", $pinnedBlock: Int!"
        } else {
            ""
        };
        let query = format!(
            r#"
            query ChangedAgreements($payer: Bytes!, $cursorBlock: BigInt!, $cursorId: Bytes!{pinned_decl}) {{
                indexingAgreements(
                    where: {{
                        or: [
                            {{ payer: $payer, lastStateChangeBlock_gt: $cursorBlock }},
                            {{ payer: $payer, lastStateChangeBlock: $cursorBlock, id_gt: $cursorId }}
                        ]
                    }}
                    orderBy: lastStateChangeBlock
                    orderDirection: asc
                    first: {SUBGRAPH_PAGE_SIZE}
                    {block_clause}
                ) {{
                    id
                    indexer
                    state
                    canceledBy
                    lastStateChangeBlock
                    acceptedAt
                    acceptedAtTx
                    canceledAt
                    canceledAtTx
                }}
                {meta_clause} {{
                    block {{
                        number
                        timestamp
                    }}
                }}
            }}
            "#
        );

        let mut variables = serde_json::json!({
            "payer": format!("{:#x}", self.config.payer_address),
            "cursorBlock": since.block.to_string(),
            "cursorId": since.id_hex(),
        });
        if let Some(block) = pinned_block {
            variables["pinnedBlock"] = serde_json::json!(block);
        }

        let response: ChangedAgreementsResponse = self.query_with_retry(&query, variables).await?;

        if let Err(err) = check_pinned_block(pinned_block, response.meta.block.number) {
            tracing::warn!(
                event = "subgraph_pin_mismatch",
                requested_block = ?pinned_block,
                response_block = response.meta.block.number,
                "Subgraph response did not honour the requested pinned block; dropping page"
            );
            return Err(err);
        }

        if let Some(ts) = response.meta.block.timestamp {
            let now = now_secs();
            let tolerance = self.config.wall_clock_skew_tolerance_secs;
            let bypass = self.config.bypass_chain_clock_defenses;
            if let Err(err) = maybe_check_subgraph_skew(ts, now, tolerance, bypass) {
                tracing::warn!(
                    event = "subgraph_skew_drop",
                    subgraph_timestamp = ts,
                    now,
                    tolerance_secs = tolerance,
                    "Subgraph timestamp past wall-clock + tolerance; dropping response as corrupt"
                );
                return Err(err);
            }
            if bypass {
                tracing::trace!(
                    event = "subgraph_skew_check_bypassed",
                    subgraph_timestamp = ts,
                    "Skew check skipped because bypass_chain_clock_defenses=true"
                );
            }
        }

        let latest_block = response.meta.block.number;
        let mut snapshots: Vec<AgreementStateSnapshot> = Vec::new();
        let mut earliest_failure: Option<Cursor> = None;
        let mut unlocatable_failures: usize = 0;

        for entity in &response.indexing_agreements {
            match parse_snapshot(entity) {
                Some(snapshot) => snapshots.push(snapshot),
                None => match parse_failure_cursor(entity) {
                    Some(failed) => {
                        earliest_failure = Some(match earliest_failure.take() {
                            Some(existing) => existing.min(failed),
                            None => failed,
                        });
                        tracing::warn!(
                            entity_id = %entity.id,
                            last_state_change_block = entity.last_state_change_block,
                            "Dropping malformed IndexingAgreement entity; cursor will hold back so it gets re-read"
                        );
                    }
                    None => {
                        unlocatable_failures += 1;
                        tracing::warn!(
                            entity_id = %entity.id,
                            "Dropping malformed IndexingAgreement entity with unparseable block; cursor will not advance"
                        );
                    }
                },
            }
        }

        // Reject the page if any pair is out of (block, id) order. The
        // keyset cursor advances to the last accepted entity assuming
        // it is the maximum so far; a misordered response would let a
        // non-maximum become the cursor and silently skip entities on
        // the next poll.
        if let Err(err) = validate_sorted_keyset(&snapshots) {
            tracing::warn!(
                event = "subgraph_response_unsorted",
                error = %err,
                "Subgraph response is not sorted by (block, id) ascending; rejecting page"
            );
            return Err(err);
        }

        let cursor = if unlocatable_failures > 0 {
            // Failure with no parseable position — hold at the input cursor.
            since.clone()
        } else if let Some(failed) = earliest_failure {
            // Drop snapshots at or past the failure and use the last good
            // one as the cursor; reconcile is idempotent.
            snapshots.retain(|s| {
                Cursor {
                    block: s.last_state_change_block,
                    id: Some(s.agreement_id),
                } < failed
            });
            snapshots
                .last()
                .map(|s| Cursor {
                    block: s.last_state_change_block,
                    id: Some(s.agreement_id),
                })
                .unwrap_or_else(|| since.clone())
        } else if response.indexing_agreements.len() >= SUBGRAPH_PAGE_SIZE {
            // Page cap hit; resume past the last row we got.
            snapshots
                .last()
                .map(|s| Cursor {
                    block: s.last_state_change_block,
                    id: Some(s.agreement_id),
                })
                .unwrap_or_else(|| since.clone())
        } else {
            // Drained — advance past `latest_block` so the keyset's
            // `block_eq` branch doesn't re-read already-consumed rows.
            Cursor::at_block(latest_block.saturating_add(1))
        };

        if cursor.block < latest_block {
            tracing::warn!(
                cursor_block = cursor.block,
                subgraph_head = latest_block,
                unlocatable_failures,
                "Parse failures or page cap held the cursor back this poll"
            );
        }

        Ok(ChangedAgreementsResult {
            snapshots,
            latest_block,
            latest_block_timestamp: response.meta.block.timestamp,
            cursor,
        })
    }
}

/// Reject a response whose `_meta.block.number` does not match the
/// pinned block requested. graph-node enforces this server-side, but a
/// non-graph-node backend could quietly serve data from a different
/// snapshot — which would break multi-page drains, where every page
/// must read from the same chain state. No-op when no pin was set.
fn check_pinned_block(
    pinned_block: Option<u64>,
    response_block: u64,
) -> Result<(), ChainEventError> {
    match pinned_block {
        Some(pinned) if pinned != response_block => Err(ChainEventError::Transient(format!(
            "subgraph returned block {response_block} when {pinned} was pinned; response dropped"
        ))),
        _ => Ok(()),
    }
}

/// Reject a subgraph response whose reported chain timestamp sits past
/// the listener's wall clock by more than `tolerance_secs`. Pulled out
/// of `get_changed_agreements` so the boundary condition (`ts ==
/// now + tolerance`) is unit-testable without an HTTP mock.
fn check_subgraph_skew(ts: u64, now: u64, tolerance_secs: u64) -> Result<(), ChainEventError> {
    let upper_bound = now.saturating_add(tolerance_secs);
    if ts > upper_bound {
        Err(ChainEventError::Transient(format!(
            "Subgraph returned timestamp {ts} > now+{tolerance_secs}s ({upper_bound}); response dropped as corrupt"
        )))
    } else {
        Ok(())
    }
}

/// Wrap `check_subgraph_skew` with the `bypass_chain_clock_defenses`
/// short-circuit. Separated so the bypass decision is unit-testable
/// without an HTTP mock.
fn maybe_check_subgraph_skew(
    ts: u64,
    now: u64,
    tolerance_secs: u64,
    bypass: bool,
) -> Result<(), ChainEventError> {
    if bypass {
        Ok(())
    } else {
        check_subgraph_skew(ts, now, tolerance_secs)
    }
}

/// Reject responses where any consecutive pair of parsed snapshots is not
/// strictly ascending on `(last_state_change_block, agreement_id)` byte-lex.
/// The keyset cursor takes the last accepted entity as its new position
/// assuming it is the maximum so far; a misordered response would let a
/// non-maximum entity become the cursor and silently skip rows on the
/// next poll. Treats any violation as transient so the listener retries.
fn validate_sorted_keyset(snapshots: &[AgreementStateSnapshot]) -> Result<(), ChainEventError> {
    for window in snapshots.windows(2) {
        let prev_key = (
            window[0].last_state_change_block,
            window[0].agreement_id.as_bytes(),
        );
        let cur_key = (
            window[1].last_state_change_block,
            window[1].agreement_id.as_bytes(),
        );
        if prev_key >= cur_key {
            return Err(ChainEventError::Transient(format!(
                "subgraph response not sorted by (block, id) ascending: \
                 prev=(block {}, id {}), cur=(block {}, id {})",
                window[0].last_state_change_block,
                window[0].agreement_id,
                window[1].last_state_change_block,
                window[1].agreement_id,
            )));
        }
    }
    Ok(())
}

/// Build a hold-back cursor pointing at a malformed entity: its block is
/// known, but its id may be malformed, in which case we cannot construct a
/// keyset position — return None and let the caller fall through to the
/// unlocatable_failures path.
fn parse_failure_cursor(entity: &IndexingAgreementEntity) -> Option<Cursor> {
    let block = entity.last_state_change_block.parse::<u64>().ok()?;
    let id = entity.id.parse().ok();
    Some(Cursor { block, id })
}

/// Parse a GraphQL `IndexingAgreement` entity into a snapshot. Returns
/// `None` if any field is malformed so the caller can filter it out without
/// halting the whole batch — individual corrupt entities log elsewhere.
fn parse_snapshot(entity: &IndexingAgreementEntity) -> Option<AgreementStateSnapshot> {
    Some(AgreementStateSnapshot {
        agreement_id: entity.id.parse().ok()?,
        indexer: entity.indexer.parse().ok()?,
        state: parse_state(&entity.state)?,
        canceled_by: parse_address_or_zero(&entity.canceled_by)?,
        last_state_change_block: entity.last_state_change_block.parse().ok()?,
        // Auxiliary audit fields (timestamps + tx hashes). These don't drive
        // reconciliation, so a malformed value defaults rather than dropping the
        // whole entity: BigInt timestamps fall back to 0 (a malformed value is
        // logged), tx hashes pass through as the raw hex string from the subgraph
        // (32-byte zero sentinel when unset).
        accepted_at: parse_bigint_timestamp(&entity.accepted_at, "acceptedAt", &entity.id),
        accepted_tx: entity.accepted_at_tx.clone(),
        canceled_at: parse_bigint_timestamp(&entity.canceled_at, "canceledAt", &entity.id),
        canceled_tx: entity.canceled_at_tx.clone(),
    })
}

/// Parse a BigInt timestamp field, defaulting to 0. `"0"`/absent is the expected
/// "unset" encoding, so only a non-empty, unparseable value is flagged -- that
/// signals a genuinely malformed upstream response rather than an unset field.
fn parse_bigint_timestamp(raw: &str, field: &str, agreement_id: &str) -> u64 {
    match raw.parse::<u64>() {
        Ok(value) => value,
        Err(_) => {
            if !raw.is_empty() {
                tracing::warn!(
                    field,
                    raw,
                    agreement_id,
                    "unparseable BigInt timestamp in subgraph response; defaulting to 0"
                );
            }
            0
        }
    }
}

/// Parse the GraphQL state enum string into the Rust enum.
fn parse_state(s: &str) -> Option<AgreementState> {
    match s {
        "NotAccepted" => Some(AgreementState::NotAccepted),
        "Accepted" => Some(AgreementState::Accepted),
        "CanceledByServiceProvider" => Some(AgreementState::CanceledByServiceProvider),
        "CanceledByPayer" => Some(AgreementState::CanceledByPayer),
        _ => None,
    }
}

/// Parse a hex-encoded address field. Graph-node serializes unset Bytes
/// fields with unpredictable padding (observed: `"0x"`, `"0x00000000"`),
/// so any hex shorter than a 20-byte address is treated as `Address::ZERO`.
fn parse_address_or_zero(hex_str: &str) -> Option<Address> {
    let stripped = hex_str.strip_prefix("0x").unwrap_or(hex_str);
    if stripped.len() < 40 {
        return Some(Address::ZERO);
    }
    hex_str.parse().ok()
}

/// Calculate retry delay with exponential backoff and jitter.
///
/// Base delay is 500ms, capped at 30s. Jitter adds +/- 25% variance.
fn calculate_retry_delay(attempt: u32) -> Duration {
    const BASE_MS: u64 = 500;
    const MAX_MS: u64 = 30_000;

    let exponential = BASE_MS.saturating_mul(2u64.saturating_pow(attempt.saturating_sub(1)));
    let capped = exponential.min(MAX_MS);

    // Add jitter: +/- 25%
    let jitter_range = capped / 4;
    if jitter_range == 0 {
        return Duration::from_millis(capped);
    }

    let mut rng = rand::rng();
    let jitter: u64 = rng.random_range(0..(jitter_range * 2));
    let with_jitter = capped.saturating_sub(jitter_range).saturating_add(jitter);

    Duration::from_millis(with_jitter)
}

/// Mock implementation for testing.
#[cfg(test)]
#[allow(dead_code)]
pub mod mock {
    use std::sync::{Arc, Mutex};

    use super::*;

    /// A mock chain event source for testing.
    pub struct MockEventSource {
        snapshots: Arc<Mutex<Vec<AgreementStateSnapshot>>>,
        latest_block: Arc<Mutex<u64>>,
        latest_block_timestamp: Arc<Mutex<Option<u64>>>,
        cursor_override: Arc<Mutex<Option<Cursor>>>,
        page_size: Arc<Mutex<Option<usize>>>,
        error: Arc<Mutex<Option<ChainEventError>>>,
    }

    impl MockEventSource {
        pub fn new() -> Self {
            Self {
                snapshots: Arc::new(Mutex::new(Vec::new())),
                latest_block: Arc::new(Mutex::new(0)),
                latest_block_timestamp: Arc::new(Mutex::new(None)),
                cursor_override: Arc::new(Mutex::new(None)),
                page_size: Arc::new(Mutex::new(None)),
                error: Arc::new(Mutex::new(None)),
            }
        }

        /// Add snapshots to return on next query.
        pub fn add_snapshots(&self, snapshots: Vec<AgreementStateSnapshot>) {
            self.snapshots.lock().unwrap().extend(snapshots);
        }

        /// Set the latest block number.
        pub fn set_latest_block(&self, block: u64) {
            *self.latest_block.lock().unwrap() = block;
        }

        /// Set the latest block timestamp.
        pub fn set_latest_block_timestamp(&self, timestamp: Option<u64>) {
            *self.latest_block_timestamp.lock().unwrap() = timestamp;
        }

        /// Force a specific cursor return; used by tests that simulate
        /// parse-failure / held-back paths.
        pub fn set_cursor_override(&self, cursor: Option<Cursor>) {
            *self.cursor_override.lock().unwrap() = cursor;
        }

        /// Mirror the real subgraph's `first:` cap so multi-page drains
        /// can be exercised end-to-end.
        pub fn set_page_size(&self, page_size: Option<usize>) {
            *self.page_size.lock().unwrap() = page_size;
        }
    }

    impl Default for MockEventSource {
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait]
    impl ChainEventSource for MockEventSource {
        async fn get_changed_agreements(
            &self,
            since: &Cursor,
            // The mock ignores `pinned_block`; tests that care about
            // pinning observe it through a dedicated mock if needed.
            _pinned_block: Option<u64>,
        ) -> Result<ChangedAgreementsResult, ChainEventError> {
            if let Some(error) = self.error.lock().unwrap().take() {
                return Err(error);
            }

            // Filter on the keyset boundary `(since.block, since.id)` and
            // order by `(block, id)` ascending so the mock matches the
            // real subgraph's traversal order.
            let mut snapshots: Vec<_> = self
                .snapshots
                .lock()
                .unwrap()
                .iter()
                .filter(|s| {
                    since
                        < &Cursor {
                            block: s.last_state_change_block,
                            id: Some(s.agreement_id),
                        }
                })
                .cloned()
                .collect();
            snapshots.sort_by_key(|s| (s.last_state_change_block, s.agreement_id));

            let latest_block = *self.latest_block.lock().unwrap();
            let latest_block_timestamp = *self.latest_block_timestamp.lock().unwrap();
            let page_size = *self.page_size.lock().unwrap();

            // Mirror `SubgraphEventSource`'s page-cap cursor derivation
            // so tests exercise the same drain shape as production.
            let truncated_at_cap = match page_size {
                Some(cap) if snapshots.len() > cap => {
                    snapshots.truncate(cap);
                    true
                }
                Some(cap) => snapshots.len() == cap,
                None => false,
            };

            let derived_cursor = if truncated_at_cap {
                snapshots
                    .last()
                    .map(|s| Cursor {
                        block: s.last_state_change_block,
                        id: Some(s.agreement_id),
                    })
                    .unwrap_or_else(|| Cursor::at_block(latest_block.saturating_add(1)))
            } else {
                Cursor::at_block(latest_block.saturating_add(1))
            };

            let cursor = self
                .cursor_override
                .lock()
                .unwrap()
                .clone()
                .unwrap_or(derived_cursor);

            Ok(ChangedAgreementsResult {
                snapshots,
                latest_block,
                latest_block_timestamp,
                cursor,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_address_or_zero_short_hex_is_zero() {
        // Graph-node may serialize unset Bytes fields as any of these
        // variants depending on internal padding; all represent "no address".
        assert_eq!(parse_address_or_zero("0x").unwrap(), Address::ZERO);
        assert_eq!(parse_address_or_zero("").unwrap(), Address::ZERO);
        assert_eq!(parse_address_or_zero("0x00000000").unwrap(), Address::ZERO);
        assert_eq!(
            parse_address_or_zero("0x00000000000000000000000000000000000000").unwrap(),
            Address::ZERO,
        );
    }

    #[test]
    fn test_parse_address_or_zero_real_address() {
        let hex = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8";
        assert_ne!(parse_address_or_zero(hex).unwrap(), Address::ZERO);
    }

    #[test]
    fn test_parse_address_or_zero_full_zero_address() {
        let hex = "0x0000000000000000000000000000000000000000";
        assert_eq!(parse_address_or_zero(hex).unwrap(), Address::ZERO);
    }

    #[test]
    fn test_parse_state_known_variants() {
        assert_eq!(
            parse_state("NotAccepted"),
            Some(AgreementState::NotAccepted)
        );
        assert_eq!(parse_state("Accepted"), Some(AgreementState::Accepted));
        assert_eq!(
            parse_state("CanceledByServiceProvider"),
            Some(AgreementState::CanceledByServiceProvider)
        );
        assert_eq!(
            parse_state("CanceledByPayer"),
            Some(AgreementState::CanceledByPayer)
        );
    }

    #[test]
    fn test_parse_state_unknown() {
        assert_eq!(parse_state("Garbage"), None);
    }

    #[test]
    fn test_agreement_state_is_canceled() {
        assert!(!AgreementState::NotAccepted.is_canceled());
        assert!(!AgreementState::Accepted.is_canceled());
        assert!(AgreementState::CanceledByServiceProvider.is_canceled());
        assert!(AgreementState::CanceledByPayer.is_canceled());
    }

    #[test]
    fn test_agreement_state_reached_accepted() {
        assert!(!AgreementState::NotAccepted.reached_accepted());
        assert!(AgreementState::Accepted.reached_accepted());
        assert!(AgreementState::CanceledByServiceProvider.reached_accepted());
        assert!(AgreementState::CanceledByPayer.reached_accepted());
    }

    fn valid_entity() -> IndexingAgreementEntity {
        IndexingAgreementEntity {
            id: "0x0102030405060708090a0b0c0d0e0f10".to_string(),
            indexer: "0x70997970C51812dc3A010C7d01b50e0d17dc79C8".to_string(),
            state: "Accepted".to_string(),
            canceled_by: "0x".to_string(),
            last_state_change_block: "150".to_string(),
            accepted_at: "1700000001".to_string(),
            accepted_at_tx: "0x1111111111111111111111111111111111111111111111111111111111111111"
                .to_string(),
            canceled_at: "0".to_string(),
            canceled_at_tx: "0x0000000000000000000000000000000000000000000000000000000000000000"
                .to_string(),
        }
    }

    #[test]
    fn test_parse_snapshot_happy_path() {
        let snapshot = parse_snapshot(&valid_entity()).expect("should parse");
        assert_eq!(snapshot.last_state_change_block, 150);
        assert_eq!(snapshot.state, AgreementState::Accepted);
        assert_eq!(snapshot.canceled_by, Address::ZERO);
        // Phase 4 enrichment: audit fields parse from the entity.
        assert_eq!(snapshot.accepted_at, 1_700_000_001);
        assert_eq!(
            snapshot.accepted_tx,
            "0x1111111111111111111111111111111111111111111111111111111111111111"
        );
        assert_eq!(snapshot.canceled_at, 0);
        assert_eq!(
            snapshot.canceled_tx,
            "0x0000000000000000000000000000000000000000000000000000000000000000"
        );
    }

    #[test]
    fn parse_bigint_timestamp_handles_valid_unset_and_malformed() {
        // Valid number parses.
        assert_eq!(
            parse_bigint_timestamp("1700000001", "acceptedAt", "0xabc"),
            1_700_000_001
        );
        // "0" / empty are the expected unset encodings -> 0 (and not flagged).
        assert_eq!(parse_bigint_timestamp("0", "acceptedAt", "0xabc"), 0);
        assert_eq!(parse_bigint_timestamp("", "acceptedAt", "0xabc"), 0);
        // A genuinely malformed value defaults to 0 (and is logged as a warning).
        assert_eq!(
            parse_bigint_timestamp("not-a-number", "acceptedAt", "0xabc"),
            0
        );
    }

    #[test]
    fn test_parse_snapshot_bad_state_returns_none_but_block_parseable() {
        // A malformed state drops the snapshot, but the block field remains
        // parseable so the caller can hold the cursor back to this block
        // and re-read on the next poll.
        let mut entity = valid_entity();
        entity.state = "UnknownVariant".to_string();
        assert!(parse_snapshot(&entity).is_none());
        assert_eq!(
            entity.last_state_change_block.parse::<u64>().ok(),
            Some(150)
        );
    }

    #[test]
    fn test_parse_snapshot_bad_block_and_bad_state_unlocatable() {
        // Both state AND block malformed: caller has no block to hold the
        // cursor at, so it falls through to `unlocatable_failures`.
        let mut entity = valid_entity();
        entity.state = "UnknownVariant".to_string();
        entity.last_state_change_block = "not-a-number".to_string();
        assert!(parse_snapshot(&entity).is_none());
        assert!(entity.last_state_change_block.parse::<u64>().is_err());
    }

    #[test]
    fn test_calculate_retry_delay_exponential() {
        let delay1 = calculate_retry_delay(1);
        assert!(delay1.as_millis() >= 375 && delay1.as_millis() <= 625);

        let delay2 = calculate_retry_delay(2);
        assert!(delay2.as_millis() >= 750 && delay2.as_millis() <= 1250);
    }

    #[test]
    fn test_calculate_retry_delay_capped() {
        let delay = calculate_retry_delay(10);
        assert!(delay.as_millis() >= 22500 && delay.as_millis() <= 37500);
    }

    #[test]
    fn test_chain_event_error_is_transient() {
        assert!(ChainEventError::Transient("test".into()).is_transient());
        assert!(!ChainEventError::Permanent("test".into()).is_transient());
    }

    #[test]
    fn test_check_subgraph_skew_within_tolerance_is_ok() {
        // ts == now: trivially fine.
        assert!(check_subgraph_skew(1_700_000_000, 1_700_000_000, 60).is_ok());
        // ts within tolerance: still fine.
        assert!(check_subgraph_skew(1_700_000_059, 1_700_000_000, 60).is_ok());
        // ts == now + tolerance: boundary, still fine (strict > guard).
        assert!(check_subgraph_skew(1_700_000_060, 1_700_000_000, 60).is_ok());
    }

    #[test]
    fn test_check_subgraph_skew_past_tolerance_is_transient_error() {
        // One second past the boundary: response is dropped.
        let err = check_subgraph_skew(1_700_000_061, 1_700_000_000, 60)
            .expect_err("ts past now+tolerance must reject");
        assert!(err.is_transient(), "skew rejection must be transient");
        let msg = err.to_string();
        assert!(msg.contains("1700000061"));
        assert!(msg.contains("dropped as corrupt"));
    }

    #[test]
    fn test_check_subgraph_skew_far_future_timestamp_rejected() {
        // Models the "subgraph poisoned" case the bound is meant to catch:
        // a timestamp far past wall clock that, without this check, would
        // ratchet the persisted timestamp into the future and freeze the
        // expiration service indefinitely.
        let err = check_subgraph_skew(u64::MAX / 2, 1_700_000_000, 60)
            .expect_err("far-future ts must reject");
        assert!(err.is_transient());
    }

    #[test]
    fn test_check_subgraph_skew_ts_in_past_is_ok() {
        // The bound only rejects future drift; the listener tolerates any
        // amount of subgraph lag (operators have a separate `subgraph_lag_seconds`
        // metric for that).
        assert!(check_subgraph_skew(1_699_000_000, 1_700_000_000, 60).is_ok());
    }

    #[test]
    fn test_maybe_check_subgraph_skew_bypass_off_runs_check() {
        // bypass=false routes through `check_subgraph_skew` unchanged.
        // A far-future ts that would normally reject must still reject.
        let err = maybe_check_subgraph_skew(u64::MAX / 2, 1_700_000_000, 60, false)
            .expect_err("bypass off must keep the check intact");
        assert!(err.is_transient());
    }

    #[test]
    fn test_maybe_check_subgraph_skew_bypass_on_skips_check() {
        // bypass=true short-circuits to Ok even for a timestamp that
        // would otherwise be rejected. Used by local-network testing
        // where `evm_increaseTime` legitimately advances chain time
        // by days.
        assert!(
            maybe_check_subgraph_skew(u64::MAX / 2, 1_700_000_000, 60, true).is_ok(),
            "bypass on must skip the check regardless of ts"
        );
    }

    #[test]
    fn test_check_pinned_block_no_pin_is_ok() {
        // No pin requested: the response block is whatever the subgraph's
        // current head is, and we accept it.
        assert!(check_pinned_block(None, 12_345).is_ok());
    }

    #[test]
    fn test_check_pinned_block_match_is_ok() {
        assert!(check_pinned_block(Some(12_345), 12_345).is_ok());
    }

    #[test]
    fn test_check_pinned_block_lower_response_rejected() {
        // Subgraph responded from an older snapshot than we asked for.
        let err = check_pinned_block(Some(12_345), 12_344).expect_err("mismatch must reject");
        assert!(err.is_transient());
        assert!(err.to_string().contains("12344"));
        assert!(err.to_string().contains("12345"));
    }

    #[test]
    fn test_check_pinned_block_higher_response_rejected() {
        // Subgraph responded from a newer snapshot than we asked for.
        // Equally a violation — multi-page drains rely on a stable view.
        let err = check_pinned_block(Some(12_345), 12_346).expect_err("mismatch must reject");
        assert!(err.is_transient());
    }

    fn snapshot_at(block: u64, id_bytes: [u8; 16]) -> AgreementStateSnapshot {
        AgreementStateSnapshot {
            agreement_id: IndexingAgreementId::from_bytes(id_bytes),
            indexer: Address::ZERO,
            state: AgreementState::Accepted,
            canceled_by: Address::ZERO,
            last_state_change_block: block,
            accepted_at: 0,
            accepted_tx: String::new(),
            canceled_at: 0,
            canceled_tx: String::new(),
        }
    }

    #[test]
    fn test_validate_sorted_keyset_accepts_strictly_ascending() {
        let mut id_low = [0u8; 16];
        id_low[0] = 0x01;
        let mut id_mid = [0u8; 16];
        id_mid[0] = 0x55;
        let mut id_high = [0u8; 16];
        id_high[0] = 0xff;

        // Same-block tie sorted by id ascending, plus a later block.
        let snapshots = vec![
            snapshot_at(50, id_low),
            snapshot_at(50, id_mid),
            snapshot_at(50, id_high),
            snapshot_at(60, id_low),
        ];
        assert!(validate_sorted_keyset(&snapshots).is_ok());
    }

    #[test]
    fn test_validate_sorted_keyset_rejects_block_regression() {
        let id = [0x42u8; 16];
        let snapshots = vec![snapshot_at(60, id), snapshot_at(50, id)];
        let err = validate_sorted_keyset(&snapshots).expect_err("must reject");
        assert!(err.is_transient());
        assert!(err.to_string().contains("not sorted"));
    }

    #[test]
    fn test_validate_sorted_keyset_rejects_id_regression_within_block() {
        // graph-node tiebreak: id ascending. A response with id descending
        // inside the same block must be rejected — the keyset cursor would
        // otherwise advance to a non-maximum and skip rows.
        let mut id_high = [0u8; 16];
        id_high[0] = 0xff;
        let mut id_low = [0u8; 16];
        id_low[0] = 0x01;

        let snapshots = vec![snapshot_at(50, id_high), snapshot_at(50, id_low)];
        let err = validate_sorted_keyset(&snapshots).expect_err("must reject");
        assert!(err.is_transient());
    }

    #[test]
    fn test_validate_sorted_keyset_rejects_duplicate() {
        // Same (block, id) twice means a duplicate entity, which violates
        // the strict-ascending invariant the keyset relies on.
        let id = [0x42u8; 16];
        let snapshots = vec![snapshot_at(50, id), snapshot_at(50, id)];
        let err = validate_sorted_keyset(&snapshots).expect_err("must reject");
        assert!(err.is_transient());
    }

    #[test]
    fn test_validate_sorted_keyset_empty_is_ok() {
        assert!(validate_sorted_keyset(&[]).is_ok());
    }

    #[test]
    fn test_validate_sorted_keyset_single_is_ok() {
        let id = [0x42u8; 16];
        assert!(validate_sorted_keyset(&[snapshot_at(50, id)]).is_ok());
    }
}
