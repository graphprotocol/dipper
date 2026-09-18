//! # Indexing Agreements
//!
//! Indexer Agreements MUST be associated with one Indexing Request, and represent the contract
//! between the DIPs Gateway (Dipper) and the indexer to index the data.
//!
//! - An agreement MUST be associated with an *indexing request*.
//! - An agreement is in effect once accepted on-chain, or until the RCA deadline expires.
//!   It can be cancelled by the customer or the indexer.
//!
//! An Indexer Agreement is created every time the Dipper runs the *Indexing Indexer Selection
//! Algorithm (IISA)* and finds an indexer to fulfill the *indexing request*.

use async_trait::async_trait;
use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
use thegraph_core::{
    DeploymentId, IndexerId,
    alloy::primitives::{Address, ChainId, U256},
};
use time::OffsetDateTime;
use url::Url;

use super::result::Result as RegistryResult;

/// Parameters for registering a new indexing agreement.
pub struct NewAgreementParams {
    pub agreement_id: IndexingAgreementId,
    pub nonce_uuid: uuid::Uuid,
    pub request_id: IndexingRequestId,
    pub deployment_id: DeploymentId,
    pub indexer_id: IndexerId,
    pub indexer_url: Url,
    pub terms: Terms,
    /// EIP-712 terms hash for the protocol-managed cancel path. `None` only for
    /// pre-migration rows.
    pub terms_version_hash: Option<Vec<u8>>,
}

/// Which party cancelled an agreement. Passed to `apply_reconciliation`
/// so the atomic state transition chooses the right terminal status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelKind {
    ByRequester,
    ByIndexer,
}

/// Outcome of an atomic `apply_reconciliation` call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReconciliationOutcome {
    pub did_accept: bool,
    pub did_cancel: bool,
}

/// One row's reconciliation request inside a batched apply.
#[derive(Debug, Clone)]
pub struct ReconciliationItem {
    pub agreement_id: IndexingAgreementId,
    pub apply_accept: bool,
    pub cancel: Option<CancelKind>,
    /// On-chain payload observed for this transition, persisted alongside the
    /// status change so a later emission sweep can rebuild the lifecycle event
    /// from the row alone. Only the fields relevant to the applied transition
    /// are populated.
    pub audit: ReconciliationAudit,
}

/// Audit payload captured at reconcile time for later event emission.
///
/// Timestamps are chain seconds; tx hashes and `canceled_by` are the 0x hex
/// string form matching the event wire representation.
#[derive(Debug, Clone, Default)]
pub struct ReconciliationAudit {
    pub accepted_at: Option<u64>,
    pub accepted_tx: Option<String>,
    pub canceled_at: Option<u64>,
    pub canceled_by: Option<String>,
    pub canceled_tx: Option<String>,
}

/// A row that needs a `terminated` lifecycle event emitted, rebuilt from the
/// agreement row alone by the emission sweep.
#[derive(Debug, Clone)]
pub struct PendingTerminatedEvent {
    pub agreement_id: IndexingAgreementId,
    pub indexer_id: IndexerId,
    pub deployment_id: DeploymentId,
    pub protocol_network: ChainId,
    pub canceled_at: Option<u64>,
    pub canceled_by: Option<String>,
    pub canceled_tx: Option<String>,
    pub updated_at: OffsetDateTime,
}

/// A row that needs an `accepted` lifecycle event emitted, rebuilt from the
/// agreement row alone by the emission sweep.
#[derive(Debug, Clone)]
pub struct PendingAcceptedEvent {
    pub agreement_id: IndexingAgreementId,
    pub indexer_id: IndexerId,
    pub deployment_id: DeploymentId,
    pub protocol_network: ChainId,
    pub accepted_at: u64,
    pub accepted_tx: String,
    pub ends_at: u64,
    pub payer: Address,
}

/// A row that needs a `request.expired` lifecycle event emitted, rebuilt from
/// the agreement row alone by the emission sweep.
#[derive(Debug, Clone)]
pub struct PendingExpiredEvent {
    pub agreement_id: IndexingAgreementId,
    pub indexer_id: IndexerId,
    pub deployment_id: DeploymentId,
    pub protocol_network: ChainId,
    pub request_proposed_at: u64,
    pub request_expired_at: u64,
}

#[async_trait]
pub trait AgreementRegistry {
    /// Get agreement by ID.
    async fn get_indexing_agreement_by_id(
        &self,
        id: &IndexingAgreementId,
    ) -> RegistryResult<Option<IndexingAgreement>>;

    /// Batch lookup of agreements by id. Missing ids are absent from the
    /// returned map. The chain listener uses this to fetch all agreements
    /// referenced by a page of subgraph snapshots in a single round-trip.
    /// Default impl loops over `get_indexing_agreement_by_id` so test
    /// mocks don't need to override; the production registry overrides
    /// with a single `WHERE id = ANY($1)` query.
    async fn get_indexing_agreements_by_ids(
        &self,
        ids: &[IndexingAgreementId],
    ) -> RegistryResult<std::collections::HashMap<IndexingAgreementId, IndexingAgreement>>
    where
        Self: Sync,
    {
        let mut out = std::collections::HashMap::with_capacity(ids.len());
        for id in ids {
            if let Some(agreement) = self.get_indexing_agreement_by_id(id).await? {
                out.insert(*id, agreement);
            }
        }
        Ok(out)
    }

    /// Get all agreements by deployment ID.
    async fn get_indexing_agreements_by_deployment_id(
        &self,
        deployment_id: &DeploymentId,
    ) -> RegistryResult<Vec<IndexingAgreement>>;

    /// Get all agreements by indexer ID.
    async fn get_indexing_agreements_by_indexer_id(
        &self,
        indexer_id: &IndexerId,
    ) -> RegistryResult<Vec<IndexingAgreement>>;

    /// Get aggregated deployment-to-indexers mapping for active agreements.
    ///
    /// Returns agreements that are in `Created` or `AcceptedOnChain` status
    /// for any of the provided indexer IDs, grouped by deployment. This performs database-side
    /// aggregation, returning only the deployment IDs and their associated indexer IDs rather
    /// than full agreement objects.
    ///
    /// Returns a map where keys are deployment IDs and values are lists of indexer IDs
    /// that have active agreements for that deployment.
    async fn get_pending_agreement_indexers_by_deployment(
        &self,
        indexer_ids: &[IndexerId],
    ) -> RegistryResult<std::collections::HashMap<DeploymentId, Vec<IndexerId>>>;

    /// Get declined `CanceledByIndexer`/`Expired`/`Rejected` indexers grouped by
    /// deployment. Each rejection reason gets its own exclusion window, as does an
    /// expiry that never had an offer transaction; see the query for the details.
    async fn get_declined_indexers_by_deployment(
        &self,
        default_lookback_days: i32,
        price_lookback_days: i32,
        transient_lookback_minutes: i32,
        uncertain_lookback_days: i32,
    ) -> RegistryResult<std::collections::HashMap<DeploymentId, Vec<IndexerId>>>;

    /// Get indexers with a recent `Unresponsive` agreement on `chain_id`. The
    /// result is skipped for that chain's deployments for `lookback_days` after
    /// the indexer last failed to respond there.
    async fn get_unresponsive_indexers(
        &self,
        lookback_days: i32,
        chain_id: ChainId,
    ) -> RegistryResult<Vec<IndexerId>>;

    /// Get all agreements by associated indexing request ID.
    async fn get_indexing_agreements_by_indexing_request_id(
        &self,
        request_id: &IndexingRequestId,
    ) -> RegistryResult<Vec<IndexingAgreement>>;

    /// Get the active agreements for an indexing request.
    ///
    /// Agreements are considered active if they are in `CREATED` or `ACCEPTED_ON_CHAIN` status.
    async fn get_active_indexing_agreements_by_indexing_request_id(
        &self,
        request_id: &IndexingRequestId,
    ) -> RegistryResult<Vec<IndexingAgreement>>;

    /// Count the agreements for a deployment that are accepted on-chain.
    ///
    /// Populates `remaining_accepted_indexing_agreements` on the `terminated`
    /// lifecycle event: the number of still-active accepted agreements left on
    /// the Subgraph (deployment) after a cancellation.
    async fn count_accepted_agreements_by_deployment(
        &self,
        deployment_id: &DeploymentId,
    ) -> RegistryResult<i64>;

    /// Register a new indexing agreement.
    ///
    /// The caller generates the `agreement_id` (on-chain bytes16) and
    /// `nonce_uuid` (used to derive the RCA nonce) before the row is inserted.
    async fn register_new_indexing_agreement(
        &self,
        params: NewAgreementParams,
    ) -> RegistryResult<IndexingAgreementId>;

    /// Register a new agreement and record a pending cancellation atomically.
    ///
    /// Used when the new agreement is replacing an old one. Both the agreement
    /// row and the pending cancellation record are created in a single
    /// transaction, so a crash cannot leave an agreement without its
    /// corresponding pending cancellation.
    async fn register_agreement_with_pending_cancellation(
        &self,
        params: NewAgreementParams,
        old_agreement_id: IndexingAgreementId,
    ) -> RegistryResult<IndexingAgreementId>;

    /// Mark an indexing agreement as `UNRESPONSIVE`.
    ///
    /// If there is no indexing agreement with the given ID, or if the agreement is not in the
    /// `CREATED` state, this method returns a [`NoRecordUpdated`](Error::NoRecordsUpdated) error.
    async fn mark_indexing_agreement_as_unresponsive(
        &self,
        id: &IndexingAgreementId,
    ) -> RegistryResult<()>;

    /// Record the on-chain tx hash of the most recent `offer()` submission
    /// for this agreement. Observability-only; does not transition status.
    /// Called once per submit (including resubmits after a dropped tx) so
    /// the DB reflects the live hash rather than an evicted one.
    async fn update_offer_tx_hash(
        &self,
        id: &IndexingAgreementId,
        tx_hash: &[u8; 32],
    ) -> RegistryResult<()>;

    /// Backfill a `terms_version_hash` recovered from on-chain state for an
    /// agreement whose local copy is missing (e.g. a row from before the
    /// column existed). Idempotent and safe to call even if a hash is
    /// already stored — callers only invoke it when the local copy is
    /// absent, so overwriting is not a concern in practice.
    async fn update_terms_version_hash(
        &self,
        id: &IndexingAgreementId,
        hash: &[u8; 32],
    ) -> RegistryResult<()>;

    /// Mark an indexing agreement as `CANCELED_BY_REQUESTER`.
    ///
    /// If there is no indexing agreement with the given ID, or if the agreement is not in the
    /// `CREATED` or `ACCEPTED_ON_CHAIN` state, this method returns a
    /// [`NoRecordUpdated`](Error::NoRecordsUpdated) error.
    async fn mark_indexing_agreement_as_canceled_by_requester(
        &self,
        id: &IndexingAgreementId,
    ) -> RegistryResult<()>;

    /// Apply a reconciliation-driven state transition atomically.
    ///
    /// Used by `chain_listener::reconcile_agreement` so the
    /// Accept-then-Cancel-in-one-snapshot path writes both status
    /// transitions in a single database transaction, preventing
    /// concurrent readers from seeing the intermediate `AcceptedOnChain`
    /// row.
    ///
    /// - `apply_accept`: attempt `Created | Expired → AcceptedOnChain`.
    ///   May be a no-op if the row is already in another status.
    /// - `cancel`: optionally apply `CanceledByRequester` or
    ///   `CanceledByIndexer` from an accepting/pre-accept state.
    ///
    /// Returns which transitions actually affected a row so callers can
    /// gate post-commit side effects (e.g. running
    /// `execute_pending_cancellations` only on a fresh accept).
    async fn apply_reconciliation(
        &self,
        id: &IndexingAgreementId,
        apply_accept: bool,
        cancel: Option<CancelKind>,
    ) -> RegistryResult<ReconciliationOutcome>;

    /// Batched `apply_reconciliation`. Returns one outcome per input id;
    /// rows whose CAS guard didn't match get both flags `false`. The
    /// default trait impl loops calling `apply_reconciliation` so test
    /// mocks don't need to override; the production `RegistryProvider`
    /// overrides with a single-transaction implementation.
    async fn apply_reconciliation_batch(
        &self,
        items: &[ReconciliationItem],
    ) -> RegistryResult<std::collections::HashMap<IndexingAgreementId, ReconciliationOutcome>>
    where
        Self: Sync,
    {
        let mut outcomes = std::collections::HashMap::with_capacity(items.len());
        for item in items {
            let outcome = self
                .apply_reconciliation(&item.agreement_id, item.apply_accept, item.cancel)
                .await?;
            outcomes.insert(item.agreement_id, outcome);
        }
        Ok(outcomes)
    }

    /// Fetch agreements awaiting a `terminated` event: terminal-cancel status,
    /// genuinely accepted on-chain, marker unset. Default returns empty so mocks
    /// need not override; the production registry runs the real query.
    async fn get_agreements_pending_terminated_emission(
        &self,
        _limit: i64,
    ) -> RegistryResult<Vec<PendingTerminatedEvent>> {
        Ok(Vec::new())
    }

    /// Stamp the `terminated` emission marker after a confirmed broker send.
    /// Default no-op so mocks need not override.
    async fn mark_terminated_event_emitted(
        &self,
        _agreement_id: &IndexingAgreementId,
    ) -> RegistryResult<()> {
        Ok(())
    }

    /// Fetch agreements awaiting an `accepted` event: `AcceptedOnChain`, accept
    /// observed by the new reconcile path, marker unset. Default returns empty so
    /// mocks need not override; the production registry runs the real query.
    async fn get_agreements_pending_accepted_emission(
        &self,
        _limit: i64,
    ) -> RegistryResult<Vec<PendingAcceptedEvent>> {
        Ok(Vec::new())
    }

    /// Stamp the `accepted` emission marker after a confirmed broker send.
    /// Default no-op so mocks need not override.
    async fn mark_accepted_event_emitted(
        &self,
        _agreement_id: &IndexingAgreementId,
    ) -> RegistryResult<()> {
        Ok(())
    }

    /// Fetch agreements awaiting a `request.expired` event: `Expired`, marker
    /// unset. Default returns empty so mocks need not override; the production
    /// registry runs the real query.
    async fn get_agreements_pending_expired_emission(
        &self,
        _limit: i64,
    ) -> RegistryResult<Vec<PendingExpiredEvent>> {
        Ok(Vec::new())
    }

    /// Stamp the `request.expired` emission marker after a confirmed broker send.
    /// Default no-op so mocks need not override.
    async fn mark_expired_event_emitted(
        &self,
        _agreement_id: &IndexingAgreementId,
    ) -> RegistryResult<()> {
        Ok(())
    }

    /// Record the accepted audit payload out-of-band (rejected-then-accepted
    /// anomaly), marking the row as genuinely accepted so its eventual
    /// `terminated` is sweep-eligible. Default no-op so mocks need not override.
    async fn record_accepted_audit(
        &self,
        _agreement_id: &IndexingAgreementId,
        _accepted_at: u64,
        _accepted_tx: &str,
    ) -> RegistryResult<()> {
        Ok(())
    }

    /// Record the cancel audit payload for a dipper-initiated cancel so the
    /// emission sweep can populate the `terminated` event fields. Default no-op
    /// so mocks need not override.
    async fn record_cancel_audit(
        &self,
        _agreement_id: &IndexingAgreementId,
        _canceled_at: u64,
        _canceled_by: &str,
        _canceled_tx: Option<&str>,
    ) -> RegistryResult<()> {
        Ok(())
    }

    /// Get `Created` agreements whose deadline has passed (by block timestamp).
    async fn get_expired_created_agreements(
        &self,
        batch_size: i64,
        chain_timestamp: u64,
    ) -> RegistryResult<Vec<IndexingAgreement>>;

    /// Mark an indexing agreement as `EXPIRED`.
    ///
    /// The RCA deadline passed without on-chain acceptance.
    /// If there is no indexing agreement with the given ID, or if the agreement is not in the
    /// `CREATED` state, this method returns a [`NoRecordUpdated`](Error::NoRecordsUpdated) error.
    async fn mark_indexing_agreement_as_expired(
        &self,
        id: &IndexingAgreementId,
    ) -> RegistryResult<()>;

    /// Mark an indexing agreement as `REJECTED` with an optional reason.
    ///
    /// The indexer rejected the proposal off-chain. The indexer may still accept on-chain
    /// before the deadline, in which case Dipper will cancel via `cancelIndexingAgreementByPayer`.
    /// If there is no indexing agreement with the given ID, or if the agreement is not in the
    /// `CREATED` state, this method returns a [`NoRecordUpdated`](Error::NoRecordsUpdated) error.
    ///
    /// The `rejection_reason` sets the exclusion window: `PRICE_TOO_LOW`,
    /// `SENDER_NOT_TRUSTED`, and `UNSPECIFIED` retry within a day; `None` (also
    /// expired/canceled agreements) waits the 30-day default.
    async fn mark_indexing_agreement_as_rejected(
        &self,
        id: &IndexingAgreementId,
        rejection_reason: Option<&str>,
    ) -> RegistryResult<()>;

    /// Get all `AcceptedOnChain` agreements for liveness checking.
    ///
    /// Results are ordered by `last_progress_at` ascending (NULLs first) so agreements
    /// that have never been checked are processed first.
    async fn get_accepted_on_chain_agreements(
        &self,
        batch_size: i64,
    ) -> RegistryResult<Vec<IndexingAgreement>>;

    /// Get agreements that should be canceled on-chain but haven't been.
    ///
    /// Returns agreements still in `AcceptedOnChain` whose parent indexing
    /// request has already been canceled. These are the orphans left behind
    /// when a `set_indexing_target_candidates(num_candidates = 0)` call
    /// flipped the request row but a transient chain-client error stopped
    /// reassessment from completing the on-chain cancel for every agreement.
    ///
    /// Results are ordered by `updated_at` ascending so the longest-orphaned
    /// agreements are retried first.
    async fn get_agreements_pending_chain_cancel(
        &self,
        batch_size: i64,
    ) -> RegistryResult<Vec<IndexingAgreement>>;

    /// Distinct service-provider addresses whose protocol-manager escrow may
    /// need on-chain reconciliation. Empty by default so mocks need not override.
    async fn get_providers_for_escrow_reconciliation(
        &self,
        _limit: i64,
    ) -> RegistryResult<Vec<Address>>
    where
        Self: Sync,
    {
        Ok(Vec::new())
    }

    /// Update the sync progress for an agreement.
    ///
    /// Called when the liveness checker observes the block height has changed
    /// (either advancing or resetting due to a resync).
    async fn update_agreement_sync_progress(
        &self,
        id: &IndexingAgreementId,
        block_height: u64,
        progress_at: time::OffsetDateTime,
    ) -> RegistryResult<()>;

    /// Count active agreements per deployment.
    ///
    /// Returns a map of deployment ID to count of `Created` or `AcceptedOnChain`
    /// agreements. Used by the liveness checker to determine the tolerance threshold
    /// for each deployment.
    async fn count_active_agreements_by_deployment(
        &self,
    ) -> RegistryResult<std::collections::HashMap<DeploymentId, usize>>;

    /// Count `Created` (in-flight, not yet accepted) agreements per indexer,
    /// plus the global total. Offer pacing reads both to decide whether an
    /// indexer or the network has spare acceptance capacity.
    async fn count_created_agreements_by_indexer(
        &self,
    ) -> RegistryResult<(std::collections::HashMap<IndexerId, u64>, u64)>;

    /// Whether any agreement is in `Created` or `AcceptedOnChain` status.
    ///
    /// Used by the chain listener's adaptive-interval check on every poll;
    /// the default impl falls back to `count_active_agreements_by_deployment`
    /// so test mocks don't need to override.
    async fn exists_active_agreements(&self) -> RegistryResult<bool>
    where
        Self: Sync,
    {
        self.count_active_agreements_by_deployment()
            .await
            .map(|m| !m.is_empty())
    }

    /// Mark an indexing agreement as `ABANDONED_BY_INDEXER`.
    ///
    /// Transitions `AcceptedOnChain → AbandonedByIndexer`. Returns the full agreement
    /// for use in the subsequent reassessment call.
    /// Returns [`NoRecordsUpdated`](Error::NoRecordsUpdated) if the agreement doesn't
    /// exist or isn't in `AcceptedOnChain` status.
    async fn mark_indexing_agreement_as_abandoned(
        &self,
        id: &IndexingAgreementId,
    ) -> RegistryResult<IndexingAgreement>;

    /// Get per-agreement rate fields from active agreements.
    ///
    /// Returns base rate, entity rate, and deployment ID for each active
    /// agreement. The caller uses these to compute optimistic DIPs fees
    /// (optionally enriched with entity counts from the subgraph) and
    /// sends the result to IISA for `stake_to_fees` differentiation.
    async fn get_agreement_fee_rates(&self) -> RegistryResult<Vec<AgreementFeeRate>>;
}

/// Per-agreement fee rate data for optimistic fee estimation.
#[derive(Debug, Clone)]
pub struct AgreementFeeRate {
    pub indexer_id: IndexerId,
    pub deployment_id: DeploymentId,
    /// Base rate in wei GRT per second.
    pub tokens_per_second: f64,
    /// Entity rate in wei GRT per entity per second.
    pub tokens_per_entity_per_second: f64,
}

/// An Indexing Agreement represents the contract between the DIPs Gateway (Dipper) and the indexer
/// to index the data.
///
/// The [`IndexingAgreement`] is as a Data Transfer Object (DTO).
#[derive(Debug, Clone)]
pub struct IndexingAgreement {
    /// The on-chain agreement ID (bytes16 primary key).
    pub id: IndexingAgreementId,

    /// The UUID v7 used to derive the RCA nonce.
    pub nonce_uuid: uuid::Uuid,

    /// The indexing agreement creation time.
    pub created_at: OffsetDateTime,

    // The indexing agreement update time.
    pub updated_at: OffsetDateTime,

    /// The indexing agreement status.
    pub status: Status,

    /// The indexing agreement associated indexing request
    pub indexing_request_id: IndexingRequestId,

    /// The indexer.
    pub indexer: Indexer,

    /// The agreement terms.
    ///
    /// It contains the agreement terms and conditions.
    pub terms: Terms,

    /// The last observed block height for the subgraph deployment.
    ///
    /// `None` until the first liveness check fires for this agreement.
    pub last_block_height: Option<u64>,

    /// When the block height was last observed to change (progress or resync).
    ///
    /// `None` until the first liveness check fires for this agreement.
    pub last_progress_at: Option<OffsetDateTime>,

    /// Reason the agreement was rejected (only set when status is Rejected).
    pub rejection_reason: Option<String>,

    /// EIP-712 terms hash stored at offer time, used by the protocol-managed
    /// cancel path. `None` only for pre-migration rows.
    pub terms_version_hash: Option<Vec<u8>>,
}

/// The _indexing agreement_ indexer information.
#[derive(Debug, Clone)]
pub struct Indexer {
    /// The indexer's ID (ETH address).
    pub id: IndexerId,
    /// The indexer's URL.
    pub url: Url,
}

/// The agreement terms. Field names align with the on-chain `RecurringCollectionAgreement`.
#[derive(Debug, Clone)]
pub struct Terms {
    /// The agreement payer (signer address).
    pub payer: Address,
    /// The indexer (service provider).
    pub service_provider: Address,
    /// The data service address (SubgraphService contract).
    pub data_service: Address,

    /// Deadline for on-chain acceptance (unix timestamp).
    pub deadline: u64,
    /// When the agreement expires (unix timestamp).
    pub ends_at: u64,

    /// Maximum tokens for the initial subgraph sync.
    pub max_initial_tokens: U256,
    /// Maximum tokens per second for ongoing indexing.
    pub max_ongoing_tokens_per_second: U256,

    /// Minimum seconds per collection.
    pub min_seconds_per_collection: u32,
    /// Maximum seconds per collection.
    pub max_seconds_per_collection: u32,

    /// Bitmask of payer-declared conditions (e.g. eligibility check).
    ///
    /// Must be 0 unless the payer is a contract that implements the
    /// corresponding callback interfaces. Against an EOA payer, any
    /// non-zero value will cause the on-chain `offer()` and `accept()`
    /// calls to revert.
    pub conditions: u16,

    /// The agreement metadata.
    pub metadata: TermsMetadata,
}

/// Pricing and deployment metadata for the agreement.
#[derive(Debug, Clone)]
pub struct TermsMetadata {
    /// Tokens per second (base rate) in wei GRT.
    pub tokens_per_second: U256,
    /// Tokens per entity per second in wei GRT.
    pub tokens_per_entity_per_second: U256,

    /// The Subgraph deployment ID to index.
    pub subgraph_deployment_id: DeploymentId,

    /// The protocol network, e.g. `eip155:42161` (Arbitrum).
    pub protocol_network: ChainId,
    /// Indexed chain, e.g., `eip155:1` (Ethereum Mainnet).
    pub chain_id: ChainId,

    /// Unix-seconds proposal time, in the same clock as the acceptance deadline.
    pub proposed_at: u64,
}

/// The status of the [`IndexingAgreement`].
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
pub enum Status {
    /// The [`IndexingAgreement`] was created, but has not been sent to the indexer, yet.
    #[default]
    Created,

    /// The [`IndexingAgreement`] was registered, but the agreement request failed.
    ///
    /// This is a terminal state.
    Unresponsive,

    /// The associated [`IndexingRequest`] got cancelled.
    ///
    /// The [`IndexingAgreement`] is cancelled and no longer in effect.
    ///
    /// This is a terminal state.
    CanceledByRequester,

    /// The indexer canceled the indexer agreement.
    ///
    /// The [`IndexingAgreement`] is cancelled and no longer in effect.
    ///
    /// This is a terminal state.
    CanceledByIndexer,

    /// The [`IndexingAgreement`] is expired.
    ///
    /// The indexer indexed the data and the agreement is no longer in effect.
    ///
    /// This is a terminal state.
    Expired,

    /// The [`IndexingAgreement`] was accepted on-chain.
    ///
    /// The on-chain `IndexingAgreementAccepted` event was observed for this agreement.
    AcceptedOnChain,

    /// The indexer rejected the agreement proposal off-chain.
    ///
    /// The indexer may still accept on-chain before the deadline. If they do,
    /// Dipper will cancel the agreement via `cancelIndexingAgreementByPayer`.
    Rejected,

    /// The liveness checker detected no indexing progress within the tolerance window.
    ///
    /// Dipper canceled the agreement via `cancelIndexingAgreementByPayer` and will
    /// trigger reassignment to find a replacement indexer.
    ///
    /// This is a terminal state.
    AbandonedByIndexer,
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let status = match self {
            Status::Created => "CREATED",
            Status::Unresponsive => "UNRESPONSIVE",
            Status::CanceledByRequester => "CANCELED_BY_REQUESTER",
            Status::CanceledByIndexer => "CANCELED_BY_INDEXER",
            Status::Expired => "EXPIRED",
            Status::AcceptedOnChain => "ACCEPTED_ON_CHAIN",
            Status::Rejected => "REJECTED",
            Status::AbandonedByIndexer => "ABANDONED_BY_INDEXER",
        };
        f.write_str(status)
    }
}

impl TryFrom<dipper_pgregistry::IndexingAgreement> for IndexingAgreement {
    type Error = anyhow::Error;

    fn try_from(value: dipper_pgregistry::IndexingAgreement) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id,
            nonce_uuid: value.nonce_uuid,
            created_at: value.created_at,
            updated_at: value.updated_at,
            status: match value.status {
                dipper_pgregistry::IndexingAgreementStatus::Created => Status::Created,
                dipper_pgregistry::IndexingAgreementStatus::Unresponsive => Status::Unresponsive,
                dipper_pgregistry::IndexingAgreementStatus::CanceledByRequester => {
                    Status::CanceledByRequester
                }
                dipper_pgregistry::IndexingAgreementStatus::CanceledByIndexer => {
                    Status::CanceledByIndexer
                }
                dipper_pgregistry::IndexingAgreementStatus::Expired => Status::Expired,
                dipper_pgregistry::IndexingAgreementStatus::AcceptedOnChain => {
                    Status::AcceptedOnChain
                }
                dipper_pgregistry::IndexingAgreementStatus::Rejected => Status::Rejected,
                dipper_pgregistry::IndexingAgreementStatus::AbandonedByIndexer => {
                    Status::AbandonedByIndexer
                }
                _ => {
                    return Err(anyhow::anyhow!("Invalid status: {:?}", value.status));
                }
            },
            indexing_request_id: value.indexing_request_id,
            indexer: value.indexer.into(),
            terms: value.terms.into(),
            last_block_height: value.last_block_height,
            last_progress_at: value.last_progress_at,
            rejection_reason: value.rejection_reason,
            terms_version_hash: value.terms_version_hash,
        })
    }
}

impl From<dipper_pgregistry::IndexingAgreementIndexer> for Indexer {
    fn from(value: dipper_pgregistry::IndexingAgreementIndexer) -> Self {
        Self {
            id: value.id,
            url: value.url,
        }
    }
}

impl From<dipper_pgregistry::IndexingAgreementTerms> for Terms {
    fn from(value: dipper_pgregistry::IndexingAgreementTerms) -> Self {
        Self {
            payer: value.payer,
            service_provider: value.service_provider,
            data_service: value.data_service,
            deadline: value.deadline,
            ends_at: value.ends_at,
            max_initial_tokens: value.max_initial_tokens,
            max_ongoing_tokens_per_second: value.max_ongoing_tokens_per_second,
            min_seconds_per_collection: value.min_seconds_per_collection,
            max_seconds_per_collection: value.max_seconds_per_collection,
            conditions: value.conditions,
            metadata: value.metadata.into(),
        }
    }
}

impl From<dipper_pgregistry::IndexingAgreementTermsMetadata> for TermsMetadata {
    fn from(value: dipper_pgregistry::IndexingAgreementTermsMetadata) -> Self {
        Self {
            tokens_per_second: value.tokens_per_second,
            tokens_per_entity_per_second: value.tokens_per_entity_per_second,
            subgraph_deployment_id: value.subgraph_deployment_id,
            protocol_network: value.protocol_network,
            chain_id: value.chain_id,
            proposed_at: value.proposed_at,
        }
    }
}

impl From<Terms> for dipper_pgregistry::IndexingAgreementTerms {
    fn from(value: Terms) -> Self {
        Self {
            payer: value.payer,
            service_provider: value.service_provider,
            data_service: value.data_service,
            deadline: value.deadline,
            ends_at: value.ends_at,
            max_initial_tokens: value.max_initial_tokens,
            max_ongoing_tokens_per_second: value.max_ongoing_tokens_per_second,
            min_seconds_per_collection: value.min_seconds_per_collection,
            max_seconds_per_collection: value.max_seconds_per_collection,
            conditions: value.conditions,
            metadata: value.metadata.into(),
        }
    }
}

impl From<TermsMetadata> for dipper_pgregistry::IndexingAgreementTermsMetadata {
    fn from(value: TermsMetadata) -> Self {
        Self {
            tokens_per_second: value.tokens_per_second,
            tokens_per_entity_per_second: value.tokens_per_entity_per_second,
            subgraph_deployment_id: value.subgraph_deployment_id,
            protocol_network: value.protocol_network,
            chain_id: value.chain_id,
            proposed_at: value.proposed_at,
        }
    }
}

/// The _indexing agreement_ [`fake`] implementation for test data generation.
#[cfg(test)]
pub mod fake_impl {
    use fake::{Dummy, Faker, Rng};

    use super::*;

    impl Dummy<Faker> for Indexer {
        fn dummy_with_rng<R: Rng + ?Sized>(config: &Faker, rng: &mut R) -> Self {
            Self {
                id: IndexerId::dummy_with_rng(config, rng),
                url: Url::dummy_with_rng(config, rng),
            }
        }
    }

    impl Dummy<Faker> for Terms {
        fn dummy_with_rng<R: Rng + ?Sized>(config: &Faker, rng: &mut R) -> Self {
            Self {
                payer: Address::new(<[u8; 20]>::dummy_with_rng(config, rng)),
                service_provider: Address::new(<[u8; 20]>::dummy_with_rng(config, rng)),
                data_service: Address::new(<[u8; 20]>::dummy_with_rng(config, rng)),
                deadline: u64::dummy_with_rng(config, rng),
                ends_at: u64::dummy_with_rng(config, rng),
                max_initial_tokens: U256::from_be_bytes(<[u8; 32]>::dummy_with_rng(config, rng)),
                max_ongoing_tokens_per_second: U256::from_be_bytes(<[u8; 32]>::dummy_with_rng(
                    config, rng,
                )),
                min_seconds_per_collection: u32::dummy_with_rng(config, rng),
                max_seconds_per_collection: u32::dummy_with_rng(config, rng),
                conditions: 0,
                metadata: TermsMetadata::dummy_with_rng(config, rng),
            }
        }
    }

    impl Dummy<Faker> for TermsMetadata {
        fn dummy_with_rng<R: Rng + ?Sized>(config: &Faker, rng: &mut R) -> Self {
            Self {
                tokens_per_second: U256::from_be_bytes(<[u8; 32]>::dummy_with_rng(config, rng)),
                tokens_per_entity_per_second: U256::from_be_bytes(<[u8; 32]>::dummy_with_rng(
                    config, rng,
                )),
                subgraph_deployment_id: DeploymentId::dummy_with_rng(config, rng),
                protocol_network: ChainId::dummy_with_rng(config, rng),
                chain_id: ChainId::dummy_with_rng(config, rng),
                proposed_at: u64::dummy_with_rng(config, rng),
            }
        }
    }
}
