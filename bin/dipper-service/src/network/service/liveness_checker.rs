//! Liveness checker for detecting indexers who silently abandon active agreements.
//!
//! After an indexer accepts an agreement on-chain (`AcceptedOnChain`), they may stop
//! indexing without any on-chain signal. This service polls each indexer's status
//! endpoint every 5 minutes and tracks whether the reported block height for the
//! relevant deployment is advancing.
//!
//! If no upward progress is observed for a threshold window — scaled by how many
//! active agreements the deployment has — the agreement is canceled as payer and
//! reassignment is triggered.
//!
//! ## Threshold
//!
//! | Active agreements on deployment | Tolerance |
//! |---------------------------------|-----------|
//! | 1                               | 1 day     |
//! | 2                               | 2 days    |
//! | 3                               | 3 days    |
//! | 4+                              | max days  |
//!
//! ## Progress tracking
//!
//! Block height is queried via `POST {indexer_url}/status` (no auth required).
//! Agreements are grouped by indexer URL so one HTTP call covers all deployments
//! for a given indexer.
//!
//! - Block height increased → reset `last_progress_at`
//! - Block height decreased (resync) → also reset `last_progress_at`
//! - Block height unchanged → check elapsed time against threshold
//! - Indexer unreachable → treat as no progress (threshold check still applies)
//! - Deployment missing from response → treat as no progress
//!
//! ## URL Refresh
//!
//! Indexer URLs are looked up fresh from the network topology on each cycle,
//! not read from the stored agreement. This ensures URL changes are detected.

use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};

use thegraph_core::{DeploymentId, IndexerId};
use time::OffsetDateTime;
use tokio::{sync::mpsc, time::MissedTickBehavior};
use url::Url;

use crate::{
    chain_client::{ChainClient, ChainClientError},
    config::LivenessCheckerConfig,
    network::provider::NetworkProviderService,
    registry::{
        AgreementRegistry, IndexingAgreement, IndexingRequestRegistry, PendingCancellationRegistry,
    },
    worker::service::{JobPriority, WorkerQueue},
};

/// Handle for controlling the liveness checker service lifecycle.
#[derive(Clone)]
pub struct Handle {
    tx_stop: mpsc::Sender<()>,
}

impl Handle {
    /// Stop the liveness checker service gracefully.
    pub async fn stop(&self) {
        if self.tx_stop.is_closed() {
            return;
        }
        let _ = self.tx_stop.send(()).await;
        self.tx_stop.closed().await;
    }
}

/// Context required by the liveness checker service.
pub struct Ctx<R, W, C> {
    /// Registry for querying and updating agreements.
    pub registry: R,
    /// Worker queue for submitting reassessment jobs.
    pub worker_queue: W,
    /// Chain client for canceling agreements on-chain.
    pub chain_client: C,
    /// Network provider for looking up fresh indexer URLs.
    pub network: NetworkProviderService,
    /// Indexing agreement config (manager address for cancel dispatch).
    pub agreement_conf: Arc<crate::config::IndexingAgreementConfig>,
    /// Service configuration.
    pub config: LivenessCheckerConfig,
}

/// Create a new liveness checker service.
///
/// Returns a handle for controlling the service and a future that must be spawned
/// on a runtime. The service periodically polls indexer status endpoints and cancels
/// agreements where no indexing progress is observed within the tolerance window.
pub fn new<R, W, C>(ctx: Ctx<R, W, C>) -> (Handle, impl Future<Output = anyhow::Result<()>>)
where
    R: AgreementRegistry + IndexingRequestRegistry + PendingCancellationRegistry + Send + Sync,
    W: WorkerQueue + Send + Sync,
    C: ChainClient + Send + Sync,
{
    let (tx_stop, mut rx_stop) = mpsc::channel(1);

    let Ctx {
        registry,
        worker_queue,
        chain_client,
        network,
        agreement_conf,
        config,
    } = ctx;

    let service = async move {
        tracing::info!(
            interval_secs = config.interval.as_secs(),
            max_tolerance_days = config.max_tolerance_days,
            batch_size = config.batch_size,
            "liveness checker started"
        );

        let mut timer = tokio::time::interval(config.interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        const DB_QUERY_TIMEOUT: Duration = Duration::from_secs(30);
        const DB_UPDATE_TIMEOUT: Duration = Duration::from_secs(10);
        const QUEUE_PUSH_TIMEOUT: Duration = Duration::from_secs(10);

        let http_client = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .build()
            .map_err(|e| anyhow::anyhow!("failed to build HTTP client: {e}"))?;

        loop {
            tokio::select! {
                _ = rx_stop.recv() => break,
                _ = timer.tick() => {},
            }

            tracing::debug!("starting liveness check");

            // 1. Fetch all AcceptedOnChain agreements
            let agreements = match tokio::time::timeout(
                DB_QUERY_TIMEOUT,
                registry.get_accepted_on_chain_agreements(config.batch_size),
            )
            .await
            {
                Ok(Ok(a)) => a,
                Ok(Err(err)) => {
                    tracing::error!(error = %err, "failed to query AcceptedOnChain agreements");
                    continue;
                }
                Err(_) => {
                    tracing::error!("timeout querying AcceptedOnChain agreements");
                    continue;
                }
            };

            if agreements.is_empty() {
                tracing::debug!("liveness check: no AcceptedOnChain agreements");
                continue;
            }

            // 2. Count active agreements per deployment for threshold calculation
            let active_counts = match tokio::time::timeout(
                DB_QUERY_TIMEOUT,
                registry.count_active_agreements_by_deployment(),
            )
            .await
            {
                Ok(Ok(counts)) => counts,
                Ok(Err(err)) => {
                    tracing::error!(error = %err, "failed to count active agreements by deployment");
                    continue;
                }
                Err(_) => {
                    tracing::error!("timeout counting active agreements by deployment");
                    continue;
                }
            };

            tracing::debug!(
                agreement_count = agreements.len(),
                "liveness check: checking agreements"
            );

            // 3. Group agreements by indexer ID for batched status queries
            let groups = group_by_indexer_id(agreements);

            for (indexer_id, group_agreements) in groups {
                // Check for shutdown between indexer groups
                if rx_stop.try_recv().is_ok() {
                    tracing::debug!("liveness checker stopping mid-cycle");
                    return Ok(());
                }

                // 4. Look up fresh URL from network topology
                let indexer_url = match network.get_indexer_by_id(&indexer_id) {
                    Some(indexer) => indexer.url,
                    None => {
                        // Indexer no longer in network registry — treat as unreachable
                        tracing::warn!(
                            indexer_id = %indexer_id,
                            "indexer not found in network registry, treating as unreachable"
                        );
                        // Fall through with empty block_heights to trigger threshold checks
                        process_agreements_with_no_data(
                            &group_agreements,
                            &active_counts,
                            &config,
                            &registry,
                            &worker_queue,
                            &chain_client,
                            &agreement_conf,
                            DB_UPDATE_TIMEOUT,
                            QUEUE_PUSH_TIMEOUT,
                        )
                        .await;
                        continue;
                    }
                };

                // 5. Query the indexer status endpoint for all deployments in this group
                let deployment_ids: Vec<String> = group_agreements
                    .iter()
                    .map(|a| a.terms.metadata.subgraph_deployment_id.to_string())
                    .collect();

                let block_heights =
                    match query_indexer_status(&http_client, &indexer_url, &deployment_ids).await {
                        Ok(heights) => heights,
                        Err(err) => {
                            tracing::warn!(
                                indexer_url = %indexer_url,
                                error = %err,
                                "failed to query indexer status, treating as unreachable"
                            );
                            // Unreachable indexer: use empty map so all get current_block = None
                            HashMap::new()
                        }
                    };

                // 6. Process each agreement in the group
                for agreement in group_agreements {
                    let deployment_id_str =
                        agreement.terms.metadata.subgraph_deployment_id.to_string();
                    let current_block = block_heights.get(&deployment_id_str).copied().flatten();

                    let now = OffsetDateTime::now_utc();

                    let action = decide_liveness_action(
                        agreement.last_block_height,
                        current_block,
                        agreement.last_progress_at,
                        now,
                        agreement.terms.metadata.subgraph_deployment_id,
                        &active_counts,
                        config.max_tolerance_days,
                    );

                    match action {
                        LivenessAction::InitializeTracking(block)
                        | LivenessAction::ResetProgress(block) => {
                            record_progress(&registry, &agreement, block, now, DB_UPDATE_TIMEOUT)
                                .await;
                        }
                        LivenessAction::WithinTolerance => {
                            let last_progress = agreement.last_progress_at.unwrap_or(now);
                            let elapsed = now - last_progress;
                            let threshold = tolerance_duration(
                                agreement.terms.metadata.subgraph_deployment_id,
                                &active_counts,
                                config.max_tolerance_days,
                            );
                            tracing::debug!(
                                agreement_id = %agreement.id,
                                deployment = %deployment_id_str,
                                last_block = agreement.last_block_height.unwrap_or(0),
                                elapsed_hours = elapsed.whole_hours(),
                                threshold_hours = threshold.whole_hours(),
                                "agreement block unchanged but within tolerance"
                            );
                        }
                        LivenessAction::CancelAndReassess => {
                            let last_progress = agreement.last_progress_at.unwrap_or(now);
                            let elapsed = now - last_progress;
                            let threshold = tolerance_duration(
                                agreement.terms.metadata.subgraph_deployment_id,
                                &active_counts,
                                config.max_tolerance_days,
                            );
                            tracing::warn!(
                                agreement_id = %agreement.id,
                                indexer_url = %indexer_url,
                                deployment = %deployment_id_str,
                                last_block = agreement.last_block_height.unwrap_or(0),
                                elapsed_hours = elapsed.whole_hours(),
                                threshold_hours = threshold.whole_hours(),
                                "agreement stale: no indexing progress detected"
                            );
                            cancel_and_reassess(
                                &agreement,
                                &registry,
                                &worker_queue,
                                &chain_client,
                                &agreement_conf,
                                DB_UPDATE_TIMEOUT,
                                QUEUE_PUSH_TIMEOUT,
                            )
                            .await;
                        }
                    }
                }
            }

            tracing::debug!("liveness check completed");
        }

        tracing::debug!("liveness checker stopped");
        Ok(())
    };

    (Handle { tx_stop }, service)
}

/// Group agreements by indexer ID for batched status queries.
fn group_by_indexer_id(
    agreements: Vec<IndexingAgreement>,
) -> HashMap<IndexerId, Vec<IndexingAgreement>> {
    let mut groups: HashMap<IndexerId, Vec<IndexingAgreement>> = HashMap::new();
    for agreement in agreements {
        groups
            .entry(agreement.indexer.id)
            .or_default()
            .push(agreement);
    }
    groups
}

/// Compute the tolerance duration for a deployment based on active agreement count.
///
/// Returns `min(active_count, max_days)` days as a duration.
fn tolerance_duration(
    deployment_id: DeploymentId,
    active_counts: &HashMap<DeploymentId, usize>,
    max_tolerance_days: u32,
) -> time::Duration {
    let count = active_counts.get(&deployment_id).copied().unwrap_or(1);
    let days = (count as u32).min(max_tolerance_days).max(1);
    time::Duration::days(days as i64)
}

/// Process agreements when we have no status data (indexer not in registry).
///
/// All agreements get `current_block = None`, triggering threshold checks.
#[allow(clippy::too_many_arguments)]
async fn process_agreements_with_no_data<R, W, C>(
    agreements: &[IndexingAgreement],
    active_counts: &HashMap<DeploymentId, usize>,
    config: &LivenessCheckerConfig,
    registry: &R,
    worker_queue: &W,
    chain_client: &C,
    agreement_conf: &crate::config::IndexingAgreementConfig,
    db_timeout: Duration,
    queue_timeout: Duration,
) where
    R: AgreementRegistry + IndexingRequestRegistry + PendingCancellationRegistry + Send + Sync,
    W: WorkerQueue + Send + Sync,
    C: ChainClient + Send + Sync,
{
    let now = OffsetDateTime::now_utc();

    for agreement in agreements {
        let action = decide_liveness_action(
            agreement.last_block_height,
            None, // No data available
            agreement.last_progress_at,
            now,
            agreement.terms.metadata.subgraph_deployment_id,
            active_counts,
            config.max_tolerance_days,
        );

        match action {
            LivenessAction::InitializeTracking(block) => {
                // First check with no data — initialize at 0
                record_progress(registry, agreement, block, now, db_timeout).await;
            }
            LivenessAction::ResetProgress(_) => {
                // Should not happen when current_block is None
                unreachable!("ResetProgress should not occur with current_block = None");
            }
            LivenessAction::WithinTolerance => {
                let last_progress = agreement.last_progress_at.unwrap_or(now);
                let elapsed = now - last_progress;
                let threshold = tolerance_duration(
                    agreement.terms.metadata.subgraph_deployment_id,
                    active_counts,
                    config.max_tolerance_days,
                );
                tracing::debug!(
                    agreement_id = %agreement.id,
                    elapsed_hours = elapsed.whole_hours(),
                    threshold_hours = threshold.whole_hours(),
                    "indexer unreachable but within tolerance"
                );
            }
            LivenessAction::CancelAndReassess => {
                let last_progress = agreement.last_progress_at.unwrap_or(now);
                let elapsed = now - last_progress;
                let threshold = tolerance_duration(
                    agreement.terms.metadata.subgraph_deployment_id,
                    active_counts,
                    config.max_tolerance_days,
                );
                tracing::warn!(
                    agreement_id = %agreement.id,
                    last_block = agreement.last_block_height.unwrap_or(0),
                    elapsed_hours = elapsed.whole_hours(),
                    threshold_hours = threshold.whole_hours(),
                    "indexer unreachable and tolerance exceeded"
                );
                cancel_and_reassess(
                    agreement,
                    registry,
                    worker_queue,
                    chain_client,
                    agreement_conf,
                    db_timeout,
                    queue_timeout,
                )
                .await;
            }
        }
    }
}

/// Update sync progress for an agreement in the DB.
async fn record_progress<R>(
    registry: &R,
    agreement: &IndexingAgreement,
    block_height: u64,
    progress_at: OffsetDateTime,
    timeout: Duration,
) where
    R: AgreementRegistry + Send + Sync,
{
    match tokio::time::timeout(
        timeout,
        registry.update_agreement_sync_progress(&agreement.id, block_height, progress_at),
    )
    .await
    {
        Ok(Ok(())) => {
            tracing::debug!(
                agreement_id = %agreement.id,
                block_height,
                "recorded sync progress"
            );
        }
        Ok(Err(err)) => {
            tracing::warn!(
                agreement_id = %agreement.id,
                error = %err,
                "failed to record sync progress"
            );
        }
        Err(_) => {
            tracing::warn!(
                agreement_id = %agreement.id,
                "timeout recording sync progress"
            );
        }
    }
}

/// Cancel a stale agreement on-chain and queue reassessment.
///
/// If the on-chain cancel fails, the DB is not updated and reassessment is not
/// queued, leaving the agreement in `AcceptedOnChain` for the next cycle to retry.
#[allow(clippy::too_many_arguments)]
async fn cancel_and_reassess<R, W, C>(
    agreement: &IndexingAgreement,
    registry: &R,
    worker_queue: &W,
    chain_client: &C,
    agreement_conf: &crate::config::IndexingAgreementConfig,
    db_timeout: Duration,
    queue_timeout: Duration,
) where
    R: AgreementRegistry + IndexingRequestRegistry + PendingCancellationRegistry + Send + Sync,
    W: WorkerQueue + Send + Sync,
    C: ChainClient + Send + Sync,
{
    // 1. Cancel on-chain (mode-aware dispatch)
    let mut on_chain_cancel_tx: Option<String> = None;
    match crate::cancel_dispatch::cancel_agreement_on_chain(
        chain_client,
        registry,
        agreement,
        agreement_conf,
    )
    .await
    {
        Ok(Some(tx_hash)) => {
            tracing::info!(
                agreement_id = %agreement.id,
                tx_hash = %tx_hash,
                "canceled stale agreement on-chain"
            );
            on_chain_cancel_tx = Some(tx_hash.to_string());
        }
        Ok(None) => {
            tracing::info!(
                agreement_id = %agreement.id,
                "stale agreement already canceled on-chain; proceeding to mark abandoned"
            );
        }
        Err(err @ ChainClientError::MissingTermsVersionHash { .. }) => {
            // Permanent per-agreement condition: the on-chain agreement is
            // still live, so do NOT mark abandoned (that would hide a
            // money-draining agreement). Surface for operator action.
            tracing::error!(
                agreement_id = %agreement.id,
                error = %err,
                "cannot cancel stale agreement: missing terms_version_hash; leaving active for operator action"
            );
            return;
        }
        Err(ChainClientError::ConfigError(_)) => {
            // Chain client disabled: still proceed to mark and reassess so the
            // DB reflects the detected abandonment even without an on-chain tx.
            tracing::warn!(
                agreement_id = %agreement.id,
                "chain client not configured, skipping on-chain cancellation"
            );
        }
        Err(err) => {
            tracing::error!(
                agreement_id = %agreement.id,
                error = %err,
                "failed to cancel stale agreement on-chain, will retry next cycle"
            );
            return;
        }
    }

    // 2. Mark as abandoned in DB
    let abandoned = match tokio::time::timeout(
        db_timeout,
        registry.mark_indexing_agreement_as_abandoned(&agreement.id),
    )
    .await
    {
        Ok(Ok(a)) => a,
        Ok(Err(err)) => {
            tracing::error!(
                agreement_id = %agreement.id,
                error = %err,
                "failed to mark agreement as abandoned"
            );
            return;
        }
        Err(_) => {
            tracing::error!(
                agreement_id = %agreement.id,
                "timeout marking agreement as abandoned"
            );
            return;
        }
    };

    // The accepted agreement was canceled on-chain because the indexer went
    // stale. Record the cancel audit so the chain_listener's `terminated` sweep
    // announces it durably: this row is marked `AbandonedByIndexer` (terminal)
    // and was accepted on-chain, so it is sweep-eligible.
    let manager = agreement_conf.recurring_agreement_manager().to_string();
    if let Err(err) = registry
        .record_cancel_audit(
            &agreement.id,
            dipper_core::time::now_secs(),
            &manager,
            on_chain_cancel_tx.as_deref(),
        )
        .await
    {
        tracing::warn!(
            agreement_id = %agreement.id,
            error = %err,
            "failed to record cancel audit; terminated event may emit with fallback fields"
        );
    }

    // Clean up pending cancellations: if this abandoned agreement was a
    // replacement, the old agreement it was replacing should stay active.
    if let Err(err) = registry
        .delete_pending_cancellations_by_new_agreement(agreement.id)
        .await
    {
        tracing::warn!(
            agreement_id = %agreement.id,
            error = %err,
            "failed to clean up pending cancellations for abandoned agreement"
        );
    }

    // 3. Fetch the indexing request for num_candidates
    let request = match tokio::time::timeout(
        db_timeout,
        registry.get_indexing_request_by_id(&abandoned.indexing_request_id),
    )
    .await
    {
        Ok(Ok(Some(r))) => r,
        Ok(Ok(None)) => {
            tracing::warn!(
                agreement_id = %agreement.id,
                indexing_request_id = %abandoned.indexing_request_id,
                "indexing request not found for abandoned agreement"
            );
            return;
        }
        Ok(Err(err)) => {
            tracing::warn!(
                agreement_id = %agreement.id,
                error = %err,
                "failed to fetch indexing request for abandoned agreement"
            );
            return;
        }
        Err(_) => {
            tracing::warn!(
                agreement_id = %agreement.id,
                "timeout fetching indexing request for abandoned agreement"
            );
            return;
        }
    };

    // 4. Queue reassessment
    let push_result = tokio::time::timeout(
        queue_timeout,
        worker_queue.reassess_indexing_request(
            abandoned.indexing_request_id,
            abandoned.terms.metadata.subgraph_deployment_id,
            abandoned.terms.metadata.chain_id,
            request.num_candidates,
            // Background: abandonment remediation yields to interactive work.
            JobPriority::Background,
        ),
    )
    .await;

    match push_result {
        Ok(Ok(_job_id)) => {
            tracing::info!(
                agreement_id = %agreement.id,
                indexing_request_id = %abandoned.indexing_request_id,
                "queued reassessment for abandoned agreement"
            );
        }
        Ok(Err(err)) => {
            tracing::warn!(
                agreement_id = %agreement.id,
                error = %err,
                "failed to queue reassessment for abandoned agreement"
            );
        }
        Err(_) => {
            tracing::warn!(
                agreement_id = %agreement.id,
                "timeout queuing reassessment for abandoned agreement"
            );
        }
    }
}

/// The action the liveness checker should take for a single agreement after evaluating its sync state.
#[derive(Debug, PartialEq)]
pub(crate) enum LivenessAction {
    /// No prior state: record the current block height to start the clock.
    InitializeTracking(u64),
    /// Block height changed (up or down, including resync): reset the progress clock.
    ResetProgress(u64),
    /// Block height unchanged and within tolerance: no action.
    WithinTolerance,
    /// Block height unchanged and tolerance exceeded: cancel and reassess.
    CancelAndReassess,
}

/// Pure decision function — determines what action to take for one agreement.
///
/// No I/O, no async, fully unit testable.
pub(crate) fn decide_liveness_action(
    last_block_height: Option<u64>,
    current_block: Option<u64>,
    last_progress_at: Option<OffsetDateTime>,
    now: OffsetDateTime,
    deployment_id: DeploymentId,
    active_counts: &HashMap<DeploymentId, usize>,
    max_tolerance_days: u32,
) -> LivenessAction {
    match (last_block_height, current_block) {
        // First check, no prior state — initialize tracking
        (None, Some(block)) => LivenessAction::InitializeTracking(block),
        (None, None) => LivenessAction::InitializeTracking(0),

        // Prior state exists, can see current block, block changed — reset progress
        (Some(last), Some(current)) if current != last => LivenessAction::ResetProgress(current),

        // Prior state exists but can't see current block (unreachable or deployment missing)
        // OR block unchanged — treat as no progress, check threshold
        (Some(_), None) | (Some(_), Some(_)) => {
            let last_progress = last_progress_at.unwrap_or(now);
            let elapsed = now - last_progress;
            let threshold = tolerance_duration(deployment_id, active_counts, max_tolerance_days);
            if elapsed > threshold {
                LivenessAction::CancelAndReassess
            } else {
                LivenessAction::WithinTolerance
            }
        }
    }
}

/// Query a single indexer's status endpoint for sync progress.
///
/// Returns a map of deployment ID (IPFS hash string) to the latest block height,
/// or `None` if the deployment is not found in the response.
///
/// Returns an error only when the HTTP call itself fails (network error, timeout,
/// non-2xx response). GraphQL-level errors are logged and treated as empty results.
async fn query_indexer_status(
    client: &reqwest::Client,
    indexer_url: &Url,
    deployment_ids: &[String],
) -> anyhow::Result<HashMap<String, Option<u64>>> {
    let status_url = indexer_url
        .join("status")
        .map_err(|e| anyhow::anyhow!("failed to construct status URL from {indexer_url}: {e}"))?;

    // Build GraphQL query listing all deployment IDs
    let ids_str = deployment_ids
        .iter()
        .map(|id| format!("\"{id}\""))
        .collect::<Vec<_>>()
        .join(", ");

    let query = format!(
        "{{ indexingStatuses(subgraphs: [{ids_str}]) {{ subgraph chains {{ latestBlock {{ number }} }} }} }}"
    );

    let body = serde_json::json!({ "query": query });

    let response = client
        .post(status_url)
        .json(&body)
        .send()
        .await?
        .error_for_status()?;

    let json: serde_json::Value = response.json().await?;

    let mut result: HashMap<String, Option<u64>> = HashMap::new();

    let statuses = match json
        .get("data")
        .and_then(|d| d.get("indexingStatuses"))
        .and_then(|s| s.as_array())
    {
        Some(arr) => arr,
        None => {
            tracing::debug!(
                indexer_url = %indexer_url,
                "indexingStatuses not present in response"
            );
            return Ok(result);
        }
    };

    for status in statuses {
        let subgraph = match status.get("subgraph").and_then(|s| s.as_str()) {
            Some(s) => s.to_string(),
            None => continue,
        };

        // Extract latestBlock.number from the first chain entry
        let block_number = status
            .get("chains")
            .and_then(|c| c.as_array())
            .and_then(|arr| arr.first())
            .and_then(|chain| chain.get("latestBlock"))
            .and_then(|lb| lb.get("number"))
            .and_then(|n| {
                // latestBlock.number is a BigInt — may be string or number in JSON
                if let Some(s) = n.as_str() {
                    s.parse::<u64>().ok()
                } else {
                    n.as_u64()
                }
            });

        result.insert(subgraph, block_number);
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    use async_trait::async_trait;
    use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
    use dipper_pgmq::JobId;
    use thegraph_core::{
        DeploymentId, IndexerId,
        alloy::primitives::{Address, B256, ChainId, U256},
    };
    use time::OffsetDateTime;
    use url::Url;

    use super::{
        LivenessAction, cancel_and_reassess, decide_liveness_action, group_by_indexer_id,
        record_progress, tolerance_duration,
    };
    use crate::{
        chain_client::{ChainClient, ChainClientError},
        config::LivenessCheckerConfig,
        registry::{
            AgreementFeeRate, IndexingAgreement, IndexingAgreementStatus, IndexingAgreementTerms,
            IndexingAgreementTermsMetadata, IndexingRequest, IndexingRequestRegistry,
            PendingCancellationRegistry, Result as RegistryResult, StubAgreementRegistry,
        },
        worker::service::{JobPriority, WorkerQueue},
    };

    // ---- Test helpers ----

    fn make_agreement(
        indexer_id: IndexerId,
        url: Url,
        deployment_id: DeploymentId,
        last_block_height: Option<u64>,
        last_progress_at: Option<OffsetDateTime>,
    ) -> IndexingAgreement {
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        let terms = IndexingAgreementTerms {
            payer: Address::ZERO,
            service_provider: Address::ZERO,
            data_service: Address::ZERO,
            deadline: 0,
            ends_at: 0,
            max_initial_tokens: U256::ZERO,
            max_ongoing_tokens_per_second: U256::ZERO,
            min_seconds_per_collection: 0,
            max_seconds_per_collection: 0,
            conditions: 0,
            metadata: IndexingAgreementTermsMetadata {
                tokens_per_second: U256::ZERO,
                tokens_per_entity_per_second: U256::ZERO,
                subgraph_deployment_id: deployment_id,
                protocol_network: 1u64,
                chain_id: 1u64,
                proposed_at: 0,
            },
        };
        IndexingAgreement {
            id: agreement_id,
            nonce_uuid: uuid::Uuid::now_v7(),
            created_at: OffsetDateTime::now_utc(),
            updated_at: OffsetDateTime::now_utc(),
            status: IndexingAgreementStatus::AcceptedOnChain,
            indexing_request_id: IndexingRequestId::new(),
            indexer: crate::registry::Indexer {
                id: indexer_id,
                url,
            },
            terms,
            last_block_height,
            last_progress_at,
            rejection_reason: None,
            // The manager cancel path requires a 32-byte stored terms hash.
            terms_version_hash: Some(vec![0u8; 32]),
        }
    }

    fn make_request(id: IndexingRequestId, num_candidates: usize) -> IndexingRequest {
        let dep: DeploymentId = "QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9"
            .parse()
            .unwrap();
        IndexingRequest {
            id,
            created_at: OffsetDateTime::now_utc(),
            updated_at: OffsetDateTime::now_utc(),
            status: crate::registry::IndexingRequestStatus::Open,
            requested_by: Address::ZERO,
            deployment_id: dep,
            deployment_chain_id: 1u64,
            num_candidates,
        }
    }

    // ---- Mocks ----

    #[derive(Clone, Default)]
    struct MockCalls {
        progress_updates: Arc<Mutex<Vec<(IndexingAgreementId, u64)>>>,
        abandoned: Arc<Mutex<Vec<IndexingAgreementId>>>,
        reassessments: Arc<Mutex<Vec<IndexingRequestId>>>,
        chain_cancels: Arc<Mutex<Vec<[u8; 16]>>>,
        /// Ids passed to `record_cancel_audit` -- the signal the handler drives
        /// the terminated event (the chain_listener sweep emits from this audit).
        cancel_audits: Arc<Mutex<Vec<IndexingAgreementId>>>,
    }

    struct MockRegistry {
        calls: MockCalls,
        mark_abandoned_result: Arc<Mutex<Option<RegistryResult<IndexingAgreement>>>>,
        get_request_result: Arc<Mutex<Option<RegistryResult<Option<IndexingRequest>>>>>,
    }

    impl MockRegistry {
        fn new(calls: MockCalls, agreement: IndexingAgreement) -> Self {
            let abandoned_agreement = {
                let mut a = agreement.clone();
                a.status = IndexingAgreementStatus::AbandonedByIndexer;
                a
            };
            let request = make_request(agreement.indexing_request_id, 2);
            Self {
                calls,
                mark_abandoned_result: Arc::new(Mutex::new(Some(Ok(abandoned_agreement)))),
                get_request_result: Arc::new(Mutex::new(Some(Ok(Some(request))))),
            }
        }

        fn with_chain_error(calls: MockCalls, agreement: IndexingAgreement) -> Self {
            let mut mock = Self::new(calls, agreement);
            mock.mark_abandoned_result = Arc::new(Mutex::new(None));
            mock
        }
    }

    #[async_trait]
    impl StubAgreementRegistry for MockRegistry {
        async fn get_unresponsive_indexers(
            &self,
            _lookback_days: i32,
            _chain_id: thegraph_core::alloy::primitives::ChainId,
        ) -> RegistryResult<Vec<IndexerId>> {
            Ok(vec![])
        }
        async fn update_agreement_sync_progress(
            &self,
            id: &IndexingAgreementId,
            block_height: u64,
            _progress_at: OffsetDateTime,
        ) -> RegistryResult<()> {
            self.calls
                .progress_updates
                .lock()
                .unwrap()
                .push((*id, block_height));
            Ok(())
        }
        async fn mark_indexing_agreement_as_abandoned(
            &self,
            id: &IndexingAgreementId,
        ) -> RegistryResult<IndexingAgreement> {
            self.calls.abandoned.lock().unwrap().push(*id);
            self.mark_abandoned_result
                .lock()
                .unwrap()
                .take()
                .expect("mark_abandoned called more than once")
        }

        async fn record_cancel_audit(
            &self,
            agreement_id: &IndexingAgreementId,
            _canceled_at: u64,
            _canceled_by: &str,
            _canceled_tx: Option<&str>,
        ) -> RegistryResult<()> {
            self.calls.cancel_audits.lock().unwrap().push(*agreement_id);
            Ok(())
        }

        async fn get_agreement_fee_rates(&self) -> RegistryResult<Vec<AgreementFeeRate>> {
            Ok(vec![])
        }
    }

    #[async_trait]
    impl IndexingRequestRegistry for MockRegistry {
        async fn set_indexing_target_candidates(
            &self,
            _by: Address,
            _dep: DeploymentId,
            _chain: ChainId,
            _n: usize,
        ) -> RegistryResult<crate::registry::SetTargetOutcome> {
            unimplemented!()
        }
        async fn get_all_indexing_requests(&self) -> RegistryResult<Vec<IndexingRequest>> {
            unimplemented!()
        }
        async fn get_indexing_request_by_id(
            &self,
            _id: &IndexingRequestId,
        ) -> RegistryResult<Option<IndexingRequest>> {
            self.get_request_result
                .lock()
                .unwrap()
                .take()
                .expect("get_indexing_request_by_id called more than once")
        }
        async fn get_indexing_requests_by_deployment_id(
            &self,
            _dep: &DeploymentId,
        ) -> RegistryResult<Vec<IndexingRequest>> {
            unimplemented!()
        }
        async fn get_open_indexing_requests_for_reassessment(
            &self,
            _min_age: i64,
            _batch: i64,
        ) -> RegistryResult<Vec<IndexingRequest>> {
            unimplemented!()
        }
    }

    #[async_trait]
    impl PendingCancellationRegistry for MockRegistry {
        async fn get_pending_cancellations_by_new_agreement(
            &self,
            _new_agreement_id: IndexingAgreementId,
        ) -> RegistryResult<Vec<crate::registry::PendingCancellation>> {
            Ok(vec![])
        }
        async fn delete_pending_cancellations_by_new_agreement(
            &self,
            _new_agreement_id: IndexingAgreementId,
        ) -> RegistryResult<()> {
            Ok(())
        }
        async fn delete_pending_cancellation(
            &self,
            _new_agreement_id: IndexingAgreementId,
            _old_agreement_id: IndexingAgreementId,
        ) -> RegistryResult<()> {
            Ok(())
        }
        async fn list_executable_pending_cancellations(
            &self,
            _limit: i64,
        ) -> RegistryResult<Vec<IndexingAgreementId>> {
            Ok(vec![])
        }
    }

    #[derive(Clone)]
    struct MockWorkerQueue {
        calls: MockCalls,
    }

    #[async_trait]
    impl WorkerQueue for MockWorkerQueue {
        async fn send_indexing_agreement_proposal(
            &self,
            _url: Url,
            _agr_id: IndexingAgreementId,
            _req_id: IndexingRequestId,
            _dep: DeploymentId,
            _chain: ChainId,
            _priority: JobPriority,
        ) -> anyhow::Result<JobId> {
            unimplemented!()
        }
        async fn reassess_indexing_request(
            &self,
            req_id: IndexingRequestId,
            _dep: DeploymentId,
            _chain: ChainId,
            _n: usize,
            _priority: JobPriority,
        ) -> anyhow::Result<JobId> {
            self.calls.reassessments.lock().unwrap().push(req_id);
            Ok(JobId::default())
        }
        async fn cancel_rejected_agreement_on_chain(
            &self,
            _agr_id: IndexingAgreementId,
            _priority: JobPriority,
        ) -> anyhow::Result<JobId> {
            unimplemented!()
        }
        async fn submit_offer(
            &self,
            _agreement_id: IndexingAgreementId,
            _indexing_request_id: IndexingRequestId,
            _indexer_url: Url,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
            _priority: JobPriority,
        ) -> anyhow::Result<JobId> {
            unimplemented!()
        }
    }

    struct MockChainClient {
        calls: MockCalls,
        result: Result<B256, ChainClientError>,
    }

    impl MockChainClient {
        fn success(calls: MockCalls) -> Self {
            Self {
                calls,
                result: Ok(B256::ZERO),
            }
        }

        fn config_error(calls: MockCalls) -> Self {
            Self {
                calls,
                result: Err(ChainClientError::ConfigError("disabled".into())),
            }
        }

        fn rpc_error(calls: MockCalls) -> Self {
            Self {
                calls,
                result: Err(ChainClientError::RpcError(anyhow::anyhow!("network error"))),
            }
        }
    }

    #[async_trait]
    impl ChainClient for MockChainClient {
        async fn latest_block_timestamp(&self) -> Result<u64, ChainClientError> {
            // Err by default so a test must mock this explicitly to take the
            // live-chain-head path instead of silently reading timestamp 0.
            Err(ChainClientError::RpcError(anyhow::anyhow!(
                "latest_block_timestamp not mocked"
            )))
        }

        async fn offer_via_manager(
            &self,
            _rca: &dipper_rpc::indexer::indexer_client::sol::RecurringCollectionAgreement,
        ) -> Result<Option<B256>, ChainClientError> {
            Ok(None)
        }

        async fn cancel_via_manager(
            &self,
            _collector: thegraph_core::alloy::primitives::Address,
            agreement_id: &[u8; 16],
            _version_hash: B256,
            _options: u16,
        ) -> Result<Option<B256>, ChainClientError> {
            // Route manager cancels through the same recorder and result so the
            // existing cancel-path assertions hold.
            self.calls.chain_cancels.lock().unwrap().push(*agreement_id);
            match &self.result {
                Ok(hash) => Ok(Some(*hash)),
                Err(ChainClientError::ConfigError(s)) => {
                    Err(ChainClientError::ConfigError(s.clone()))
                }
                Err(ChainClientError::RpcError(e)) => {
                    Err(ChainClientError::RpcError(anyhow::anyhow!("{e}")))
                }
                Err(e) => Err(ChainClientError::RpcError(anyhow::anyhow!("{e}"))),
            }
        }

        async fn reconcile_provider(
            &self,
            _collector: thegraph_core::alloy::primitives::Address,
            _provider: thegraph_core::alloy::primitives::Address,
        ) -> Result<Option<B256>, ChainClientError> {
            // Not exercised by liveness_checker tests.
            Ok(None)
        }

        async fn agreement_still_active(
            &self,
            _agreement_id: &[u8; 16],
        ) -> Result<bool, ChainClientError> {
            // Cancel dispatch reads back after a mined cancel; reporting
            // not-active means "cancel confirmed", which these tests expect.
            Ok(false)
        }

        async fn fetch_agreement_version_hash(
            &self,
            _agreement_id: &[u8; 16],
        ) -> Result<Option<B256>, ChainClientError> {
            // These tests always construct agreements with a stored hash, so
            // recovery is never exercised.
            unimplemented!("not exercised by liveness_checker tests")
        }
    }

    const DB_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
    const QUEUE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

    /// Default agreement config for the cancel-path tests.
    fn test_agreement_conf() -> crate::config::IndexingAgreementConfig {
        crate::config::IndexingAgreementConfig {
            data_service: thegraph_core::alloy::primitives::Address::ZERO,
            recurring_collector: thegraph_core::alloy::primitives::Address::ZERO,
            recurring_agreement_manager: thegraph_core::alloy::primitives::Address::ZERO,
            max_agreement_grt_per_30_days: 0.0,
            max_seconds_per_collection: 0,
            min_seconds_per_collection: 0,
            duration_seconds: 0,
            deadline_seconds: 0,
            max_grt_per_30_days: std::collections::BTreeMap::new(),
            max_grt_per_billion_entities_per_30_days: 0.0,
            declined_indexer_lookback_days: 0,
            price_rejection_lookback_days: 0,
            transient_rejection_lookback_minutes: 0,
            uncertain_rejection_lookback_days: 0,
            unresponsive_indexer_lookback_days: 0,
            mass_unresponsive_trip_fraction: 0.5,
            mass_unresponsive_reset_fraction: 0.25,
            dips_accepting_snapshot_max_age_hours: 48,
            dips_accepting_cache_ttl_seconds: 300,
            max_in_flight_offers_per_indexer: None,
            max_in_flight_offers_total: None,
        }
    }

    // ---- Pure function tests ----

    #[test]
    fn test_threshold_scales_with_active_agreements() {
        let deployment: DeploymentId = "QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9"
            .parse()
            .unwrap();

        let max = 4u32;

        // 1 active agreement → 1 day
        let counts = HashMap::from([(deployment, 1usize)]);
        assert_eq!(tolerance_duration(deployment, &counts, max).whole_days(), 1);

        // 2 active agreements → 2 days
        let counts = HashMap::from([(deployment, 2usize)]);
        assert_eq!(tolerance_duration(deployment, &counts, max).whole_days(), 2);

        // 3 active agreements → 3 days
        let counts = HashMap::from([(deployment, 3usize)]);
        assert_eq!(tolerance_duration(deployment, &counts, max).whole_days(), 3);

        // 4 active agreements → capped at max (4 days)
        let counts = HashMap::from([(deployment, 4usize)]);
        assert_eq!(tolerance_duration(deployment, &counts, max).whole_days(), 4);

        // 10 active agreements → still capped at max
        let counts = HashMap::from([(deployment, 10usize)]);
        assert_eq!(tolerance_duration(deployment, &counts, max).whole_days(), 4);

        // Deployment not found → defaults to 1 day
        let counts = HashMap::new();
        assert_eq!(tolerance_duration(deployment, &counts, max).whole_days(), 1);
    }

    #[test]
    fn test_group_by_indexer_id() {
        let indexer_a = IndexerId::from(Address::repeat_byte(0x0a));
        let indexer_b = IndexerId::from(Address::repeat_byte(0x0b));
        let url: Url = "http://indexer.example.com/".parse().unwrap();
        let dep: DeploymentId = "QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9"
            .parse()
            .unwrap();

        let a1 = make_agreement(indexer_a, url.clone(), dep, None, None);
        let a2 = make_agreement(indexer_a, url.clone(), dep, None, None);
        let b1 = make_agreement(indexer_b, url.clone(), dep, None, None);

        let groups = group_by_indexer_id(vec![a1, a2, b1]);

        assert_eq!(groups.len(), 2);
        assert_eq!(groups[&indexer_a].len(), 2);
        assert_eq!(groups[&indexer_b].len(), 1);
    }

    #[test]
    fn test_default_config() {
        let config = LivenessCheckerConfig::default();
        assert!(!config.enabled); // off by default
        assert_eq!(config.interval, std::time::Duration::from_secs(300));
        assert_eq!(config.max_tolerance_days, 4);
        assert_eq!(config.request_timeout, std::time::Duration::from_secs(10));
        assert_eq!(config.batch_size, 500);
    }

    // ---- decide_liveness_action pure function tests ----

    #[test]
    fn test_first_check_initializes_with_current_block() {
        // Arrange
        let dep: DeploymentId = "QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9"
            .parse()
            .unwrap();
        let counts = HashMap::new();
        let now = OffsetDateTime::now_utc();

        // Act
        let action = decide_liveness_action(None, Some(500), None, now, dep, &counts, 4);

        // Assert
        assert_eq!(action, LivenessAction::InitializeTracking(500));
    }

    #[test]
    fn test_first_check_missing_deployment_initializes_at_zero() {
        // Arrange
        let dep: DeploymentId = "QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9"
            .parse()
            .unwrap();
        let counts = HashMap::new();
        let now = OffsetDateTime::now_utc();

        // Act — no block in response (deployment missing)
        let action = decide_liveness_action(None, None, None, now, dep, &counts, 4);

        // Assert
        assert_eq!(action, LivenessAction::InitializeTracking(0));
    }

    #[test]
    fn test_block_increase_resets_progress() {
        // Arrange
        let dep: DeploymentId = "QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9"
            .parse()
            .unwrap();
        let counts = HashMap::new();
        let now = OffsetDateTime::now_utc();

        // Act
        let action = decide_liveness_action(Some(100), Some(200), None, now, dep, &counts, 4);

        // Assert
        assert_eq!(action, LivenessAction::ResetProgress(200));
    }

    #[test]
    fn test_block_decrease_resets_progress() {
        // Arrange — resync scenario: block went backwards
        let dep: DeploymentId = "QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9"
            .parse()
            .unwrap();
        let counts = HashMap::new();
        let now = OffsetDateTime::now_utc();

        // Act
        let action = decide_liveness_action(Some(200), Some(50), None, now, dep, &counts, 4);

        // Assert
        assert_eq!(action, LivenessAction::ResetProgress(50));
    }

    #[test]
    fn test_within_tolerance_no_action() {
        // Arrange — block unchanged, 12h elapsed, 1-day threshold
        let dep: DeploymentId = "QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9"
            .parse()
            .unwrap();
        let counts = HashMap::from([(dep, 1usize)]);
        let now = OffsetDateTime::now_utc();
        let last_progress = now - time::Duration::hours(12);

        // Act
        let action = decide_liveness_action(
            Some(100),
            Some(100),
            Some(last_progress),
            now,
            dep,
            &counts,
            4,
        );

        // Assert
        assert_eq!(action, LivenessAction::WithinTolerance);
    }

    #[test]
    fn test_stale_triggers_cancel() {
        // Arrange — block unchanged, 25h elapsed, 1-day threshold
        let dep: DeploymentId = "QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9"
            .parse()
            .unwrap();
        let counts = HashMap::from([(dep, 1usize)]);
        let now = OffsetDateTime::now_utc();
        let last_progress = now - time::Duration::hours(25);

        // Act
        let action = decide_liveness_action(
            Some(100),
            Some(100),
            Some(last_progress),
            now,
            dep,
            &counts,
            4,
        );

        // Assert
        assert_eq!(action, LivenessAction::CancelAndReassess);
    }

    #[test]
    fn test_unreachable_within_tolerance_no_action() {
        // Arrange — indexer unreachable (current_block = None), but within tolerance
        let dep: DeploymentId = "QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9"
            .parse()
            .unwrap();
        let counts = HashMap::from([(dep, 1usize)]);
        let now = OffsetDateTime::now_utc();
        let last_progress = now - time::Duration::hours(12);

        // Act — prior state exists but can't see current block
        let action = decide_liveness_action(
            Some(100),
            None, // Unreachable
            Some(last_progress),
            now,
            dep,
            &counts,
            4,
        );

        // Assert — should check threshold, not skip
        assert_eq!(action, LivenessAction::WithinTolerance);
    }

    #[test]
    fn test_unreachable_exceeds_tolerance_triggers_cancel() {
        // Arrange — indexer unreachable and tolerance exceeded
        let dep: DeploymentId = "QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9"
            .parse()
            .unwrap();
        let counts = HashMap::from([(dep, 1usize)]);
        let now = OffsetDateTime::now_utc();
        let last_progress = now - time::Duration::hours(25);

        // Act — prior state exists but can't see current block
        let action = decide_liveness_action(
            Some(100),
            None, // Unreachable
            Some(last_progress),
            now,
            dep,
            &counts,
            4,
        );

        // Assert — should trigger cancellation
        assert_eq!(action, LivenessAction::CancelAndReassess);
    }

    // ---- cancel_and_reassess behavior tests ----

    #[tokio::test]
    async fn test_cancel_and_reassess_success() {
        // Arrange
        let dep: DeploymentId = "QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9"
            .parse()
            .unwrap();
        let indexer_id = IndexerId::from(Address::ZERO);
        let url: Url = "http://indexer.example.com/".parse().unwrap();
        let agreement = make_agreement(
            indexer_id,
            url,
            dep,
            Some(100),
            Some(OffsetDateTime::now_utc()),
        );
        let req_id = agreement.indexing_request_id;
        let agr_id = agreement.id;

        let calls = MockCalls::default();
        let registry = MockRegistry::new(calls.clone(), agreement.clone());
        let queue = MockWorkerQueue {
            calls: calls.clone(),
        };
        let chain = MockChainClient::success(calls.clone());

        // Act
        cancel_and_reassess(
            &agreement,
            &registry,
            &queue,
            &chain,
            &test_agreement_conf(),
            DB_TIMEOUT,
            QUEUE_TIMEOUT,
        )
        .await;

        // Assert
        assert_eq!(
            calls.chain_cancels.lock().unwrap().as_slice(),
            &[agreement.id.into_bytes()]
        );
        assert_eq!(calls.abandoned.lock().unwrap().as_slice(), &[agr_id]);
        assert_eq!(calls.reassessments.lock().unwrap().as_slice(), &[req_id]);
    }

    #[tokio::test]
    async fn cancel_and_reassess_records_cancel_audit() {
        // The stale-agreement cancel no longer emits `terminated` directly: it
        // records the cancel audit and the chain_listener sweep announces it.
        let dep: DeploymentId = "QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9"
            .parse()
            .unwrap();
        let indexer_id = IndexerId::from(Address::ZERO);
        let url: Url = "http://indexer.example.com/".parse().unwrap();
        let agreement = make_agreement(
            indexer_id,
            url,
            dep,
            Some(100),
            Some(OffsetDateTime::now_utc()),
        );
        let agr_id = agreement.id;

        let calls = MockCalls::default();
        let registry = MockRegistry::new(calls.clone(), agreement.clone());
        let queue = MockWorkerQueue {
            calls: calls.clone(),
        };
        let chain = MockChainClient::success(calls.clone());

        cancel_and_reassess(
            &agreement,
            &registry,
            &queue,
            &chain,
            &test_agreement_conf(),
            DB_TIMEOUT,
            QUEUE_TIMEOUT,
        )
        .await;

        assert_eq!(
            calls.cancel_audits.lock().unwrap().as_slice(),
            &[agr_id],
            "exactly one cancel audit recorded for the stale agreement"
        );
    }

    #[tokio::test]
    async fn test_cancel_and_reassess_config_error_proceeds() {
        // Arrange: chain client disabled (ConfigError) → still mark abandoned and reassess
        let dep: DeploymentId = "QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9"
            .parse()
            .unwrap();
        let indexer_id = IndexerId::from(Address::ZERO);
        let url: Url = "http://indexer.example.com/".parse().unwrap();
        let agreement = make_agreement(
            indexer_id,
            url,
            dep,
            Some(100),
            Some(OffsetDateTime::now_utc()),
        );
        let req_id = agreement.indexing_request_id;
        let agr_id = agreement.id;

        let calls = MockCalls::default();
        let registry = MockRegistry::new(calls.clone(), agreement.clone());
        let queue = MockWorkerQueue {
            calls: calls.clone(),
        };
        let chain = MockChainClient::config_error(calls.clone());

        // Act
        cancel_and_reassess(
            &agreement,
            &registry,
            &queue,
            &chain,
            &test_agreement_conf(),
            DB_TIMEOUT,
            QUEUE_TIMEOUT,
        )
        .await;

        // Assert: no on-chain cancel (ConfigError is treated as disabled, not a real error)
        // but DB mark and reassessment still happen
        assert_eq!(
            calls.chain_cancels.lock().unwrap().as_slice(),
            &[agreement.id.into_bytes()]
        );
        assert_eq!(calls.abandoned.lock().unwrap().as_slice(), &[agr_id]);
        assert_eq!(calls.reassessments.lock().unwrap().as_slice(), &[req_id]);
    }

    #[tokio::test]
    async fn test_cancel_and_reassess_chain_error_skips() {
        // Arrange: transient RPC error → do nothing (retry next cycle)
        let dep: DeploymentId = "QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9"
            .parse()
            .unwrap();
        let indexer_id = IndexerId::from(Address::ZERO);
        let url: Url = "http://indexer.example.com/".parse().unwrap();
        let agreement = make_agreement(
            indexer_id,
            url,
            dep,
            Some(100),
            Some(OffsetDateTime::now_utc()),
        );

        let calls = MockCalls::default();
        let registry = MockRegistry::with_chain_error(calls.clone(), agreement.clone());
        let queue = MockWorkerQueue {
            calls: calls.clone(),
        };
        let chain = MockChainClient::rpc_error(calls.clone());

        // Act
        cancel_and_reassess(
            &agreement,
            &registry,
            &queue,
            &chain,
            &test_agreement_conf(),
            DB_TIMEOUT,
            QUEUE_TIMEOUT,
        )
        .await;

        // Assert: chain cancel attempted, but DB and queue untouched
        assert_eq!(
            calls.chain_cancels.lock().unwrap().as_slice(),
            &[agreement.id.into_bytes()]
        );
        assert!(calls.abandoned.lock().unwrap().is_empty());
        assert!(calls.reassessments.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_record_progress_calls_registry() {
        // Arrange
        let dep: DeploymentId = "QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9"
            .parse()
            .unwrap();
        let indexer_id = IndexerId::from(Address::ZERO);
        let url: Url = "http://indexer.example.com/".parse().unwrap();
        let agreement = make_agreement(indexer_id, url, dep, None, None);
        let agr_id = agreement.id;

        let calls = MockCalls::default();
        let registry = MockRegistry::new(calls.clone(), agreement.clone());

        let now = OffsetDateTime::now_utc();

        // Act
        record_progress(&registry, &agreement, 12345, now, DB_TIMEOUT).await;

        // Assert
        let updates = calls.progress_updates.lock().unwrap();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0], (agr_id, 12345));
    }
}
