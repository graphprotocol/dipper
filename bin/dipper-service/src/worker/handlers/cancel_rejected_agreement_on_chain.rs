//! Cancel a rejected agreement that was accepted on-chain
//!
//! When an indexer rejects an agreement off-chain but later accepts it on-chain,
//! the chain listener detects this and queues this job to cancel the agreement
//! via the RecurringAgreementManager.

use std::{sync::Arc, time::Duration};

use dipper_core::ids::IndexingAgreementId;

use crate::{
    cancel_dispatch::cancel_agreement_on_chain,
    chain_client::{ChainClient, ChainClientError},
    config::IndexingAgreementConfig,
    registry::{AgreementRegistry, IndexingAgreementStatus},
    worker::result::{JobError, JobResult},
};

pub struct Ctx<R, T> {
    pub registry: R,
    pub chain_client: T,
    pub agreement_conf: Arc<IndexingAgreementConfig>,
}

/// Cancel a rejected agreement on-chain.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Message {
    pub agreement_id: IndexingAgreementId,
}

/// Cancel a rejected agreement on-chain.
///
/// This is called when an indexer rejected the proposal off-chain but then accepted
/// on-chain anyway. We cancel the agreement via `cancelIndexingAgreementByPayer` to
/// ensure the indexer doesn't receive payment for work we didn't want.
pub async fn handle<R, T>(ctx: Ctx<R, T>, Message { agreement_id }: &Message) -> JobResult<()>
where
    R: AgreementRegistry + Sync,
    T: ChainClient,
{
    // Look up the agreement
    let agreement = ctx
        .registry
        .get_indexing_agreement_by_id(agreement_id)
        .await
        .map_err(|err| JobError::Fatal(err.into()))?;

    let agreement = match agreement {
        Some(a) => a,
        None => {
            tracing::error!(
                agreement_id = %agreement_id,
                "Agreement not found for on-chain cancellation"
            );
            return Ok(());
        }
    };

    // Verify the agreement is in Rejected status (off-chain rejection that got accepted on-chain)
    // The chain listener should only queue this job for Rejected agreements
    if agreement.status != IndexingAgreementStatus::Rejected {
        tracing::warn!(
            agreement_id = %agreement_id,
            status = %agreement.status,
            "Agreement not in Rejected status, skipping on-chain cancellation"
        );
        return Ok(());
    }

    tracing::info!(
        agreement_id = %agreement_id,
        indexer_id = %agreement.indexer.id,
        "Canceling rejected agreement on-chain"
    );

    // Send the cancellation transaction (mode-aware dispatch).
    let on_chain_cancel_tx: Option<String> = match cancel_agreement_on_chain(
        &ctx.chain_client,
        &ctx.registry,
        &agreement,
        &ctx.agreement_conf,
    )
    .await
    {
        Ok(Some(tx_hash)) => {
            tracing::info!(
                agreement_id = %agreement_id,
                tx_hash = %tx_hash,
                "Successfully submitted on-chain cancellation"
            );
            Some(tx_hash.to_string())
        }
        Ok(None) => {
            tracing::info!(
                agreement_id = %agreement_id,
                "Rejected agreement already canceled on-chain; reconciling local state"
            );
            None
        }
        Err(err @ ChainClientError::MissingTermsVersionHash { .. }) => {
            // Permanent: the hash never appears, so retrying can't help. Fail
            // terminally and leave the live agreement for operator action.
            tracing::error!(
                agreement_id = %agreement_id,
                error = %err,
                "Cannot cancel rejected agreement: missing terms_version_hash"
            );
            return Err(JobError::Fatal(err.into()));
        }
        Err(err) => {
            tracing::warn!(
                agreement_id = %agreement_id,
                error = %err,
                "Failed to cancel agreement on-chain, will retry"
            );
            // Retry with backoff - on-chain transactions can fail due to gas issues, nonce, etc.
            return Err(JobError::Retryable(err.into(), Duration::from_secs(30)));
        }
    };

    // When the row was actually flipped to terminal, record the cancel audit so
    // the chain_listener's `terminated` sweep announces it durably. The accept
    // was recorded when the rejected-then-accepted anomaly was first detected, so
    // the row is sweep-eligible. If the mark failed, the row stays `Rejected` and
    // the chain_listener observes the on-chain cancel and flips it itself, then
    // the same sweep emits -- so nothing is lost either way.
    if mark_cancellation_complete(&ctx.registry, agreement_id).await {
        let manager = ctx.agreement_conf.recurring_agreement_manager().to_string();
        if let Err(err) = ctx
            .registry
            .record_cancel_audit(
                agreement_id,
                dipper_core::time::now_secs(),
                &manager,
                on_chain_cancel_tx.as_deref(),
            )
            .await
        {
            tracing::warn!(
                agreement_id = %agreement_id,
                error = %err,
                "failed to record cancel audit; terminated event may emit with fallback fields"
            );
        }
    }

    Ok(())
}

/// Flip the local row to CanceledByRequester after either a fresh on-chain
/// cancel or the discovery that the agreement was already canceled on-chain.
/// Failures here are logged but not fatal — the on-chain side is already in
/// the right state, so the next reconciliation pass can re-attempt the DB
/// update without risking a duplicate transaction.
///
/// Returns `true` when the row was marked terminal. The caller emits the
/// `terminated` event only on `true`: if the mark failed the row stays
/// `Rejected` (non-terminal), so the chain_listener will observe the on-chain
/// cancel and emit `terminated` itself — emitting here too would duplicate.
async fn mark_cancellation_complete<R>(registry: &R, agreement_id: &IndexingAgreementId) -> bool
where
    R: AgreementRegistry + Sync,
{
    match registry
        .mark_indexing_agreement_as_canceled_by_requester(agreement_id)
        .await
    {
        Ok(()) => {
            tracing::info!(
                agreement_id = %agreement_id,
                old_status = "REJECTED",
                new_status = "CANCELED_BY_REQUESTER",
                reason = "canceled_on_chain_after_rejection",
                "agreement state transition"
            );
            true
        }
        Err(err) => {
            tracing::error!(
                agreement_id = %agreement_id,
                error = %err,
                "Failed to update agreement status after on-chain cancellation"
            );
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use dipper_core::ids::IndexingRequestId;
    use dipper_rpc::indexer::indexer_client::sol::RecurringCollectionAgreement;
    use thegraph_core::{
        DeploymentId, IndexerId,
        alloy::primitives::{Address, B256, U256},
    };
    use time::OffsetDateTime;
    use url::Url;

    use super::*;
    use crate::{
        chain_client::{ChainClient, ChainClientError},
        registry::{
            IndexingAgreement, IndexingAgreementStatus, IndexingAgreementTerms,
            IndexingAgreementTermsMetadata,
        },
    };

    // =========================================================================
    // Mock implementations
    // =========================================================================

    /// Registry that returns a single configurable agreement and records the
    /// terminal-cancel transition + cancel-audit calls the handler drives.
    /// `Clone` shares the tracked state (Arc), so a test can clone one into the
    /// `Ctx` and still assert on the original after `handle` consumes the ctx.
    #[derive(Clone)]
    struct MockRegistry {
        agreement: Arc<Mutex<Option<IndexingAgreement>>>,
        marked_canceled: Arc<Mutex<Vec<IndexingAgreementId>>>,
        /// Ids passed to `record_cancel_audit` -- the signal the handler drives
        /// the terminated event (the chain_listener sweep emits from this audit).
        recorded_cancel_audit: Arc<Mutex<Vec<IndexingAgreementId>>>,
        /// When true, `mark_indexing_agreement_as_canceled_by_requester` errors.
        fail_mark: bool,
    }

    impl MockRegistry {
        fn new(agreement: IndexingAgreement) -> Self {
            Self {
                agreement: Arc::new(Mutex::new(Some(agreement))),
                marked_canceled: Arc::new(Mutex::new(Vec::new())),
                recorded_cancel_audit: Arc::new(Mutex::new(Vec::new())),
                fail_mark: false,
            }
        }

        fn with_mark_failure(agreement: IndexingAgreement) -> Self {
            Self {
                fail_mark: true,
                ..Self::new(agreement)
            }
        }
    }

    #[async_trait]
    impl AgreementRegistry for MockRegistry {
        async fn get_indexing_agreement_by_id(
            &self,
            _id: &IndexingAgreementId,
        ) -> crate::registry::Result<Option<IndexingAgreement>> {
            Ok(self.agreement.lock().unwrap().clone())
        }

        async fn get_indexing_agreements_by_deployment_id(
            &self,
            _deployment_id: &DeploymentId,
        ) -> crate::registry::Result<Vec<IndexingAgreement>> {
            Ok(vec![])
        }

        async fn get_indexing_agreements_by_indexer_id(
            &self,
            _indexer_id: &IndexerId,
        ) -> crate::registry::Result<Vec<IndexingAgreement>> {
            Ok(vec![])
        }

        async fn get_pending_agreement_indexers_by_deployment(
            &self,
            _indexer_ids: &[IndexerId],
        ) -> crate::registry::Result<std::collections::HashMap<DeploymentId, Vec<IndexerId>>>
        {
            Ok(std::collections::HashMap::new())
        }

        async fn get_declined_indexers_by_deployment(
            &self,
            _default_lookback_days: i32,
            _price_lookback_days: i32,
            _transient_lookback_minutes: i32,
            _uncertain_lookback_days: i32,
        ) -> crate::registry::Result<std::collections::HashMap<DeploymentId, Vec<IndexerId>>>
        {
            Ok(std::collections::HashMap::new())
        }

        async fn get_indexing_agreements_by_indexing_request_id(
            &self,
            _request_id: &IndexingRequestId,
        ) -> crate::registry::Result<Vec<IndexingAgreement>> {
            Ok(vec![])
        }

        async fn get_active_indexing_agreements_by_indexing_request_id(
            &self,
            _request_id: &IndexingRequestId,
        ) -> crate::registry::Result<Vec<IndexingAgreement>> {
            Ok(vec![])
        }

        async fn count_accepted_agreements_by_deployment(
            &self,
            _deployment_id: &DeploymentId,
        ) -> crate::registry::Result<i64> {
            Ok(0)
        }

        async fn record_cancel_audit(
            &self,
            agreement_id: &IndexingAgreementId,
            _canceled_at: u64,
            _canceled_by: &str,
            _canceled_tx: Option<&str>,
        ) -> crate::registry::Result<()> {
            self.recorded_cancel_audit
                .lock()
                .unwrap()
                .push(*agreement_id);
            Ok(())
        }

        async fn register_new_indexing_agreement(
            &self,
            _params: crate::registry::NewAgreementParams,
        ) -> crate::registry::Result<IndexingAgreementId> {
            Ok(IndexingAgreementId::from_bytes(rand::random()))
        }

        async fn register_agreement_with_pending_cancellation(
            &self,
            _params: crate::registry::NewAgreementParams,
            _old_agreement_id: IndexingAgreementId,
        ) -> crate::registry::Result<IndexingAgreementId> {
            Ok(IndexingAgreementId::from_bytes(rand::random()))
        }

        async fn get_unresponsive_indexers(
            &self,
            _lookback_days: i32,
            _chain_id: thegraph_core::alloy::primitives::ChainId,
        ) -> crate::registry::Result<Vec<IndexerId>> {
            unimplemented!()
        }

        async fn mark_indexing_agreement_as_unresponsive(
            &self,
            _id: &IndexingAgreementId,
        ) -> crate::registry::Result<()> {
            unimplemented!()
        }

        async fn count_created_agreements_by_indexer(
            &self,
        ) -> crate::registry::Result<(std::collections::HashMap<IndexerId, u64>, u64)> {
            unimplemented!()
        }

        async fn update_offer_tx_hash(
            &self,
            _id: &IndexingAgreementId,
            _tx_hash: &[u8; 32],
        ) -> crate::registry::Result<()> {
            Ok(())
        }

        async fn update_terms_version_hash(
            &self,
            _id: &IndexingAgreementId,
            _hash: &[u8; 32],
        ) -> crate::registry::Result<()> {
            Ok(())
        }

        async fn mark_indexing_agreement_as_canceled_by_requester(
            &self,
            id: &IndexingAgreementId,
        ) -> crate::registry::Result<()> {
            if self.fail_mark {
                return Err(crate::registry::Error::NoRecordsUpdated);
            }
            self.marked_canceled.lock().unwrap().push(*id);
            Ok(())
        }

        async fn apply_reconciliation(
            &self,
            _id: &IndexingAgreementId,
            _apply_accept: bool,
            _cancel: Option<crate::registry::CancelKind>,
        ) -> crate::registry::Result<crate::registry::ReconciliationOutcome> {
            Ok(crate::registry::ReconciliationOutcome {
                did_accept: false,
                did_cancel: false,
            })
        }

        async fn get_expired_created_agreements(
            &self,
            _batch_size: i64,
            _chain_timestamp: u64,
        ) -> crate::registry::Result<Vec<IndexingAgreement>> {
            Ok(vec![])
        }

        async fn mark_indexing_agreement_as_expired(
            &self,
            _id: &IndexingAgreementId,
        ) -> crate::registry::Result<()> {
            Ok(())
        }

        async fn mark_indexing_agreement_as_rejected(
            &self,
            _id: &IndexingAgreementId,
            _rejection_reason: Option<&str>,
        ) -> crate::registry::Result<()> {
            Ok(())
        }

        async fn get_accepted_on_chain_agreements(
            &self,
            _batch_size: i64,
        ) -> crate::registry::Result<Vec<IndexingAgreement>> {
            Ok(vec![])
        }

        async fn get_agreements_pending_chain_cancel(
            &self,
            _batch_size: i64,
        ) -> crate::registry::Result<Vec<IndexingAgreement>> {
            Ok(vec![])
        }

        async fn update_agreement_sync_progress(
            &self,
            _id: &IndexingAgreementId,
            _block_height: u64,
            _progress_at: time::OffsetDateTime,
        ) -> crate::registry::Result<()> {
            Ok(())
        }

        async fn count_active_agreements_by_deployment(
            &self,
        ) -> crate::registry::Result<std::collections::HashMap<DeploymentId, usize>> {
            Ok(std::collections::HashMap::new())
        }

        async fn mark_indexing_agreement_as_abandoned(
            &self,
            _id: &IndexingAgreementId,
        ) -> crate::registry::Result<IndexingAgreement> {
            Err(crate::registry::Error::NoRecordsUpdated)
        }

        async fn get_agreement_fee_rates(
            &self,
        ) -> crate::registry::Result<Vec<crate::registry::AgreementFeeRate>> {
            Ok(vec![])
        }
    }

    /// Chain client whose manager cancel always mines (returns a tx hash) and
    /// whose post-cancel liveness read reports the agreement is no longer active,
    /// so the cancel is confirmed.
    #[derive(Default)]
    struct MockChainClient;

    #[async_trait]
    impl ChainClient for MockChainClient {
        async fn latest_block_timestamp(&self) -> Result<u64, ChainClientError> {
            Err(ChainClientError::RpcError(anyhow::anyhow!(
                "latest_block_timestamp not mocked"
            )))
        }

        async fn offer_via_manager(
            &self,
            _rca: &RecurringCollectionAgreement,
        ) -> Result<Option<B256>, ChainClientError> {
            Ok(None)
        }

        async fn cancel_via_manager(
            &self,
            _collector: Address,
            _agreement_id: &[u8; 16],
            _version_hash: B256,
            _options: u16,
        ) -> Result<Option<B256>, ChainClientError> {
            Ok(Some(B256::ZERO))
        }

        async fn reconcile_provider(
            &self,
            _collector: Address,
            _provider: Address,
        ) -> Result<Option<B256>, ChainClientError> {
            Ok(None)
        }

        async fn agreement_still_active(
            &self,
            _agreement_id: &[u8; 16],
        ) -> Result<bool, ChainClientError> {
            Ok(false)
        }

        async fn fetch_agreement_version_hash(
            &self,
            _agreement_id: &[u8; 16],
        ) -> Result<Option<B256>, ChainClientError> {
            // These tests always construct agreements with a stored hash, so
            // recovery is never exercised.
            unimplemented!("not exercised by cancel_rejected_agreement_on_chain tests")
        }
    }

    fn test_agreement_conf() -> Arc<IndexingAgreementConfig> {
        Arc::new(IndexingAgreementConfig {
            data_service: Address::ZERO,
            recurring_collector: Address::ZERO,
            recurring_agreement_manager: Address::ZERO,
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
        })
    }

    fn test_deployment_id() -> DeploymentId {
        "QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9"
            .parse()
            .unwrap()
    }

    fn make_agreement(status: IndexingAgreementStatus) -> IndexingAgreement {
        IndexingAgreement {
            id: IndexingAgreementId::from_bytes(rand::random()),
            nonce_uuid: uuid::Uuid::now_v7(),
            created_at: OffsetDateTime::now_utc(),
            updated_at: OffsetDateTime::now_utc(),
            status,
            indexing_request_id: IndexingRequestId::new(),
            indexer: crate::registry::Indexer {
                id: IndexerId::from(Address::ZERO),
                url: Url::parse("https://indexer.example").unwrap(),
            },
            terms: IndexingAgreementTerms {
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
                    subgraph_deployment_id: test_deployment_id(),
                    protocol_network: 1u64,
                    chain_id: 1u64,
                    proposed_at: 0,
                },
            },
            last_block_height: None,
            last_progress_at: None,
            rejection_reason: None,
            // 32-byte hash so the on-chain cancel path is exercised.
            terms_version_hash: Some(vec![0u8; 32]),
        }
    }

    // =========================================================================
    // Tests
    // =========================================================================

    fn ctx_for(registry: MockRegistry) -> Ctx<MockRegistry, MockChainClient> {
        Ctx {
            registry,
            chain_client: MockChainClient,
            agreement_conf: test_agreement_conf(),
        }
    }

    #[tokio::test]
    async fn rejected_agreement_records_cancel_audit_once() {
        // The handler no longer emits `terminated` directly: it records the cancel
        // audit, and the chain_listener sweep announces it durably. Assert exactly
        // one audit was recorded for the agreement.
        let agreement = make_agreement(IndexingAgreementStatus::Rejected);
        let agreement_id = agreement.id;
        let registry = MockRegistry::new(agreement);

        let result = handle(ctx_for(registry.clone()), &Message { agreement_id }).await;
        assert!(result.is_ok(), "handle should succeed: {result:?}");

        let recorded = registry.recorded_cancel_audit.lock().unwrap().clone();
        assert_eq!(recorded, vec![agreement_id], "exactly one cancel audit");
    }

    #[tokio::test]
    async fn failed_local_mark_records_no_cancel_audit() {
        // On-chain cancel succeeds but the local DB mark fails, leaving the row
        // non-terminal. The handler must NOT record cancel audit -- the
        // chain_listener will observe the on-chain cancel, flip the row, and the
        // sweep emits from there.
        let agreement = make_agreement(IndexingAgreementStatus::Rejected);
        let agreement_id = agreement.id;
        let registry = MockRegistry::with_mark_failure(agreement);

        let result = handle(ctx_for(registry.clone()), &Message { agreement_id }).await;
        assert!(result.is_ok(), "handle should still return Ok: {result:?}");
        assert!(
            registry.recorded_cancel_audit.lock().unwrap().is_empty(),
            "no cancel audit recorded when the local mark failed"
        );
    }

    #[tokio::test]
    async fn non_rejected_agreement_records_nothing() {
        let agreement = make_agreement(IndexingAgreementStatus::Created);
        let agreement_id = agreement.id;
        let registry = MockRegistry::new(agreement);

        let result = handle(ctx_for(registry.clone()), &Message { agreement_id }).await;
        assert!(result.is_ok(), "handle should return Ok: {result:?}");
        assert!(
            registry.recorded_cancel_audit.lock().unwrap().is_empty(),
            "no cancel audit recorded for a non-Rejected agreement"
        );
    }
}
