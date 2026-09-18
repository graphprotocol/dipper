//! Test-only stub for [`AgreementRegistry`] so each test mock overrides only
//! the methods its test exercises instead of hand-writing the whole trait.

use std::collections::HashMap;

use async_trait::async_trait;
use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
use thegraph_core::{
    DeploymentId, IndexerId,
    alloy::primitives::{Address, ChainId},
};

use super::{
    agreement::{
        AgreementFeeRate, AgreementRegistry, CancelKind, IndexingAgreement, NewAgreementParams,
        PendingAcceptedEvent, PendingExpiredEvent, PendingTerminatedEvent, ReconciliationItem,
        ReconciliationOutcome,
    },
    result::Result,
};

/// Panic-by-default twin of [`AgreementRegistry`]. The blanket impl below turns
/// any implementor into a full registry, so a mock overrides just what it needs;
/// an unexpected call panics with the method name.
#[async_trait]
pub trait StubAgreementRegistry: Send + Sync {
    async fn get_indexing_agreement_by_id(
        &self,
        _id: &IndexingAgreementId,
    ) -> Result<Option<IndexingAgreement>> {
        unimplemented!("get_indexing_agreement_by_id")
    }

    // Mirrors the trait's default (per-id loop) so overriding the single-id
    // getter is enough for batched callers too.
    async fn get_indexing_agreements_by_ids(
        &self,
        ids: &[IndexingAgreementId],
    ) -> Result<HashMap<IndexingAgreementId, IndexingAgreement>> {
        let mut out = HashMap::with_capacity(ids.len());
        for id in ids {
            if let Some(agreement) = self.get_indexing_agreement_by_id(id).await? {
                out.insert(*id, agreement);
            }
        }
        Ok(out)
    }

    async fn get_indexing_agreements_by_deployment_id(
        &self,
        _deployment_id: &DeploymentId,
    ) -> Result<Vec<IndexingAgreement>> {
        unimplemented!("get_indexing_agreements_by_deployment_id")
    }

    async fn get_indexing_agreements_by_indexer_id(
        &self,
        _indexer_id: &IndexerId,
    ) -> Result<Vec<IndexingAgreement>> {
        unimplemented!("get_indexing_agreements_by_indexer_id")
    }

    async fn get_pending_agreement_indexers_by_deployment(
        &self,
        _indexer_ids: &[IndexerId],
    ) -> Result<HashMap<DeploymentId, Vec<IndexerId>>> {
        unimplemented!("get_pending_agreement_indexers_by_deployment")
    }

    async fn get_declined_indexers_by_deployment(
        &self,
        _default_lookback_days: i32,
        _price_lookback_days: i32,
        _transient_lookback_minutes: i32,
        _uncertain_lookback_days: i32,
    ) -> Result<HashMap<DeploymentId, Vec<IndexerId>>> {
        unimplemented!("get_declined_indexers_by_deployment")
    }

    async fn get_unresponsive_indexers(
        &self,
        _lookback_days: i32,
        _chain_id: ChainId,
    ) -> Result<Vec<IndexerId>> {
        unimplemented!("get_unresponsive_indexers")
    }

    async fn get_indexing_agreements_by_indexing_request_id(
        &self,
        _request_id: &IndexingRequestId,
    ) -> Result<Vec<IndexingAgreement>> {
        unimplemented!("get_indexing_agreements_by_indexing_request_id")
    }

    async fn get_active_indexing_agreements_by_indexing_request_id(
        &self,
        _request_id: &IndexingRequestId,
    ) -> Result<Vec<IndexingAgreement>> {
        unimplemented!("get_active_indexing_agreements_by_indexing_request_id")
    }

    async fn register_new_indexing_agreement(
        &self,
        _params: NewAgreementParams,
    ) -> Result<IndexingAgreementId> {
        unimplemented!("register_new_indexing_agreement")
    }

    async fn register_agreement_with_pending_cancellation(
        &self,
        _params: NewAgreementParams,
        _old_agreement_id: IndexingAgreementId,
    ) -> Result<IndexingAgreementId> {
        unimplemented!("register_agreement_with_pending_cancellation")
    }

    async fn mark_indexing_agreement_as_unresponsive(
        &self,
        _id: &IndexingAgreementId,
    ) -> Result<()> {
        unimplemented!("mark_indexing_agreement_as_unresponsive")
    }

    async fn update_offer_tx_hash(
        &self,
        _id: &IndexingAgreementId,
        _tx_hash: &[u8; 32],
    ) -> Result<()> {
        unimplemented!("update_offer_tx_hash")
    }

    async fn update_terms_version_hash(
        &self,
        _id: &IndexingAgreementId,
        _hash: &[u8; 32],
    ) -> Result<()> {
        unimplemented!("update_terms_version_hash")
    }

    async fn mark_indexing_agreement_as_canceled_by_requester(
        &self,
        _id: &IndexingAgreementId,
    ) -> Result<()> {
        unimplemented!("mark_indexing_agreement_as_canceled_by_requester")
    }

    async fn apply_reconciliation(
        &self,
        _id: &IndexingAgreementId,
        _apply_accept: bool,
        _cancel: Option<CancelKind>,
    ) -> Result<ReconciliationOutcome> {
        unimplemented!("apply_reconciliation")
    }

    // Mirrors the trait's default (per-item loop over `apply_reconciliation`).
    async fn apply_reconciliation_batch(
        &self,
        items: &[ReconciliationItem],
    ) -> Result<HashMap<IndexingAgreementId, ReconciliationOutcome>> {
        let mut outcomes = HashMap::with_capacity(items.len());
        for item in items {
            let outcome = self
                .apply_reconciliation(&item.agreement_id, item.apply_accept, item.cancel)
                .await?;
            outcomes.insert(item.agreement_id, outcome);
        }
        Ok(outcomes)
    }

    async fn get_expired_created_agreements(
        &self,
        _batch_size: i64,
        _chain_timestamp: u64,
    ) -> Result<Vec<IndexingAgreement>> {
        unimplemented!("get_expired_created_agreements")
    }

    async fn mark_indexing_agreement_as_expired(&self, _id: &IndexingAgreementId) -> Result<()> {
        unimplemented!("mark_indexing_agreement_as_expired")
    }

    async fn mark_indexing_agreement_as_rejected(
        &self,
        _id: &IndexingAgreementId,
        _rejection_reason: Option<&str>,
    ) -> Result<()> {
        unimplemented!("mark_indexing_agreement_as_rejected")
    }

    async fn get_accepted_on_chain_agreements(
        &self,
        _batch_size: i64,
    ) -> Result<Vec<IndexingAgreement>> {
        unimplemented!("get_accepted_on_chain_agreements")
    }

    async fn get_agreements_pending_chain_cancel(
        &self,
        _batch_size: i64,
    ) -> Result<Vec<IndexingAgreement>> {
        unimplemented!("get_agreements_pending_chain_cancel")
    }

    // Mirrors the trait's default (empty list).
    async fn get_providers_for_escrow_reconciliation(&self, _limit: i64) -> Result<Vec<Address>> {
        Ok(Vec::new())
    }

    async fn update_agreement_sync_progress(
        &self,
        _id: &IndexingAgreementId,
        _block_height: u64,
        _progress_at: time::OffsetDateTime,
    ) -> Result<()> {
        unimplemented!("update_agreement_sync_progress")
    }

    async fn count_active_agreements_by_deployment(&self) -> Result<HashMap<DeploymentId, usize>> {
        unimplemented!("count_active_agreements_by_deployment")
    }

    async fn count_created_agreements_by_indexer(&self) -> Result<(HashMap<IndexerId, u64>, u64)> {
        unimplemented!("count_created_agreements_by_indexer")
    }

    // Mirrors the trait's default (non-empty active-agreement counts).
    async fn exists_active_agreements(&self) -> Result<bool> {
        self.count_active_agreements_by_deployment()
            .await
            .map(|m| !m.is_empty())
    }

    async fn mark_indexing_agreement_as_abandoned(
        &self,
        _id: &IndexingAgreementId,
    ) -> Result<IndexingAgreement> {
        unimplemented!("mark_indexing_agreement_as_abandoned")
    }

    async fn get_agreement_fee_rates(&self) -> Result<Vec<AgreementFeeRate>> {
        unimplemented!("get_agreement_fee_rates")
    }

    async fn count_accepted_agreements_by_deployment(
        &self,
        _deployment_id: &DeploymentId,
    ) -> Result<i64> {
        unimplemented!("count_accepted_agreements_by_deployment")
    }

    // The emission-sweep and audit methods mirror the real trait's defaults
    // (empty batch / no-op) so only emission-focused mocks override them.
    async fn get_agreements_pending_terminated_emission(
        &self,
        _limit: i64,
    ) -> Result<Vec<PendingTerminatedEvent>> {
        Ok(Vec::new())
    }

    async fn mark_terminated_event_emitted(
        &self,
        _agreement_id: &IndexingAgreementId,
    ) -> Result<()> {
        Ok(())
    }

    async fn get_agreements_pending_accepted_emission(
        &self,
        _limit: i64,
    ) -> Result<Vec<PendingAcceptedEvent>> {
        Ok(Vec::new())
    }

    async fn mark_accepted_event_emitted(&self, _agreement_id: &IndexingAgreementId) -> Result<()> {
        Ok(())
    }

    async fn get_agreements_pending_expired_emission(
        &self,
        _limit: i64,
    ) -> Result<Vec<PendingExpiredEvent>> {
        Ok(Vec::new())
    }

    async fn mark_expired_event_emitted(&self, _agreement_id: &IndexingAgreementId) -> Result<()> {
        Ok(())
    }

    async fn record_accepted_audit(
        &self,
        _agreement_id: &IndexingAgreementId,
        _accepted_at: u64,
        _accepted_tx: &str,
    ) -> Result<()> {
        Ok(())
    }

    async fn record_cancel_audit(
        &self,
        _agreement_id: &IndexingAgreementId,
        _canceled_at: u64,
        _canceled_by: &str,
        _canceled_tx: Option<&str>,
    ) -> Result<()> {
        Ok(())
    }
}

// Every stub is a full AgreementRegistry: each method delegates to the stub
// trait, whose defaults panic unless the mock overrides them.
#[async_trait]
impl<T: StubAgreementRegistry> AgreementRegistry for T {
    async fn get_indexing_agreement_by_id(
        &self,
        id: &IndexingAgreementId,
    ) -> Result<Option<IndexingAgreement>> {
        StubAgreementRegistry::get_indexing_agreement_by_id(self, id).await
    }

    async fn get_indexing_agreements_by_ids(
        &self,
        ids: &[IndexingAgreementId],
    ) -> Result<HashMap<IndexingAgreementId, IndexingAgreement>> {
        StubAgreementRegistry::get_indexing_agreements_by_ids(self, ids).await
    }

    async fn get_indexing_agreements_by_deployment_id(
        &self,
        deployment_id: &DeploymentId,
    ) -> Result<Vec<IndexingAgreement>> {
        StubAgreementRegistry::get_indexing_agreements_by_deployment_id(self, deployment_id).await
    }

    async fn get_indexing_agreements_by_indexer_id(
        &self,
        indexer_id: &IndexerId,
    ) -> Result<Vec<IndexingAgreement>> {
        StubAgreementRegistry::get_indexing_agreements_by_indexer_id(self, indexer_id).await
    }

    async fn get_pending_agreement_indexers_by_deployment(
        &self,
        indexer_ids: &[IndexerId],
    ) -> Result<HashMap<DeploymentId, Vec<IndexerId>>> {
        StubAgreementRegistry::get_pending_agreement_indexers_by_deployment(self, indexer_ids).await
    }

    async fn get_declined_indexers_by_deployment(
        &self,
        default_lookback_days: i32,
        price_lookback_days: i32,
        transient_lookback_minutes: i32,
        uncertain_lookback_days: i32,
    ) -> Result<HashMap<DeploymentId, Vec<IndexerId>>> {
        StubAgreementRegistry::get_declined_indexers_by_deployment(
            self,
            default_lookback_days,
            price_lookback_days,
            transient_lookback_minutes,
            uncertain_lookback_days,
        )
        .await
    }

    async fn get_unresponsive_indexers(
        &self,
        lookback_days: i32,
        chain_id: ChainId,
    ) -> Result<Vec<IndexerId>> {
        StubAgreementRegistry::get_unresponsive_indexers(self, lookback_days, chain_id).await
    }

    async fn get_indexing_agreements_by_indexing_request_id(
        &self,
        request_id: &IndexingRequestId,
    ) -> Result<Vec<IndexingAgreement>> {
        StubAgreementRegistry::get_indexing_agreements_by_indexing_request_id(self, request_id)
            .await
    }

    async fn get_active_indexing_agreements_by_indexing_request_id(
        &self,
        request_id: &IndexingRequestId,
    ) -> Result<Vec<IndexingAgreement>> {
        StubAgreementRegistry::get_active_indexing_agreements_by_indexing_request_id(
            self, request_id,
        )
        .await
    }

    async fn register_new_indexing_agreement(
        &self,
        params: NewAgreementParams,
    ) -> Result<IndexingAgreementId> {
        StubAgreementRegistry::register_new_indexing_agreement(self, params).await
    }

    async fn register_agreement_with_pending_cancellation(
        &self,
        params: NewAgreementParams,
        old_agreement_id: IndexingAgreementId,
    ) -> Result<IndexingAgreementId> {
        StubAgreementRegistry::register_agreement_with_pending_cancellation(
            self,
            params,
            old_agreement_id,
        )
        .await
    }

    async fn mark_indexing_agreement_as_unresponsive(
        &self,
        id: &IndexingAgreementId,
    ) -> Result<()> {
        StubAgreementRegistry::mark_indexing_agreement_as_unresponsive(self, id).await
    }

    async fn update_offer_tx_hash(
        &self,
        id: &IndexingAgreementId,
        tx_hash: &[u8; 32],
    ) -> Result<()> {
        StubAgreementRegistry::update_offer_tx_hash(self, id, tx_hash).await
    }

    async fn update_terms_version_hash(
        &self,
        id: &IndexingAgreementId,
        hash: &[u8; 32],
    ) -> Result<()> {
        StubAgreementRegistry::update_terms_version_hash(self, id, hash).await
    }

    async fn mark_indexing_agreement_as_canceled_by_requester(
        &self,
        id: &IndexingAgreementId,
    ) -> Result<()> {
        StubAgreementRegistry::mark_indexing_agreement_as_canceled_by_requester(self, id).await
    }

    async fn apply_reconciliation(
        &self,
        id: &IndexingAgreementId,
        apply_accept: bool,
        cancel: Option<CancelKind>,
    ) -> Result<ReconciliationOutcome> {
        StubAgreementRegistry::apply_reconciliation(self, id, apply_accept, cancel).await
    }

    async fn apply_reconciliation_batch(
        &self,
        items: &[ReconciliationItem],
    ) -> Result<HashMap<IndexingAgreementId, ReconciliationOutcome>> {
        StubAgreementRegistry::apply_reconciliation_batch(self, items).await
    }

    async fn get_expired_created_agreements(
        &self,
        batch_size: i64,
        chain_timestamp: u64,
    ) -> Result<Vec<IndexingAgreement>> {
        StubAgreementRegistry::get_expired_created_agreements(self, batch_size, chain_timestamp)
            .await
    }

    async fn mark_indexing_agreement_as_expired(&self, id: &IndexingAgreementId) -> Result<()> {
        StubAgreementRegistry::mark_indexing_agreement_as_expired(self, id).await
    }

    async fn mark_indexing_agreement_as_rejected(
        &self,
        id: &IndexingAgreementId,
        rejection_reason: Option<&str>,
    ) -> Result<()> {
        StubAgreementRegistry::mark_indexing_agreement_as_rejected(self, id, rejection_reason).await
    }

    async fn get_accepted_on_chain_agreements(
        &self,
        batch_size: i64,
    ) -> Result<Vec<IndexingAgreement>> {
        StubAgreementRegistry::get_accepted_on_chain_agreements(self, batch_size).await
    }

    async fn get_agreements_pending_chain_cancel(
        &self,
        batch_size: i64,
    ) -> Result<Vec<IndexingAgreement>> {
        StubAgreementRegistry::get_agreements_pending_chain_cancel(self, batch_size).await
    }

    async fn get_providers_for_escrow_reconciliation(&self, limit: i64) -> Result<Vec<Address>> {
        StubAgreementRegistry::get_providers_for_escrow_reconciliation(self, limit).await
    }

    async fn update_agreement_sync_progress(
        &self,
        id: &IndexingAgreementId,
        block_height: u64,
        progress_at: time::OffsetDateTime,
    ) -> Result<()> {
        StubAgreementRegistry::update_agreement_sync_progress(self, id, block_height, progress_at)
            .await
    }

    async fn count_active_agreements_by_deployment(&self) -> Result<HashMap<DeploymentId, usize>> {
        StubAgreementRegistry::count_active_agreements_by_deployment(self).await
    }

    async fn count_created_agreements_by_indexer(&self) -> Result<(HashMap<IndexerId, u64>, u64)> {
        StubAgreementRegistry::count_created_agreements_by_indexer(self).await
    }

    async fn exists_active_agreements(&self) -> Result<bool> {
        StubAgreementRegistry::exists_active_agreements(self).await
    }

    async fn mark_indexing_agreement_as_abandoned(
        &self,
        id: &IndexingAgreementId,
    ) -> Result<IndexingAgreement> {
        StubAgreementRegistry::mark_indexing_agreement_as_abandoned(self, id).await
    }

    async fn get_agreement_fee_rates(&self) -> Result<Vec<AgreementFeeRate>> {
        StubAgreementRegistry::get_agreement_fee_rates(self).await
    }

    async fn count_accepted_agreements_by_deployment(
        &self,
        deployment_id: &DeploymentId,
    ) -> Result<i64> {
        StubAgreementRegistry::count_accepted_agreements_by_deployment(self, deployment_id).await
    }

    async fn get_agreements_pending_terminated_emission(
        &self,
        limit: i64,
    ) -> Result<Vec<PendingTerminatedEvent>> {
        StubAgreementRegistry::get_agreements_pending_terminated_emission(self, limit).await
    }

    async fn mark_terminated_event_emitted(
        &self,
        agreement_id: &IndexingAgreementId,
    ) -> Result<()> {
        StubAgreementRegistry::mark_terminated_event_emitted(self, agreement_id).await
    }

    async fn get_agreements_pending_accepted_emission(
        &self,
        limit: i64,
    ) -> Result<Vec<PendingAcceptedEvent>> {
        StubAgreementRegistry::get_agreements_pending_accepted_emission(self, limit).await
    }

    async fn mark_accepted_event_emitted(&self, agreement_id: &IndexingAgreementId) -> Result<()> {
        StubAgreementRegistry::mark_accepted_event_emitted(self, agreement_id).await
    }

    async fn get_agreements_pending_expired_emission(
        &self,
        limit: i64,
    ) -> Result<Vec<PendingExpiredEvent>> {
        StubAgreementRegistry::get_agreements_pending_expired_emission(self, limit).await
    }

    async fn mark_expired_event_emitted(&self, agreement_id: &IndexingAgreementId) -> Result<()> {
        StubAgreementRegistry::mark_expired_event_emitted(self, agreement_id).await
    }

    async fn record_accepted_audit(
        &self,
        agreement_id: &IndexingAgreementId,
        accepted_at: u64,
        accepted_tx: &str,
    ) -> Result<()> {
        StubAgreementRegistry::record_accepted_audit(self, agreement_id, accepted_at, accepted_tx)
            .await
    }

    async fn record_cancel_audit(
        &self,
        agreement_id: &IndexingAgreementId,
        canceled_at: u64,
        canceled_by: &str,
        canceled_tx: Option<&str>,
    ) -> Result<()> {
        StubAgreementRegistry::record_cancel_audit(
            self,
            agreement_id,
            canceled_at,
            canceled_by,
            canceled_tx,
        )
        .await
    }
}
