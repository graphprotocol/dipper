mod agreement;
#[cfg(test)]
mod agreement_stub;
mod indexer_denylist;
mod indexing_request;
mod pending_cancellation;
mod result;

use async_trait::async_trait;
use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
use dipper_pgregistry::PgRegistry;
use sqlx::{Pool, Postgres};
use thegraph_core::{
    DeploymentId, IndexerId,
    alloy::primitives::{Address, ChainId},
};

// Re-export for tests only
#[cfg(test)]
pub use self::agreement::{Indexer, PendingAcceptedEvent};
#[cfg(test)]
pub use self::agreement_stub::StubAgreementRegistry;
use self::result::Result as RegistryResult;
pub use self::{
    agreement::{
        AgreementFeeRate, AgreementRegistry, CancelKind, IndexingAgreement, NewAgreementParams,
        ReconciliationAudit, ReconciliationItem, ReconciliationOutcome,
        Status as IndexingAgreementStatus, Terms as IndexingAgreementTerms,
        TermsMetadata as IndexingAgreementTermsMetadata,
    },
    indexer_denylist::IndexerDenylistRegistry,
    indexing_request::{
        IndexingRequest, IndexingRequestRegistry, SetTargetOutcome, Status as IndexingRequestStatus,
    },
    pending_cancellation::{PendingCancellation, PendingCancellationRegistry},
    result::{Error, Result},
};

impl From<NewAgreementParams> for dipper_pgregistry::NewAgreementParams {
    fn from(params: NewAgreementParams) -> Self {
        Self {
            agreement_id: params.agreement_id,
            nonce_uuid: params.nonce_uuid,
            request_id: params.request_id,
            deployment_id: params.deployment_id,
            indexer_id: params.indexer_id,
            indexer_url: params.indexer_url,
            terms: params.terms.into(),
            terms_version_hash: params.terms_version_hash,
        }
    }
}

/// Resolve the `remaining_accepted_indexing_agreements` value for a `terminated`
/// lifecycle event: the number of still-accepted agreements on the deployment.
///
/// MUST be called AFTER the just-cancelled agreement's terminal status is persisted,
/// so the count (which matches only `AcceptedOnChain`) excludes it and returns only
/// the *other* agreements still active on the subgraph.
///
/// Returns `-1` (unknown) on a registry error rather than a misleading `0` — the
/// proto defines `0` as "the Subgraph has no remaining accepted agreements", so a
/// transient DB failure must not be reported as "fully unstaffed".
pub(crate) async fn remaining_accepted_indexing_agreements<R>(
    registry: &R,
    deployment_id: &DeploymentId,
) -> i32
where
    R: AgreementRegistry,
{
    match registry
        .count_accepted_agreements_by_deployment(deployment_id)
        .await
    {
        Ok(count) => i32::try_from(count).unwrap_or(i32::MAX),
        Err(err) => {
            tracing::warn!(
                error = %err,
                deployment_id = %deployment_id,
                "failed to count remaining accepted agreements for terminated event; \
                 emitting -1 (unknown)"
            );
            -1
        }
    }
}

/// Filter and log conversion errors instead of silently dropping them: use as
/// `.filter_map(filter_map_with_logging)` so failed conversions leave a warning.
fn filter_map_with_logging<T, E: std::fmt::Display>(
    result: std::result::Result<T, E>,
) -> Option<T> {
    match result {
        Ok(value) => Some(value),
        Err(e) => {
            tracing::warn!(error = %e, "skipping record with conversion error");
            None
        }
    }
}

/// A service for interacting with the registry, including registering new
/// indexing requests and indexing agreements.
#[derive(Clone)]
pub struct RegistryProvider {
    inner: PgRegistry,
}

impl RegistryProvider {
    /// Creates a new registry service.
    pub fn new(db: Pool<Postgres>) -> Self {
        Self {
            inner: PgRegistry::new(db),
        }
    }
}

#[async_trait]
impl IndexingRequestRegistry for RegistryProvider {
    async fn set_indexing_target_candidates(
        &self,
        requested_by: Address,
        deployment_id: DeploymentId,
        deployment_chain_id: ChainId,
        num_candidates: usize,
    ) -> RegistryResult<crate::registry::indexing_request::SetTargetOutcome> {
        self.inner
            .set_indexing_target_candidates(
                requested_by,
                deployment_id,
                deployment_chain_id,
                num_candidates as i32,
            )
            .await
            .map_err(Into::into)
            .map(Into::into)
    }

    async fn get_all_indexing_requests(&self) -> RegistryResult<Vec<IndexingRequest>> {
        Ok(self
            .inner
            .get_all_indexing_requests()
            .await?
            .into_iter()
            .map(IndexingRequest::try_from)
            .filter_map(filter_map_with_logging)
            .collect())
    }

    async fn get_indexing_request_by_id(
        &self,
        id: &IndexingRequestId,
    ) -> RegistryResult<Option<IndexingRequest>> {
        Ok(self
            .inner
            .get_indexing_request_by_id(id)
            .await?
            .map(TryInto::try_into)
            .and_then(Result::ok))
    }

    async fn get_indexing_requests_by_deployment_id(
        &self,
        deployment_id: &DeploymentId,
    ) -> RegistryResult<Vec<IndexingRequest>> {
        Ok(self
            .inner
            .get_indexing_requests_by_deployment_id(deployment_id)
            .await?
            .into_iter()
            .map(IndexingRequest::try_from)
            .filter_map(filter_map_with_logging)
            .collect())
    }

    async fn get_open_indexing_requests_for_reassessment(
        &self,
        min_age_seconds: i64,
        batch_size: i64,
    ) -> RegistryResult<Vec<IndexingRequest>> {
        Ok(self
            .inner
            .get_open_indexing_requests_for_reassessment(min_age_seconds, batch_size)
            .await?
            .into_iter()
            .map(IndexingRequest::try_from)
            .filter_map(filter_map_with_logging)
            .collect())
    }

    async fn set_indexing_request_shortfall_active(
        &self,
        id: &IndexingRequestId,
        active: bool,
    ) -> RegistryResult<bool> {
        Ok(self
            .inner
            .set_indexing_request_shortfall_active(id, active)
            .await?)
    }
}

#[async_trait]
impl AgreementRegistry for RegistryProvider {
    async fn get_indexing_agreement_by_id(
        &self,
        id: &IndexingAgreementId,
    ) -> RegistryResult<Option<IndexingAgreement>> {
        Ok(self
            .inner
            .get_indexing_agreement_by_id(id)
            .await?
            .map(TryInto::try_into)
            .and_then(Result::ok))
    }

    async fn get_indexing_agreements_by_ids(
        &self,
        ids: &[IndexingAgreementId],
    ) -> RegistryResult<std::collections::HashMap<IndexingAgreementId, IndexingAgreement>> {
        let raw = self.inner.get_indexing_agreements_by_ids(ids).await?;
        Ok(raw
            .into_iter()
            .filter_map(|(id, raw_agreement)| {
                IndexingAgreement::try_from(raw_agreement)
                    .map(|a| (id, a))
                    .map_err(|e| {
                        tracing::warn!(error = %e, agreement_id = %id, "skipping agreement with conversion error");
                    })
                    .ok()
            })
            .collect())
    }

    async fn get_indexing_agreements_by_deployment_id(
        &self,
        deployment_id: &DeploymentId,
    ) -> RegistryResult<Vec<IndexingAgreement>> {
        Ok(self
            .inner
            .get_indexing_agreements_by_deployment_id(deployment_id)
            .await?
            .into_iter()
            .map(IndexingAgreement::try_from)
            .filter_map(filter_map_with_logging)
            .collect())
    }

    async fn get_indexing_agreements_by_indexer_id(
        &self,
        indexer_id: &IndexerId,
    ) -> RegistryResult<Vec<IndexingAgreement>> {
        Ok(self
            .inner
            .get_indexing_agreements_by_indexer_id(indexer_id)
            .await?
            .into_iter()
            .map(IndexingAgreement::try_from)
            .filter_map(filter_map_with_logging)
            .collect())
    }

    async fn get_pending_agreement_indexers_by_deployment(
        &self,
        indexer_ids: &[IndexerId],
    ) -> RegistryResult<std::collections::HashMap<DeploymentId, Vec<IndexerId>>> {
        Ok(self
            .inner
            .get_pending_agreement_indexers_by_deployment(indexer_ids)
            .await?)
    }

    async fn get_declined_indexers_by_deployment(
        &self,
        default_lookback_days: i32,
        price_lookback_days: i32,
        transient_lookback_minutes: i32,
        uncertain_lookback_days: i32,
    ) -> RegistryResult<std::collections::HashMap<DeploymentId, Vec<IndexerId>>> {
        Ok(self
            .inner
            .get_declined_indexers_by_deployment(
                default_lookback_days,
                price_lookback_days,
                transient_lookback_minutes,
                uncertain_lookback_days,
            )
            .await?)
    }

    async fn get_unresponsive_indexers(
        &self,
        lookback_days: i32,
        chain_id: ChainId,
    ) -> RegistryResult<Vec<IndexerId>> {
        Ok(self
            .inner
            .get_unresponsive_indexers(lookback_days, chain_id)
            .await?)
    }

    async fn get_indexing_agreements_by_indexing_request_id(
        &self,
        request_id: &IndexingRequestId,
    ) -> RegistryResult<Vec<IndexingAgreement>> {
        Ok(self
            .inner
            .get_indexing_agreements_by_indexing_request_id(request_id)
            .await?
            .into_iter()
            .map(IndexingAgreement::try_from)
            .filter_map(filter_map_with_logging)
            .collect())
    }
    async fn get_active_indexing_agreements_by_indexing_request_id(
        &self,
        request_id: &IndexingRequestId,
    ) -> RegistryResult<Vec<IndexingAgreement>> {
        Ok(self
            .inner
            .get_active_indexing_agreements_by_indexing_request_id(request_id)
            .await?
            .into_iter()
            .map(IndexingAgreement::try_from)
            .filter_map(filter_map_with_logging)
            .collect())
    }
    async fn count_accepted_agreements_by_deployment(
        &self,
        deployment_id: &DeploymentId,
    ) -> RegistryResult<i64> {
        self.inner
            .count_accepted_agreements_by_deployment(deployment_id)
            .await
            .map_err(Into::into)
    }
    async fn register_new_indexing_agreement(
        &self,
        params: NewAgreementParams,
    ) -> RegistryResult<IndexingAgreementId> {
        self.inner
            .register_new_indexing_agreement(params.into())
            .await
            .map_err(Into::into)
    }

    async fn register_agreement_with_pending_cancellation(
        &self,
        params: NewAgreementParams,
        old_agreement_id: IndexingAgreementId,
    ) -> RegistryResult<IndexingAgreementId> {
        self.inner
            .register_agreement_with_pending_cancellation(params.into(), old_agreement_id)
            .await
            .map_err(Into::into)
    }

    async fn mark_indexing_agreement_as_unresponsive(
        &self,
        id: &IndexingAgreementId,
    ) -> RegistryResult<()> {
        self.inner
            .mark_indexing_agreement_as_unresponsive(id)
            .await
            .map_err(Into::into)
    }

    async fn update_offer_tx_hash(
        &self,
        id: &IndexingAgreementId,
        tx_hash: &[u8; 32],
    ) -> RegistryResult<()> {
        self.inner
            .update_offer_tx_hash(id, tx_hash)
            .await
            .map_err(Into::into)
    }

    async fn update_terms_version_hash(
        &self,
        id: &IndexingAgreementId,
        hash: &[u8; 32],
    ) -> RegistryResult<()> {
        self.inner
            .update_terms_version_hash(id, hash)
            .await
            .map_err(Into::into)
    }

    async fn mark_indexing_agreement_as_canceled_by_requester(
        &self,
        id: &IndexingAgreementId,
    ) -> RegistryResult<()> {
        self.inner
            .mark_indexing_agreement_as_canceled_by_requester(id)
            .await
            .map_err(Into::into)
    }

    async fn apply_reconciliation(
        &self,
        id: &IndexingAgreementId,
        apply_accept: bool,
        cancel: Option<agreement::CancelKind>,
    ) -> RegistryResult<agreement::ReconciliationOutcome> {
        let pg_cancel = cancel.map(|k| match k {
            agreement::CancelKind::ByRequester => dipper_pgregistry::CancelKind::ByRequester,
            agreement::CancelKind::ByIndexer => dipper_pgregistry::CancelKind::ByIndexer,
        });
        let outcome = self
            .inner
            .apply_reconciliation(id, apply_accept, pg_cancel)
            .await?;
        Ok(agreement::ReconciliationOutcome {
            did_accept: outcome.did_accept,
            did_cancel: outcome.did_cancel,
        })
    }

    async fn apply_reconciliation_batch(
        &self,
        items: &[agreement::ReconciliationItem],
    ) -> RegistryResult<
        std::collections::HashMap<IndexingAgreementId, agreement::ReconciliationOutcome>,
    > {
        let pg_items: Vec<dipper_pgregistry::ReconciliationItem> = items
            .iter()
            .map(|item| dipper_pgregistry::ReconciliationItem {
                agreement_id: item.agreement_id,
                apply_accept: item.apply_accept,
                cancel: item.cancel.map(|k| match k {
                    agreement::CancelKind::ByRequester => {
                        dipper_pgregistry::CancelKind::ByRequester
                    }
                    agreement::CancelKind::ByIndexer => dipper_pgregistry::CancelKind::ByIndexer,
                }),
                audit: dipper_pgregistry::ReconciliationAudit {
                    accepted_at: item.audit.accepted_at,
                    accepted_tx: item.audit.accepted_tx.clone(),
                    canceled_at: item.audit.canceled_at,
                    canceled_by: item.audit.canceled_by.clone(),
                    canceled_tx: item.audit.canceled_tx.clone(),
                },
            })
            .collect();
        let outcomes = self.inner.apply_reconciliation_batch(&pg_items).await?;
        Ok(outcomes
            .into_iter()
            .map(|(id, o)| {
                (
                    id,
                    agreement::ReconciliationOutcome {
                        did_accept: o.did_accept,
                        did_cancel: o.did_cancel,
                    },
                )
            })
            .collect())
    }

    async fn get_agreements_pending_terminated_emission(
        &self,
        limit: i64,
    ) -> RegistryResult<Vec<agreement::PendingTerminatedEvent>> {
        Ok(self
            .inner
            .get_agreements_pending_terminated_emission(limit)
            .await?
            .into_iter()
            .map(|p| agreement::PendingTerminatedEvent {
                agreement_id: p.agreement_id,
                indexer_id: p.indexer_id,
                deployment_id: p.deployment_id,
                protocol_network: p.protocol_network,
                canceled_at: p.canceled_at,
                canceled_by: p.canceled_by,
                canceled_tx: p.canceled_tx,
                updated_at: p.updated_at,
            })
            .collect())
    }

    async fn mark_terminated_event_emitted(
        &self,
        agreement_id: &IndexingAgreementId,
    ) -> RegistryResult<()> {
        self.inner
            .mark_terminated_event_emitted(agreement_id)
            .await?;
        Ok(())
    }

    async fn get_agreements_pending_accepted_emission(
        &self,
        limit: i64,
    ) -> RegistryResult<Vec<agreement::PendingAcceptedEvent>> {
        Ok(self
            .inner
            .get_agreements_pending_accepted_emission(limit)
            .await?
            .into_iter()
            .map(|p| agreement::PendingAcceptedEvent {
                agreement_id: p.agreement_id,
                indexer_id: p.indexer_id,
                deployment_id: p.deployment_id,
                protocol_network: p.protocol_network,
                accepted_at: p.accepted_at,
                accepted_tx: p.accepted_tx,
                ends_at: p.ends_at,
                payer: p.payer,
            })
            .collect())
    }

    async fn mark_accepted_event_emitted(
        &self,
        agreement_id: &IndexingAgreementId,
    ) -> RegistryResult<()> {
        self.inner.mark_accepted_event_emitted(agreement_id).await?;
        Ok(())
    }

    async fn get_agreements_pending_expired_emission(
        &self,
        limit: i64,
    ) -> RegistryResult<Vec<agreement::PendingExpiredEvent>> {
        Ok(self
            .inner
            .get_agreements_pending_expired_emission(limit)
            .await?
            .into_iter()
            .map(|p| agreement::PendingExpiredEvent {
                agreement_id: p.agreement_id,
                indexer_id: p.indexer_id,
                deployment_id: p.deployment_id,
                protocol_network: p.protocol_network,
                request_proposed_at: p.request_proposed_at,
                request_expired_at: p.request_expired_at,
            })
            .collect())
    }

    async fn mark_expired_event_emitted(
        &self,
        agreement_id: &IndexingAgreementId,
    ) -> RegistryResult<()> {
        self.inner.mark_expired_event_emitted(agreement_id).await?;
        Ok(())
    }

    async fn record_accepted_audit(
        &self,
        agreement_id: &IndexingAgreementId,
        accepted_at: u64,
        accepted_tx: &str,
    ) -> RegistryResult<()> {
        self.inner
            .record_accepted_audit(agreement_id, accepted_at, accepted_tx)
            .await?;
        Ok(())
    }

    async fn record_cancel_audit(
        &self,
        agreement_id: &IndexingAgreementId,
        canceled_at: u64,
        canceled_by: &str,
        canceled_tx: Option<&str>,
    ) -> RegistryResult<()> {
        self.inner
            .record_cancel_audit(agreement_id, canceled_at, canceled_by, canceled_tx)
            .await?;
        Ok(())
    }

    async fn get_expired_created_agreements(
        &self,
        batch_size: i64,
        chain_timestamp: u64,
    ) -> RegistryResult<Vec<IndexingAgreement>> {
        Ok(self
            .inner
            .get_expired_created_agreements(batch_size, chain_timestamp)
            .await?
            .into_iter()
            .map(IndexingAgreement::try_from)
            .filter_map(filter_map_with_logging)
            .collect())
    }

    async fn mark_indexing_agreement_as_expired(
        &self,
        id: &IndexingAgreementId,
    ) -> RegistryResult<()> {
        self.inner
            .mark_indexing_agreement_as_expired(id)
            .await
            .map_err(Into::into)
    }

    async fn mark_indexing_agreement_as_rejected(
        &self,
        id: &IndexingAgreementId,
        rejection_reason: Option<&str>,
    ) -> RegistryResult<()> {
        self.inner
            .mark_indexing_agreement_as_rejected(id, rejection_reason)
            .await
            .map_err(Into::into)
    }

    async fn get_accepted_on_chain_agreements(
        &self,
        batch_size: i64,
    ) -> RegistryResult<Vec<IndexingAgreement>> {
        Ok(self
            .inner
            .get_accepted_on_chain_agreements(batch_size)
            .await?
            .into_iter()
            .map(IndexingAgreement::try_from)
            .filter_map(filter_map_with_logging)
            .collect())
    }

    async fn get_agreements_pending_chain_cancel(
        &self,
        batch_size: i64,
    ) -> RegistryResult<Vec<IndexingAgreement>> {
        Ok(self
            .inner
            .get_agreements_pending_chain_cancel(batch_size)
            .await?
            .into_iter()
            .map(IndexingAgreement::try_from)
            .filter_map(filter_map_with_logging)
            .collect())
    }

    async fn get_providers_for_escrow_reconciliation(
        &self,
        limit: i64,
    ) -> RegistryResult<Vec<Address>> {
        self.inner
            .get_providers_for_escrow_reconciliation(limit)
            .await
            .map_err(Into::into)
    }

    async fn update_agreement_sync_progress(
        &self,
        id: &IndexingAgreementId,
        block_height: u64,
        progress_at: time::OffsetDateTime,
    ) -> RegistryResult<()> {
        self.inner
            .update_agreement_sync_progress(id, block_height, progress_at)
            .await
            .map_err(Into::into)
    }

    async fn count_active_agreements_by_deployment(
        &self,
    ) -> RegistryResult<std::collections::HashMap<DeploymentId, usize>> {
        self.inner
            .count_active_agreements_by_deployment()
            .await
            .map_err(Into::into)
    }

    async fn count_created_agreements_by_indexer(
        &self,
    ) -> RegistryResult<(std::collections::HashMap<IndexerId, u64>, u64)> {
        self.inner
            .count_created_agreements_by_indexer()
            .await
            .map_err(Into::into)
    }

    async fn exists_active_agreements(&self) -> RegistryResult<bool> {
        self.inner
            .exists_active_agreements()
            .await
            .map_err(Into::into)
    }

    async fn mark_indexing_agreement_as_abandoned(
        &self,
        id: &IndexingAgreementId,
    ) -> RegistryResult<IndexingAgreement> {
        let raw = self.inner.mark_indexing_agreement_as_abandoned(id).await?;
        // The conversion only fails for Unknown status; since we just wrote
        // AbandonedByIndexer, this cannot fail in practice.
        IndexingAgreement::try_from(raw)
            .map_err(|_| dipper_pgregistry::Error::NoRecordsUpdated.into())
    }

    async fn get_agreement_fee_rates(&self) -> RegistryResult<Vec<AgreementFeeRate>> {
        self.inner
            .get_agreement_fee_rates()
            .await
            .map(|rows| {
                rows.into_iter()
                    .map(
                        |(_agreement_id, indexer_id, deployment_id, base_rate, entity_rate)| {
                            AgreementFeeRate {
                                indexer_id,
                                deployment_id,
                                tokens_per_second: base_rate,
                                tokens_per_entity_per_second: entity_rate,
                            }
                        },
                    )
                    .collect()
            })
            .map_err(Into::into)
    }
}

#[async_trait]
impl IndexerDenylistRegistry for RegistryProvider {
    async fn get_indexer_denylist(&self) -> RegistryResult<Vec<IndexerId>> {
        self.inner.get_indexer_denylist().await.map_err(Into::into)
    }
}

#[async_trait]
impl PendingCancellationRegistry for RegistryProvider {
    async fn get_pending_cancellations_by_new_agreement(
        &self,
        new_agreement_id: IndexingAgreementId,
    ) -> RegistryResult<Vec<PendingCancellation>> {
        let rows = self
            .inner
            .get_pending_cancellations_by_new_agreement(new_agreement_id)
            .await?;
        Ok(rows
            .into_iter()
            .map(|old_agreement_id| PendingCancellation { old_agreement_id })
            .collect())
    }

    async fn delete_pending_cancellations_by_new_agreement(
        &self,
        new_agreement_id: IndexingAgreementId,
    ) -> RegistryResult<()> {
        self.inner
            .delete_pending_cancellations_by_new_agreement(new_agreement_id)
            .await
            .map_err(Into::into)
    }

    async fn delete_pending_cancellation(
        &self,
        new_agreement_id: IndexingAgreementId,
        old_agreement_id: IndexingAgreementId,
    ) -> RegistryResult<()> {
        self.inner
            .delete_pending_cancellation(new_agreement_id, old_agreement_id)
            .await
            .map_err(Into::into)
    }

    async fn list_executable_pending_cancellations(
        &self,
        limit: i64,
    ) -> RegistryResult<Vec<IndexingAgreementId>> {
        self.inner
            .list_executable_pending_cancellations(limit)
            .await
            .map_err(Into::into)
    }
}

#[async_trait]
impl crate::network::service::chain_listener::ChainListenerStateRegistry for RegistryProvider {
    async fn get_chain_listener_state(
        &self,
        chain_id: u64,
    ) -> RegistryResult<Option<crate::network::service::chain_listener::ChainListenerState>> {
        Ok(self
            .inner
            .get_chain_listener_state(chain_id)
            .await?
            .map(
                |row| crate::network::service::chain_listener::ChainListenerState {
                    _chain_id: row.chain_id,
                    last_processed_block: row.last_processed_block,
                    last_processed_id: row.last_processed_id,
                    last_processed_block_timestamp: row.last_processed_block_timestamp,
                },
            ))
    }

    async fn update_chain_listener_state(
        &self,
        chain_id: u64,
        cursor: &crate::network::service::chain_events::Cursor,
        last_processed_block_timestamp: Option<u64>,
    ) -> RegistryResult<()> {
        self.inner
            .update_chain_listener_state(
                chain_id,
                cursor.block,
                cursor.id,
                last_processed_block_timestamp,
            )
            .await
            .map_err(Into::into)
    }
}
