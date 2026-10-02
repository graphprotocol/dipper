//! On-chain cancel dispatch. Every cancel goes through
//! [`cancel_agreement_on_chain`] so the manager-routed path lives in one place.

use dipper_core::time::now_secs;
use thegraph_core::alloy::primitives::B256;

use crate::{
    chain_client::{ChainClient, ChainClientError},
    config::IndexingAgreementConfig,
    registry::{
        AgreementRegistry, IndexingAgreement, IndexingAgreementStatus, Result as RegistryResult,
    },
};

/// Pass both ACTIVE and PENDING; local status lags the chain, so let the
/// collector no-op the absent scope. PENDING revokes an offer not yet accepted.
/// Never SCOPE_SIGNED (=4): acceptance is offer-based, so revoking the stored
/// offer is enough.
const SCOPE_ACTIVE: u16 = 1;
const SCOPE_PENDING: u16 = 2;
const SCOPE_BOTH: u16 = SCOPE_ACTIVE | SCOPE_PENDING;

/// Cancel an agreement on-chain through the RecurringAgreementManager. Passes
/// both scope bits so the collector cancels whichever scope the agreement is in,
/// and treats a missing or short stored hash as `MissingTermsVersionHash`.
pub async fn cancel_agreement_on_chain<T: ChainClient>(
    chain_client: &T,
    agreement: &IndexingAgreement,
    config: &IndexingAgreementConfig,
) -> Result<Option<B256>, ChainClientError> {
    let version_hash = agreement
        .terms_version_hash
        .as_deref()
        .filter(|h| h.len() == 32)
        .map(B256::from_slice)
        .ok_or_else(|| ChainClientError::MissingTermsVersionHash {
            agreement_id: agreement.id.to_string(),
        })?;
    // Hazard: the manager's cancel mines successfully even when it does nothing
    // (stale/wrong hash, unknown id, already-terminal). So after a submitted
    // cancel we re-read on-chain and surface CancelNotConfirmed if still active.
    let outcome = chain_client
        .cancel_via_manager(
            config.recurring_collector(),
            agreement.id.as_bytes(),
            version_hash,
            SCOPE_BOTH,
        )
        .await?;

    // cancel_via_manager only returns Ok(Some) (its tx always submits);
    // Ok(None) is reserved. Verify only when a cancel actually mined.
    if outcome.is_some()
        && chain_client
            .agreement_still_active(agreement.id.as_bytes())
            .await?
    {
        return Err(ChainClientError::CancelNotConfirmed {
            agreement_id: agreement.id.to_string(),
        });
    }
    Ok(outcome)
}

/// What [`start_cancel`] left an agreement as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelStarted {
    /// It was accepted and its cancel landed: now `CanceledByRequester`.
    Ended,
    /// Still `Cancelling`; the chain listener finishes it once it can't go live.
    Cancelling,
}

/// Start ending an agreement that may be live on-chain. It is marked `Cancelling` before
/// its cancel goes out, so an offer for it still in flight withdraws itself on landing.
/// Fails, sending nothing, when the mark can't be written.
pub async fn start_cancel<R, T>(
    registry: &R,
    chain_client: &T,
    agreement: &IndexingAgreement,
    config: &IndexingAgreementConfig,
) -> RegistryResult<CancelStarted>
where
    R: AgreementRegistry + Sync,
    T: ChainClient,
{
    registry
        .mark_indexing_agreement_as_cancelling(&agreement.id)
        .await?;
    let tx_hash = match cancel_agreement_on_chain(chain_client, agreement, config).await {
        Ok(tx_hash) => tx_hash,
        Err(err) => {
            tracing::warn!(
                agreement_id = %agreement.id,
                error = %err,
                "On-chain cancel failed; the chain listener retries it"
            );
            return Ok(CancelStarted::Cancelling);
        }
    };
    tracing::info!(
        agreement_id = %agreement.id,
        tx_hash = ?tx_hash,
        "Submitted on-chain cancellation"
    );
    // An offer never accepted could still land and be accepted until its deadline.
    if agreement.status != IndexingAgreementStatus::AcceptedOnChain {
        return Ok(CancelStarted::Cancelling);
    }
    Ok(
        if confirm_cancelled(registry, agreement, tx_hash, config).await {
            CancelStarted::Ended
        } else {
            CancelStarted::Cancelling
        },
    )
}

/// Mark an agreement the chain shows dipper ended `CanceledByRequester`, recording the cancel
/// when its transaction is known, so the `terminated` sweep announces it. False, logged, when
/// the mark fails; it stays `Cancelling` for the cancel retry.
pub async fn confirm_cancelled<R: AgreementRegistry + Sync>(
    registry: &R,
    agreement: &IndexingAgreement,
    tx_hash: Option<B256>,
    config: &IndexingAgreementConfig,
) -> bool {
    if let Err(err) = registry
        .mark_indexing_agreement_as_canceled_by_requester(&agreement.id)
        .await
    {
        tracing::warn!(
            agreement_id = %agreement.id,
            error = %err,
            "Failed to mark an ended agreement cancelled; the cancel retry tries again"
        );
        return false;
    }
    tracing::info!(
        agreement_id = %agreement.id,
        indexing_request_id = %agreement.indexing_request_id,
        old_status = "CANCELLING",
        new_status = "CANCELED_BY_REQUESTER",
        reason = "cancel_confirmed_on_chain",
        "agreement state transition"
    );
    if tx_hash.is_some() {
        record_cancel(registry, agreement, tx_hash, config).await;
    }
    true
}

/// Record dipper's own cancel of an accepted agreement, so the `terminated` sweep
/// announces it.
async fn record_cancel<R: AgreementRegistry + Sync>(
    registry: &R,
    agreement: &IndexingAgreement,
    tx_hash: Option<B256>,
    config: &IndexingAgreementConfig,
) {
    let manager = config.recurring_agreement_manager().to_string();
    let tx = tx_hash.map(|hash| hash.to_string());
    if let Err(err) = registry
        .record_cancel_audit(&agreement.id, now_secs(), &manager, tx.as_deref())
        .await
    {
        tracing::warn!(
            agreement_id = %agreement.id,
            error = %err,
            "failed to record cancel audit; terminated event may emit with fallback fields"
        );
    }
}

/// What [`cancel_if_live`] found and did.
#[derive(Debug)]
pub enum LiveCancel {
    /// The chain showed nothing live, so no cancel was sent.
    NotLive,
    /// A cancel went out and the chain confirmed the agreement ended.
    Ended(Option<B256>),
    /// The chain could not be read, so nothing was sent.
    ReadFailed(ChainClientError),
    /// The cancel failed or did not end the agreement.
    CancelFailed(ChainClientError),
}

/// Cancel an agreement on-chain only if the chain shows it live: a pending offer, or
/// accepted and not yet ended. Reading first saves a wasted transaction, since a cancel
/// of an agreement that already ended still mines.
pub async fn cancel_if_live<T: ChainClient>(
    chain_client: &T,
    agreement: &IndexingAgreement,
    config: &IndexingAgreementConfig,
) -> LiveCancel {
    match chain_client
        .agreement_still_active(agreement.id.as_bytes())
        .await
    {
        Err(err) => LiveCancel::ReadFailed(err),
        Ok(false) => LiveCancel::NotLive,
        Ok(true) => match cancel_agreement_on_chain(chain_client, agreement, config).await {
            Ok(tx_hash) => LiveCancel::Ended(tx_hash),
            Err(err) => LiveCancel::CancelFailed(err),
        },
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
    use dipper_rpc::indexer::indexer_client::sol::RecurringCollectionAgreement;
    use thegraph_core::{
        DeploymentId, IndexerId,
        alloy::primitives::{Address, B256, U256},
    };
    use time::OffsetDateTime;
    use url::Url;

    use super::{SCOPE_BOTH, cancel_agreement_on_chain};
    use crate::{
        chain_client::{ChainClient, ChainClientError},
        config::IndexingAgreementConfig,
        registry::{
            IndexingAgreement, IndexingAgreementStatus, IndexingAgreementTerms,
            IndexingAgreementTermsMetadata,
        },
    };

    /// (collector, agreement_id, version_hash, options) per manager cancel.
    type ManagerCancelArgs = (Address, [u8; 16], B256, u16);

    /// Records which on-chain cancel ran and with what arguments.
    /// `still_active_after_cancel` is the post-cancel verification read result;
    /// `active_reads` counts how many times that read fired.
    #[derive(Default)]
    struct RecordingChainClient {
        manager_cancels: Mutex<Vec<ManagerCancelArgs>>,
        still_active_after_cancel: bool,
        active_reads: Mutex<u32>,
    }

    #[async_trait]
    impl ChainClient for RecordingChainClient {
        async fn latest_block_timestamp(&self) -> Result<u64, ChainClientError> {
            // Err by default so a test must mock this explicitly to take the
            // live-chain-head path instead of silently reading timestamp 0.
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
            collector: Address,
            agreement_id: &[u8; 16],
            version_hash: B256,
            options: u16,
        ) -> Result<Option<B256>, ChainClientError> {
            self.manager_cancels.lock().unwrap().push((
                collector,
                *agreement_id,
                version_hash,
                options,
            ));
            Ok(Some(B256::ZERO))
        }

        async fn reconcile_provider(
            &self,
            _collector: Address,
            _provider: Address,
        ) -> Result<Option<B256>, ChainClientError> {
            Ok(None)
        }
        async fn reconcile_agreement(
            &self,
            _collector: Address,
            _agreement_id: &[u8; 16],
        ) -> Result<Option<B256>, ChainClientError> {
            unimplemented!()
        }

        async fn agreement_still_active(
            &self,
            _agreement_id: &[u8; 16],
        ) -> Result<bool, ChainClientError> {
            *self.active_reads.lock().unwrap() += 1;
            Ok(self.still_active_after_cancel)
        }
        async fn agreement_ended_by_indexer(
            &self,
            _agreement_id: &[u8; 16],
        ) -> Result<bool, ChainClientError> {
            Ok(false)
        }
    }

    fn manager_conf(collector: Address) -> IndexingAgreementConfig {
        IndexingAgreementConfig {
            recurring_collector: collector,
            recurring_agreement_manager: Address::repeat_byte(0x33),
            ..IndexingAgreementConfig::for_tests()
        }
    }

    pub(crate) fn agreement(
        status: IndexingAgreementStatus,
        hash: Option<Vec<u8>>,
    ) -> IndexingAgreement {
        let deployment_id: DeploymentId = "QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9"
            .parse()
            .unwrap();
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
                    subgraph_deployment_id: deployment_id,
                    protocol_network: 1u64,
                    chain_id: 1u64,
                    proposed_at: 0,
                },
            },
            last_block_height: None,
            last_progress_at: None,
            rejection_reason: None,
            terms_version_hash: hash,
        }
    }

    #[tokio::test]
    async fn manager_cancel_uses_both_scopes_for_accepted() {
        // comp-1: a manager cancel passes BOTH scope bits so the collector
        // cancels whichever scope the agreement is actually in, instead of a
        // stale local status picking one and silently no-opping the other.
        let collector = Address::repeat_byte(0x11);
        let client = RecordingChainClient::default();
        let ag = agreement(
            IndexingAgreementStatus::AcceptedOnChain,
            Some(vec![7u8; 32]),
        );

        cancel_agreement_on_chain(&client, &ag, &manager_conf(collector))
            .await
            .expect("cancel dispatch");

        let calls = client.manager_cancels.lock().unwrap();
        assert_eq!(calls.len(), 1);
        let (got_collector, got_id, got_hash, got_options) = calls[0];
        assert_eq!(got_collector, collector);
        assert_eq!(&got_id, ag.id.as_bytes());
        assert_eq!(got_hash, B256::from_slice(&[7u8; 32]));
        assert_eq!(got_options, SCOPE_BOTH);
        assert_eq!(got_options, 3, "both scope bits set");
    }

    #[tokio::test]
    async fn manager_cancel_uses_both_scopes_for_rejected_but_accepted_on_chain() {
        // comp-1 regression: DB status is Rejected while the agreement is active
        // on-chain (the cancel-on-reject backstop). The cancel must still send
        // SCOPE_BOTH (3); a status-derived SCOPE_PENDING would let the contract no-op.
        let client = RecordingChainClient::default();
        let ag = agreement(IndexingAgreementStatus::Rejected, Some(vec![9u8; 32]));

        cancel_agreement_on_chain(&client, &ag, &manager_conf(Address::ZERO))
            .await
            .expect("cancel dispatch");

        let calls = client.manager_cancels.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].3, SCOPE_BOTH);
    }

    #[tokio::test]
    async fn manager_cancel_missing_hash_is_distinct_error_and_sends_nothing() {
        // eh-1: a missing hash must be the distinct MissingTermsVersionHash, not
        // a ConfigError the liveness checker reads as "chain client disabled"
        // and would silently abandon while the agreement stays live on-chain.
        let client = RecordingChainClient::default();
        let ag = agreement(IndexingAgreementStatus::AcceptedOnChain, None);

        let err = cancel_agreement_on_chain(&client, &ag, &manager_conf(Address::ZERO))
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            ChainClientError::MissingTermsVersionHash { .. }
        ));
        assert!(client.manager_cancels.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn manager_cancel_wrong_length_hash_is_missing_hash_error() {
        // A present-but-not-32-byte hash is as unusable as a missing one.
        let client = RecordingChainClient::default();
        let ag = agreement(
            IndexingAgreementStatus::AcceptedOnChain,
            Some(vec![1u8; 16]),
        );

        let err = cancel_agreement_on_chain(&client, &ag, &manager_conf(Address::ZERO))
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            ChainClientError::MissingTermsVersionHash { .. }
        ));
        assert!(client.manager_cancels.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn manager_cancel_still_active_returns_not_confirmed() {
        // The manager cancel mined but the post-cancel read shows the agreement
        // is still live on-chain (silent no-op). Dispatch must surface
        // CancelNotConfirmed so the caller retries instead of marking terminal.
        let client = RecordingChainClient {
            still_active_after_cancel: true,
            ..Default::default()
        };
        let ag = agreement(
            IndexingAgreementStatus::AcceptedOnChain,
            Some(vec![7u8; 32]),
        );

        let err = cancel_agreement_on_chain(&client, &ag, &manager_conf(Address::ZERO))
            .await
            .unwrap_err();

        assert!(matches!(err, ChainClientError::CancelNotConfirmed { .. }));
        assert_eq!(client.manager_cancels.lock().unwrap().len(), 1);
        assert_eq!(*client.active_reads.lock().unwrap(), 1, "verified once");
    }

    #[tokio::test]
    async fn manager_cancel_no_longer_active_returns_ok() {
        // The post-cancel read shows the agreement left the active set, so the
        // cancel took effect and dispatch returns Ok for the caller to finalize.
        let client = RecordingChainClient {
            still_active_after_cancel: false,
            ..Default::default()
        };
        let ag = agreement(
            IndexingAgreementStatus::AcceptedOnChain,
            Some(vec![7u8; 32]),
        );

        let out = cancel_agreement_on_chain(&client, &ag, &manager_conf(Address::ZERO))
            .await
            .expect("cancel confirmed");

        assert!(out.is_some());
        assert_eq!(client.manager_cancels.lock().unwrap().len(), 1);
        assert_eq!(*client.active_reads.lock().unwrap(), 1, "verified once");
    }
}
