//! On-chain cancel dispatch. Every cancel goes through
//! [`cancel_agreement_on_chain`] so the manager-routed path lives in one place.

use thegraph_core::alloy::primitives::B256;

use crate::{
    chain_client::{ChainClient, ChainClientError},
    config::IndexingAgreementConfig,
    registry::IndexingAgreement,
};

/// Pass both ACTIVE and PENDING; local status lags the chain, so let the
/// collector no-op the absent scope. Never SCOPE_SIGNED (=4): acceptance is
/// offer-based and dipper never retracts a pending offer, so it isn't needed.
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

#[cfg(test)]
mod tests {
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
    }

    fn manager_conf(collector: Address) -> IndexingAgreementConfig {
        IndexingAgreementConfig {
            data_service: Address::ZERO,
            recurring_collector: collector,
            recurring_agreement_manager: Address::repeat_byte(0x33),
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

    fn agreement(status: IndexingAgreementStatus, hash: Option<Vec<u8>>) -> IndexingAgreement {
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
