//! On-chain cancel dispatch. Every cancel goes through
//! [`cancel_agreement_on_chain`] so the manager-routed path lives in one place.

use thegraph_core::alloy::primitives::B256;

use crate::{
    chain_client::{ChainClient, ChainClientError},
    config::IndexingAgreementConfig,
    registry::{AgreementRegistry, IndexingAgreement},
};

/// Pass both ACTIVE and PENDING; local status lags the chain, so let the
/// collector no-op the absent scope. Never SCOPE_SIGNED (=4): acceptance is
/// offer-based and dipper never retracts a pending offer, so it isn't needed.
const SCOPE_ACTIVE: u16 = 1;
const SCOPE_PENDING: u16 = 2;
const SCOPE_BOTH: u16 = SCOPE_ACTIVE | SCOPE_PENDING;

/// Resolve the version hash to cancel with: the locally stored one, or — if
/// missing or malformed (e.g. a pre-migration row with no `terms_version_hash`
/// column value) — the authoritative one read back from
/// `getAgreementDetails`. A recovered hash is best-effort persisted to the
/// registry so future cancels don't need to re-fetch it; a persistence
/// failure is logged and otherwise ignored, since the recovered hash is
/// still used for this call regardless.
async fn resolve_version_hash<T: ChainClient, R: AgreementRegistry>(
    chain_client: &T,
    registry: &R,
    agreement: &IndexingAgreement,
) -> Result<B256, ChainClientError> {
    if let Some(hash) = agreement
        .terms_version_hash
        .as_deref()
        .filter(|h| h.len() == 32)
        .map(B256::from_slice)
    {
        return Ok(hash);
    }

    let recovered = chain_client
        .fetch_agreement_version_hash(agreement.id.as_bytes())
        .await?
        .ok_or_else(|| ChainClientError::MissingTermsVersionHash {
            agreement_id: agreement.id.to_string(),
        })?;

    if let Err(err) = registry
        .update_terms_version_hash(&agreement.id, recovered.as_slice().try_into().unwrap())
        .await
    {
        tracing::warn!(
            agreement_id = %agreement.id,
            error = %err,
            "recovered terms_version_hash from chain but failed to persist it; will \
             re-recover on the next cancel attempt"
        );
    }
    Ok(recovered)
}

/// Cancel an agreement on-chain through the RecurringAgreementManager. Passes
/// both scope bits so the collector cancels whichever scope the agreement is in.
/// If the local `terms_version_hash` is missing, first tries to recover it from
/// `getAgreementDetails` before giving up with `MissingTermsVersionHash`.
pub async fn cancel_agreement_on_chain<T: ChainClient, R: AgreementRegistry>(
    chain_client: &T,
    registry: &R,
    agreement: &IndexingAgreement,
    config: &IndexingAgreementConfig,
) -> Result<Option<B256>, ChainClientError> {
    let version_hash = resolve_version_hash(chain_client, registry, agreement).await?;
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
            IndexingAgreementTermsMetadata, StubAgreementRegistry,
        },
    };

    /// Panic-by-default registry: fine for every test that never exercises
    /// the missing-hash recovery path (the only registry call dispatch makes).
    struct StubRegistry;
    impl StubAgreementRegistry for StubRegistry {}

    /// Records `update_terms_version_hash` calls for the recovery tests.
    #[derive(Default)]
    struct RecordingRegistry {
        persisted_hashes: Mutex<Vec<(IndexingAgreementId, [u8; 32])>>,
    }

    #[async_trait]
    impl StubAgreementRegistry for RecordingRegistry {
        async fn update_terms_version_hash(
            &self,
            id: &IndexingAgreementId,
            hash: &[u8; 32],
        ) -> crate::registry::Result<()> {
            self.persisted_hashes.lock().unwrap().push((*id, *hash));
            Ok(())
        }
    }

    /// (collector, agreement_id, version_hash, options) per manager cancel.
    type ManagerCancelArgs = (Address, [u8; 16], B256, u16);

    /// Records which on-chain cancel ran and with what arguments.
    /// `still_active_after_cancel` is the post-cancel verification read result;
    /// `active_reads` counts how many times that read fired.
    /// `on_chain_version_hash` is what a `fetch_agreement_version_hash` recovery
    /// read returns; `version_hash_reads` counts how many times it fired.
    #[derive(Default)]
    struct RecordingChainClient {
        manager_cancels: Mutex<Vec<ManagerCancelArgs>>,
        still_active_after_cancel: bool,
        active_reads: Mutex<u32>,
        on_chain_version_hash: Option<B256>,
        version_hash_reads: Mutex<u32>,
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

        async fn agreement_still_active(
            &self,
            _agreement_id: &[u8; 16],
        ) -> Result<bool, ChainClientError> {
            *self.active_reads.lock().unwrap() += 1;
            Ok(self.still_active_after_cancel)
        }

        async fn fetch_agreement_version_hash(
            &self,
            _agreement_id: &[u8; 16],
        ) -> Result<Option<B256>, ChainClientError> {
            *self.version_hash_reads.lock().unwrap() += 1;
            Ok(self.on_chain_version_hash)
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

        cancel_agreement_on_chain(&client, &StubRegistry, &ag, &manager_conf(collector))
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

        cancel_agreement_on_chain(&client, &StubRegistry, &ag, &manager_conf(Address::ZERO))
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

        let err =
            cancel_agreement_on_chain(&client, &StubRegistry, &ag, &manager_conf(Address::ZERO))
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

        let err =
            cancel_agreement_on_chain(&client, &StubRegistry, &ag, &manager_conf(Address::ZERO))
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

        let err =
            cancel_agreement_on_chain(&client, &StubRegistry, &ag, &manager_conf(Address::ZERO))
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

        let out =
            cancel_agreement_on_chain(&client, &StubRegistry, &ag, &manager_conf(Address::ZERO))
                .await
                .expect("cancel confirmed");

        assert!(out.is_some());
        assert_eq!(client.manager_cancels.lock().unwrap().len(), 1);
        assert_eq!(*client.active_reads.lock().unwrap(), 1, "verified once");
    }

    #[tokio::test]
    async fn manager_cancel_recovers_missing_hash_from_chain_and_persists_it() {
        // #638 item 3: a row with no local terms_version_hash (e.g. pre-migration)
        // must not be permanently uncancelable. If the RecurringCollector still
        // has the hash on record, recover it from there, use it for this cancel,
        // and best-effort persist it so future cancels don't need to re-fetch.
        let recovered_hash = B256::from_slice(&[3u8; 32]);
        let client = RecordingChainClient {
            on_chain_version_hash: Some(recovered_hash),
            still_active_after_cancel: false,
            ..Default::default()
        };
        let registry = RecordingRegistry::default();
        let ag = agreement(IndexingAgreementStatus::AcceptedOnChain, None);

        let out = cancel_agreement_on_chain(&client, &registry, &ag, &manager_conf(Address::ZERO))
            .await
            .expect("recovered hash unblocks the cancel");

        assert!(out.is_some());
        assert_eq!(
            *client.version_hash_reads.lock().unwrap(),
            1,
            "recovery read fired once"
        );
        let calls = client.manager_cancels.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].2, recovered_hash, "cancel used the recovered hash");

        let persisted = registry.persisted_hashes.lock().unwrap();
        assert_eq!(persisted.len(), 1);
        assert_eq!(persisted[0], (ag.id, *recovered_hash));
    }

    #[tokio::test]
    async fn manager_cancel_missing_hash_with_nothing_on_chain_is_still_missing_hash_error() {
        // The contract itself has no versionHash on record either (e.g. the
        // agreement was never offered) — recovery has nothing to recover, so
        // this must still surface as MissingTermsVersionHash, not attempt a
        // cancel with a zero hash.
        let client = RecordingChainClient {
            on_chain_version_hash: None,
            ..Default::default()
        };
        let registry = RecordingRegistry::default();
        let ag = agreement(IndexingAgreementStatus::AcceptedOnChain, None);

        let err = cancel_agreement_on_chain(&client, &registry, &ag, &manager_conf(Address::ZERO))
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            ChainClientError::MissingTermsVersionHash { .. }
        ));
        assert_eq!(*client.version_hash_reads.lock().unwrap(), 1);
        assert!(client.manager_cancels.lock().unwrap().is_empty());
        assert!(registry.persisted_hashes.lock().unwrap().is_empty());
    }
}
