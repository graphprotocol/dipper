//! Submit an RCA offer on-chain after the indexer has accepted the proposal.
//!
//! This handler runs after `send_indexing_agreement_proposal` receives an
//! Accept response from the indexer. The worker pipeline is:
//!
//! 1. `reassess_indexing_request` selects indexers via IISA and registers
//!    each agreement in the DB.
//! 2. `send_indexing_agreement_proposal` sends the gRPC proposal to the
//!    indexer, which validates pricing/metadata/networks and responds
//!    Accept or Reject.
//! 3. On Accept, `submit_offer` (this handler) posts the RCA offer on-chain
//!    via `RecurringCollector.offer()`. The indexer-agent then calls
//!    `acceptIndexingAgreement` — the contract checks `rcaOffers`.
//!
//! Nothing here is idempotent across a crash: a re-run submits the offer again. The
//! `rcaOffers` mapping on `RecurringCollector` sits in an ERC-7201 namespaced storage
//! struct with no public getter, so the chain cannot cheaply be asked what already landed.

use std::{sync::Arc, time::Duration};

use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
use thegraph_core::{DeploymentId, alloy::primitives::ChainId};
use url::Url;

use crate::{
    cancel_dispatch::cancel_agreement_on_chain,
    chain_client::{ChainClient, ChainClientError, decode_revert_reason},
    config::IndexingAgreementConfig,
    indexer_rpc_client::into_sol_rca,
    registry::{AgreementRegistry, IndexingAgreement, IndexingAgreementStatus},
    worker::{
        context::ReassessLock,
        result::{JobError, JobResult},
    },
};

/// Backoff base for a tx the RPC accepted and then dropped from the mempool.
pub const DROPPED_TX_RETRY_BASE: Duration = Duration::from_secs(5);

/// Retry shortly while a reassessment runs or waits; not counted as a failure.
const DEFER_WHILE_REASSESSING: JobError = JobError::Deferred(Duration::from_secs(1));

/// Backoff base for a transient submission failure: RPC, gas or nonce.
pub const TRANSIENT_RETRY_BASE: Duration = Duration::from_secs(30);

pub struct Ctx<R, T> {
    pub registry: R,
    pub chain_client: T,
    pub agreement_conf: Arc<IndexingAgreementConfig>,
    /// Taken shared while the offer is checked and sent (see `ReassessLock`).
    pub reassess_lock: ReassessLock,
}

/// Submit an RCA offer on-chain.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Message {
    pub agreement_id: IndexingAgreementId,
    pub indexing_request_id: IndexingRequestId,
    pub indexer_url: Url,
    pub deployment_id: DeploymentId,
    pub deployment_chain_id: ChainId,
}

#[expect(
    clippy::cognitive_complexity,
    reason = "predates this lint; fix when next touched"
)]
pub async fn handle<R, T>(
    ctx: Ctx<R, T>,
    Message {
        agreement_id,
        indexing_request_id,
        indexer_url: _,
        deployment_id: _,
        deployment_chain_id: _,
    }: &Message,
) -> JobResult<()>
where
    R: AgreementRegistry,
    T: ChainClient,
{
    // Held from the status check until the offer lands: a reassessment's cancel
    // then either follows this offer (later nonce, same wallet) and withdraws it,
    // or finished first and the check below sees the agreement cancelled.
    let reassess_guard = ctx.reassess_lock.offer().ok_or(DEFER_WHILE_REASSESSING)?;

    // Fetch the agreement. Skip silently if it's already been transitioned
    // out of Created (e.g. expired by the reassignment service).
    let agreement = match ctx
        .registry
        .get_indexing_agreement_by_id(agreement_id)
        .await
        .map_err(|err| JobError::Fatal(err.into()))?
    {
        None => {
            tracing::error!(
                agreement_id = %agreement_id,
                "Agreement not found in registry at submit_offer"
            );
            return Ok(());
        }
        Some(a) if a.status != IndexingAgreementStatus::Created => {
            tracing::warn!(
                agreement_id = %agreement_id,
                status = %a.status,
                "Agreement not in Created status, skipping offer submission"
            );
            return Ok(());
        }
        Some(a) => a,
    };

    // Rebuild the on-chain RCA struct from the stored terms. The bytes must be
    // identical to what send_indexing_agreement_proposal encoded for gRPC, so
    // the on-chain offerHash matches what the indexer computed locally.
    let (rca, derived_id) = into_sol_rca(agreement.nonce_uuid, agreement.terms.clone());

    // Sanity check: the derived on-chain ID must match the agreement ID we
    // stored at registration time. If it doesn't, our conversion path has
    // drifted and every downstream step will fail. Treat as fatal.
    if &derived_id != agreement_id.as_bytes() {
        tracing::error!(
            agreement_id = %agreement_id,
            derived = %format_args!("0x{}", derived_id.iter().map(|b| format!("{b:02x}")).collect::<String>()),
            "Derived on-chain ID does not match stored agreement ID"
        );
        return Err(JobError::Fatal(anyhow::anyhow!(
            "derived on-chain ID drift"
        )));
    }

    tracing::info!(
        indexing_request_id = %indexing_request_id,
        agreement_id = %agreement_id,
        "Submitting RCA offer on-chain"
    );

    // The RecurringAgreementManager is the on-chain payer, so route the offer
    // through it rather than posting directly.
    match ctx.chain_client.offer_via_manager(&rca).await {
        Ok(None) => {
            // The chain client reports nothing was submitted. The live client always
            // submits, so this only fires for a client that skips the offer itself.
            tracing::info!(
                agreement_id = %agreement_id,
                "Offer needed no transaction, proceeding to dispatch"
            );
        }
        Ok(Some(tx_hash)) => {
            tracing::info!(
                agreement_id = %agreement_id,
                tx_hash = %tx_hash,
                "Offer submitted on-chain successfully"
            );
            // Observability only: record which tx hash actually mined.
            // Any failure here is non-fatal to the overall flow.
            if let Err(err) = ctx
                .registry
                .update_offer_tx_hash(agreement_id, tx_hash.as_ref())
                .await
            {
                tracing::warn!(
                    agreement_id = %agreement_id,
                    tx_hash = %tx_hash,
                    error = %err,
                    "Failed to persist offer_tx_hash; continuing"
                );
            }
        }
        Err(err @ ChainClientError::TxDropped { .. }) => {
            // Accepted by the RPC but never mined — typically evicted by a
            // colliding-nonce tx. The nonce was re-synced; re-running resubmits
            // with a fresh nonce. No idempotency guard, so a replay re-sends.
            tracing::warn!(
                agreement_id = %agreement_id,
                error = %err,
                "Offer tx dropped from mempool, will retry with fresh nonce"
            );
            return Err(JobError::Retryable(err.into(), DROPPED_TX_RETRY_BASE));
        }
        Err(ChainClientError::ContractRevert { selector, data }) => {
            // A gas-estimation revert won't clear on a quick retry: bad terms
            // revert forever, state-dependent causes (pause, escrow) outlast the
            // backoff. Fail the job; the expiration sweep reassigns at deadline.
            let reason = decode_revert_reason(selector, &data);
            tracing::error!(
                agreement_id = %agreement_id,
                reason = %reason,
                "Offer reverted on-chain, dropping the submission job"
            );
            return Err(JobError::Fatal(anyhow::anyhow!(
                "offer revert will not clear on retry: {reason}"
            )));
        }
        Err(err) => {
            // Other transient submission failures (RPC, gas, nonce). Retry with
            // backoff -- build_and_send_call already has bounded nonce retries,
            // so returning Retryable here escalates to the worker-level backoff.
            tracing::warn!(
                agreement_id = %agreement_id,
                error = %err,
                "Failed to submit offer on-chain, will retry"
            );
            return Err(JobError::Retryable(err.into(), TRANSIENT_RETRY_BASE));
        }
    }

    // Landed, so any later cancel follows it; stop holding up reassessments.
    drop(reassess_guard);
    withdraw_if_cancelled_meanwhile(&ctx, agreement_id).await;

    // Offer is confirmed on-chain (or was already there). The indexer-agent will
    // pick up the pending_rca_proposals row and call acceptIndexingAgreement. No
    // further enqueue needed; chain_listener detects the acceptance event.
    Ok(())
}

/// Withdraw the offer just sent if its agreement was cancelled while it was in
/// flight. The chain listener cancels replaced agreements without the reassess
/// lock, so its cancel can land just ahead of this offer with nothing to withdraw.
async fn withdraw_if_cancelled_meanwhile<R, T>(ctx: &Ctx<R, T>, agreement_id: &IndexingAgreementId)
where
    R: AgreementRegistry,
    T: ChainClient,
{
    let Some(agreement) = cancelled_meanwhile(&ctx.registry, agreement_id).await else {
        return;
    };
    match cancel_agreement_on_chain(&ctx.chain_client, &agreement, &ctx.agreement_conf).await {
        Ok(tx_hash) => tracing::info!(
            agreement_id = %agreement_id,
            tx_hash = ?tx_hash,
            "Withdrew an offer whose agreement was cancelled while it was being sent"
        ),
        Err(err) => tracing::warn!(
            agreement_id = %agreement_id,
            error = %err,
            "Failed to withdraw an offer whose agreement was cancelled while it was being \
             sent; it stays open until its deadline"
        ),
    }
}

/// The agreement, if it was cancelled after this job's status check.
async fn cancelled_meanwhile<R: AgreementRegistry>(
    registry: &R,
    agreement_id: &IndexingAgreementId,
) -> Option<IndexingAgreement> {
    match registry.get_indexing_agreement_by_id(agreement_id).await {
        Ok(Some(agreement)) if agreement.status == IndexingAgreementStatus::CanceledByRequester => {
            Some(agreement)
        }
        Ok(_) => None,
        Err(err) => {
            tracing::warn!(
                agreement_id = %agreement_id,
                error = %err,
                "Failed to re-read agreement after its offer landed; a cancel made meanwhile \
                 would leave the offer open until its deadline"
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use thegraph_core::{
        alloy::primitives::{Address, B256, Bytes, U256},
        deployment_id, indexer_id,
    };

    use super::*;
    use crate::{
        indexer_rpc_client::compute_on_chain_id,
        registry::{
            IndexingAgreement, IndexingAgreementTerms, IndexingAgreementTermsMetadata,
            StubAgreementRegistry,
        },
    };

    /// Shared with the chain mock so a test can change the row mid-send.
    type SharedAgreement = Arc<Mutex<Option<IndexingAgreement>>>;

    struct MockRegistry {
        agreement: SharedAgreement,
    }

    #[async_trait]
    impl StubAgreementRegistry for MockRegistry {
        async fn get_indexing_agreement_by_id(
            &self,
            _id: &IndexingAgreementId,
        ) -> crate::registry::Result<Option<IndexingAgreement>> {
            Ok(self.agreement.lock().unwrap().clone())
        }
        async fn update_offer_tx_hash(
            &self,
            _id: &IndexingAgreementId,
            _tx_hash: &[u8; 32],
        ) -> crate::registry::Result<()> {
            Ok(())
        }
    }

    /// Yields the configured result once; a second call means the handler
    /// retried inside one run, and a call with no result configured means the
    /// handler sent when it must not. Records whether a reassessment could have
    /// taken `reassess_lock` while the offer was being sent.
    struct MockChainClient {
        offer_result: Mutex<Option<Result<Option<B256>, ChainClientError>>>,
        reassess_lock: ReassessLock,
        reassessment_could_start_mid_send: Arc<Mutex<Option<bool>>>,
        /// When set, the agreement is cancelled locally while the offer is sent,
        /// as the chain listener does when a replacement is accepted.
        cancel_mid_send: Option<SharedAgreement>,
        cancelled: Arc<Mutex<Vec<[u8; 16]>>>,
    }

    #[async_trait]
    impl ChainClient for MockChainClient {
        async fn offer_via_manager(
            &self,
            _rca: &dipper_rpc::indexer::indexer_client::sol::RecurringCollectionAgreement,
        ) -> Result<Option<B256>, ChainClientError> {
            *self.reassessment_could_start_mid_send.lock().unwrap() =
                Some(self.reassess_lock.reassessment_could_start_now());
            if let Some(agreement) = &self.cancel_mid_send
                && let Some(row) = agreement.lock().unwrap().as_mut()
            {
                row.status = IndexingAgreementStatus::CanceledByRequester;
            }
            self.offer_result
                .lock()
                .unwrap()
                .take()
                .expect("offer_via_manager called more than once, or when it must not send")
        }
        async fn cancel_via_manager(
            &self,
            _collector: Address,
            agreement_id: &[u8; 16],
            _version_hash: B256,
            _options: u16,
        ) -> Result<Option<B256>, ChainClientError> {
            self.cancelled.lock().unwrap().push(*agreement_id);
            Ok(Some(B256::repeat_byte(0xcd)))
        }
        async fn reconcile_provider(
            &self,
            _collector: Address,
            _provider: Address,
        ) -> Result<Option<B256>, ChainClientError> {
            unimplemented!()
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
            Ok(false)
        }
        async fn latest_block_timestamp(&self) -> Result<u64, ChainClientError> {
            unimplemented!()
        }
    }

    fn make_test_agreement() -> IndexingAgreement {
        use time::OffsetDateTime;

        let terms = IndexingAgreementTerms {
            payer: Address::ZERO,
            service_provider: Address::ZERO,
            data_service: Address::ZERO,
            deadline: 0,
            ends_at: 0,
            max_initial_tokens: U256::ZERO,
            max_ongoing_tokens_per_second: U256::ZERO,
            min_seconds_per_collection: 60,
            max_seconds_per_collection: 240,
            conditions: 0,
            metadata: IndexingAgreementTermsMetadata {
                tokens_per_second: U256::ZERO,
                tokens_per_entity_per_second: U256::ZERO,
                subgraph_deployment_id: deployment_id!(
                    "QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv"
                ),
                protocol_network: 1,
                chain_id: 1,
                proposed_at: 0,
            },
        };
        let nonce_uuid = uuid::Uuid::now_v7();
        // The handler recomputes the on-chain ID from the terms and bails on
        // a mismatch, so the fixture ID must be derived rather than random.
        let id = compute_on_chain_id(nonce_uuid, &terms);
        IndexingAgreement {
            id,
            nonce_uuid,
            created_at: OffsetDateTime::now_utc(),
            updated_at: OffsetDateTime::now_utc(),
            status: IndexingAgreementStatus::Created,
            indexing_request_id: IndexingRequestId::new(),
            indexer: crate::registry::Indexer {
                id: indexer_id!("1111111111111111111111111111111111111111"),
                url: "https://indexer.example.com".parse().unwrap(),
            },
            terms,
            last_block_height: None,
            last_progress_at: None,
            rejection_reason: None,
            terms_version_hash: None,
        }
    }

    fn make_message(agreement_id: IndexingAgreementId) -> Message {
        Message {
            agreement_id,
            indexing_request_id: IndexingRequestId::new(),
            indexer_url: "https://indexer.example.com".parse().unwrap(),
            deployment_id: deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv"),
            deployment_chain_id: 1,
        }
    }

    fn ctx_with_offer_result(
        agreement: IndexingAgreement,
        offer_result: Result<Option<B256>, ChainClientError>,
    ) -> Ctx<MockRegistry, MockChainClient> {
        ctx_with_lock(agreement, Some(offer_result), ReassessLock::default())
    }

    /// `offer_result: None` makes any send panic.
    fn ctx_with_lock(
        agreement: IndexingAgreement,
        offer_result: Option<Result<Option<B256>, ChainClientError>>,
        reassess_lock: ReassessLock,
    ) -> Ctx<MockRegistry, MockChainClient> {
        Ctx {
            registry: MockRegistry {
                agreement: Arc::new(Mutex::new(Some(agreement))),
            },
            chain_client: MockChainClient {
                offer_result: Mutex::new(offer_result),
                reassess_lock: reassess_lock.clone(),
                reassessment_could_start_mid_send: Arc::default(),
                cancel_mid_send: None,
                cancelled: Arc::default(),
            },
            agreement_conf: Arc::new(test_agreement_conf()),
            reassess_lock,
        }
    }

    fn test_agreement_conf() -> crate::config::IndexingAgreementConfig {
        crate::config::IndexingAgreementConfig {
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
        }
    }

    #[tokio::test]
    async fn withdraws_its_offer_when_the_agreement_was_cancelled_while_it_was_sent() {
        //* Arrange - the agreement is cancelled locally while the offer is in flight,
        // so the cancel found nothing to withdraw and the offer would stay open
        let mut agreement = make_test_agreement();
        agreement.terms_version_hash = Some(vec![7u8; 32]);
        let agreement_id = agreement.id;
        let message = make_message(agreement_id);
        let mut ctx = ctx_with_offer_result(agreement, Ok(Some(B256::repeat_byte(0xab))));
        ctx.chain_client.cancel_mid_send = Some(ctx.registry.agreement.clone());
        let cancelled = ctx.chain_client.cancelled.clone();

        //* Act
        let result = handle(ctx, &message).await;

        //* Assert
        assert!(result.is_ok(), "got {result:?}");
        assert_eq!(*cancelled.lock().unwrap(), vec![*agreement_id.as_bytes()]);
    }

    #[tokio::test]
    async fn keeps_its_offer_when_the_agreement_is_still_wanted() {
        //* Arrange
        let agreement = make_test_agreement();
        let message = make_message(agreement.id);
        let ctx = ctx_with_offer_result(agreement, Ok(Some(B256::repeat_byte(0xab))));
        let cancelled = ctx.chain_client.cancelled.clone();

        //* Act
        let result = handle(ctx, &message).await;

        //* Assert
        assert!(result.is_ok(), "got {result:?}");
        assert!(cancelled.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn waits_without_sending_while_a_reassessment_holds_the_lock() {
        //* Arrange - a reassessment holds the lock; no offer result is configured,
        // so a send would panic
        let agreement = make_test_agreement();
        let message = make_message(agreement.id);
        let lock = ReassessLock::default();
        let _reassessment = lock.reassessment().await.expect("lock is free");
        let ctx = ctx_with_lock(agreement, None, lock);

        //* Act
        let result = handle(ctx, &message).await;

        //* Assert - deferred, not failed, so it runs once the reassessment ends
        assert!(
            matches!(result, Err(JobError::Deferred(delay)) if delay == Duration::from_secs(1)),
            "an offer must wait while a reassessment runs, got {result:?}"
        );
    }

    #[tokio::test]
    async fn no_reassessment_can_start_while_the_offer_is_sent() {
        //* Arrange
        let agreement = make_test_agreement();
        let message = make_message(agreement.id);
        let ctx = ctx_with_offer_result(agreement, Ok(Some(B256::repeat_byte(0xab))));
        let lock = ctx.reassess_lock.clone();
        let could_start = ctx.chain_client.reassessment_could_start_mid_send.clone();

        //* Act
        let result = handle(ctx, &message).await;

        //* Assert - the lock was held during the send and is free once the job ends
        assert!(result.is_ok(), "got {result:?}");
        assert_eq!(
            *could_start.lock().unwrap(),
            Some(false),
            "a reassessment must not be able to start while the offer is sent"
        );
        assert!(
            lock.reassessment_could_start_now(),
            "the job must release the lock when it ends"
        );
    }

    #[tokio::test]
    async fn sends_alongside_another_offer() {
        //* Arrange - another offer job holds the lock shared
        let agreement = make_test_agreement();
        let message = make_message(agreement.id);
        let lock = ReassessLock::default();
        let _other_offer = lock.offer().expect("lock is free");
        let ctx = ctx_with_lock(agreement, Some(Ok(Some(B256::repeat_byte(0xab)))), lock);

        //* Act
        let result = handle(ctx, &message).await;

        //* Assert
        assert!(
            result.is_ok(),
            "offers must not wait for each other, got {result:?}"
        );
    }

    #[tokio::test]
    async fn skips_an_agreement_a_reassessment_already_cancelled() {
        //* Arrange - the reassessment ran first and cancelled the agreement; no offer
        // result is configured, so a send would panic
        let mut agreement = make_test_agreement();
        agreement.status = IndexingAgreementStatus::CanceledByRequester;
        let message = make_message(agreement.id);
        let ctx = ctx_with_lock(agreement, None, ReassessLock::default());

        //* Act
        let result = handle(ctx, &message).await;

        //* Assert
        assert!(result.is_ok(), "got {result:?}");
    }

    #[tokio::test]
    async fn contract_revert_fails_the_job_instead_of_retrying() {
        //* Arrange - the offer reverts with the observed window selector
        let agreement = make_test_agreement();
        let message = make_message(agreement.id);
        let revert = ChainClientError::ContractRevert {
            selector: [0xe4, 0x57, 0x63, 0x96],
            data: Bytes::copy_from_slice(&[0xe4, 0x57, 0x63, 0x96]),
        };
        let ctx = ctx_with_offer_result(agreement, Err(revert));

        //* Act
        let result = handle(ctx, &message).await;

        //* Assert - Fatal removes the job from the queue; Retryable would loop
        assert!(
            matches!(result, Err(JobError::Fatal(_))),
            "a deterministic revert must fail the job, got {result:?}"
        );
    }

    #[tokio::test]
    async fn transient_rpc_error_stays_retryable() {
        //* Arrange - the offer fails with a transient RPC error
        let agreement = make_test_agreement();
        let message = make_message(agreement.id);
        let rpc_error = ChainClientError::RpcError(anyhow::anyhow!("rpc unreachable"));
        let ctx = ctx_with_offer_result(agreement, Err(rpc_error));

        //* Act
        let result = handle(ctx, &message).await;

        //* Assert
        assert!(
            matches!(result, Err(JobError::Retryable(_, _))),
            "a transient failure must stay retryable, got {result:?}"
        );
    }
}
