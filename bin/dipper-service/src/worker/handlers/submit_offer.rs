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
    cancel_dispatch::{LiveCancel, cancel_if_live},
    chain_client::{ChainClient, ChainClientError, decode_revert_reason},
    config::IndexingAgreementConfig,
    indexer_rpc_client::into_sol_rca,
    registry::{AgreementRegistry, IndexingAgreement, IndexingAgreementStatus},
    worker::result::{JobError, JobResult},
};

/// Backoff base for a tx the RPC accepted and then dropped from the mempool.
pub const DROPPED_TX_RETRY_BASE: Duration = Duration::from_secs(5);

/// Backoff base for a transient submission failure: RPC, gas or nonce.
pub const TRANSIENT_RETRY_BASE: Duration = Duration::from_secs(30);

pub struct Ctx<R, T> {
    pub registry: R,
    pub chain_client: T,
    pub agreement_conf: Arc<IndexingAgreementConfig>,
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
    let agreement = match next_step(&ctx.registry, agreement_id).await? {
        NextStep::Offer(agreement) => agreement,
        NextStep::Withdraw(agreement) => {
            return withdraw_offer_if_stored(&ctx, &agreement).await;
        }
        NextStep::Skip => return Ok(()),
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

    // Every cancel of an unaccepted agreement marks it before it is sent, so a cancel
    // that went out ahead of this offer, and found nothing to withdraw, shows here.
    if let Some(agreement) = cancelled_meanwhile(&ctx.registry, agreement_id).await? {
        return withdraw_offer_if_stored(&ctx, &agreement).await;
    }

    // Offer is confirmed on-chain (or was already there). The indexer-agent will
    // pick up the pending_rca_proposals row and call acceptIndexingAgreement. No
    // further enqueue needed; chain_listener detects the acceptance event.
    Ok(())
}

/// What this run of the job does with its agreement.
enum NextStep {
    /// Still wanted: send the offer.
    Offer(IndexingAgreement),
    /// Dipper cancelled it: withdraw any offer an earlier attempt left on-chain.
    Withdraw(IndexingAgreement),
    /// Gone, expired or otherwise past offering.
    Skip,
}

async fn next_step<R: AgreementRegistry>(
    registry: &R,
    agreement_id: &IndexingAgreementId,
) -> JobResult<NextStep> {
    let agreement = registry
        .get_indexing_agreement_by_id(agreement_id)
        .await
        .map_err(|err| JobError::Fatal(err.into()))?;
    Ok(match agreement {
        None => {
            tracing::error!(
                agreement_id = %agreement_id,
                "Agreement not found in registry at submit_offer"
            );
            NextStep::Skip
        }
        Some(a) if a.status == IndexingAgreementStatus::Created => NextStep::Offer(a),
        Some(a) if dipper_cancelled(a.status) => NextStep::Withdraw(a),
        Some(a) => {
            tracing::warn!(
                agreement_id = %agreement_id,
                status = %a.status,
                "Agreement not in Created status, skipping offer submission"
            );
            NextStep::Skip
        }
    })
}

/// Withdraw the agreement's offer if one is on-chain: dipper cancelled it while
/// this job's offer was in flight or before this retry. A failure retries the job,
/// which comes back here through its status check.
async fn withdraw_offer_if_stored<R, T: ChainClient>(
    ctx: &Ctx<R, T>,
    agreement: &IndexingAgreement,
) -> JobResult<()> {
    match cancel_if_live(&ctx.chain_client, agreement, &ctx.agreement_conf).await {
        LiveCancel::NotLive => Ok(()),
        LiveCancel::Ended(tx_hash) => {
            tracing::info!(
                agreement_id = %agreement.id,
                tx_hash = ?tx_hash,
                "Withdrew the offer of an agreement dipper had cancelled"
            );
            Ok(())
        }
        LiveCancel::CancelFailed(err @ ChainClientError::MissingTermsVersionHash { .. }) => {
            tracing::error!(
                agreement_id = %agreement.id,
                error = %err,
                "Cannot withdraw the offer of a cancelled agreement; it stays open until its deadline"
            );
            Err(JobError::Fatal(err.into()))
        }
        LiveCancel::ReadFailed(err) | LiveCancel::CancelFailed(err) => {
            Err(retry_withdraw(agreement, err))
        }
    }
}

fn retry_withdraw(agreement: &IndexingAgreement, err: ChainClientError) -> JobError {
    tracing::warn!(
        agreement_id = %agreement.id,
        error = %err,
        "Failed to withdraw the offer of a cancelled agreement, will retry"
    );
    JobError::Retryable(err.into(), TRANSIENT_RETRY_BASE)
}

/// Whether dipper has cancelled the agreement, or started to.
fn dipper_cancelled(status: IndexingAgreementStatus) -> bool {
    matches!(
        status,
        IndexingAgreementStatus::Cancelling | IndexingAgreementStatus::CanceledByRequester
    )
}

/// The agreement, if it was cancelled after this job's status check. A failed read
/// retries the job, whose status check then withdraws the offer of a cancelled one.
async fn cancelled_meanwhile<R: AgreementRegistry>(
    registry: &R,
    agreement_id: &IndexingAgreementId,
) -> JobResult<Option<IndexingAgreement>> {
    match registry.get_indexing_agreement_by_id(agreement_id).await {
        Ok(Some(agreement)) if dipper_cancelled(agreement.status) => Ok(Some(agreement)),
        Ok(_) => Ok(None),
        Err(err) => {
            tracing::warn!(
                agreement_id = %agreement_id,
                error = %err,
                "Failed to re-read agreement after its offer landed, will retry"
            );
            Err(JobError::Retryable(err.into(), TRANSIENT_RETRY_BASE))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU32, Ordering},
    };

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
        /// When set, every read after the first fails.
        later_reads_fail: bool,
        reads: AtomicU32,
    }

    #[async_trait]
    impl StubAgreementRegistry for MockRegistry {
        async fn get_indexing_agreement_by_id(
            &self,
            _id: &IndexingAgreementId,
        ) -> crate::registry::Result<Option<IndexingAgreement>> {
            if self.reads.fetch_add(1, Ordering::SeqCst) > 0 && self.later_reads_fail {
                return Err(crate::registry::Error::NoRecordsUpdated);
            }
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
    /// handler sent when it must not.
    struct MockChainClient {
        offer_result: Mutex<Option<Result<Option<B256>, ChainClientError>>>,
        /// When set, the agreement is cancelled locally while the offer is sent,
        /// as the chain listener does when a replacement is accepted.
        cancel_mid_send: Option<SharedAgreement>,
        cancelled: Arc<Mutex<Vec<[u8; 16]>>>,
        /// Whether the agreement's offer (or the agreement) is live on-chain: set
        /// by a mined offer, cleared by a cancel.
        on_chain: Arc<AtomicBool>,
        fail_cancel: bool,
    }

    #[async_trait]
    impl ChainClient for MockChainClient {
        async fn offer_via_manager(
            &self,
            _rca: &dipper_rpc::indexer::indexer_client::sol::RecurringCollectionAgreement,
        ) -> Result<Option<B256>, ChainClientError> {
            if let Some(agreement) = &self.cancel_mid_send
                && let Some(row) = agreement.lock().unwrap().as_mut()
            {
                row.status = IndexingAgreementStatus::Cancelling;
            }
            let result = self
                .offer_result
                .lock()
                .unwrap()
                .take()
                .expect("offer_via_manager called more than once, or when it must not send");
            if matches!(result, Ok(Some(_))) {
                self.on_chain.store(true, Ordering::SeqCst);
            }
            result
        }
        async fn cancel_via_manager(
            &self,
            _collector: Address,
            agreement_id: &[u8; 16],
            _version_hash: B256,
            _options: u16,
        ) -> Result<Option<B256>, ChainClientError> {
            if self.fail_cancel {
                return Err(ChainClientError::RpcError(anyhow::anyhow!("rpc down")));
            }
            self.cancelled.lock().unwrap().push(*agreement_id);
            self.on_chain.store(false, Ordering::SeqCst);
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
            Ok(self.on_chain.load(Ordering::SeqCst))
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
        ctx_with(agreement, Some(offer_result))
    }

    /// `offer_result: None` makes any send panic.
    fn ctx_with(
        agreement: IndexingAgreement,
        offer_result: Option<Result<Option<B256>, ChainClientError>>,
    ) -> Ctx<MockRegistry, MockChainClient> {
        Ctx {
            registry: MockRegistry {
                agreement: Arc::new(Mutex::new(Some(agreement))),
                later_reads_fail: false,
                reads: AtomicU32::new(0),
            },
            chain_client: MockChainClient {
                offer_result: Mutex::new(offer_result),
                cancel_mid_send: None,
                cancelled: Arc::default(),
                on_chain: Arc::default(),
                fail_cancel: false,
            },
            agreement_conf: Arc::new(test_agreement_conf()),
        }
    }

    fn test_agreement_conf() -> crate::config::IndexingAgreementConfig {
        crate::config::IndexingAgreementConfig::for_tests()
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
    async fn retries_when_it_cannot_check_the_agreement_after_its_offer_lands() {
        //* Arrange - finishing here would leave the offer open had the agreement
        // been cancelled while it was sent
        let agreement = make_test_agreement();
        let message = make_message(agreement.id);
        let mut ctx = ctx_with_offer_result(agreement, Ok(Some(B256::repeat_byte(0xab))));
        ctx.registry.later_reads_fail = true;

        //* Act
        let result = handle(ctx, &message).await;

        //* Assert
        assert!(
            matches!(result, Err(JobError::Retryable(..))),
            "got {result:?}"
        );
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
    async fn skips_an_agreement_a_reassessment_already_cancelled() {
        //* Arrange - the reassessment ran first and cancelled the agreement; no offer
        // result is configured, so a send would panic
        let mut agreement = make_test_agreement();
        agreement.status = IndexingAgreementStatus::CanceledByRequester;
        let message = make_message(agreement.id);
        let ctx = ctx_with(agreement, None);
        let cancelled = ctx.chain_client.cancelled.clone();

        //* Act
        let result = handle(ctx, &message).await;

        //* Assert - and with no offer on-chain, nothing to withdraw
        assert!(result.is_ok(), "got {result:?}");
        assert!(cancelled.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn withdraws_a_stored_offer_of_an_agreement_already_cancelled() {
        for status in [
            IndexingAgreementStatus::Cancelling,
            IndexingAgreementStatus::CanceledByRequester,
        ] {
            //* Arrange - an earlier attempt sent the offer, then the agreement was
            // cancelled before this retry; no offer result, so a send would panic
            let mut agreement = make_test_agreement();
            agreement.status = status;
            agreement.terms_version_hash = Some(vec![7u8; 32]);
            let agreement_id = agreement.id;
            let message = make_message(agreement_id);
            let ctx = ctx_with(agreement, None);
            ctx.chain_client.on_chain.store(true, Ordering::SeqCst);
            let cancelled = ctx.chain_client.cancelled.clone();

            //* Act
            let result = handle(ctx, &message).await;

            //* Assert
            assert!(result.is_ok(), "got {result:?}");
            assert_eq!(*cancelled.lock().unwrap(), vec![*agreement_id.as_bytes()]);
        }
    }

    #[tokio::test]
    async fn retries_a_withdraw_that_fails() {
        //* Arrange - cancelled while the offer was in flight, and the withdraw fails
        let mut agreement = make_test_agreement();
        agreement.terms_version_hash = Some(vec![7u8; 32]);
        let message = make_message(agreement.id);
        let mut ctx = ctx_with_offer_result(agreement, Ok(Some(B256::repeat_byte(0xab))));
        ctx.chain_client.cancel_mid_send = Some(ctx.registry.agreement.clone());
        ctx.chain_client.fail_cancel = true;

        //* Act
        let result = handle(ctx, &message).await;

        //* Assert - a retry finds the row cancelled and withdraws through the check
        assert!(
            matches!(result, Err(JobError::Retryable(_, _))),
            "got {result:?}"
        );
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
