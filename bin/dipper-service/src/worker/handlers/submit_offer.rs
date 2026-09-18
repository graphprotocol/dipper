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

use std::time::Duration;

use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
use thegraph_core::{DeploymentId, alloy::primitives::ChainId};
use url::Url;

use crate::{
    chain_client::{ChainClient, ChainClientError, decode_revert_reason},
    indexer_rpc_client::into_sol_rca,
    registry::{AgreementRegistry, IndexingAgreementStatus},
    worker::result::{JobError, JobResult},
};

/// Backoff base for a tx the RPC accepted and then dropped from the mempool.
pub const DROPPED_TX_RETRY_BASE: Duration = Duration::from_secs(5);

/// Backoff base for a transient submission failure: RPC, gas or nonce.
pub const TRANSIENT_RETRY_BASE: Duration = Duration::from_secs(30);

pub struct Ctx<R, T> {
    pub registry: R,
    pub chain_client: T,
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

    // Offer is confirmed on-chain (or was already there). The indexer-agent will
    // pick up the pending_rca_proposals row and call acceptIndexingAgreement. No
    // further enqueue needed; chain_listener detects the acceptance event.
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

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

    struct MockRegistry {
        agreement: Option<IndexingAgreement>,
    }

    #[async_trait]
    impl StubAgreementRegistry for MockRegistry {
        async fn get_indexing_agreement_by_id(
            &self,
            _id: &IndexingAgreementId,
        ) -> crate::registry::Result<Option<IndexingAgreement>> {
            Ok(self.agreement.clone())
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
    /// retried inside one run, which must never happen.
    struct MockChainClient {
        offer_result: Mutex<Option<Result<Option<B256>, ChainClientError>>>,
    }

    #[async_trait]
    impl ChainClient for MockChainClient {
        async fn offer_via_manager(
            &self,
            _rca: &dipper_rpc::indexer::indexer_client::sol::RecurringCollectionAgreement,
        ) -> Result<Option<B256>, ChainClientError> {
            self.offer_result
                .lock()
                .unwrap()
                .take()
                .expect("offer_via_manager called more than once")
        }
        async fn cancel_via_manager(
            &self,
            _collector: Address,
            _agreement_id: &[u8; 16],
            _version_hash: B256,
            _options: u16,
        ) -> Result<Option<B256>, ChainClientError> {
            unimplemented!()
        }
        async fn reconcile_provider(
            &self,
            _collector: Address,
            _provider: Address,
        ) -> Result<Option<B256>, ChainClientError> {
            unimplemented!()
        }
        async fn agreement_still_active(
            &self,
            _agreement_id: &[u8; 16],
        ) -> Result<bool, ChainClientError> {
            unimplemented!()
        }
        async fn fetch_agreement_version_hash(
            &self,
            _agreement_id: &[u8; 16],
        ) -> Result<Option<B256>, ChainClientError> {
            unimplemented!()
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
        Ctx {
            registry: MockRegistry {
                agreement: Some(agreement),
            },
            chain_client: MockChainClient {
                offer_result: Mutex::new(Some(offer_result)),
            },
        }
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
