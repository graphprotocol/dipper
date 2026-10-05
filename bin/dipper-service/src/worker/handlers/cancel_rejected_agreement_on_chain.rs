//! Kept so jobs queued before an upgrade still run. The chain listener now moves an
//! agreement dipper rejected or cancelled that went live on-chain anyway back into
//! `Cancelling` itself, and the cancel retry ends it; this job does the same.

use std::time::Duration;

use dipper_core::ids::IndexingAgreementId;

use crate::{
    cancel_dispatch::reopen_if_live,
    chain_client::ChainClient,
    registry::{AgreementRegistry, IndexingAgreementStatus},
    worker::result::{JobError, JobResult},
};

pub struct Ctx<R, T> {
    pub registry: R,
    pub chain_client: T,
}

/// Cancel on-chain an agreement dipper rejected or cancelled that was accepted anyway.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Message {
    pub agreement_id: IndexingAgreementId,
}

/// Hand an agreement dipper rejected or cancelled that is live on-chain to the cancel retry.
pub async fn handle<R, T>(ctx: Ctx<R, T>, Message { agreement_id }: &Message) -> JobResult<()>
where
    R: AgreementRegistry + Sync,
    T: ChainClient,
{
    let Some(agreement) = ctx
        .registry
        .get_indexing_agreement_by_id(agreement_id)
        .await
        .map_err(|err| JobError::Fatal(err.into()))?
    else {
        tracing::warn!(%agreement_id, "Agreement not found for on-chain cancellation");
        return Ok(());
    };
    if !matches!(
        agreement.status,
        IndexingAgreementStatus::Rejected | IndexingAgreementStatus::CanceledByRequester
    ) {
        return Ok(());
    }
    reopen_if_live(&ctx.registry, &ctx.chain_client, &agreement)
        .await
        .map(|_| ())
        .map_err(|err| JobError::Retryable(err.into(), Duration::from_secs(30)))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use dipper_rpc::indexer::indexer_client::sol::RecurringCollectionAgreement;
    use thegraph_core::alloy::primitives::{Address, B256};

    use super::*;
    use crate::{
        cancel_dispatch::tests::agreement,
        chain_client::{AgreementOnChain, ChainClientError},
        registry::{IndexingAgreement, StubAgreementRegistry},
    };

    struct MockRegistry {
        agreement: IndexingAgreement,
        reopened: Arc<Mutex<Vec<IndexingAgreementId>>>,
    }

    #[async_trait]
    impl StubAgreementRegistry for MockRegistry {
        async fn get_indexing_agreement_by_id(
            &self,
            _id: &IndexingAgreementId,
        ) -> crate::registry::Result<Option<IndexingAgreement>> {
            Ok(Some(self.agreement.clone()))
        }
        async fn reopen_indexing_agreement_cancel(
            &self,
            id: &IndexingAgreementId,
        ) -> crate::registry::Result<()> {
            self.reopened.lock().unwrap().push(*id);
            Ok(())
        }
    }

    struct MockChain {
        live: bool,
    }

    #[async_trait]
    impl ChainClient for MockChain {
        async fn offer_via_manager(
            &self,
            _rca: &RecurringCollectionAgreement,
        ) -> Result<Option<B256>, ChainClientError> {
            unimplemented!()
        }
        async fn cancel_via_manager(
            &self,
            _collector: Address,
            _agreement_id: &[u8; 16],
            _version_hash: B256,
            _options: u16,
        ) -> Result<Option<B256>, ChainClientError> {
            panic!("the cancel retry sends cancels, not this job")
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
        async fn agreement_on_chain(
            &self,
            _agreement_id: &[u8; 16],
        ) -> Result<AgreementOnChain, ChainClientError> {
            Ok(AgreementOnChain::live_if(self.live))
        }
        async fn latest_block_timestamp(&self) -> Result<u64, ChainClientError> {
            unimplemented!()
        }
    }

    async fn run(status: IndexingAgreementStatus, live: bool) -> Vec<IndexingAgreementId> {
        let reopened = Arc::new(Mutex::new(Vec::new()));
        let agreement = agreement(status, Some(vec![7u8; 32]));
        let message = Message {
            agreement_id: agreement.id,
        };
        let ctx = Ctx {
            registry: MockRegistry {
                agreement,
                reopened: Arc::clone(&reopened),
            },
            chain_client: MockChain { live },
        };

        handle(ctx, &message).await.expect("job ok");

        reopened.lock().unwrap().clone()
    }

    #[tokio::test]
    async fn hands_a_live_agreement_dipper_ended_to_the_cancel_retry() {
        for status in [
            IndexingAgreementStatus::Rejected,
            IndexingAgreementStatus::CanceledByRequester,
        ] {
            assert_eq!(run(status, true).await.len(), 1, "{status}");
        }
    }

    #[tokio::test]
    async fn leaves_an_agreement_that_already_ended_or_is_still_wanted() {
        assert!(
            run(IndexingAgreementStatus::CanceledByRequester, false)
                .await
                .is_empty()
        );
        assert!(
            run(IndexingAgreementStatus::AcceptedOnChain, true)
                .await
                .is_empty()
        );
    }
}
