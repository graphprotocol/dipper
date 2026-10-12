//! Moves an agreement dipper rejected or cancelled that is live on-chain after all back into
//! `Cancelling`, with the accept the chain listener saw on record. The listener queues this
//! rather than reading the chain itself, so a slow read can't hold up the rest of its page.

use std::time::Duration;

use dipper_core::ids::IndexingAgreementId;

use crate::{
    cancel_dispatch::{Reopen, reopen_if_live},
    chain_client::ChainClient,
    registry::{AgreementRegistry, IndexingAgreement, IndexingAgreementStatus},
    worker::result::{JobError, JobResult},
};

/// How long a job whose database read or write failed waits before running again.
const RETRY_DELAY: Duration = Duration::from_secs(30);

/// How long a job that couldn't read a cancelled agreement on-chain waits to read it again, at
/// the cancel retry's own pace. It keeps reading until the chain answers.
const CHAIN_READ_RETRY_DELAY: Duration = Duration::from_secs(300);

pub struct Ctx<R, T> {
    pub registry: R,
    pub chain_client: T,
}

/// Check on-chain an agreement dipper rejected or cancelled that was accepted anyway.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Message {
    pub agreement_id: IndexingAgreementId,
    /// The accept the chain listener saw; absent from jobs queued before it was carried.
    #[serde(default)]
    pub accept: Option<ChainAccept>,
}

/// When an agreement was accepted on-chain, and in which transaction.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChainAccept {
    pub at: u64,
    pub tx: String,
}

/// Hand an agreement dipper rejected or cancelled that is live on-chain to the cancel retry.
pub async fn handle<R, T>(
    ctx: Ctx<R, T>,
    Message {
        agreement_id,
        accept,
    }: &Message,
) -> JobResult<()>
where
    R: AgreementRegistry + Sync,
    T: ChainClient,
{
    let Some(agreement) = ctx
        .registry
        .get_indexing_agreement_by_id(agreement_id)
        .await
        .map_err(|err| retry(agreement_id, "read", err.into()))?
    else {
        tracing::warn!(%agreement_id, "Agreement not found for on-chain check");
        return Ok(());
    };
    let cancelling = match agreement.status {
        IndexingAgreementStatus::Rejected | IndexingAgreementStatus::CanceledByRequester => {
            reopen(&ctx, &agreement).await?
        }
        // Moved back by an earlier run of this job, or a copy of it, so only its accept is left.
        IndexingAgreementStatus::Cancelling => true,
        _ => return Ok(()),
    };
    if !cancelling {
        return Ok(());
    }
    let Some(accept) = accept else {
        return Ok(());
    };
    ctx.registry
        .record_accepted_audit(agreement_id, accept.at, &accept.tx)
        .await
        .map_err(|err| retry(agreement_id, "record the on-chain accept of", err.into()))
}

/// Move an agreement back into `Cancelling` if the chain shows it live; whether it moved.
async fn reopen<R, T>(ctx: &Ctx<R, T>, agreement: &IndexingAgreement) -> JobResult<bool>
where
    R: AgreementRegistry + Sync,
    T: ChainClient,
{
    let agreement_id = &agreement.id;
    match reopen_if_live(&ctx.registry, &ctx.chain_client, agreement)
        .await
        .map_err(|err| retry(agreement_id, "reopen the cancel of", err.into()))?
    {
        Reopen::Reopened => Ok(true),
        Reopen::Left => Ok(false),
        Reopen::Unread(err) => {
            tracing::warn!(
                %agreement_id,
                error = %err,
                retry_secs = CHAIN_READ_RETRY_DELAY.as_secs(),
                "Failed to read whether a cancelled agreement reported live is live; will read again"
            );
            Err(JobError::Deferred(CHAIN_READ_RETRY_DELAY))
        }
    }
}

/// Log a failed database read or write and run the job again later.
fn retry(agreement_id: &IndexingAgreementId, action: &str, err: anyhow::Error) -> JobError {
    tracing::warn!(
        %agreement_id,
        error = format!("{err:#}"),
        "Failed to {action} an agreement dipper ended that was accepted on-chain; will retry"
    );
    JobError::Retryable(err, RETRY_DELAY)
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
        registry::StubAgreementRegistry,
    };

    #[derive(Default)]
    struct State {
        agreement: Option<IndexingAgreement>,
        /// What each reopen was told about the chain.
        reopened: Vec<bool>,
        accepts: Vec<ChainAccept>,
        fail_read: bool,
        fail_accept: bool,
    }

    #[derive(Clone, Default)]
    struct MockRegistry(Arc<Mutex<State>>);

    fn db_down() -> crate::registry::Error {
        crate::registry::Error::BackendError(dipper_pgregistry::Error::DbError(
            sqlx::Error::Protocol("simulated outage".into()),
        ))
    }

    #[async_trait]
    impl StubAgreementRegistry for MockRegistry {
        async fn get_indexing_agreement_by_id(
            &self,
            _id: &IndexingAgreementId,
        ) -> crate::registry::Result<Option<IndexingAgreement>> {
            let state = self.0.lock().unwrap();
            if state.fail_read {
                return Err(db_down());
            }
            Ok(state.agreement.clone())
        }
        async fn reopen_indexing_agreement_cancel(
            &self,
            _id: &IndexingAgreementId,
            seen_live: bool,
        ) -> crate::registry::Result<()> {
            let mut state = self.0.lock().unwrap();
            let agreement = state.agreement.as_mut().expect("agreement");
            agreement.status = IndexingAgreementStatus::Cancelling;
            state.reopened.push(seen_live);
            Ok(())
        }
        async fn record_accepted_audit(
            &self,
            _agreement_id: &IndexingAgreementId,
            accepted_at: u64,
            accepted_tx: &str,
        ) -> crate::registry::Result<()> {
            let mut state = self.0.lock().unwrap();
            if state.fail_accept {
                return Err(db_down());
            }
            state.accepts.push(ChainAccept {
                at: accepted_at,
                tx: accepted_tx.to_string(),
            });
            Ok(())
        }
    }

    /// Every agreement reads as live or not, or the read fails when `live` is `None`.
    struct MockChain {
        live: Option<bool>,
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
            self.live
                .map(AgreementOnChain::live_if)
                .ok_or_else(|| ChainClientError::RpcError(anyhow::anyhow!("every endpoint failed")))
        }
        async fn latest_block_timestamp(&self) -> Result<u64, ChainClientError> {
            unimplemented!()
        }
    }

    fn chain_accept() -> ChainAccept {
        ChainAccept {
            at: 1_700_000_001,
            tx: "0x11".to_string(),
        }
    }

    fn registry(status: IndexingAgreementStatus) -> MockRegistry {
        let registry = MockRegistry::default();
        registry.0.lock().unwrap().agreement = Some(agreement(status, Some(vec![7u8; 32])));
        registry
    }

    async fn run_job(registry: &MockRegistry, live: Option<bool>) -> JobResult<()> {
        let agreement_id = registry.0.lock().unwrap().agreement.as_ref().unwrap().id;
        let message = Message {
            agreement_id,
            accept: Some(chain_accept()),
        };
        let ctx = Ctx {
            registry: registry.clone(),
            chain_client: MockChain { live },
        };
        handle(ctx, &message).await
    }

    #[tokio::test]
    async fn hands_a_live_agreement_dipper_ended_to_the_cancel_retry() {
        for status in [
            IndexingAgreementStatus::Rejected,
            IndexingAgreementStatus::CanceledByRequester,
        ] {
            let registry = registry(status);

            run_job(&registry, Some(true)).await.expect("job ok");

            let state = registry.0.lock().unwrap();
            assert_eq!(state.reopened, vec![true], "{status}");
            assert_eq!(
                state.accepts,
                vec![chain_accept()],
                "{status}: its accept is recorded, so the cancel retry treats it as paying"
            );
        }
    }

    #[tokio::test]
    async fn leaves_an_agreement_that_already_ended_or_is_still_wanted() {
        // An accept on record for one left as it is would be announced with no end to follow.
        for (status, live) in [
            (IndexingAgreementStatus::Rejected, false),
            (IndexingAgreementStatus::CanceledByRequester, false),
            (IndexingAgreementStatus::AcceptedOnChain, true),
        ] {
            let registry = registry(status);

            run_job(&registry, Some(live)).await.expect("job ok");

            let state = registry.0.lock().unwrap();
            assert!(state.reopened.is_empty(), "{status}");
            assert!(state.accepts.is_empty(), "{status}");
        }
    }

    #[tokio::test]
    async fn reopens_a_rejected_agreement_whose_chain_read_fails() {
        // Its indexer may be paid, so it is reopened; it may have ended, so the end on record
        // stays, and the cancel retry reads it again.
        let registry = registry(IndexingAgreementStatus::Rejected);

        run_job(&registry, None).await.expect("job ok");

        let state = registry.0.lock().unwrap();
        assert_eq!(state.reopened, vec![false]);
        assert_eq!(state.accepts, vec![chain_accept()]);
    }

    #[tokio::test]
    async fn reads_a_cancelled_agreement_again_when_its_chain_read_fails() {
        // Most likely dipper's cancel landed and the subgraph is behind, so it isn't reopened on
        // a guess, but it may be an offer accepted after dipper withdrew it, so it is read again.
        let registry = registry(IndexingAgreementStatus::CanceledByRequester);

        let unread = run_job(&registry, None).await;
        let read = run_job(&registry, Some(true)).await;

        assert!(matches!(
            unread,
            Err(JobError::Deferred(CHAIN_READ_RETRY_DELAY))
        ));
        read.expect("job ok");
        let state = registry.0.lock().unwrap();
        assert_eq!(state.reopened, vec![true], "reopened only once read live");
        assert_eq!(state.accepts, vec![chain_accept()]);
    }

    #[tokio::test]
    async fn retries_an_accept_that_failed_to_record_without_reopening_again() {
        let registry = registry(IndexingAgreementStatus::CanceledByRequester);
        registry.0.lock().unwrap().fail_accept = true;

        let first = run_job(&registry, Some(true)).await;
        registry.0.lock().unwrap().fail_accept = false;
        run_job(&registry, Some(true)).await.expect("retry ok");

        assert!(matches!(first, Err(JobError::Retryable(..))));
        let state = registry.0.lock().unwrap();
        assert_eq!(state.reopened, vec![true]);
        assert_eq!(state.accepts, vec![chain_accept()]);
    }

    #[tokio::test]
    async fn retries_when_the_agreement_cannot_be_read() {
        let registry = registry(IndexingAgreementStatus::Rejected);
        registry.0.lock().unwrap().fail_read = true;

        let result = run_job(&registry, Some(true)).await;

        assert!(matches!(result, Err(JobError::Retryable(..))));
        assert!(registry.0.lock().unwrap().reopened.is_empty());
    }

    /// A job queued before the listener carried the accept still runs.
    #[test]
    fn reads_a_job_queued_without_an_accept() {
        let id = IndexingAgreementId::from_bytes([1; 16]);
        let queued = serde_json::json!({ "agreement_id": id });

        let message: Message = serde_json::from_value(queued).expect("old message");

        assert_eq!(message.agreement_id, id);
        assert_eq!(message.accept, None);
    }
}
