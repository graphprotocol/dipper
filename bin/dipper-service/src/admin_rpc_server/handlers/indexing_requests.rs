use std::{collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use dipper_core::{ids::IndexingRequestId, state::FromState};
use dipper_producer::events::SubgraphIndexingAgreementEventsProducer;
use dipper_rpc::admin::{
    SignedMessage,
    indexing_requests::{
        IndexingRequest, IndexingRequestStatus, IndexingRequestsRpcServer,
        SetIndexingTargetCandidates,
    },
};
use jsonrpsee::{core::RpcResult, types::ErrorObject};
use thegraph_core::{DeploymentId, alloy::primitives::Address};

use super::error_handling::{handle_list_result, handle_optional_result};
use crate::{
    registry::{
        IndexingRequest as IndexingRequestRecord, IndexingRequestRegistry,
        IndexingRequestStatus as IndexingRequestRecordStatus,
    },
    set_indexing_target::{ApplyError, SetIndexingTarget, apply_set_indexing_target},
    signing::eip712::Eip712Signer,
    worker::service::{JobPriority, WorkerQueue},
};

/// The substate for the [`IndexingRequestsRpc`] handler
///
/// See: https://docs.rs/axum/0.7.7/axum/extract/struct.State.html#substates
pub struct Ctx<R, W> {
    pub signer: Arc<Eip712Signer>,
    pub gateway_operator_allowlist: Arc<BTreeSet<Address>>,
    pub registry: R,
    pub worker: W,
    pub max_candidates: usize,
    pub subgraph_indexing_agreements_events_emitter:
        Arc<dyn SubgraphIndexingAgreementEventsProducer>,
}

pub struct RpcServerImpl<R, W>(Ctx<R, W>);

impl<R, W> RpcServerImpl<R, W> {
    /// Create a new instance of the `IndexingRequestsRpcServerImpl` with the given context
    pub fn with_context<C>(ctx: &C) -> Self
    where
        Ctx<R, W>: FromState<C>,
    {
        Self(FromState::from_state(ctx))
    }
}

#[async_trait]
impl<R, W> IndexingRequestsRpcServer for RpcServerImpl<R, W>
where
    R: IndexingRequestRegistry + Clone + Send + Sync + 'static,
    W: WorkerQueue + Clone + Send + Sync + 'static,
{
    async fn get_all_indexing_requests(&self) -> RpcResult<Vec<IndexingRequest>> {
        handle_list_result(
            self.registry.get_all_indexing_requests().await,
            "Failed to get all indexing requests",
            into_indexing_request,
        )
    }

    async fn get_indexing_request_by_id(
        &self,
        id: IndexingRequestId,
    ) -> RpcResult<IndexingRequest> {
        handle_optional_result(
            self.registry.get_indexing_request_by_id(&id).await,
            "Failed to get indexing request by id",
            into_indexing_request,
        )
    }

    async fn get_indexing_requests_by_deployment_id(
        &self,
        deployment_id: DeploymentId,
    ) -> RpcResult<Vec<IndexingRequest>> {
        handle_list_result(
            self.registry
                .get_indexing_requests_by_deployment_id(&deployment_id)
                .await,
            "Failed to get indexing requests by deployment id",
            into_indexing_request,
        )
    }

    async fn set_indexing_target_candidates(
        &self,
        req: SignedMessage<SetIndexingTargetCandidates>,
    ) -> RpcResult<Option<IndexingRequestId>> {
        let requested_by = match self.signer.recover_signer(&req) {
            Ok(addr) => addr,
            Err(err) => {
                tracing::debug!(error=?err, "Failed to recover signer");
                return Err(ErrorObject::borrowed(401, "Unauthorized", None));
            }
        };
        if !self.gateway_operator_allowlist.contains(&requested_by) {
            return Err(ErrorObject::borrowed(403, "Forbidden", None));
        }

        let SetIndexingTargetCandidates {
            deployment_id,
            chain_id,
            num_candidates,
        } = req.into_message();

        let num_candidates = num_candidates.unwrap_or(self.max_candidates);

        apply_set_indexing_target(
            &self.registry,
            &self.worker,
            &self.subgraph_indexing_agreements_events_emitter,
            self.signer.chain_id(),
            SetIndexingTarget {
                requested_by,
                deployment_id,
                deployment_chain_id: chain_id,
                num_candidates,
                // Interactive: a caller is waiting on this set-target result.
                priority: JobPriority::Interactive,
            },
        )
        .await
        .map_err(|err| match err {
            ApplyError::Registry(_) => ErrorObject::borrowed(503, "Service unavailable", None),
            ApplyError::QueueReassess { .. } => {
                ErrorObject::borrowed(500, "Internal server error", None)
            }
        })
    }
}

impl<R, W> std::ops::Deref for RpcServerImpl<R, W> {
    type Target = Ctx<R, W>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

fn into_indexing_request(request: IndexingRequestRecord) -> IndexingRequest {
    IndexingRequest {
        id: request.id,
        created_at: request.created_at,
        updated_at: request.updated_at,
        status: into_indexing_request_status(request.status),
        requested_by: request.requested_by,
        deployment_id: request.deployment_id,
    }
}

fn into_indexing_request_status(status: IndexingRequestRecordStatus) -> IndexingRequestStatus {
    match status {
        IndexingRequestRecordStatus::Open => IndexingRequestStatus::Open,
        IndexingRequestRecordStatus::Canceled => IndexingRequestStatus::Canceled,
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
    use dipper_rpc::admin::indexing_requests::SetIndexingTargetCandidates;
    use thegraph_core::{
        DeploymentId,
        alloy::{
            primitives::{Address, ChainId},
            signers::local::PrivateKeySigner,
        },
        deployment_id,
        signed_message::sign,
    };
    use url::Url;

    use super::*;
    use crate::{
        registry::{Result as RegistryResult, SetTargetOutcome},
        test_support::{CapturedEvent, CapturingEventsProducer},
        worker::queue::JobId,
    };

    /// The protocol (signer) chain id. Deliberately different from the
    /// request's deployment `chain_id` so the assertion that the emitted
    /// event carries the *signer* chain id is meaningful.
    const SIGNER_CHAIN_ID: ChainId = 42161;

    /// The request's deployment chain id (a data-source chain), intentionally
    /// distinct from [`SIGNER_CHAIN_ID`].
    const DEPLOYMENT_CHAIN_ID: ChainId = 1;

    /// A registry whose `set_indexing_target_candidates` returns a configured
    /// outcome. All other trait methods are unused by the handler path under
    /// test and therefore `unimplemented!()`.
    #[derive(Clone)]
    struct MockRegistry {
        outcome: SetTargetOutcome,
    }

    #[async_trait]
    impl IndexingRequestRegistry for MockRegistry {
        async fn set_indexing_target_candidates(
            &self,
            _requested_by: Address,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
            _num_candidates: usize,
        ) -> RegistryResult<SetTargetOutcome> {
            Ok(self.outcome.clone())
        }

        async fn get_all_indexing_requests(&self) -> RegistryResult<Vec<IndexingRequestRecord>> {
            unimplemented!()
        }

        async fn get_indexing_request_by_id(
            &self,
            _id: &IndexingRequestId,
        ) -> RegistryResult<Option<IndexingRequestRecord>> {
            unimplemented!()
        }

        async fn get_indexing_requests_by_deployment_id(
            &self,
            _deployment_id: &DeploymentId,
        ) -> RegistryResult<Vec<IndexingRequestRecord>> {
            unimplemented!()
        }

        async fn get_open_indexing_requests_for_reassessment(
            &self,
            _min_age_seconds: i64,
            _batch_size: i64,
        ) -> RegistryResult<Vec<IndexingRequestRecord>> {
            unimplemented!()
        }
    }

    /// A worker that accepts `reassess_indexing_request` and returns a default
    /// `JobId`. All other queue methods are unused by this path.
    #[derive(Clone)]
    struct MockWorker;

    #[async_trait]
    impl WorkerQueue for MockWorker {
        async fn send_indexing_agreement_proposal(
            &self,
            _candidate_url: Url,
            _agreement_id: IndexingAgreementId,
            _indexing_request_id: IndexingRequestId,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
            _priority: crate::worker::queue::JobPriority,
        ) -> anyhow::Result<JobId> {
            unimplemented!()
        }

        async fn reassess_indexing_request(
            &self,
            _indexing_request_id: IndexingRequestId,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
            _num_candidates: usize,
            _priority: crate::worker::queue::JobPriority,
        ) -> anyhow::Result<JobId> {
            Ok(JobId::default())
        }

        async fn cancel_rejected_agreement_on_chain(
            &self,
            _agreement_id: IndexingAgreementId,
            _priority: crate::worker::queue::JobPriority,
        ) -> anyhow::Result<JobId> {
            unimplemented!()
        }

        async fn submit_offer(
            &self,
            _agreement_id: IndexingAgreementId,
            _indexing_request_id: IndexingRequestId,
            _indexer_url: Url,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
            _priority: crate::worker::queue::JobPriority,
        ) -> anyhow::Result<JobId> {
            unimplemented!()
        }
    }

    /// Build a signed `set_indexing_target_candidates` request under the admin
    /// EIP-712 domain, returning both the signer (so its address can be
    /// allowlisted) and the wrapped `SignedMessage`.
    fn signed_request(
        deployment_id: DeploymentId,
        chain_id: ChainId,
        num_candidates: Option<usize>,
    ) -> (PrivateKeySigner, SignedMessage<SetIndexingTargetCandidates>) {
        let signer = PrivateKeySigner::random();
        let domain = dipper_rpc::admin::eip712_domain();
        let message = SetIndexingTargetCandidates {
            deployment_id,
            chain_id,
            num_candidates,
        };
        let inner = sign(&signer, &domain, message).expect("signing failed");
        (signer, inner.into())
    }

    /// Assemble an `RpcServerImpl` with [`SIGNER_CHAIN_ID`], allowlist
    /// `allowed`, and a registry returning `outcome`; also returns the shared
    /// events capture for assertions.
    fn server(
        allowed: Address,
        outcome: SetTargetOutcome,
    ) -> (
        RpcServerImpl<MockRegistry, MockWorker>,
        CapturingEventsProducer,
    ) {
        let domain = dipper_rpc::admin::eip712_domain();
        // The signer's own address is irrelevant to recovery here (the handler
        // recovers the *message* signer), so any address works.
        let signer = Eip712Signer::new(SIGNER_CHAIN_ID, domain);

        let events = CapturingEventsProducer::new();

        let ctx = Ctx {
            signer: Arc::new(signer),
            gateway_operator_allowlist: Arc::new(BTreeSet::from([allowed])),
            registry: MockRegistry { outcome },
            worker: MockWorker,
            max_candidates: 10,
            subgraph_indexing_agreements_events_emitter: Arc::new(events.clone()),
        };

        (RpcServerImpl(ctx), events)
    }

    #[tokio::test]
    async fn inserted_emits_single_request_received_with_signer_chain_id() {
        let deployment = deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv");
        let num_candidates = 7usize;

        let (signer, req) = signed_request(deployment, DEPLOYMENT_CHAIN_ID, Some(num_candidates));
        let (server, events) = server(
            signer.address(),
            SetTargetOutcome::Inserted {
                id: IndexingRequestId::new(),
            },
        );

        let result = server.set_indexing_target_candidates(req).await;
        assert!(result.is_ok(), "handler returned an error: {result:?}");

        let captured = events.events();
        assert_eq!(captured.len(), 1, "expected exactly one emitted event");

        match &captured[0] {
            CapturedEvent::RequestReceived {
                deployment: ev_deployment,
                chain_id: ev_chain_id,
                event,
            } => {
                assert_eq!(*ev_deployment, deployment, "deployment id mismatch");
                assert_eq!(
                    *ev_chain_id, SIGNER_CHAIN_ID,
                    "event must carry the signer (protocol) chain id, not the deployment chain id"
                );
                assert_ne!(
                    *ev_chain_id, DEPLOYMENT_CHAIN_ID,
                    "event chain id must not be the request's deployment chain id"
                );
                assert_eq!(
                    event.agreements_requested, num_candidates as i32,
                    "agreements_requested mismatch"
                );
            }
            other => panic!("expected RequestReceived, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn noop_emits_no_event() {
        assert_no_event_for(SetTargetOutcome::NoOp {
            id: IndexingRequestId::new(),
        })
        .await;
    }

    #[tokio::test]
    async fn updated_emits_no_event() {
        assert_no_event_for(SetTargetOutcome::Updated {
            id: IndexingRequestId::new(),
            new_num_candidates: 3,
        })
        .await;
    }

    #[tokio::test]
    async fn canceled_emits_no_event() {
        assert_no_event_for(SetTargetOutcome::Canceled {
            id: IndexingRequestId::new(),
        })
        .await;
    }

    /// Drive the handler with `outcome` and assert no lifecycle event is
    /// emitted. `Updated`/`Canceled`/`NoOp` are not "request received".
    async fn assert_no_event_for(outcome: SetTargetOutcome) {
        let deployment = deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv");

        let (signer, req) = signed_request(deployment, DEPLOYMENT_CHAIN_ID, Some(5));
        let (server, events) = server(signer.address(), outcome);

        let result = server.set_indexing_target_candidates(req).await;
        assert!(result.is_ok(), "handler returned an error: {result:?}");

        assert!(
            events.events().is_empty(),
            "expected no emitted events, got {:?}",
            events.events()
        );
    }
}
