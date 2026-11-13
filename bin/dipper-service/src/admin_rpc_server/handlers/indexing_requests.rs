use std::{collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use dipper_core::{ids::IndexingRequestId, state::FromState};
use dipper_producer::{events::SubgraphIndexingAgreementEventsProducer, proto};
use dipper_rpc::admin::{
    SignedMessage,
    indexing_requests::{
        DEFAULT_PAGE_SIZE, IndexingRequest, IndexingRequestStatus, IndexingRequestsPage,
        IndexingRequestsRpcServer, MAX_PAGE_SIZE, SetIndexingTargetCandidates,
    },
};
use jsonrpsee::{core::RpcResult, types::ErrorObject};
use thegraph_core::{DeploymentId, alloy::primitives::Address};

use super::error_handling::{handle_list_result, handle_optional_result};
use crate::{
    config::MAX_NUM_CANDIDATES,
    registry::{
        IndexingRequest as IndexingRequestRecord, IndexingRequestRegistry,
        IndexingRequestStatus as IndexingRequestRecordStatus, RequestOrderRegistry,
        SetTargetOutcome,
    },
    signing::eip712::Eip712Signer,
    worker::service::{JobPriority, WorkerQueue},
};

/// How long after it was signed a request is still accepted. Long enough for a slow connection,
/// and short enough that a request someone recorded is soon no use to them.
const MAX_SIGNED_AT_AGE_SECS: u64 = 300;

/// How far ahead of this server's clock a request may be dated, for clocks that disagree a
/// little. Kept small because the newest signing time accepted blocks every request signed
/// before it: a request dated far ahead would lock its operator out until that time came.
const MAX_SIGNED_AT_AHEAD_SECS: u64 = 30;

/// The substate for the [`IndexingRequestsRpc`] handler
///
/// See: https://docs.rs/axum/0.7.7/axum/extract/struct.State.html#substates
pub struct Ctx<R, W> {
    pub signer: Arc<Eip712Signer>,
    pub gateway_operator_allowlist: Arc<BTreeSet<Address>>,
    pub registry: R,
    pub worker: W,
    pub max_candidates: usize,
    /// The current time in seconds since the Unix epoch, a field so tests can set it.
    pub clock: fn() -> u64,
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
    R: IndexingRequestRegistry + RequestOrderRegistry + Clone + Send + Sync + 'static,
    W: WorkerQueue + Clone + Send + Sync + 'static,
{
    async fn get_all_indexing_requests(
        &self,
        after: Option<IndexingRequestId>,
        limit: Option<u32>,
        status: Option<IndexingRequestStatus>,
    ) -> RpcResult<IndexingRequestsPage> {
        let limit = match limit {
            None => DEFAULT_PAGE_SIZE,
            Some(0) => {
                return Err(ErrorObject::borrowed(400, "limit must be at least 1", None));
            }
            // Capped, not refused, so a caller asking for "as many as you allow" gets a page.
            Some(limit) => limit.min(MAX_PAGE_SIZE),
        };
        let status = match status {
            None => None,
            Some(IndexingRequestStatus::Open) => Some(IndexingRequestRecordStatus::Open),
            Some(IndexingRequestStatus::Canceled) => Some(IndexingRequestRecordStatus::Canceled),
            // Any text that is not a status reads as `Unknown`. No stored request is in it, so
            // answering would return an empty page that looks like "none match".
            Some(IndexingRequestStatus::Unknown) => {
                return Err(ErrorObject::borrowed(
                    400,
                    "status must be OPEN or CANCELED",
                    None,
                ));
            }
        };
        match self
            .registry
            .get_indexing_requests_page(after, status, limit)
            .await
        {
            Ok(page) => Ok(IndexingRequestsPage {
                requests: page
                    .requests
                    .into_iter()
                    .map(into_indexing_request)
                    .collect(),
                next: page.next,
            }),
            Err(err) => {
                tracing::error!(error = ?err, "Failed to get a page of indexing requests");
                Err(ErrorObject::borrowed(503, "Service unavailable", None))
            }
        }
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

    #[expect(
        clippy::too_many_lines,
        reason = "predates this lint; fix when next touched"
    )]
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
            // A request signed for the dipper on another chain lands here too, since it
            // recovers to an address nobody allowed, so say which chain this one serves.
            return Err(ErrorObject::owned(
                403,
                format!(
                    "Forbidden: the request's signer {requested_by} is not an allowed operator. \
                     This dipper serves chain {}; a request signed for another chain recovers \
                     to a different signer",
                    self.signer.chain_id()
                ),
                None::<()>,
            ));
        }

        let SetIndexingTargetCandidates {
            deployment_id,
            chain_id,
            num_candidates,
            signed_at,
        } = req.into_message();

        let now = (self.clock)();
        let refusal = if now.saturating_sub(signed_at) > MAX_SIGNED_AT_AGE_SECS {
            Some(format!(
                "more than {MAX_SIGNED_AT_AGE_SECS} seconds before this server's clock ({now}); \
                 sign and send it again"
            ))
        } else if signed_at.saturating_sub(now) > MAX_SIGNED_AT_AHEAD_SECS {
            Some(format!(
                "more than {MAX_SIGNED_AT_AHEAD_SECS} seconds ahead of this server's clock \
                 ({now}); check the clock of the machine that signed it"
            ))
        } else {
            None
        };
        if let Some(refusal) = refusal {
            tracing::warn!(
                %requested_by,
                signed_at,
                now,
                "Refused a request signed too far from this server's clock"
            );
            return Err(ErrorObject::owned(
                401,
                format!("the request was signed at {signed_at}, {refusal}"),
                None::<()>,
            ));
        }

        let num_candidates = num_candidates.unwrap_or(self.max_candidates);
        let agreements_requested = match i32::try_from(num_candidates) {
            Ok(count) if num_candidates <= MAX_NUM_CANDIDATES => count,
            _ => {
                return Err(ErrorObject::owned(
                    400,
                    format!(
                        "num_candidates must be at most {MAX_NUM_CANDIDATES}, got {num_candidates}"
                    ),
                    None::<()>,
                ));
            }
        };

        // Last of the checks, so a request refused for any other reason leaves no mark behind
        // that would block the operator's next one.
        match self
            .registry
            .accept_request_signed_at(requested_by, deployment_id, chain_id, signed_at)
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                tracing::warn!(
                    %requested_by,
                    %deployment_id,
                    %chain_id,
                    signed_at,
                    "Refused a request signed no later than one already accepted"
                );
                return Err(ErrorObject::owned(
                    409,
                    format!(
                        "a request for this deployment and chain signed at or after {signed_at} \
                         was already accepted; sign and send it again"
                    ),
                    None::<()>,
                ));
            }
            Err(err) => {
                tracing::error!(error=?err, "Failed to check the request's signing time");
                return Err(ErrorObject::borrowed(503, "Service unavailable", None));
            }
        }

        let outcome = match self
            .registry
            .set_indexing_target_candidates(requested_by, deployment_id, chain_id, num_candidates)
            .await
        {
            Ok(outcome) => outcome,
            Err(err) => {
                tracing::error!(error=?err, "Failed to set indexing target candidates");
                return Err(ErrorObject::borrowed(503, "Service unavailable", None));
            }
        };

        // Translate the outcome into the appropriate follow-up worker job and the
        // wire-level return value.
        let (id_opt, reassess_count): (Option<IndexingRequestId>, Option<usize>) = match outcome {
            SetTargetOutcome::Inserted { id } => {
                tracing::info!(
                    indexing_request_id = %id,
                    %requested_by,
                    %deployment_id,
                    %chain_id,
                    num_candidates,
                    "Inserted new indexing request"
                );

                // A new request was received: emit the lifecycle event. Only the
                // `Inserted` outcome is a genuinely new request; `Updated`/`Canceled`
                // are later transitions in the lifecycle, not "request received".
                // `the_graph_network` is the protocol network (signer chain id), not
                // the deployment's data-source `chain_id`.
                self.subgraph_indexing_agreements_events_emitter
                    .produce_subgraph_indexing_agreement_request_received(
                        deployment_id,
                        self.signer.chain_id(),
                        proto::SubgraphIndexingAgreementRequestReceived {
                            agreements_requested,
                        },
                    );

                (Some(id), Some(num_candidates))
            }
            SetTargetOutcome::Updated {
                id,
                new_num_candidates,
            } => {
                tracing::info!(
                    indexing_request_id = %id,
                    %requested_by,
                    %deployment_id,
                    %chain_id,
                    num_candidates = new_num_candidates,
                    "Updated num_candidates on open indexing request"
                );
                (Some(id), Some(new_num_candidates))
            }
            SetTargetOutcome::NoOp { id } => {
                tracing::debug!(
                    indexing_request_id = %id,
                    "Set target candidates is a no-op (count unchanged)"
                );
                (Some(id), None)
            }
            SetTargetOutcome::Canceled { id } => {
                tracing::info!(
                    indexing_request_id = %id,
                    %requested_by,
                    %deployment_id,
                    %chain_id,
                    "Canceled indexing request (target candidates set to zero)"
                );
                (Some(id), Some(0))
            }
            SetTargetOutcome::NoOpAlreadyEmpty => {
                tracing::warn!(
                    %requested_by,
                    %deployment_id,
                    %chain_id,
                    "set_indexing_target_candidates with num_candidates=0 against a key with no open request \
                     - nothing to cancel"
                );
                (None, None)
            }
        };

        // Queue reassessment if the row changed. Reassessment computes the
        // diff between the IISA target group of size `num_candidates` and the
        // current active agreements, then grows or shrinks accordingly. With
        // num_candidates=0 it shrinks to zero, firing the on-chain cancel for
        // every active agreement on the key.
        if let (Some(id), Some(count)) = (id_opt, reassess_count)
            && let Err(err) = self
                .worker
                .reassess_indexing_request(
                    id,
                    deployment_id,
                    chain_id,
                    count,
                    // Interactive: a caller is waiting on this set-target result.
                    JobPriority::Interactive,
                )
                .await
        {
            tracing::error!(
                indexing_request_id = %id,
                error = ?err,
                "Failed to queue task: 'reassess_indexing_request'"
            );
            return Err(ErrorObject::borrowed(500, "Internal server error", None));
        }

        Ok(id_opt)
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
        registry::{RequestsPage, Result as RegistryResult},
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

    /// The time on the test server's clock, in seconds since the Unix epoch.
    const NOW: u64 = 1_800_000_000;

    fn clock() -> u64 {
        NOW
    }

    /// A registry whose `set_indexing_target_candidates` returns a configured
    /// outcome. All other trait methods are unused by the handler path under
    /// test and therefore `unimplemented!()`.
    #[derive(Clone)]
    struct MockRegistry {
        outcome: SetTargetOutcome,
        /// The indexer count of every request that got past the signature check.
        accepted_counts: Arc<std::sync::Mutex<Vec<usize>>>,
        /// The newest signing time accepted so far, kept the way the database keeps it.
        newest_signed_at: Arc<std::sync::Mutex<Option<u64>>>,
        /// The page every listing returns.
        page: RequestsPage,
        /// What each listing asked for: where to start, which status, how many.
        pages_asked: Arc<std::sync::Mutex<Vec<PageAsked>>>,
    }

    type PageAsked = (
        Option<IndexingRequestId>,
        Option<IndexingRequestRecordStatus>,
        u32,
    );

    #[async_trait]
    impl RequestOrderRegistry for MockRegistry {
        async fn accept_request_signed_at(
            &self,
            _requested_by: Address,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
            signed_at: u64,
        ) -> RegistryResult<bool> {
            let mut newest = self.newest_signed_at.lock().unwrap();
            let is_newest = newest.is_none_or(|newest| newest < signed_at);
            if is_newest {
                *newest = Some(signed_at);
            }
            Ok(is_newest)
        }
    }

    #[async_trait]
    impl IndexingRequestRegistry for MockRegistry {
        async fn set_indexing_target_candidates(
            &self,
            _requested_by: Address,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
            num_candidates: usize,
        ) -> RegistryResult<SetTargetOutcome> {
            self.accepted_counts.lock().unwrap().push(num_candidates);
            Ok(self.outcome.clone())
        }

        async fn get_indexing_requests_page(
            &self,
            after: Option<IndexingRequestId>,
            status: Option<IndexingRequestRecordStatus>,
            limit: u32,
        ) -> RegistryResult<RequestsPage> {
            self.pages_asked
                .lock()
                .unwrap()
                .push((after, status, limit));
            Ok(self.page.clone())
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
        async fn get_short_requests_on_deployments_with_ended_agreements(
            &self,
            _since_seconds: i64,
            _batch_size: i64,
        ) -> crate::registry::Result<Vec<crate::registry::IndexingRequest>> {
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
            _accept: Option<crate::worker::service::ChainAccept>,
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

    /// Build a signed `set_indexing_target_candidates` request using the
    /// canonical `thegraph_core::signed_message::sign` helper and the admin
    /// EIP-712 domain, returning both the signer (so its address can be
    /// allowlisted) and the wrapped `SignedMessage`.
    fn signed_request(
        deployment_id: DeploymentId,
        chain_id: ChainId,
        num_candidates: Option<usize>,
    ) -> (PrivateKeySigner, SignedMessage<SetIndexingTargetCandidates>) {
        let signer = PrivateKeySigner::random();
        let request = signed_for(
            SIGNER_CHAIN_ID,
            &signer,
            deployment_id,
            chain_id,
            num_candidates,
            NOW,
        );
        (signer, request)
    }

    /// Sign a `set_indexing_target_candidates` request at `signed_at`, for the dipper that
    /// serves the protocol on `protocol_chain`.
    fn signed_for(
        protocol_chain: ChainId,
        signer: &PrivateKeySigner,
        deployment_id: DeploymentId,
        chain_id: ChainId,
        num_candidates: Option<usize>,
        signed_at: u64,
    ) -> SignedMessage<SetIndexingTargetCandidates> {
        let domain = dipper_rpc::admin::eip712_domain(protocol_chain);
        let message = SetIndexingTargetCandidates {
            deployment_id,
            chain_id,
            num_candidates,
            signed_at,
        };
        sign(signer, &domain, message)
            .expect("signing failed")
            .into()
    }

    /// What anyone who saw `request` on the wire can send without the key: the same signature
    /// over a body of their choosing, here with `num_candidates` swapped.
    fn with_count(
        request: &SignedMessage<SetIndexingTargetCandidates>,
        num_candidates: Option<usize>,
    ) -> SignedMessage<SetIndexingTargetCandidates> {
        SignedMessage::new(
            SetIndexingTargetCandidates {
                deployment_id: request.message().deployment_id,
                chain_id: request.message().chain_id,
                num_candidates,
                signed_at: request.message().signed_at,
            },
            request.signature,
        )
    }

    /// Assemble an `RpcServerImpl` whose signer's chain id is
    /// [`SIGNER_CHAIN_ID`], whose allowlist contains `allowed`, and whose
    /// registry returns `outcome`. Returns the server and the shared events
    /// capture for assertions.
    fn server(
        allowed: Address,
        outcome: SetTargetOutcome,
    ) -> (
        RpcServerImpl<MockRegistry, MockWorker>,
        CapturingEventsProducer,
    ) {
        server_on(SIGNER_CHAIN_ID, allowed, outcome)
    }

    /// [`server`], for a dipper whose protocol network is `signer_chain`.
    fn server_on(
        signer_chain: ChainId,
        allowed: Address,
        outcome: SetTargetOutcome,
    ) -> (
        RpcServerImpl<MockRegistry, MockWorker>,
        CapturingEventsProducer,
    ) {
        let domain = dipper_rpc::admin::eip712_domain(signer_chain);
        // The signer's own address is irrelevant to recovery here (the handler
        // recovers the *message* signer), so any address works.
        let signer = Eip712Signer::new(signer_chain, domain);

        let events = CapturingEventsProducer::new();

        let ctx = Ctx {
            signer: Arc::new(signer),
            gateway_operator_allowlist: Arc::new(BTreeSet::from([allowed])),
            registry: MockRegistry {
                outcome,
                accepted_counts: Arc::default(),
                newest_signed_at: Arc::default(),
                page: RequestsPage::default(),
                pages_asked: Arc::default(),
            },
            worker: MockWorker,
            max_candidates: 10,
            clock,
            subgraph_indexing_agreements_events_emitter: Arc::new(events.clone()),
        };

        (RpcServerImpl(ctx), events)
    }

    /// A server for the listing tests, which no signature or outcome matters to.
    fn listing_server() -> RpcServerImpl<MockRegistry, MockWorker> {
        let outcome = SetTargetOutcome::Inserted {
            id: IndexingRequestId::new(),
        };
        server(Address::ZERO, outcome).0
    }

    fn stored_request(id: IndexingRequestId) -> IndexingRequestRecord {
        IndexingRequestRecord {
            id,
            created_at: time::OffsetDateTime::UNIX_EPOCH,
            updated_at: time::OffsetDateTime::UNIX_EPOCH,
            status: IndexingRequestRecordStatus::Open,
            requested_by: Address::ZERO,
            deployment_id: deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv"),
            deployment_chain_id: DEPLOYMENT_CHAIN_ID,
            num_candidates: 1,
        }
    }

    #[tokio::test]
    async fn a_listing_is_1_page_by_default_and_never_larger_than_the_cap() {
        for (asked, read) in [
            (None, 1_000),
            (Some(1), 1),
            (Some(5_000), 5_000),
            (Some(5_001), 5_000),
            (Some(u32::MAX), 5_000),
        ] {
            let server = listing_server();

            server
                .get_all_indexing_requests(None, asked, None)
                .await
                .unwrap();

            let pages_asked = server.registry.pages_asked.lock().unwrap().clone();
            assert_eq!(pages_asked, [(None, None, read)], "asked for {asked:?}");
        }
    }

    #[tokio::test]
    async fn a_listing_of_0_requests_is_refused() {
        let server = listing_server();

        let err = server
            .get_all_indexing_requests(None, Some(0), None)
            .await
            .err()
            .expect("a page of 0 requests should be refused");

        assert_eq!(err.code(), 400, "{err:?}");
        assert!(err.message().contains("limit"), "{err:?}");
        assert!(server.registry.pages_asked.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_listing_by_a_status_that_is_neither_open_nor_cancelled_is_refused() {
        // Any text that is not a status reads as `Unknown`, which no stored request is in, so
        // answering it would return an empty list that looks like "none match".
        let server = listing_server();

        let err = server
            .get_all_indexing_requests(None, None, Some(IndexingRequestStatus::Unknown))
            .await
            .err()
            .expect("an unknown status should be refused");

        assert_eq!(err.code(), 400, "{err:?}");
        assert!(err.message().contains("status"), "{err:?}");
        assert!(server.registry.pages_asked.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_listing_passes_on_where_to_start_and_the_status_and_returns_the_page() {
        let (first, second, start) = (
            IndexingRequestId::new(),
            IndexingRequestId::new(),
            IndexingRequestId::new(),
        );
        let mut server = listing_server();
        server.0.registry.page = RequestsPage {
            requests: vec![stored_request(first), stored_request(second)],
            next: Some(second),
        };

        let page = server
            .get_all_indexing_requests(Some(start), Some(2), Some(IndexingRequestStatus::Canceled))
            .await
            .unwrap();

        let pages_asked = server.registry.pages_asked.lock().unwrap().clone();
        assert_eq!(
            pages_asked,
            [(Some(start), Some(IndexingRequestRecordStatus::Canceled), 2)]
        );
        let ids: Vec<_> = page.requests.iter().map(|request| request.id).collect();
        assert_eq!(ids, [first, second]);
        assert_eq!(page.next, Some(second));
    }

    #[tokio::test]
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        reason = "predates this lint; fix when next touched"
    )]
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

    /// A count above the maximum is refused before anything is stored, including counts that a
    /// 32-bit column would otherwise wrap to a small or negative number.
    #[tokio::test]
    async fn refuses_a_count_above_the_maximum() {
        let deployment = deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv");
        for count in [MAX_NUM_CANDIDATES + 1, 2_147_483_648, 4_294_967_298] {
            let (signer, req) = signed_request(deployment, DEPLOYMENT_CHAIN_ID, Some(count));
            let (server, events) = server(
                signer.address(),
                SetTargetOutcome::Inserted {
                    id: IndexingRequestId::new(),
                },
            );

            let result = server.set_indexing_target_candidates(req).await;

            let err = result.expect_err("a count above the maximum must be refused");
            assert_eq!(err.code(), 400, "count {count}: {err:?}");
            assert_eq!(
                err.message(),
                format!("num_candidates must be at most {MAX_NUM_CANDIDATES}, got {count}")
            );
            assert!(
                events.events().is_empty(),
                "count {count} must not be recorded"
            );
        }
    }

    #[tokio::test]
    async fn accepts_the_maximum_count() {
        let deployment = deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv");
        let (signer, req) =
            signed_request(deployment, DEPLOYMENT_CHAIN_ID, Some(MAX_NUM_CANDIDATES));
        let (server, events) = server(
            signer.address(),
            SetTargetOutcome::Inserted {
                id: IndexingRequestId::new(),
            },
        );

        let result = server.set_indexing_target_candidates(req).await;

        assert!(result.is_ok(), "handler returned an error: {result:?}");
        assert_eq!(events.events().len(), 1);
    }

    /// The counts of the requests that got past every check and reached the registry.
    fn accepted_counts(server: &RpcServerImpl<MockRegistry, MockWorker>) -> Vec<usize> {
        server.registry.accepted_counts.lock().unwrap().clone()
    }

    /// A signed cancel (a count of 0) must not also pass as a request for the default count,
    /// which is what the same body asks for once the count is left out.
    #[tokio::test]
    async fn a_signed_cancel_is_refused_with_the_count_removed() {
        let deployment = deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv");
        let (signer, cancel) = signed_request(deployment, DEPLOYMENT_CHAIN_ID, Some(0));
        let (server, _events) = server(
            signer.address(),
            SetTargetOutcome::Inserted {
                id: IndexingRequestId::new(),
            },
        );

        let result = server
            .set_indexing_target_candidates(with_count(&cancel, None))
            .await;

        let err = result.expect_err("a cancel with its count removed must be refused");
        assert_eq!(err.code(), 403, "{err:?}");
        assert_eq!(accepted_counts(&server), Vec::<usize>::new());
    }

    /// A request for the default count (the count left out) must not also pass as a cancel.
    #[tokio::test]
    async fn a_signed_default_count_is_refused_as_a_cancel() {
        let deployment = deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv");
        let (signer, request) = signed_request(deployment, DEPLOYMENT_CHAIN_ID, None);
        let (server, _events) = server(
            signer.address(),
            SetTargetOutcome::Canceled {
                id: IndexingRequestId::new(),
            },
        );

        let result = server
            .set_indexing_target_candidates(with_count(&request, Some(0)))
            .await;

        let err = result.expect_err("a default-count request must be refused as a cancel");
        assert_eq!(err.code(), 403, "{err:?}");
        assert_eq!(accepted_counts(&server), Vec::<usize>::new());
    }

    /// A count left out is still accepted when it is what the operator signed, and asks for
    /// the server's default.
    #[tokio::test]
    async fn a_signed_default_count_asks_for_the_default() {
        let deployment = deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv");
        let (signer, request) = signed_request(deployment, DEPLOYMENT_CHAIN_ID, None);
        let (server, _events) = server(
            signer.address(),
            SetTargetOutcome::Inserted {
                id: IndexingRequestId::new(),
            },
        );

        let result = server.set_indexing_target_candidates(request).await;

        assert!(result.is_ok(), "handler returned an error: {result:?}");
        assert_eq!(accepted_counts(&server), vec![server.max_candidates]);
    }

    /// A request signed for the dipper on 1 network is refused by the dipper on another, even
    /// where the same operator key is allowed, and the refusal names the chain it serves.
    #[tokio::test]
    async fn a_request_for_one_network_is_refused_on_another() {
        const ARBITRUM_ONE: ChainId = 42161;
        const ARBITRUM_SEPOLIA: ChainId = 421_614;
        let deployment = deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv");
        let signer = PrivateKeySigner::random();
        let request = || {
            signed_for(
                ARBITRUM_ONE,
                &signer,
                deployment,
                DEPLOYMENT_CHAIN_ID,
                Some(3),
                NOW,
            )
        };
        let outcome = SetTargetOutcome::Inserted {
            id: IndexingRequestId::new(),
        };
        let (mainnet, _events) = server_on(ARBITRUM_ONE, signer.address(), outcome.clone());
        let (testnet, _events) = server_on(ARBITRUM_SEPOLIA, signer.address(), outcome);
        mainnet
            .set_indexing_target_candidates(request())
            .await
            .expect("accepted by the dipper it was signed for");

        let result = testnet.set_indexing_target_candidates(request()).await;

        let err = result.expect_err("a request for Arbitrum One must be refused on Sepolia");
        assert_eq!(err.code(), 403, "{err:?}");
        assert!(err.message().contains("serves chain 421614"), "{err:?}");
        assert_eq!(accepted_counts(&testnet), Vec::<usize>::new());
    }

    /// An old request is refused after a newer one: sending a recorded "3 indexers" again
    /// after the operator's cancel would otherwise bring the cancelled request back.
    #[tokio::test]
    async fn an_old_request_is_refused_after_a_newer_one() {
        let deployment = deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv");
        let signer = PrivateKeySigner::random();
        let sign = |count, signed_at| {
            signed_for(
                SIGNER_CHAIN_ID,
                &signer,
                deployment,
                DEPLOYMENT_CHAIN_ID,
                Some(count),
                signed_at,
            )
        };
        let (server, _events) = server(
            signer.address(),
            SetTargetOutcome::Inserted {
                id: IndexingRequestId::new(),
            },
        );
        server
            .set_indexing_target_candidates(sign(3, NOW - 10))
            .await
            .expect("the first request is accepted");
        server
            .set_indexing_target_candidates(sign(0, NOW))
            .await
            .expect("the later cancel is accepted");

        let result = server
            .set_indexing_target_candidates(sign(3, NOW - 10))
            .await;

        let err = result.expect_err("the old request must be refused after the cancel");
        assert_eq!(err.code(), 409, "{err:?}");
        assert_eq!(accepted_counts(&server), vec![3, 0]);
    }

    /// A request dated well ahead of the server's clock is refused. Accepted, it would become
    /// the newest signing time and block its operator's correctly dated requests until then.
    #[tokio::test]
    async fn a_request_dated_well_ahead_does_not_block_later_ones() {
        let deployment = deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv");
        let signer = PrivateKeySigner::random();
        let sign = |signed_at| {
            signed_for(
                SIGNER_CHAIN_ID,
                &signer,
                deployment,
                DEPLOYMENT_CHAIN_ID,
                Some(3),
                signed_at,
            )
        };
        let (server, _events) = server(
            signer.address(),
            SetTargetOutcome::Inserted {
                id: IndexingRequestId::new(),
            },
        );
        let from_a_fast_clock = server.set_indexing_target_candidates(sign(NOW + 200)).await;

        let result = server.set_indexing_target_candidates(sign(NOW)).await;

        let err = from_a_fast_clock.expect_err("a request dated 200 seconds ahead is refused");
        assert_eq!(err.code(), 401, "{err:?}");
        assert!(result.is_ok(), "handler returned an error: {result:?}");
        assert_eq!(accepted_counts(&server), vec![3]);
    }

    /// The same request is refused the second time it arrives.
    #[tokio::test]
    async fn a_request_sent_twice_is_refused_the_second_time() {
        let deployment = deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv");
        let signer = PrivateKeySigner::random();
        let sign = || {
            signed_for(
                SIGNER_CHAIN_ID,
                &signer,
                deployment,
                DEPLOYMENT_CHAIN_ID,
                Some(3),
                NOW,
            )
        };
        let (server, _events) = server(
            signer.address(),
            SetTargetOutcome::Inserted {
                id: IndexingRequestId::new(),
            },
        );
        server
            .set_indexing_target_candidates(sign())
            .await
            .expect("the request is accepted the first time");

        let result = server.set_indexing_target_candidates(sign()).await;

        let err = result.expect_err("the same request must be refused the second time");
        assert_eq!(err.code(), 409, "{err:?}");
        assert_eq!(accepted_counts(&server), vec![3]);
    }

    /// A count the server refuses leaves no trace: the same operator's next request, signed at
    /// the same second, is still accepted.
    #[tokio::test]
    async fn a_refused_count_does_not_block_the_next_request() {
        let deployment = deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv");
        let signer = PrivateKeySigner::random();
        let sign = |count| {
            signed_for(
                SIGNER_CHAIN_ID,
                &signer,
                deployment,
                DEPLOYMENT_CHAIN_ID,
                Some(count),
                NOW,
            )
        };
        let (server, _events) = server(
            signer.address(),
            SetTargetOutcome::Inserted {
                id: IndexingRequestId::new(),
            },
        );
        server
            .set_indexing_target_candidates(sign(MAX_NUM_CANDIDATES + 1))
            .await
            .expect_err("a count above the maximum is refused");

        let result = server.set_indexing_target_candidates(sign(3)).await;

        assert!(result.is_ok(), "handler returned an error: {result:?}");
        assert_eq!(accepted_counts(&server), vec![3]);
    }

    /// A request signed further from the server's clock than the limit is refused, whether it
    /// is old (a recorded request sent again later) or dated ahead.
    #[tokio::test]
    async fn a_request_signed_too_far_from_now_is_refused() {
        let deployment = deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv");
        let too_old = NOW - MAX_SIGNED_AT_AGE_SECS - 1;
        let too_far_ahead = NOW + MAX_SIGNED_AT_AHEAD_SECS + 1;
        for signed_at in [too_old, too_far_ahead, 0] {
            let signer = PrivateKeySigner::random();
            let request = signed_for(
                SIGNER_CHAIN_ID,
                &signer,
                deployment,
                DEPLOYMENT_CHAIN_ID,
                Some(3),
                signed_at,
            );
            let (server, events) = server(
                signer.address(),
                SetTargetOutcome::Inserted {
                    id: IndexingRequestId::new(),
                },
            );

            let result = server.set_indexing_target_candidates(request).await;

            let err = result.expect_err("a request signed too far from now must be refused");
            assert_eq!(err.code(), 401, "signed at {signed_at}: {err:?}");
            assert!(
                err.message().contains(&format!("signed at {signed_at}")),
                "{err:?}"
            );
            assert_eq!(accepted_counts(&server), Vec::<usize>::new());
            assert!(events.events().is_empty(), "signed at {signed_at}");
        }
    }

    /// A request signed exactly at the limit, on either side of the server's clock, is accepted.
    #[tokio::test]
    async fn a_request_signed_at_the_limit_is_accepted() {
        let deployment = deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv");
        let oldest = NOW - MAX_SIGNED_AT_AGE_SECS;
        let furthest_ahead = NOW + MAX_SIGNED_AT_AHEAD_SECS;
        for signed_at in [oldest, furthest_ahead] {
            let signer = PrivateKeySigner::random();
            let request = signed_for(
                SIGNER_CHAIN_ID,
                &signer,
                deployment,
                DEPLOYMENT_CHAIN_ID,
                Some(3),
                signed_at,
            );
            let (server, _events) = server(
                signer.address(),
                SetTargetOutcome::Inserted {
                    id: IndexingRequestId::new(),
                },
            );

            let result = server.set_indexing_target_candidates(request).await;

            assert!(result.is_ok(), "signed at {signed_at}: {result:?}");
            assert_eq!(accepted_counts(&server), vec![3]);
        }
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
