use std::{collections::BTreeSet, sync::Arc};

use dipper_core::state::FromState;
use dipper_producer::events::SubgraphIndexingAgreementEventsProducer;
use thegraph_core::alloy::primitives::Address;

use super::handlers::{IndexingAgreementsCtx, IndexingRequestsCtx};
use crate::{request_chain_check::RequestChainCheck, signing::eip712::Eip712Signer};

/// Shared context for the gateway operator API.
#[derive(Clone)]
pub struct Ctx<R, W> {
    /// EIP-712 signer for response authentication.
    pub signer: Arc<Eip712Signer>,

    /// Authorized gateway operator addresses (e.g., Graph Studio).
    pub gateway_operator_allowlist: Arc<BTreeSet<Address>>,

    /// The maximum number of candidates to select
    pub max_candidates: usize,

    /// The DIPs registry
    pub registry: R,

    /// The message queue worker
    pub worker: W,

    /// Subgraph Indexing Agreements Events emitter for sending on the topic, if configured and enabled
    pub subgraph_indexing_agreements_events_emitter:
        Arc<dyn SubgraphIndexingAgreementEventsProducer>,

    /// Refuses indexing requests whose chain is not their subgraph manifest's
    pub request_chain_check: Arc<dyn RequestChainCheck>,
}

impl<R, W> FromState<Ctx<R, W>> for IndexingRequestsCtx<R, W>
where
    R: Clone,
    W: Clone,
{
    fn from_state(ctx: &Ctx<R, W>) -> Self {
        Self {
            signer: ctx.signer.clone(),
            gateway_operator_allowlist: ctx.gateway_operator_allowlist.clone(),
            registry: ctx.registry.clone(),
            worker: ctx.worker.clone(),
            max_candidates: ctx.max_candidates,
            subgraph_indexing_agreements_events_emitter: ctx
                .subgraph_indexing_agreements_events_emitter
                .clone(),
            request_chain_check: ctx.request_chain_check.clone(),
        }
    }
}

impl<R, W> FromState<Ctx<R, W>> for IndexingAgreementsCtx<R>
where
    R: Clone,
{
    fn from_state(ctx: &Ctx<R, W>) -> Self {
        Self {
            registry: ctx.registry.clone(),
        }
    }
}
