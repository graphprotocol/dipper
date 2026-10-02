use std::{collections::BTreeMap, sync::Arc};

use dipper_core::state::FromState;
use dipper_producer::events::SubgraphIndexingAgreementEventsProducer;
use graph_networks_registry::NetworksRegistry;
use thegraph_core::alloy::primitives::ChainId;
use tokio::sync::Notify;

pub use super::reassess_lock::ReassessLock;
use super::{
    handlers::{
        CancelRejectedAgreementOnChainCtx, ReassessIndexingRequestCtx,
        SendIndexingAgreementProposalCtx, SubmitOfferCtx,
    },
    unresponsive_breaker::{DipsAcceptingCache, UnresponsiveBreaker},
};
use crate::{
    config::{IndexingAgreementChainPrices, IndexingAgreementConfig},
    network::{provider::NetworkProviderService, service::entity_count_cache::EntityCountCache},
    signing::eip712::Eip712Signer,
};

/// Generates a `FromState<InnerCtx<...>>` impl mapping InnerCtx fields onto a
/// handler context type. Syntax: `impl_from_state!(Target<generics> { mappings })`,
/// where a mapping is `field` (same name) or `target: source` (renamed).
macro_rules! impl_from_state {
    (
        $target:ident < $($gen:ident),* > {
            $( $field:ident $(: $source:ident)? ),* $(,)?
        }
    ) => {
        impl<R, W, C, I, T> FromState<InnerCtx<R, W, C, I, T>>
            for $target < $($gen),* >
        where
            $( $gen: Clone, )*
        {
            #[inline]
            fn from_state(state: &InnerCtx<R, W, C, I, T>) -> Self {
                Self {
                    $( $field: impl_from_state!(@clone state, $field $(, $source)?), )*
                }
            }
        }
    };

    // Clone from a renamed source field
    (@clone $state:ident, $field:ident, $source:ident) => {
        $state.$source.clone()
    };

    // Clone from a field with the same name
    (@clone $state:ident, $field:ident) => {
        $state.$field.clone()
    };
}

/// The worker context
///
/// This is a input context for the worker service
#[derive(Clone)]
pub struct Ctx<Q, R, C, I, T> {
    /// The message queue worker
    pub queue: Q,

    /// The EIP-712 signer
    pub signer: Arc<Eip712Signer>,

    /// The _indexing agreement_ configuration
    pub agreement_conf: Arc<IndexingAgreementConfig>,

    /// The RCA EIP-712 domain (for computing the persisted terms hash).
    pub rca_domain: Arc<std::sync::RwLock<thegraph_core::alloy::sol_types::Eip712Domain>>,

    /// The _indexing agreement_ per-chain pricing table
    pub pricing_table: Arc<BTreeMap<ChainId, IndexingAgreementChainPrices>>,

    /// The DIPs registry
    pub registry: R,

    /// The Network provider
    pub network: NetworkProviderService,

    /// The indexer client
    pub client: C,

    /// The Indexing Indexer Selection Algorithm (IISA) service
    pub iisa: I,

    /// The chain client for on-chain transactions
    pub chain_client: T,

    /// The graph networks registry (maps chain IDs to network names)
    pub networks_registry: Arc<NetworksRegistry>,

    /// Additional chain ID to network name mappings for dev/test chains
    pub additional_networks: Arc<BTreeMap<ChainId, String>>,

    /// Shared entity count cache for optimistic fee estimation
    pub entity_count_cache: EntityCountCache,

    /// Wakes the chain_listener when proposals are dispatched
    pub chain_listener_notify: Arc<Notify>,

    /// Mirrors `ChainListenerConfig::bypass_chain_clock_defenses`. When true the
    /// reassess handler computes agreement deadlines from chain time instead of
    /// wall clock. Must stay false in production.
    pub bypass_chain_clock_defenses: bool,

    /// The chain ID the chain_listener tracks, used to look up
    /// `last_processed_block_timestamp` when bypass is on. `None`
    /// when the chain_listener is not configured.
    pub chain_listener_chain_id: Option<u64>,

    /// Liveness watermark the worker ticks each loop iteration so the health
    /// endpoint can detect a wedged worker.
    pub liveness: crate::health::Liveness,

    /// Global reassess lock (see `ReassessLock`).
    pub reassess_lock: ReassessLock,

    /// Suppresses the network-wide unresponsive exclusion during a dipper-side outage.
    pub unresponsive_breaker: Arc<UnresponsiveBreaker>,

    /// Short-TTL cache of IISA's DIPs-accepting set (the breaker's denominator).
    pub dips_accepting_cache: DipsAcceptingCache,

    /// Number of concurrent worker loops to spawn (>=1). Defaults to 1.
    pub concurrency: usize,

    /// Subgraph Indexing Agreements Event emitter for sending indexing agreement events downstream
    pub subgraph_indexing_agreements_events_emitter:
        Arc<dyn SubgraphIndexingAgreementEventsProducer>,
}

/// The inner worker context.
///
/// This is a shared context across all message handlers.
#[derive(Clone)]
pub(super) struct InnerCtx<R, W, C, I, T> {
    /// The EIP-712 signer
    pub signer: Arc<Eip712Signer>,

    /// The _indexing agreement_ configuration
    pub agreement_conf: Arc<IndexingAgreementConfig>,

    /// The RCA EIP-712 domain (for computing the persisted terms hash).
    pub rca_domain: Arc<std::sync::RwLock<thegraph_core::alloy::sol_types::Eip712Domain>>,

    /// The _indexing agreement_ per-chain pricing table
    pub pricing_table: Arc<BTreeMap<ChainId, IndexingAgreementChainPrices>>,

    /// The DIPs registry
    pub registry: R,

    /// The Network provider
    pub network: NetworkProviderService,

    /// The message queue worker
    pub worker: W,

    /// The indexer client
    pub client: C,

    /// The Indexing Indexer Selection Algorithm (IISA) service
    pub iisa: I,

    /// The chain client for on-chain transactions
    pub chain_client: T,

    /// The graph networks registry (maps chain IDs to network names)
    pub networks_registry: Arc<NetworksRegistry>,

    /// Additional chain ID to network name mappings for dev/test chains
    pub additional_networks: Arc<BTreeMap<ChainId, String>>,

    /// Shared entity count cache for optimistic fee estimation
    pub entity_count_cache: EntityCountCache,

    /// Wakes the chain_listener when proposals are dispatched
    pub chain_listener_notify: Arc<Notify>,

    /// See `Ctx::bypass_chain_clock_defenses`.
    pub bypass_chain_clock_defenses: bool,

    /// See `Ctx::chain_listener_chain_id`.
    pub chain_listener_chain_id: Option<u64>,

    /// See `Ctx::reassess_lock`.
    pub reassess_lock: ReassessLock,

    /// See `Ctx::unresponsive_breaker`.
    pub unresponsive_breaker: Arc<UnresponsiveBreaker>,

    /// See `Ctx::dips_accepting_cache`.
    pub dips_accepting_cache: DipsAcceptingCache,

    /// See: `Ctx::subgraph_indexing_agreements_events_emitter`
    pub subgraph_indexing_agreements_events_emitter:
        Arc<dyn SubgraphIndexingAgreementEventsProducer>,
}

impl_from_state!(ReassessIndexingRequestCtx<R, W, I, T> {
    signer,
    agreement_conf,
    rca_domain,
    chain_price: pricing_table,
    registry,
    network,
    queue: worker,
    iisa,
    chain_client,
    networks_registry,
    additional_networks,
    entity_count_cache,
    chain_listener_notify,
    bypass_chain_clock_defenses,
    chain_listener_chain_id,
    reassess_lock,
    unresponsive_breaker,
    dips_accepting_cache,
    subgraph_indexing_agreements_events_emitter
});

impl_from_state!(SendIndexingAgreementProposalCtx<R, W, C> {
    registry,
    queue: worker,
    indexer_client: client,
});

impl_from_state!(CancelRejectedAgreementOnChainCtx<R, T> {
    registry,
    chain_client,
    agreement_conf,
});

impl_from_state!(SubmitOfferCtx<R, T> {
    registry,
    chain_client,
    agreement_conf,
    reassess_lock,
});
