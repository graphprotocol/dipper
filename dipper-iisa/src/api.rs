use std::collections::HashMap;

use async_trait::async_trait;
use thegraph_core::{DeploymentId, IndexerId};

/// Context for load balancing during indexer selection.
///
/// This context provides the IISA with information about current system state,
/// enabling intelligent load balancing decisions rather than naive selection.
#[derive(Debug, Clone, Default)]
pub struct SelectionContext {
    /// Indexer IDs that already have active agreements for this deployment.
    ///
    /// Used to inform the IISA about indexers that are already working on this deployment.
    pub existing_indexers: Vec<IndexerId>,

    /// For each deployment, the indexers that have pending/active agreements for it.
    ///
    /// Used to exclude indexers with pending work from new assignments.
    /// Key: Deployment ID, Value: List of indexer IDs working on that deployment.
    pub pending_agreements: HashMap<DeploymentId, Vec<IndexerId>>,

    /// Indexer IDs that should be excluded from selection entirely.
    ///
    /// Used for indexers that have been flagged for poor performance, trust issues,
    /// or other reasons that make them unsuitable for any deployment.
    /// Mapped to `blocklist` in the IISA request.
    pub indexer_denylist: Vec<IndexerId>,

    /// For each deployment, indexers that have recently declined agreements.
    ///
    /// Used to avoid re-offering agreements to indexers that recently declined.
    /// Key: Deployment ID, Value: List of indexer IDs that declined.
    pub declined_indexers: HashMap<DeploymentId, Vec<IndexerId>>,

    /// Chain ID of the deployment (e.g., "arbitrum-one").
    ///
    /// Used by IISA to filter indexers by supported chain and to look up
    /// chain-specific price ceilings.
    pub chain_id: Option<String>,

    /// Maximum GRT per 30 days for this chain (payment ceiling).
    ///
    /// Indexers with advertised prices above this ceiling are excluded from selection.
    pub max_grt_per_30_days: Option<f64>,

    /// Maximum GRT per billion entities per 30 days (per-entity payment ceiling).
    ///
    /// Indexers with an advertised per-entity price above this ceiling are excluded.
    pub max_grt_per_billion_entities_per_30_days: Option<f64>,

    /// Expected DIPs fees per indexer in GRT per 30 days.
    ///
    /// Derived from the base rate (`tokens_per_second`) in accepted agreement vouchers,
    /// plus entity rates from on-chain collection events when available. IISA adds
    /// these to Redpanda-derived query fees so `stake_to_fees` can differentiate
    /// indexers before on-chain payment claims appear.
    pub optimistic_dips_fees: HashMap<IndexerId, f64>,
}

/// The most Dipper will pay, so IISA can drop indexers asking more. Named fields keep
/// the 2 prices, which are in different units, from being swapped. `None` skips one.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct PriceCeilings {
    /// Base price for the chain, in GRT per 30 days.
    pub max_grt_per_30_days: Option<f64>,
    /// Per-entity price, in GRT per billion entities per 30 days.
    pub max_grt_per_billion_entities_per_30_days: Option<f64>,
}

/// An indexer selected by IISA with its advertised pricing.
#[derive(Debug, Clone)]
pub struct SelectedIndexer {
    pub id: IndexerId,
    /// Minimum GRT per 30 days the indexer charges for this chain.
    /// `None` if the indexer has no advertised price (legacy indexer).
    pub min_grt_per_30_days: Option<f64>,
    /// Minimum GRT per billion entities per 30 days.
    pub min_grt_per_billion_entities_per_30_days: Option<f64>,
}

/// A snapshot of the indexers that currently accept DIPs, from IISA's
/// `GET /dips-indexers`. `computed_at` is IISA's scoring-snapshot time (raw
/// ISO-8601), so callers can reject a stale snapshot.
#[derive(Debug, Clone)]
pub struct DipsAcceptingSnapshot {
    /// Scoring-snapshot timestamp (RFC 3339), or `None` when no scores are loaded.
    pub computed_at: Option<String>,
    /// Indexers that currently accept DIPs.
    pub indexers: Vec<IndexerId>,
}

/// The `SelectionError` enum represents the errors that can occur during the candidate selection
/// process.
#[derive(Debug, thiserror::Error)]
pub enum SelectionError {
    /// Indexer Selection service is not available.
    ///
    /// An error occurred while sending a request to the IISA service.
    #[error("IISA service is not available")]
    IisaServiceUnavailable,

    /// IISA answered with a status that resending the same request won't change
    /// (in practice a 4xx), usually bad config, a rotated token or a version
    /// mismatch mid-rollout. `body` is IISA's response, cut short if long.
    #[error("IISA rejected the request with HTTP {status}: {body}")]
    IisaRejected { status: u16, body: String },

    /// An error occurred during the selection process.
    #[error(transparent)]
    Error(#[from] anyhow::Error),
}

/// The `CandidateSelection` trait defines the interface for the Indexer Selection Algorithm
/// service, which selects the optimal set of indexers for a deployment.
///
/// The IISA handles candidate filtering internally using its own scores data.
/// The caller provides context about existing assignments and constraints, and receives
/// back the target state: the set of indexer IDs that should be assigned.
#[async_trait]
pub trait CandidateSelection {
    /// Select the optimal set of indexers for a deployment.
    ///
    /// Returns the target state: the set of indexers that SHOULD be assigned,
    /// along with their advertised pricing.
    /// The caller diffs against current assignments to determine adds/cancels.
    ///
    /// # Arguments
    /// * `deployment_id` - The deployment to select indexers for
    /// * `num_candidates` - Target group size (number of indexers desired)
    /// * `context` - Load balancing context with existing assignments and constraints
    async fn select_indexers(
        &self,
        deployment_id: DeploymentId,
        num_candidates: usize,
        context: &SelectionContext,
    ) -> Result<Vec<SelectedIndexer>, SelectionError>;

    /// Fetch the indexers that currently accept DIPs on `chain`, a required chain *name*
    /// (stricter than "IISA scores it"), capped to those priced at or under `ceilings`.
    async fn dips_accepting_indexers(
        &self,
        chain: &str,
        ceilings: PriceCeilings,
    ) -> Result<DipsAcceptingSnapshot, SelectionError>;
}
