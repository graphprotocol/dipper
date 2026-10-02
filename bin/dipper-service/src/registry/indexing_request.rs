//! # Indexing request
//!
//! Indexing Requests are initiated by the customer and are used to request indexing services
//! from indexers. The DIPs Gateway service (Dipper) is responsible for finding appropriate
//! indexers to fulfill the request.

use async_trait::async_trait;
use dipper_core::ids::IndexingRequestId;
use thegraph_core::{
    DeploymentId,
    alloy::primitives::{Address, ChainId},
};
use time::OffsetDateTime;

use super::result::Result as RegistryResult;

/// The Indexing Request registry trait.
///
/// This is a subset of the [`Registry`](dipper_pgregistry::result::Registry) trait that is specific to
/// indexing requests.
#[async_trait]
pub trait IndexingRequestRegistry {
    /// Idempotent upsert keyed on `(requested_by, deployment_id, deployment_chain_id)`.
    ///
    /// Returns a [`SetTargetOutcome`] describing what changed so the caller can
    /// dispatch the right follow-up worker job.
    async fn set_indexing_target_candidates(
        &self,
        requested_by: Address,
        deployment_id: DeploymentId,
        deployment_chain_id: ChainId,
        num_candidates: usize,
    ) -> RegistryResult<SetTargetOutcome>;

    /// Get all indexing requests.
    async fn get_all_indexing_requests(&self) -> RegistryResult<Vec<IndexingRequest>>;

    /// Get the indexing request by ID.
    async fn get_indexing_request_by_id(
        &self,
        id: &IndexingRequestId,
    ) -> RegistryResult<Option<IndexingRequest>>;

    /// Get all indexing requests by Deployment ID.
    async fn get_indexing_requests_by_deployment_id(
        &self,
        deployment_id: &DeploymentId,
    ) -> RegistryResult<Vec<IndexingRequest>>;

    /// Get open indexing requests eligible for reassessment.
    ///
    /// Returns requests that are in the `OPEN` status and were created at least
    /// `min_age_seconds` ago. Results are ordered by `updated_at` ascending to
    /// prioritize requests that haven't been reassessed recently.
    ///
    /// If `batch_size` is greater than 0, limits the number of results.
    /// If `batch_size` is 0 or negative, returns all matching requests.
    async fn get_open_indexing_requests_for_reassessment(
        &self,
        min_age_seconds: i64,
        batch_size: i64,
    ) -> RegistryResult<Vec<IndexingRequest>>;

    /// Atomically set the request's coverage-shortfall latch, returning whether
    /// the value actually changed (a single guarded UPDATE, so the transition is
    /// race-free).
    ///
    /// Drives the `n_indexers_unavailable` signal: reassessment fires it once on
    /// the transition INTO shortfall (`Ok(true)` with `active == true`) and clears
    /// the latch on recovery (`Ok(true)` with `active == false`), rather than
    /// re-emitting on every recurring reassessment of a persistently short request.
    /// Default `Ok(false)` (never transitions) so mocks need not override.
    async fn set_indexing_request_shortfall_active(
        &self,
        _id: &IndexingRequestId,
        _active: bool,
    ) -> RegistryResult<bool> {
        Ok(false)
    }
}

/// What `set_indexing_target_candidates` actually did.
///
/// The admin RPC handler inspects this to decide which worker job to queue
/// after the row mutation.
#[derive(Debug, Clone)]
pub enum SetTargetOutcome {
    /// No Open row existed for the key; a new row was inserted with the
    /// requested `num_candidates`. The caller should queue reassessment to
    /// drive the indexer set up from zero.
    Inserted { id: IndexingRequestId },

    /// An Open row already existed and its `num_candidates` was updated to a
    /// new non-zero value. The caller should queue reassessment to grow or
    /// shrink the indexer set to the new target.
    Updated {
        id: IndexingRequestId,
        new_num_candidates: usize,
    },

    /// An Open row existed with the same `num_candidates` as requested.
    /// Nothing to do; the caller should return the existing ID.
    NoOp { id: IndexingRequestId },

    /// `num_candidates = 0` was requested for an existing Open row. The row
    /// was flipped to Canceled. The caller should queue reassessment with
    /// `num_candidates = 0` so the shrink path cancels every agreement
    /// on-chain.
    Canceled { id: IndexingRequestId },

    /// `num_candidates = 0` was requested for a key with no Open row.
    /// The caller should warn and return `None` to the RPC client; there
    /// is nothing to cancel.
    NoOpAlreadyEmpty,
}

/// An Indexing Request represents the request for indexing services initiated by the customer.
///
/// The [`IndexingRequest`] is as a Record Data Structure.
#[derive(Debug, Clone)]
pub struct IndexingRequest {
    /// The unique identifier of the request.
    pub id: IndexingRequestId,

    /// The indexing request registration time.
    pub created_at: OffsetDateTime,

    /// The indexing request update time.
    pub updated_at: OffsetDateTime,

    /// The status of the request.
    pub status: Status,

    /// The indexing request issuer.
    ///
    /// The requester is the Ethereum address of the customer that initiated the request.
    ///
    /// Any interaction with this entity must be signed by the requester's address associated
    /// private key.
    pub requested_by: Address,

    /// The Subgraph deployment ID.
    pub deployment_id: DeploymentId,

    /// The Subgraph deployment chain ID.
    pub deployment_chain_id: ChainId,

    /// The desired number of indexers for this request.
    pub num_candidates: usize,
}

/// The status of the [`IndexingRequest`].
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
pub enum Status {
    /// The indexing request is the active target for its `(requester,
    /// deployment, chain)` key. The current `num_candidates` field on the
    /// row is the desired indexer count.
    ///
    /// Under the declarative model this is the only non-terminal state for
    /// an active assignment. Agreement churn (cancels, rejections, indexer
    /// abandonment) is absorbed by reassessment without flipping the request
    /// row back from any other state — once an `Open` row exists, it stays
    /// `Open` until a `num_candidates = 0` call retires it.
    #[default]
    Open,

    /// The indexing request was terminated by a `num_candidates = 0` call.
    /// All associated agreements MUST be cancelled. Terminal state.
    Canceled,
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let status = match self {
            Status::Open => "OPEN",
            Status::Canceled => "CANCELED",
        };
        f.write_str(status)
    }
}

impl TryFrom<dipper_pgregistry::IndexingRequest> for IndexingRequest {
    type Error = anyhow::Error;

    #[expect(
        clippy::cast_sign_loss,
        reason = "predates this lint; fix when next touched"
    )]
    fn try_from(value: dipper_pgregistry::IndexingRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id,
            created_at: value.created_at,
            updated_at: value.updated_at,
            status: value.status.try_into()?,
            requested_by: value.requested_by,
            deployment_id: value.deployment_id,
            deployment_chain_id: value.deployment_chain_id,
            num_candidates: value.num_candidates as usize,
        })
    }
}

impl TryFrom<dipper_pgregistry::IndexingRequestStatus> for Status {
    type Error = anyhow::Error;

    fn try_from(value: dipper_pgregistry::IndexingRequestStatus) -> Result<Self, Self::Error> {
        match value {
            dipper_pgregistry::IndexingRequestStatus::Open => Ok(Status::Open),
            dipper_pgregistry::IndexingRequestStatus::Canceled => Ok(Status::Canceled),
            _ => Err(anyhow::anyhow!("invalid indexing request status")),
        }
    }
}

impl From<dipper_pgregistry::IndexingRequestSetTargetOutcome> for SetTargetOutcome {
    #[expect(
        clippy::cast_sign_loss,
        reason = "predates this lint; fix when next touched"
    )]
    fn from(value: dipper_pgregistry::IndexingRequestSetTargetOutcome) -> Self {
        use dipper_pgregistry::IndexingRequestSetTargetOutcome as Pg;
        match value {
            Pg::Inserted { id } => SetTargetOutcome::Inserted { id },
            Pg::Updated {
                id,
                new_num_candidates,
            } => SetTargetOutcome::Updated {
                id,
                new_num_candidates: new_num_candidates as usize,
            },
            Pg::NoOp { id } => SetTargetOutcome::NoOp { id },
            Pg::Canceled { id } => SetTargetOutcome::Canceled { id },
            Pg::NoOpAlreadyEmpty => SetTargetOutcome::NoOpAlreadyEmpty,
        }
    }
}
