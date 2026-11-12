use dipper_core::ids::IndexingRequestId;
use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use thegraph_core::{
    DeploymentId,
    alloy::primitives::{Address, ChainId},
    signed_message::ToSolStruct,
};
use time::OffsetDateTime;

use super::message::SignedMessage;

/// The _indexing requests_ RPC methods
#[rpc(server, client)]
pub trait IndexingRequestsRpc {
    /// Get all _indexing requests_
    #[method(name = "get_all_indexing_requests")]
    async fn get_all_indexing_requests(&self) -> RpcResult<Vec<IndexingRequest>>;

    /// Get _indexing request_ by ID.
    ///
    /// Only one _indexing request_ will be returned, if it exists. Otherwise, an error will be
    /// returned.
    #[method(name = "get_indexing_request_by_id")]
    async fn get_indexing_request_by_id(&self, id: IndexingRequestId)
    -> RpcResult<IndexingRequest>;

    /// Get _indexing requests_ by deployment ID
    #[method(name = "get_indexing_requests_by_deployment_id")]
    async fn get_indexing_requests_by_deployment_id(
        &self,
        deployment_id: DeploymentId,
    ) -> RpcResult<Vec<IndexingRequest>>;

    /// Set the target number of indexer candidates for an indexing assignment.
    ///
    /// Idempotent upsert keyed on `(requester, deployment_id, chain_id)`:
    ///
    /// - First call for a key inserts a new request and queues selection.
    /// - Subsequent calls with the same `num_candidates` are a no-op.
    /// - Subsequent calls with a different `num_candidates` update the existing
    ///   Open row and queue reassessment to grow or shrink the indexer set.
    /// - A call with `num_candidates = 0` cancels every agreement under the
    ///   request and flips the row to Canceled. The next non-zero call for the
    ///   same key creates a fresh request.
    /// - A call with `num_candidates = 0` against a key with no Open row is a
    ///   no-op and logs a warning (nothing to cancel).
    ///
    /// Returns the canonical request ID for the key when one exists (insert,
    /// update, no-op, or cancel). Returns `None` for the only edge case where
    /// no ID exists: `num_candidates = 0` against a key that has never been
    /// registered or whose request has already been canceled.
    #[method(name = "set_indexing_target_candidates")]
    async fn set_indexing_target_candidates(
        &self,
        req: SignedMessage<SetIndexingTargetCandidates>,
    ) -> RpcResult<Option<IndexingRequestId>>;
}

/// Payload for [`IndexingRequestsRpc::set_indexing_target_candidates`].
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct SetIndexingTargetCandidates {
    /// The deployment ID of the subgraph
    pub deployment_id: DeploymentId,
    /// The chain ID the subgraph is indexing
    pub chain_id: ChainId,
    /// The target number of indexers to assign. Zero terminates the request.
    /// Defaults to the server-side maximum when omitted.
    #[serde(default)]
    pub num_candidates: Option<usize>,
    /// When the request was signed, in seconds since the Unix epoch. The server refuses a
    /// request signed too long ago, so one that was recorded can't be sent again later.
    pub signed_at: u64,
}

impl ToSolStruct<SetIndexingTargetCandidatesSol> for SetIndexingTargetCandidates {
    fn to_sol_struct(&self) -> SetIndexingTargetCandidatesSol {
        SetIndexingTargetCandidatesSol {
            deployment_id: self.deployment_id.into(),
            chain_id: self.chain_id,
            // Signed apart from the count, which is 0 both for a cancel and when left out:
            // without it the 2 requests would carry the same signature.
            num_candidates_set: self.num_candidates.is_some(),
            // `as u64` is safe on every target dipper builds for (all 64-bit);
            // `usize` is at most 64 bits on those platforms. The cast would
            // truncate on a hypothetical 128-bit target but no such build
            // exists today.
            num_candidates: self.num_candidates.unwrap_or(0) as u64,
            signed_at: self.signed_at,
        }
    }
}

thegraph_core::alloy::sol! {
    /// Solidity-side struct for EIP-712 signing.
    ///
    /// See: [`SetIndexingTargetCandidates::to_sol_struct`].
    struct SetIndexingTargetCandidatesSol {
        bytes32 deployment_id;
        uint64 chain_id;
        bool num_candidates_set;
        uint64 num_candidates;
        uint64 signed_at;
    }
}

/// The _indexing request_ response entity
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexingRequest {
    /// The unique identifier of the request.
    pub id: IndexingRequestId,

    /// The indexing request registration time.
    #[serde(with = "time::serde::iso8601")]
    pub created_at: OffsetDateTime,

    /// The indexing request update time.
    #[serde(with = "time::serde::iso8601")]
    pub updated_at: OffsetDateTime,

    /// The status of the request.
    pub status: IndexingRequestStatus,

    /// The indexing request issuer.
    ///
    /// The requester is the Ethereum address of the customer that initiated the request.
    ///
    /// Any interaction with this entity must be signed by the requester's address associated
    /// private key.
    pub requested_by: Address,

    /// The Subgraph deployment ID.
    pub deployment_id: DeploymentId,
}

/// The status of the [`IndexingRequest`].
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub enum IndexingRequestStatus {
    /// The indexing request is the active target for its `(requester,
    /// deployment, chain)` key. The current `num_candidates` field on the
    /// underlying row is the desired indexer count.
    ///
    /// Under the declarative model this is the only non-terminal state for
    /// an active assignment. Agreement churn (cancels, rejections, indexer
    /// abandonment) is absorbed by reassessment without flipping the row
    /// back from any other state — once an `Open` row exists, it stays
    /// `Open` until a `num_candidates = 0` call retires it.
    Open,

    /// The indexing request was terminated by a `num_candidates = 0` call.
    /// All associated agreements are cancelled. Terminal state.
    Canceled,

    /// The indexing request is in an unknown state.
    Unknown,
}

impl serde::Serialize for IndexingRequestStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let status = match self {
            IndexingRequestStatus::Open => "OPEN",
            IndexingRequestStatus::Canceled => "CANCELED",
            IndexingRequestStatus::Unknown => "UNKNOWN",
        };
        serializer.serialize_str(status)
    }
}

impl<'de> serde::Deserialize<'de> for IndexingRequestStatus {
    fn deserialize<D>(deserializer: D) -> Result<IndexingRequestStatus, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let status = String::deserialize(deserializer)?;
        let status = if status.eq_ignore_ascii_case("OPEN") {
            IndexingRequestStatus::Open
        } else if status.eq_ignore_ascii_case("CANCELED") {
            IndexingRequestStatus::Canceled
        } else {
            IndexingRequestStatus::Unknown
        };
        Ok(status)
    }
}

#[cfg(test)]
mod tests {
    use thegraph_core::{
        alloy::{primitives::B256, sol_types::SolStruct},
        deployment_id,
        signed_message::ToSolStruct,
    };

    use super::SetIndexingTargetCandidates;
    use crate::admin::eip712_domain;

    /// The hash an operator signs for a request, on the dipper serving `protocol_chain`.
    fn signing_hash(protocol_chain: u64, num_candidates: Option<usize>, signed_at: u64) -> B256 {
        SetIndexingTargetCandidates {
            deployment_id: deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv"),
            chain_id: 1,
            num_candidates,
            signed_at,
        }
        .to_sol_struct()
        .eip712_signing_hash(&eip712_domain(protocol_chain))
    }

    #[test]
    fn a_cancel_and_a_count_left_out_are_signed_differently() {
        assert_ne!(
            signing_hash(42161, Some(0), 7),
            signing_hash(42161, None, 7)
        );
    }

    #[test]
    fn the_same_request_is_signed_differently_at_another_time() {
        assert_ne!(
            signing_hash(42161, Some(3), 7),
            signing_hash(42161, Some(3), 8)
        );
    }

    #[test]
    fn the_same_request_is_signed_differently_for_another_network() {
        assert_ne!(
            signing_hash(42161, Some(3), 7),
            signing_hash(421_614, Some(3), 7)
        );
    }
}
