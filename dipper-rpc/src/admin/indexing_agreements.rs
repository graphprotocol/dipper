use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
use jsonrpsee::{client_transport::ws::Url, core::RpcResult, proc_macros::rpc};
use serde::Serializer;
use serde_with::serde_as;
use thegraph_core::{DeploymentId, IndexerId};
use time::OffsetDateTime;

/// The _indexing agreement_ RPC methods
#[rpc(server, client)]
pub trait IndexingAgreementsRpc {
    /// Get _indexing agreements_ by ID
    #[method(name = "get_agreement_by_id")]
    async fn get_agreement_by_id(
        &self,
        agreement_id: IndexingAgreementId,
    ) -> RpcResult<IndexingAgreement>;

    /// Get all _indexing agreements_ by Subgraph deployment ID
    #[method(name = "get_agreements_by_deployment_id")]
    async fn get_agreements_by_deployment_id(
        &self,
        deployment_id: DeploymentId,
    ) -> RpcResult<Vec<IndexingAgreement>>;

    /// Get all _indexing agreements_ by indexer ID
    #[method(name = "get_agreements_by_indexer_id")]
    async fn get_agreements_by_indexer_id(
        &self,
        indexer_id: IndexerId,
    ) -> RpcResult<Vec<IndexingAgreement>>;

    /// Get all _indexing agreements_ by indexing request ID
    #[method(name = "get_agreements_by_request_id")]
    async fn get_agreements_by_indexing_request_id(
        &self,
        request_id: IndexingRequestId,
    ) -> RpcResult<Vec<IndexingAgreement>>;
}

/// The _indexing agreement_ response entity
#[serde_as]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct IndexingAgreement {
    /// The indexing agreement unique ID.
    pub id: IndexingAgreementId,

    /// The indexing agreement creation time.
    #[serde(with = "time::serde::iso8601")]
    pub created_at: OffsetDateTime,

    // The indexing agreement update time.
    #[serde(with = "time::serde::iso8601")]
    pub updated_at: OffsetDateTime,

    /// The indexing agreement status.
    pub status: Status,

    /// The indexing agreement associated indexing request
    pub indexing_request_id: IndexingRequestId,

    /// The indexer's address.
    pub indexer_id: IndexerId,

    /// The indexer's URL.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub indexer_url: Url,

    /// Deadline for on-chain acceptance (unix timestamp).
    pub deadline: u64,
    /// When the agreement expires (unix timestamp).
    pub ends_at: u64,
    /// Reason the agreement was rejected (only set when status is Rejected).
    ///
    /// Values: "PRICE_TOO_LOW", "OTHER", or None.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rejection_reason: Option<String>,
}

/// The status of the [`IndexingAgreement`].
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub enum Status {
    /// The [`IndexingAgreement`] was created, but has not been sent to the indexer, yet.
    Created,

    /// The [`IndexingAgreement`] was registered, but the agreement request failed.
    ///
    /// This is a terminal state.
    Unresponsive,

    /// The associated [`IndexingRequest`] got cancelled.
    ///
    /// The [`IndexingAgreement`] is cancelled and no longer in effect.
    ///
    /// This is a terminal state.
    CanceledByRequester,

    /// The indexer canceled the indexer agreement.
    ///
    /// The [`IndexingAgreement`] is cancelled and no longer in effect.
    ///
    /// This is a terminal state.
    CanceledByIndexer,

    /// The [`IndexingAgreement`] is expired.
    ///
    /// The indexer indexed the data and the agreement is no longer in effect.
    ///
    /// This is a terminal state.
    Expired,

    /// The [`IndexingAgreement`] was accepted on-chain.
    AcceptedOnChain,

    /// The indexer rejected the agreement proposal off-chain.
    Rejected,

    /// The liveness checker detected no indexing progress within the tolerance window.
    ///
    /// Dipper canceled the agreement via `cancelIndexingAgreementByPayer` and will
    /// trigger reassignment to find a replacement indexer.
    ///
    /// This is a terminal state.
    AbandonedByIndexer,

    /// Dipper is cancelling the agreement on-chain; it may still be live there.
    Cancelling,

    /// A fallback for unknown status values.
    Unknown,
}

impl serde::Serialize for Status {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let status = match self {
            Status::Created => "CREATED",
            Status::Unresponsive => "UNRESPONSIVE",
            Status::CanceledByRequester => "CANCELED_BY_REQUESTER",
            Status::CanceledByIndexer => "CANCELED_BY_INDEXER",
            Status::Expired => "EXPIRED",
            Status::AcceptedOnChain => "ACCEPTED_ON_CHAIN",
            Status::Rejected => "REJECTED",
            Status::AbandonedByIndexer => "ABANDONED_BY_INDEXER",
            Status::Cancelling => "CANCELLING",
            Status::Unknown => "UNKNOWN",
        };
        serializer.serialize_str(status)
    }
}

impl<'de> serde::Deserialize<'de> for Status {
    fn deserialize<D>(deserializer: D) -> Result<Status, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let status = String::deserialize(deserializer)?;
        let status = match status.to_uppercase().as_str() {
            "CREATED" => Status::Created,
            "UNRESPONSIVE" => Status::Unresponsive,
            "CANCELED_BY_REQUESTER" => Status::CanceledByRequester,
            "CANCELED_BY_INDEXER" => Status::CanceledByIndexer,
            "EXPIRED" => Status::Expired,
            "ACCEPTED_ON_CHAIN" => Status::AcceptedOnChain,
            "REJECTED" => Status::Rejected,
            "ABANDONED_BY_INDEXER" => Status::AbandonedByIndexer,
            "CANCELLING" => Status::Cancelling,
            _ => Status::Unknown,
        };
        Ok(status)
    }
}
