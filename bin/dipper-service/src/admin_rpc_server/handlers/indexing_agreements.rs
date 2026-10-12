use async_trait::async_trait;
use dipper_core::{
    ids::{IndexingAgreementId, IndexingRequestId},
    state::FromState,
};
use dipper_rpc::admin::indexing_agreements::{
    IndexingAgreement, IndexingAgreementsRpcServer, Status as IndexingAgreementStatus,
};
use jsonrpsee::core::RpcResult;
use thegraph_core::{DeploymentId, IndexerId};

use super::error_handling::{handle_list_result, handle_optional_result};
use crate::registry::{
    AgreementRegistry, IndexingAgreement as IndexingAgreementRecord,
    IndexingAgreementStatus as IndexingAgreementRecordStatus,
};

/// The substate for the [`IndexingAgreementsRpc`] handler
///
/// See: https://docs.rs/axum/0.7.7/axum/extract/struct.State.html#substates
pub struct Ctx<R> {
    pub registry: R,
}

pub struct RpcServerImpl<R>(Ctx<R>);

impl<R> RpcServerImpl<R> {
    /// Create a new instance of the `IndexingAgreementsRpcServerImpl` with the given context
    pub fn with_context<C>(ctx: &C) -> Self
    where
        Ctx<R>: FromState<C>,
    {
        Self(FromState::from_state(ctx))
    }
}

impl<R> std::ops::Deref for RpcServerImpl<R> {
    type Target = Ctx<R>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[async_trait]
impl<R> IndexingAgreementsRpcServer for RpcServerImpl<R>
where
    R: AgreementRegistry + Clone + Send + Sync + 'static,
{
    async fn get_agreement_by_id(
        &self,
        agreement_id: IndexingAgreementId,
    ) -> RpcResult<IndexingAgreement> {
        handle_optional_result(
            self.registry
                .get_indexing_agreement_by_id(&agreement_id)
                .await,
            "Failed to get indexing agreement by id",
            into_indexing_agreement,
        )
    }

    async fn get_agreements_by_deployment_id(
        &self,
        deployment_id: DeploymentId,
    ) -> RpcResult<Vec<IndexingAgreement>> {
        handle_list_result(
            self.registry
                .get_indexing_agreements_by_deployment_id(&deployment_id)
                .await,
            "Failed to get indexing agreements by deployment id",
            into_indexing_agreement,
        )
    }

    async fn get_agreements_by_indexer_id(
        &self,
        indexer_id: IndexerId,
    ) -> RpcResult<Vec<IndexingAgreement>> {
        handle_list_result(
            self.registry
                .get_indexing_agreements_by_indexer_id(&indexer_id)
                .await,
            "Failed to get indexing agreements by indexer id",
            into_indexing_agreement,
        )
    }

    async fn get_agreements_by_indexing_request_id(
        &self,
        request_id: IndexingRequestId,
    ) -> RpcResult<Vec<IndexingAgreement>> {
        handle_list_result(
            self.registry
                .get_indexing_agreements_by_indexing_request_id(&request_id)
                .await,
            "Failed to get indexing agreements by indexing request id",
            into_indexing_agreement,
        )
    }
}

#[inline]
fn into_indexing_agreement(agreement: IndexingAgreementRecord) -> IndexingAgreement {
    IndexingAgreement {
        id: agreement.id,
        created_at: agreement.created_at,
        updated_at: agreement.updated_at,
        status: into_indexing_agreement_status(agreement.status),
        indexing_request_id: agreement.indexing_request_id,
        indexer_id: agreement.indexer.id,
        indexer_url: agreement.indexer.url,
        deadline: agreement.terms.deadline,
        ends_at: agreement.terms.ends_at,
        rejection_reason: agreement.rejection_reason,
    }
}

#[inline]
fn into_indexing_agreement_status(
    status: IndexingAgreementRecordStatus,
) -> IndexingAgreementStatus {
    match status {
        IndexingAgreementRecordStatus::Created => IndexingAgreementStatus::Created,
        IndexingAgreementRecordStatus::Unresponsive => IndexingAgreementStatus::Unresponsive,
        IndexingAgreementRecordStatus::CanceledByRequester => {
            IndexingAgreementStatus::CanceledByRequester
        }
        IndexingAgreementRecordStatus::CanceledByIndexer => {
            IndexingAgreementStatus::CanceledByIndexer
        }
        IndexingAgreementRecordStatus::Expired => IndexingAgreementStatus::Expired,
        IndexingAgreementRecordStatus::AcceptedOnChain => IndexingAgreementStatus::AcceptedOnChain,
        IndexingAgreementRecordStatus::Rejected => IndexingAgreementStatus::Rejected,
        IndexingAgreementRecordStatus::AbandonedByIndexer => {
            IndexingAgreementStatus::AbandonedByIndexer
        }
        IndexingAgreementRecordStatus::Cancelling => IndexingAgreementStatus::Cancelling,
    }
}
