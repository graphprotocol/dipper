//! Applies a set-indexing-target request end to end: the registry upsert, the
//! request-received lifecycle event, and the follow-up reassessment job. Both
//! front doors share it: the admin RPC handler and the Kafka request consumer.

use std::sync::Arc;

use dipper_core::ids::IndexingRequestId;
use dipper_producer::{events::SubgraphIndexingAgreementEventsProducer, proto};
use thegraph_core::{
    DeploymentId,
    alloy::primitives::{Address, ChainId},
};

use crate::{
    registry::{IndexingRequestRegistry, SetTargetOutcome},
    worker::service::{JobPriority, WorkerQueue},
};

/// A set-indexing-target request, independent of which front door it came in
/// through.
pub struct SetIndexingTarget {
    /// Who asked (recovered RPC signer, or the consumer's configured identity).
    pub requested_by: Address,
    /// The subgraph deployment to index.
    pub deployment_id: DeploymentId,
    /// The chain the deployment indexes (its data source), keying the request.
    pub deployment_chain_id: ChainId,
    /// The target number of indexers; 0 cancels the request.
    pub num_candidates: usize,
    /// Priority for the follow-up reassessment job.
    pub priority: JobPriority,
}

/// Errors from applying a set-indexing-target request. Both variants are
/// already logged with full context when returned.
#[derive(Debug, thiserror::Error)]
pub enum ApplyError {
    /// The registry upsert failed; nothing was changed or queued.
    #[error("failed to set indexing target candidates")]
    Registry(#[source] crate::registry::Error),

    /// The row change is committed but the reassessment job was not queued.
    /// Retry the queue push itself, not the whole apply: a repeated apply
    /// lands on the registry's no-op path and never queues the job.
    #[error("failed to queue reassessment for indexing request {id}")]
    QueueReassess {
        id: IndexingRequestId,
        #[source]
        source: anyhow::Error,
    },
}

/// Upserts the indexing request, emits the request-received lifecycle event on
/// a genuinely new request, and queues reassessment when the row changed.
/// Returns the request id, or `None` when there was nothing to act on.
pub async fn apply_set_indexing_target<R, W>(
    registry: &R,
    worker: &W,
    events: &Arc<dyn SubgraphIndexingAgreementEventsProducer>,
    the_graph_network: ChainId,
    request: SetIndexingTarget,
) -> Result<Option<IndexingRequestId>, ApplyError>
where
    R: IndexingRequestRegistry + Send + Sync,
    W: WorkerQueue + Send + Sync,
{
    let SetIndexingTarget {
        requested_by,
        deployment_id,
        deployment_chain_id,
        num_candidates,
        priority,
    } = request;

    let outcome = match registry
        .set_indexing_target_candidates(
            requested_by,
            deployment_id,
            deployment_chain_id,
            num_candidates,
        )
        .await
    {
        Ok(outcome) => outcome,
        Err(err) => {
            tracing::error!(error=?err, "Failed to set indexing target candidates");
            return Err(ApplyError::Registry(err));
        }
    };

    // Translate the outcome into the appropriate follow-up worker job and the
    // request id to hand back.
    let (id_opt, reassess_count): (Option<IndexingRequestId>, Option<usize>) = match outcome {
        SetTargetOutcome::Inserted { id } => {
            tracing::info!(
                indexing_request_id = %id,
                %requested_by,
                %deployment_id,
                chain_id = %deployment_chain_id,
                num_candidates,
                "Inserted new indexing request"
            );

            // Only `Inserted` is a genuinely new request, so only it emits the
            // lifecycle event; `the_graph_network` is the protocol network,
            // not the deployment's data-source `chain_id`.
            events.produce_subgraph_indexing_agreement_request_received(
                deployment_id,
                the_graph_network,
                proto::SubgraphIndexingAgreementRequestReceived {
                    agreements_requested: num_candidates as i32,
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
                chain_id = %deployment_chain_id,
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
                chain_id = %deployment_chain_id,
                "Canceled indexing request (target candidates set to zero)"
            );
            (Some(id), Some(0))
        }
        SetTargetOutcome::NoOpAlreadyEmpty => {
            tracing::warn!(
                %requested_by,
                %deployment_id,
                chain_id = %deployment_chain_id,
                "set_indexing_target_candidates with num_candidates=0 against a key with no open request \
                 - nothing to cancel"
            );
            (None, None)
        }
    };

    // Queue reassessment if the row changed: it diffs the IISA target group of
    // size `num_candidates` against the active agreements and grows or shrinks
    // to match; 0 shrinks to nothing, cancelling every agreement on-chain.
    if let (Some(id), Some(count)) = (id_opt, reassess_count)
        && let Err(err) = worker
            .reassess_indexing_request(id, deployment_id, deployment_chain_id, count, priority)
            .await
    {
        tracing::error!(
            indexing_request_id = %id,
            error = ?err,
            "Failed to queue task: 'reassess_indexing_request'"
        );
        return Err(ApplyError::QueueReassess { id, source: err });
    }

    Ok(id_opt)
}
