//! Studio requests whose subgraph manifest IPFS can't return. Dropping one loses it, since nobody
//! sends a record again, and waiting for it holds up every later record of its partition. So it
//! is stored in the database, the partition moves on, and it is applied once the manifest reads.

use std::time::Duration;

use async_trait::async_trait;
use thegraph_core::{
    DeploymentId,
    alloy::primitives::{Address, ChainId},
};
use tokio::sync::watch;

use super::{ProposedTarget, RETRY_BACKOFF, Retries, sleep_or_shutdown};

/// How long a record whose manifest can't be read is tried again in place before it is set
/// aside. A manifest only just published reaches IPFS within this, and the partition waits.
pub(super) const SET_ASIDE_AFTER: Duration = Duration::from_secs(3 * 60);

/// How long after a request is set aside its manifest is read again for the first time.
pub(super) const FIRST_RETRY_AFTER: Duration = Duration::from_secs(5 * 60);

/// The `event` tag of the alert for a Studio request set aside until its manifest can be read.
pub(super) const SET_ASIDE_EVENT: &str = "studio_request_set_aside";

/// A Studio indexing request kept until its subgraph manifest can be read. There is at most 1
/// for each requester, deployment and chain: the one from the latest record consumed for them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetAsideRequest {
    pub requested_by: Address,
    pub deployment_id: DeploymentId,
    pub deployment_chain_id: ChainId,
    /// The number of indexers asked for, never 0: a cancel is applied without the manifest.
    pub num_candidates: usize,
    /// Where the request's record sits in Kafka.
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    /// The id Studio gave the request's event.
    pub event_id: String,
    /// Why the manifest could not be read, as of the last attempt.
    pub reason: String,
    /// How many times the manifest was read again since the request was set aside.
    pub attempts: u32,
    /// How long ago the request was set aside.
    pub waited: Duration,
}

/// Registry for the Studio requests set aside until their manifest can be read.
#[async_trait]
pub trait SetAsideRequestRegistry {
    /// Keep `request`, its manifest to be read again in `retry_in`, in place of any request kept
    /// for the same requester, deployment and chain. `false`, changing nothing, when that same
    /// record is already kept, as when Kafka delivers it twice.
    async fn set_aside_request(
        &self,
        request: &SetAsideRequest,
        retry_in: Duration,
    ) -> Result<bool, crate::registry::Error>;

    /// Remove the request kept for a requester, deployment and chain, whichever record it came
    /// from. Returns whether there was one.
    async fn forget_set_aside_request(
        &self,
        requested_by: Address,
        deployment_id: DeploymentId,
        deployment_chain_id: ChainId,
    ) -> Result<bool, crate::registry::Error>;
}

/// Stores the request of the record `retries` is for, so its partition can move past it;
/// `false` if a stop came first. A database failure is retried here for as long as it lasts: a
/// record passed before its request is stored would be lost.
pub(super) async fn set_aside<R>(
    registry: &R,
    (requested_by, target, topic, reason): (Address, &ProposedTarget, &str, &str),
    retries: &mut Retries,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> bool
where
    R: SetAsideRequestRegistry + Send + Sync,
{
    let request = &SetAsideRequest {
        requested_by,
        deployment_id: target.deployment_id,
        deployment_chain_id: target.deployment_chain_id,
        num_candidates: target.num_candidates,
        topic: topic.to_string(),
        partition: retries.partition,
        offset: retries.offset,
        event_id: target.event_id.clone(),
        reason: reason.to_string(),
        attempts: 0,
        waited: Duration::ZERO,
    };
    loop {
        match registry.set_aside_request(request, FIRST_RETRY_AFTER).await {
            Ok(true) => {
                alert_set_aside(request);
                return true;
            }
            // Delivered a 2nd time: the request is waiting already, and was alerted on then.
            Ok(false) => {
                tracing::info!(
                    partition = request.partition,
                    offset = request.offset,
                    deployment_id = %request.deployment_id,
                    "a Studio indexing request read again is already set aside"
                );
                return true;
            }
            Err(err) => {
                retries.failed("setting it aside", &err);
                if sleep_or_shutdown(shutdown_rx, RETRY_BACKOFF).await {
                    return false;
                }
            }
        }
    }
}

/// Raise an alert for a Studio request set aside. It has not taken effect and Studio is not
/// told, so this line is what tells an operator that a developer is waiting on IPFS.
fn alert_set_aside(request: &SetAsideRequest) {
    tracing::warn!(
        event = SET_ASIDE_EVENT,
        topic = request.topic,
        partition = request.partition,
        offset = request.offset,
        event_id = %request.event_id,
        deployment_id = %request.deployment_id,
        chain_id = request.deployment_chain_id,
        num_candidates = request.num_candidates,
        "Set a Studio indexing request aside because its subgraph manifest could not be read \
         from IPFS: {}. Nothing is changed yet. Dipper reads the manifest again in {} minutes, \
         then less and less often, and applies the request once it can, unless a newer request \
         for the same subgraph arrives first",
        request.reason,
        FIRST_RETRY_AFTER.as_secs() / 60
    );
}
