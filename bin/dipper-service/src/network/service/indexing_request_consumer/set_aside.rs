//! Studio requests whose subgraph manifest IPFS can't return. Dropping one loses it, since nobody
//! sends a record again, and waiting for it holds up every later record of its partition. So it
//! is stored in the database, the partition moves on, and it is applied once the manifest reads.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use dipper_core::ids::IndexingRequestId;
use dipper_producer::events::SubgraphIndexingAgreementEventsProducer;
use thegraph_core::{
    DeploymentId,
    alloy::primitives::{Address, ChainId},
};
use tokio::sync::watch;

use super::{
    ProposedTarget, RETRY_BACKOFF, Retries, alert_refused_record, sleep_or_shutdown, stop_requested,
};
use crate::{
    registry::IndexingRequestRegistry,
    request_chain_check::{ChainCheckError, RequestChainCheck},
    set_indexing_target::{ApplyError, SetIndexingTarget, apply_set_indexing_target, check_chain},
    worker::service::{JobPriority, WorkerQueue},
};

/// How long a record whose manifest can't be read is tried again in place before it is set
/// aside. It gives a manifest that was only just published time to turn up, and is as long as
/// the records behind it on the partition are kept waiting.
pub(super) const SET_ASIDE_AFTER: Duration = Duration::from_secs(3 * 60);

/// How long after a request is set aside its manifest is read again for the first time.
pub(super) const FIRST_RETRY_AFTER: Duration = Duration::from_secs(5 * 60);

/// The longest wait between 2 reads of the manifest of a request set aside.
const LONGEST_RETRY_WAIT: Duration = Duration::from_secs(3_600);

/// How often the requests set aside are looked over for manifests due another read.
const CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// How long a request stays set aside before it is alerted on a 2nd time.
const STILL_WAITING_AFTER: Duration = Duration::from_secs(24 * 3_600);

/// How long the loop of a partition waits on the manifest check of a ready request. The check
/// answers from memory unless dipper restarted since the read, and the partition must not wait.
const READY_CHECK_TIMEOUT: Duration = Duration::from_secs(5);

/// The `event` tag of the alert for a Studio request set aside until its manifest can be read.
pub(super) const SET_ASIDE_EVENT: &str = "studio_request_set_aside";

/// The `event` tag of the alert for a Studio request that is still set aside 24 hours on.
pub(super) const STILL_WAITING_EVENT: &str = "studio_request_still_waiting";

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

    /// Remove `request` now that it is applied or refused. `false`, changing nothing, if it was
    /// removed, or replaced by a later record, since it was listed.
    async fn forget_set_aside_record(
        &self,
        request: &SetAsideRequest,
    ) -> Result<bool, crate::registry::Error>;

    /// The requests kept from `topic` whose manifest is due another read, the longest overdue
    /// first.
    async fn due_set_aside_requests(
        &self,
        topic: &str,
    ) -> Result<Vec<SetAsideRequest>, crate::registry::Error>;

    /// The requests kept from a partition of `topic` whose manifest could be read, in the
    /// order their records were read.
    async fn ready_set_aside_requests(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<Vec<SetAsideRequest>, crate::registry::Error>;

    /// Mark `request` ready to apply, now that its manifest could be read. Changes nothing if
    /// it was removed, or replaced by a later record, since it was listed.
    async fn mark_set_aside_request_ready(
        &self,
        request: &SetAsideRequest,
    ) -> Result<(), crate::registry::Error>;

    /// Count 1 more failed read of `request`'s manifest, for `reason`, and leave it waiting for
    /// the next in `retry_in`. Changes nothing if it was removed, or replaced by a later
    /// record, since it was listed.
    async fn retry_set_aside_request_later(
        &self,
        request: &SetAsideRequest,
        reason: &str,
        retry_in: Duration,
    ) -> Result<(), crate::registry::Error>;

    /// The requests kept from `topic` for at least `waiting` that were not alerted on as still
    /// waiting, each recorded as alerted on in the same step, so that it is returned once.
    async fn claim_still_waiting_requests(
        &self,
        topic: &str,
        waiting: Duration,
    ) -> Result<Vec<SetAsideRequest>, crate::registry::Error>;
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

/// How long to wait before the next read of a manifest that `attempts` reads since its request
/// was set aside could not get: 5 minutes at first, twice as long each time, 1 hour at most.
pub(super) fn retry_wait(attempts: u32) -> Duration {
    let doubled = FIRST_RETRY_AFTER.saturating_mul(2_u32.saturating_pow(attempts));
    doubled.min(LONGEST_RETRY_WAIT)
}

/// Reads the manifests of the requests set aside from `topic` again, each when it falls due,
/// until asked to stop. It applies nothing: a request whose manifest it could read is marked
/// ready, for the loop of its partition to apply between the records it reads.
pub(super) async fn check_set_aside_requests<R>(
    registry: R,
    chain_check: Arc<dyn RequestChainCheck>,
    topic: String,
    mut shutdown_rx: watch::Receiver<bool>,
) where
    R: SetAsideRequestRegistry + Send + Sync,
{
    loop {
        alert_still_waiting(&registry, &topic).await;
        if read_due_manifests(&registry, chain_check.as_ref(), &topic, &mut shutdown_rx).await
            || sleep_or_shutdown(&mut shutdown_rx, CHECK_INTERVAL).await
        {
            return;
        }
    }
}

/// Reads the manifest of each request due another read, and records what came of it; `true`
/// if a stop came first. A read can take IPFS its whole timeout, which is why it is done here
/// and not by the loop of the request's partition.
async fn read_due_manifests<R>(
    registry: &R,
    chain_check: &dyn RequestChainCheck,
    topic: &str,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> bool
where
    R: SetAsideRequestRegistry + Send + Sync,
{
    for request in listed(registry.due_set_aside_requests(topic).await) {
        let read = tokio::select! {
            biased;
            () = stop_requested(shutdown_rx) => return true,
            read = chain_check.check(&request.deployment_id, request.deployment_chain_id) => read,
        };
        let recorded = match read {
            Err(err) if !err.refuses() => wait_longer(registry, &request, &err).await,
            // Read, whatever it says of the request: the partition's loop applies or drops it.
            _ => registry.mark_set_aside_request_ready(&request).await,
        };
        log_if_unrecorded(&request, recorded);
    }
    false
}

/// The requests a pass is to go over, or none once a failure to list them is logged: the next
/// pass lists them again.
fn listed(listed: Result<Vec<SetAsideRequest>, crate::registry::Error>) -> Vec<SetAsideRequest> {
    listed.unwrap_or_else(|error| {
        tracing::error!(
            %error,
            "failed to list the Studio indexing requests set aside; trying again shortly"
        );
        Vec::new()
    })
}

/// Logs a failed write to a request set aside. Nothing is lost by one: the request stays as it
/// was, and the next pass comes back to it.
fn log_if_unrecorded<T>(request: &SetAsideRequest, recorded: Result<T, crate::registry::Error>) {
    if let Err(error) = recorded {
        tracing::error!(
            %error,
            partition = request.partition,
            offset = request.offset,
            deployment_id = %request.deployment_id,
            "failed to update a Studio indexing request set aside; trying again shortly"
        );
    }
}

/// Leaves `request` waiting for the next read of its manifest, after one that failed with `err`.
async fn wait_longer<R>(
    registry: &R,
    request: &SetAsideRequest,
    err: &ChainCheckError,
) -> Result<(), crate::registry::Error>
where
    R: SetAsideRequestRegistry + Send + Sync,
{
    let attempts = request.attempts.saturating_add(1);
    let wait = retry_wait(attempts);
    tracing::info!(
        partition = request.partition,
        offset = request.offset,
        deployment_id = %request.deployment_id,
        attempts,
        retry_in_secs = wait.as_secs(),
        error = %err,
        "the subgraph manifest of a Studio indexing request set aside still can't be read"
    );
    registry
        .retry_set_aside_request_later(request, &err.to_string(), wait)
        .await
}

/// Raises the alert for each request that has been set aside for 24 hours. The database
/// records each as alerted on in the step that lists it, so the alert is raised once.
async fn alert_still_waiting<R>(registry: &R, topic: &str)
where
    R: SetAsideRequestRegistry + Send + Sync,
{
    let claimed = registry.claim_still_waiting_requests(topic, STILL_WAITING_AFTER);
    for request in listed(claimed.await) {
        tracing::error!(
            event = STILL_WAITING_EVENT,
            topic = request.topic,
            partition = request.partition,
            offset = request.offset,
            event_id = %request.event_id,
            deployment_id = %request.deployment_id,
            chain_id = request.deployment_chain_id,
            num_candidates = request.num_candidates,
            attempts = request.attempts,
            "A Studio indexing request has been set aside for {} hours and has still not taken \
             effect, because its subgraph manifest can't be read from IPFS: {}. Dipper goes on \
             reading the manifest every hour, and applies the request only once it can be read",
            request.waited.as_secs() / 3_600,
            request.reason
        );
    }
}

/// Applies the requests set aside from a partition whose manifest could be read; `true` if a
/// stop came first. The partition's loop calls it between fetches, so that 1 task handles a
/// request set aside and any later record for the same subgraph, in the order it meets them.
#[allow(clippy::too_many_arguments)]
pub(super) async fn apply_ready_requests<R, W>(
    registry: &R,
    worker_queue: &W,
    events: &Arc<dyn SubgraphIndexingAgreementEventsProducer>,
    chain_check: &dyn RequestChainCheck,
    protocol_chain_id: ChainId,
    topic: &str,
    partition: i32,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> bool
where
    R: IndexingRequestRegistry + SetAsideRequestRegistry + Send + Sync,
    W: WorkerQueue + Send + Sync,
{
    let ready = listed(registry.ready_set_aside_requests(topic, partition).await);
    for request in ready {
        let target = SetIndexingTarget {
            requested_by: request.requested_by,
            deployment_id: request.deployment_id,
            deployment_chain_id: request.deployment_chain_id,
            num_candidates: request.num_candidates,
            priority: JobPriority::Interactive,
            // Applied a 2nd time after a failure between its write and its job, as a record
            // delivered twice is.
            reassess_unchanged: true,
        };
        // The manifest was read moments ago, so the check answers from memory. If it has to
        // ask IPFS after all and gets no quick answer, the request goes back to waiting.
        let checked = tokio::select! {
            biased;
            () = stop_requested(shutdown_rx) => return true,
            checked = check_chain(chain_check, target) => checked,
            () = tokio::time::sleep(READY_CHECK_TIMEOUT) => Err(no_quick_answer()),
        };
        let applied = match checked {
            Ok(checked) => {
                let network = protocol_chain_id;
                apply_set_indexing_target(registry, worker_queue, events, network, checked).await
            }
            Err(err) => Err(err),
        };
        finish_ready_request(registry, &request, applied).await;
    }
    false
}

/// The error for a ready request whose manifest check did not answer in time.
fn no_quick_answer() -> ApplyError {
    ApplyError::ChainCheckUnavailable(ChainCheckError::Unavailable(format!(
        "no answer in {} seconds",
        READY_CHECK_TIMEOUT.as_secs()
    )))
}

/// Removes a ready request that was applied or that its manifest refuses, and leaves any other
/// in the database to be tried again.
async fn finish_ready_request<R>(
    registry: &R,
    request: &SetAsideRequest,
    applied: Result<Option<IndexingRequestId>, ApplyError>,
) where
    R: SetAsideRequestRegistry + Send + Sync,
{
    let applied = match applied {
        Ok(_) => {
            tracing::info!(
                partition = request.partition,
                offset = request.offset,
                event_id = %request.event_id,
                deployment_id = %request.deployment_id,
                waited_secs = request.waited.as_secs(),
                "applied a Studio indexing request that was set aside, now that its subgraph \
                 manifest could be read"
            );
            true
        }
        Err(ApplyError::WrongChain(err)) => {
            let target = ProposedTarget {
                event_id: request.event_id.clone(),
                deployment_id: request.deployment_id,
                deployment_chain_id: request.deployment_chain_id,
                num_candidates: request.num_candidates,
            };
            alert_refused_record(request.partition, request.offset, &target, &err);
            false
        }
        Err(ApplyError::ManifestNotFound(err) | ApplyError::ChainCheckUnavailable(err)) => {
            return log_if_unrecorded(request, wait_longer(registry, request, &err).await);
        }
        // The database or the job queue failed, and logged it. The request stays ready, so the
        // next pass applies it again: a write already made is then a no-op, and queues its job.
        Err(ApplyError::Registry(_) | ApplyError::QueueReassess { .. }) => return,
    };
    // Removed as the record it was listed from: a request stored for the same subgraph since
    // then came from a later record, and still has to be applied.
    let forgotten = registry.forget_set_aside_record(request).await;
    if applied && matches!(forgotten, Ok(false)) {
        warn_newer_request_arrived(request);
    }
    log_if_unrecorded(request, forgotten);
}

/// Says that a request was applied while another for the same subgraph arrived. A later record
/// is stored and still to come; a request over the admin RPC may have been applied before it.
fn warn_newer_request_arrived(request: &SetAsideRequest) {
    tracing::warn!(
        partition = request.partition,
        offset = request.offset,
        event_id = %request.event_id,
        deployment_id = %request.deployment_id,
        chain_id = request.deployment_chain_id,
        num_candidates = request.num_candidates,
        "a Studio indexing request that was set aside was applied just as a newer request for \
         the same subgraph arrived; if the newer one came over the admin RPC, send it again"
    );
}
