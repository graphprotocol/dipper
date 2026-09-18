//! Consumes the subgraph indexing request events Studio produces on Redpanda
//! and applies each one as a set-indexing-target change, making the topic a
//! second front door to the same path the admin RPC serves.

use std::{future::Future, sync::Arc, time::Duration};

use async_trait::async_trait;
use dipper_producer::{
    events::SubgraphIndexingAgreementEventsProducer,
    kafka::{ConsumerError, KafkaConsumer, OffsetAt},
    prost::Message as _,
    proto::studio,
};
use thegraph_core::{
    DeploymentId,
    alloy::primitives::{Address, ChainId},
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
};

use crate::{
    config::IndexingRequestConsumerConfig,
    registry::IndexingRequestRegistry,
    set_indexing_target::{ApplyError, SetIndexingTarget, apply_set_indexing_target},
    worker::service::{JobPriority, WorkerQueue},
};

/// The propose event type Studio sends.
const EVENT_TYPE_PROPOSE: &str = "subgraph.indexing.request.propose";

/// Studio's producer also defines a terminate event, but nothing sends it:
/// cancellation is a propose with a count of 0. Logged and skipped if seen.
const EVENT_TYPE_TERMINATE: &str = "subgraph.indexing.agreements.terminate";

/// Extra connection attempts after the first before startup fails visibly.
const CONNECT_MAX_RETRIES: u32 = 5;

/// Delay before retrying after a fetch or apply failure.
const RETRY_BACKOFF: Duration = Duration::from_secs(5);

/// Pause after a fetch below the high watermark that returned no usable
/// records, so a run of dropped batches cannot spin the loop hot.
const EMPTY_FETCH_PAUSE: Duration = Duration::from_secs(1);

/// How often to re-read topic metadata to notice a partition count change.
const PARTITION_METADATA_CHECK_INTERVAL: Duration = Duration::from_secs(300);

/// Bound on one metadata re-read. Deliberately shorter than the 5-second stop
/// cap, since the stop channel is not polled while the read is in flight.
const PARTITION_METADATA_CHECK_TIMEOUT: Duration = Duration::from_secs(3);

/// Handle for controlling the indexing request consumer lifecycle
#[derive(Clone)]
pub struct Handle {
    tx_stop: mpsc::Sender<()>,
}

impl Handle {
    /// Stop the consumer gracefully
    pub async fn stop(&self) {
        if self.tx_stop.is_closed() {
            return;
        }

        let _ = self.tx_stop.send(()).await;
        self.tx_stop.closed().await;
    }
}

/// Registry for persisting per-partition consumer progress.
#[async_trait]
pub trait KafkaConsumerOffsetRegistry {
    /// Get the next offset to fetch for a topic partition, `None` on first run.
    async fn get_kafka_consumer_offset(
        &self,
        topic: &str,
        partition_id: i32,
    ) -> Result<Option<i64>, crate::registry::Error>;

    /// Record the next offset to fetch for a topic partition.
    async fn set_kafka_consumer_offset(
        &self,
        topic: &str,
        partition_id: i32,
        next_offset: i64,
    ) -> Result<(), crate::registry::Error>;
}

/// Context required by the indexing request consumer service
pub struct Ctx<R, W> {
    /// Registry for indexing requests and consumer offsets
    pub registry: R,
    /// Worker queue for the reassessment jobs that follow an applied request
    pub worker_queue: W,
    /// Lifecycle events emitter (request-received on newly inserted requests)
    pub events: Arc<dyn SubgraphIndexingAgreementEventsProducer>,
    /// The protocol network chain id (signer chain id), for validating the
    /// envelope's network and stamping emitted lifecycle events
    pub protocol_chain_id: ChainId,
    /// Ceiling on the indexer count a consumed request may ask for; larger
    /// counts are clamped with a warning. Kafka records carry no signature,
    /// so this door gets a cap the signed admin RPC does not need.
    pub max_candidates: usize,
    /// Service configuration
    pub config: IndexingRequestConsumerConfig,
}

/// Create a new indexing request consumer service: a control handle plus a
/// future to spawn that reads Studio's propose events from Kafka, applies each
/// as a set-indexing-target change, and records its progress per partition.
pub fn new<R, W>(ctx: Ctx<R, W>) -> (Handle, impl Future<Output = anyhow::Result<()>>)
where
    R: IndexingRequestRegistry + KafkaConsumerOffsetRegistry + Clone + Send + Sync + 'static,
    W: WorkerQueue + Clone + Send + Sync + 'static,
{
    let (tx_stop, mut rx_stop) = mpsc::channel(1);

    let service = async move {
        let consumer = match connect_with_retries(&ctx.config, &mut rx_stop).await {
            Ok(Some(consumer)) => Arc::new(consumer),
            // Stop was requested while still connecting; a clean exit.
            Ok(None) => return Ok(()),
            Err(err) => return Err(err),
        };

        let partitions = consumer.partitions();
        tracing::info!(
            topic = consumer.topic(),
            partitions = partitions.len(),
            requested_by = %ctx.config.requested_by,
            "indexing request consumer started"
        );

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut tasks: JoinSet<()> = JoinSet::new();
        for partition in partitions {
            tasks.spawn(partition_loop(
                Arc::clone(&consumer),
                partition,
                ctx.registry.clone(),
                ctx.worker_queue.clone(),
                Arc::clone(&ctx.events),
                ctx.protocol_chain_id,
                ctx.max_candidates,
                ctx.config.clone(),
                shutdown_rx.clone(),
            ));
        }

        // Partition loops only return on shutdown, so one finishing early means
        // it panicked or hit a bug; tear the service down so the process
        // restarts instead of consuming a partial set of partitions. The
        // metadata timer notices a topic growing partitions: new partitions
        // would otherwise be consumed by nobody, silently losing requests, so
        // that also restarts the service to pick up the full layout.
        let serving = tasks.len();
        let mut metadata_check = tokio::time::interval(PARTITION_METADATA_CHECK_INTERVAL);
        metadata_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let result = loop {
            tokio::select! {
                _ = rx_stop.recv() => break Ok(()),
                joined = tasks.join_next() => break match joined {
                    Some(Ok(())) => Err(anyhow::anyhow!(
                        "an indexing request consumer partition loop exited unexpectedly"
                    )),
                    Some(Err(err)) => Err(anyhow::anyhow!(
                        "an indexing request consumer partition loop panicked: {err}"
                    )),
                    None => Err(anyhow::anyhow!(
                        "the indexing request consumer had no partition loops to run"
                    )),
                },
                _ = metadata_check.tick() => {
                    let count = tokio::time::timeout(
                        PARTITION_METADATA_CHECK_TIMEOUT,
                        consumer.current_partition_count(),
                    )
                    .await;
                    match count {
                        Ok(Ok(count)) if count > serving => break Err(anyhow::anyhow!(
                            "topic '{}' now has {count} partitions but this consumer serves \
                             {serving}; restarting to consume the full set",
                            consumer.topic()
                        )),
                        // Kafka partitions only ever grow, so a lower count is a
                        // stale or partial metadata read, never a real change;
                        // restarting the process over it would be a false alarm.
                        Ok(Ok(count)) if count < serving => tracing::warn!(
                            count,
                            serving,
                            "metadata reported fewer partitions than this consumer serves; \
                             ignoring the stale read"
                        ),
                        Ok(Ok(_)) => {}
                        Ok(Err(err)) => {
                            tracing::warn!(error = %err, "failed to re-check topic partition metadata");
                        }
                        Err(_) => {
                            tracing::warn!("topic partition metadata re-check timed out");
                        }
                    }
                },
            }
        };

        let _ = shutdown_tx.send(true);
        while tasks.join_next().await.is_some() {}

        tracing::info!("indexing request consumer stopped");
        result
    };

    (Handle { tx_stop }, service)
}

/// Connects to the brokers, retrying transient failures a bounded number of
/// times. A missing topic fails immediately: it is configuration, and reading
/// from a wrong or absent topic must be loud, not an idle consumer.
async fn connect_with_retries(
    config: &IndexingRequestConsumerConfig,
    rx_stop: &mut mpsc::Receiver<()>,
) -> anyhow::Result<Option<KafkaConsumer>> {
    let mut attempt: u32 = 0;
    loop {
        match KafkaConsumer::connect(&config.kafka).await {
            Ok(consumer) => return Ok(Some(consumer)),
            Err(err @ ConsumerError::TopicNotFound { .. }) => {
                return Err(anyhow::anyhow!(
                    "indexing request consumer startup failed: {err}; check the configured topic \
                     name against the topic Studio produces on"
                ));
            }
            Err(err) if attempt < CONNECT_MAX_RETRIES => {
                attempt += 1;
                let delay = Duration::from_secs(2u64.pow(attempt.min(5)));
                tracing::warn!(
                    attempt,
                    delay_secs = delay.as_secs(),
                    error = %err,
                    "indexing request consumer connect failed, retrying"
                );
                tokio::select! {
                    _ = rx_stop.recv() => return Ok(None),
                    _ = tokio::time::sleep(delay) => {}
                }
            }
            Err(err) => {
                return Err(anyhow::anyhow!(
                    "indexing request consumer failed to connect after {} attempts: {err}",
                    CONNECT_MAX_RETRIES + 1
                ));
            }
        }
    }
}

/// Consumes one partition sequentially: fetch from the persisted offset, apply
/// each record, then persist the offset past it. Delivery is at-least-once;
/// redelivering an open request's count is a registry no-op, though a replay
/// reaching back past a cancellation briefly re-opens it until the cancel
/// replays too, which is why re-anchoring below never rewinds further than
/// the broker forces it to.
#[allow(clippy::too_many_arguments)]
async fn partition_loop<R, W>(
    consumer: Arc<KafkaConsumer>,
    partition: i32,
    registry: R,
    worker_queue: W,
    events: Arc<dyn SubgraphIndexingAgreementEventsProducer>,
    protocol_chain_id: ChainId,
    max_candidates: usize,
    config: IndexingRequestConsumerConfig,
    mut shutdown_rx: watch::Receiver<bool>,
) where
    R: IndexingRequestRegistry + KafkaConsumerOffsetRegistry + Send + Sync,
    W: WorkerQueue + Send + Sync,
{
    let topic = consumer.topic().to_string();
    let expected_network = format!("eip155:{protocol_chain_id}");
    let max_wait_ms = config.max_wait.as_millis().min(i32::MAX as u128) as i32;

    // Resume from the persisted offset; a partition never seen before starts at
    // the earliest available record so requests published before the consumer's
    // first deploy are not lost.
    let mut next_offset = loop {
        let restored = match registry.get_kafka_consumer_offset(&topic, partition).await {
            Ok(restored) => restored,
            Err(err) => {
                tracing::error!(partition, error = %err, "failed to load consumer offset, retrying");
                if sleep_or_shutdown(&mut shutdown_rx, RETRY_BACKOFF).await {
                    return;
                }
                continue;
            }
        };
        match restored {
            Some(offset) => break offset,
            None => match consumer.offset(partition, OffsetAt::Earliest).await {
                Ok(earliest) => break earliest,
                Err(err) => {
                    tracing::error!(partition, error = %err, "failed to query earliest offset, retrying");
                    if sleep_or_shutdown(&mut shutdown_rx, RETRY_BACKOFF).await {
                        return;
                    }
                }
            },
        }
    };
    tracing::debug!(partition, next_offset, "partition consumer resuming");

    loop {
        let fetch = tokio::select! {
            _ = shutdown_rx.changed() => return,
            fetch = consumer.fetch(partition, next_offset, config.fetch_max_bytes, max_wait_ms) => fetch,
        };

        let (records, high_watermark) = match fetch {
            Ok((records, high_watermark)) => (records, high_watermark),
            Err(err) if err.is_offset_out_of_range() => {
                // The broker refuses a cursor outside its retained range: below
                // the log start when retention deleted records, or above the
                // log end when the topic was recreated or truncated. Clamp to
                // the nearest live edge; always rewinding to earliest would
                // replay the whole retained topic in the truncation case.
                let range = match consumer.offset(partition, OffsetAt::Earliest).await {
                    Ok(earliest) => match consumer.offset(partition, OffsetAt::Latest).await {
                        Ok(latest) => Some((earliest, latest)),
                        Err(err) => {
                            tracing::error!(partition, error = %err, "failed to query latest offset");
                            None
                        }
                    },
                    Err(err) => {
                        tracing::error!(partition, error = %err, "failed to query earliest offset");
                        None
                    }
                };
                match range {
                    Some((earliest, latest)) => {
                        let re_anchored = if next_offset < earliest {
                            earliest
                        } else {
                            latest
                        };
                        tracing::warn!(
                            partition,
                            stale_offset = next_offset,
                            earliest,
                            latest,
                            re_anchored,
                            "consumer offset fell outside the broker's retained range; re-anchoring"
                        );
                        next_offset = re_anchored;
                        persist_offset(&registry, &topic, partition, next_offset).await;
                    }
                    None => {
                        if sleep_or_shutdown(&mut shutdown_rx, RETRY_BACKOFF).await {
                            return;
                        }
                    }
                }
                continue;
            }
            Err(err) => {
                tracing::error!(partition, error = %err, "fetch failed, backing off");
                if sleep_or_shutdown(&mut shutdown_rx, RETRY_BACKOFF).await {
                    return;
                }
                continue;
            }
        };

        let records_was_empty = records.is_empty();
        for record_and_offset in records {
            let offset = record_and_offset.offset;
            match decode_propose(record_and_offset.record.value.as_deref(), &expected_network) {
                Err(reason) => {
                    tracing::warn!(
                        topic,
                        partition,
                        offset,
                        reason,
                        key = ?record_and_offset.record.key.as_deref().map(String::from_utf8_lossy),
                        "skipping unprocessable indexing request record"
                    );
                }
                Ok(target) => {
                    tracing::debug!(
                        event_id = %target.event_id,
                        deployment_id = %target.deployment_id,
                        deployment_chain_id = target.deployment_chain_id,
                        num_candidates = target.num_candidates,
                        "consumed indexing request propose event"
                    );
                    let applied = apply_target_with_retries(
                        &registry,
                        &worker_queue,
                        &events,
                        protocol_chain_id,
                        config.requested_by,
                        max_candidates,
                        &target,
                        partition,
                        offset,
                        &mut shutdown_rx,
                    )
                    .await;
                    if !applied {
                        // Shutdown arrived before the record took effect; the
                        // unadvanced offset redelivers it on the next run.
                        return;
                    }
                }
            }

            next_offset = offset + 1;
            persist_offset(&registry, &topic, partition, next_offset).await;
        }

        // An empty fetch below the high watermark (e.g. a batch of records the
        // client filtered out) would otherwise loop again instantly: pause so
        // a run of them cannot spin hot.
        if records_was_empty && high_watermark > next_offset {
            tracing::debug!(
                partition,
                next_offset,
                high_watermark,
                "fetch below the high watermark returned no records; pausing"
            );
            if sleep_or_shutdown(&mut shutdown_rx, EMPTY_FETCH_PAUSE).await {
                return;
            }
        }
    }
}

/// Persist consumer progress. Failure is logged but does not halt consumption:
/// the in-memory cursor stays correct and a later write covers the gap, at the
/// cost of some redelivery after a restart (which is safe).
async fn persist_offset<R: KafkaConsumerOffsetRegistry>(
    registry: &R,
    topic: &str,
    partition: i32,
    next_offset: i64,
) {
    if let Err(err) = registry
        .set_kafka_consumer_offset(topic, partition, next_offset)
        .await
    {
        tracing::error!(
            topic,
            partition,
            next_offset,
            error = %err,
            "failed to persist consumer offset; progress will be re-delivered after a restart"
        );
    }
}

/// Wait out a backoff, returning `true` when shutdown was requested instead.
async fn sleep_or_shutdown(shutdown_rx: &mut watch::Receiver<bool>, delay: Duration) -> bool {
    tokio::select! {
        _ = shutdown_rx.changed() => true,
        _ = tokio::time::sleep(delay) => false,
    }
}

/// Apply a decoded propose until it fully takes effect, returning `false` if
/// shutdown was requested first. A registry failure retries the whole apply
/// (nothing was committed); a queue failure retries only the job push, because
/// the row change is already committed and re-running the apply would land on
/// the registry's no-op path and silently drop the reassessment.
#[allow(clippy::too_many_arguments)]
async fn apply_target_with_retries<R, W>(
    registry: &R,
    worker_queue: &W,
    events: &Arc<dyn SubgraphIndexingAgreementEventsProducer>,
    protocol_chain_id: ChainId,
    requested_by: Address,
    max_candidates: usize,
    target: &ProposedTarget,
    partition: i32,
    offset: i64,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> bool
where
    R: IndexingRequestRegistry + Send + Sync,
    W: WorkerQueue + Send + Sync,
{
    // Cap the count from the wire: this door has no signature to vouch for the
    // sender, so an absurd target must not reach indexer selection unclamped.
    // 0 passes through untouched, since it is the cancellation shape.
    let num_candidates = if target.num_candidates > max_candidates {
        tracing::warn!(
            partition,
            offset,
            requested = target.num_candidates,
            max_candidates,
            "clamping the requested indexer count to the configured maximum"
        );
        max_candidates
    } else {
        target.num_candidates
    };

    let queue_retry_id = loop {
        let result = apply_set_indexing_target(
            registry,
            worker_queue,
            events,
            protocol_chain_id,
            SetIndexingTarget {
                requested_by,
                deployment_id: target.deployment_id,
                deployment_chain_id: target.deployment_chain_id,
                num_candidates,
                // Interactive: a developer just asked for this in Studio.
                priority: JobPriority::Interactive,
            },
        )
        .await;

        match result {
            Ok(_) => return true,
            Err(err @ ApplyError::Registry(_)) => {
                tracing::error!(
                    partition,
                    offset,
                    error = %err,
                    "failed to apply indexing request, retrying"
                );
                if sleep_or_shutdown(shutdown_rx, RETRY_BACKOFF).await {
                    return false;
                }
            }
            Err(ApplyError::QueueReassess { id, .. }) => break id,
        }
    };

    loop {
        if sleep_or_shutdown(shutdown_rx, RETRY_BACKOFF).await {
            // One last immediate attempt: the row change is already committed,
            // so leaving without the job strands the request until the daily
            // reassignment sweep next queues it.
            return retry_reassess_push(worker_queue, queue_retry_id, num_candidates, target).await;
        }
        if retry_reassess_push(worker_queue, queue_retry_id, num_candidates, target).await {
            return true;
        }
    }
}

/// One attempt at the reassessment push that failed inside the apply.
async fn retry_reassess_push<W>(
    worker_queue: &W,
    id: dipper_core::ids::IndexingRequestId,
    num_candidates: usize,
    target: &ProposedTarget,
) -> bool
where
    W: WorkerQueue + Send + Sync,
{
    match worker_queue
        .reassess_indexing_request(
            id,
            target.deployment_id,
            target.deployment_chain_id,
            num_candidates,
            JobPriority::Interactive,
        )
        .await
    {
        Ok(_) => true,
        Err(err) => {
            tracing::error!(
                indexing_request_id = %id,
                error = ?err,
                "retrying the reassessment job push"
            );
            false
        }
    }
}

/// A validated propose event, reduced to what the registry call needs.
#[derive(Debug, PartialEq, Eq)]
struct ProposedTarget {
    event_id: String,
    deployment_id: DeploymentId,
    deployment_chain_id: ChainId,
    num_candidates: usize,
}

/// Decode a record value into a propose target, or the reason to skip it.
/// Unknown event types are tolerated by design: Studio may add types before
/// the dipper learns them, and they must not wedge the partition.
fn decode_propose(
    value: Option<&[u8]>,
    expected_network: &str,
) -> Result<ProposedTarget, &'static str> {
    let Some(value) = value else {
        return Err("empty record value");
    };

    let event = match studio::SubgraphIndexingRequestEvent::decode(value) {
        Ok(event) => event,
        Err(_) => return Err("undecodable protobuf"),
    };

    match event.event_type.as_str() {
        EVENT_TYPE_PROPOSE => {}
        EVENT_TYPE_TERMINATE => return Err("terminate event (cancellation is a propose with 0)"),
        _ => return Err("unknown event type"),
    }

    if event.the_graph_network_caip2id != expected_network {
        return Err("event is for a different protocol network");
    }

    let Some(studio::subgraph_indexing_request_event::Payload::SubgraphIndexingRequestPropose(
        propose,
    )) = event.payload
    else {
        return Err("propose event without a propose payload");
    };

    let Ok(deployment_id) = event.subgraph_deployment_qm_hash.parse::<DeploymentId>() else {
        return Err("invalid subgraph deployment hash");
    };

    let Some(deployment_chain_id) = parse_eip155_caip2(&propose.indexed_network_caip2id) else {
        // The field is the dipper's addition to Studio's schema; until Studio
        // sends it, every message lands here and this warn is the signal.
        return Err("missing or invalid indexed network caip2 id");
    };

    let Ok(num_candidates) = usize::try_from(propose.indexing_agreements_requested) else {
        return Err("negative indexing agreements requested");
    };

    Ok(ProposedTarget {
        event_id: event.event_id,
        deployment_id,
        deployment_chain_id,
        num_candidates,
    })
}

/// Parse an `eip155:{chain_id}` CAIP-2 identifier into its numeric chain id.
fn parse_eip155_caip2(value: &str) -> Option<ChainId> {
    value.strip_prefix("eip155:")?.parse::<ChainId>().ok()
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
    use thegraph_core::{DeploymentId, deployment_id};
    use url::Url;

    use super::*;
    use crate::{
        registry::{
            IndexingRequest as IndexingRequestRecord, Result as RegistryResult, SetTargetOutcome,
        },
        test_support::{CapturedEvent, CapturingEventsProducer},
        worker::queue::JobId,
    };

    /// The protocol (signer) chain id, distinct from the indexed chain below
    /// so assertions can tell the 2 apart.
    const PROTOCOL_CHAIN_ID: ChainId = 42161;

    /// The chain the test deployment indexes.
    const INDEXED_CHAIN_ID: ChainId = 1;

    const QM_HASH: &str = "QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv";

    /// Ceiling on requested indexer counts in these tests.
    const TEST_MAX_CANDIDATES: usize = 10;

    fn requester() -> Address {
        "0x8f8c426f956876325b1e037c6eae9b189952994c"
            .parse()
            .expect("valid address")
    }

    /// Encode a propose envelope the way Studio's producer does.
    fn encode_propose(
        event_type: &str,
        network: &str,
        qm_hash: &str,
        indexed_network: &str,
        count: i32,
    ) -> Vec<u8> {
        let event = studio::SubgraphIndexingRequestEvent {
            event_id: "01912345-6789-7abc-def0-123456789abc".to_string(),
            event_type: event_type.to_string(),
            event_version: "1.0".to_string(),
            timestamp: "2026-08-24T10:30:00.123Z".to_string(),
            subgraph_deployment_qm_hash: qm_hash.to_string(),
            the_graph_network_caip2id: network.to_string(),
            payload: Some(
                studio::subgraph_indexing_request_event::Payload::SubgraphIndexingRequestPropose(
                    studio::SubgraphIndexingRequestPropose {
                        indexing_agreements_requested: count,
                        indexed_network_caip2id: indexed_network.to_string(),
                    },
                ),
            ),
        };
        event.encode_to_vec()
    }

    fn valid_propose_bytes() -> Vec<u8> {
        encode_propose(EVENT_TYPE_PROPOSE, "eip155:42161", QM_HASH, "eip155:1", 3)
    }

    // -------- decode_propose --------

    #[test]
    fn decodes_a_valid_propose_event() {
        let bytes = valid_propose_bytes();
        let target = decode_propose(Some(&bytes), "eip155:42161").expect("decodes");

        assert_eq!(target.deployment_id, deployment_id!(QM_HASH));
        assert_eq!(target.deployment_chain_id, INDEXED_CHAIN_ID);
        assert_eq!(target.num_candidates, 3);
        assert_eq!(target.event_id, "01912345-6789-7abc-def0-123456789abc");
    }

    #[test]
    fn tolerates_unknown_fields_appended_to_the_envelope() {
        // A future schema revision adds fields this consumer does not know:
        // field 15, wire type 2 (length-delimited), 3 bytes of payload.
        let mut bytes = valid_propose_bytes();
        bytes.extend_from_slice(&[0x7A, 0x03, b'a', b'b', b'c']);

        let target = decode_propose(Some(&bytes), "eip155:42161").expect("decodes");
        assert_eq!(target.num_candidates, 3);
    }

    #[test]
    fn skips_a_zero_count_as_a_valid_cancellation() {
        // Count 0 is not a skip: it is the agreed cancellation shape.
        let bytes = encode_propose(EVENT_TYPE_PROPOSE, "eip155:42161", QM_HASH, "eip155:1", 0);
        let target = decode_propose(Some(&bytes), "eip155:42161").expect("decodes");
        assert_eq!(target.num_candidates, 0);
    }

    #[test]
    fn rejects_an_empty_record_value() {
        assert_eq!(
            decode_propose(None, "eip155:42161"),
            Err("empty record value")
        );
    }

    #[test]
    fn rejects_undecodable_bytes() {
        // 0xFF is a field-15 wire-type-7 tag; wire type 7 does not exist.
        let garbage = [0xFF, 0xFF, 0xFF];
        assert_eq!(
            decode_propose(Some(&garbage), "eip155:42161"),
            Err("undecodable protobuf")
        );
    }

    #[test]
    fn rejects_an_unknown_event_type() {
        let bytes = encode_propose(
            "subgraph.indexing.request.some_future_type",
            "eip155:42161",
            QM_HASH,
            "eip155:1",
            3,
        );
        assert_eq!(
            decode_propose(Some(&bytes), "eip155:42161"),
            Err("unknown event type")
        );
    }

    #[test]
    fn rejects_a_terminate_event() {
        let bytes = encode_propose(EVENT_TYPE_TERMINATE, "eip155:42161", QM_HASH, "eip155:1", 3);
        assert!(matches!(
            decode_propose(Some(&bytes), "eip155:42161"),
            Err(reason) if reason.contains("terminate")
        ));
    }

    #[test]
    fn rejects_an_event_for_another_protocol_network() {
        let bytes = encode_propose(EVENT_TYPE_PROPOSE, "eip155:421614", QM_HASH, "eip155:1", 3);
        assert_eq!(
            decode_propose(Some(&bytes), "eip155:42161"),
            Err("event is for a different protocol network")
        );
    }

    #[test]
    fn rejects_a_propose_without_a_payload() {
        let event = studio::SubgraphIndexingRequestEvent {
            event_id: "e".to_string(),
            event_type: EVENT_TYPE_PROPOSE.to_string(),
            event_version: "1.0".to_string(),
            timestamp: "t".to_string(),
            subgraph_deployment_qm_hash: QM_HASH.to_string(),
            the_graph_network_caip2id: "eip155:42161".to_string(),
            payload: None,
        };
        assert_eq!(
            decode_propose(Some(&event.encode_to_vec()), "eip155:42161"),
            Err("propose event without a propose payload")
        );
    }

    #[test]
    fn rejects_an_invalid_deployment_hash() {
        let bytes = encode_propose(
            EVENT_TYPE_PROPOSE,
            "eip155:42161",
            "not-a-deployment-hash",
            "eip155:1",
            3,
        );
        assert_eq!(
            decode_propose(Some(&bytes), "eip155:42161"),
            Err("invalid subgraph deployment hash")
        );
    }

    #[test]
    fn rejects_a_missing_indexed_network() {
        // Studio has not added the field yet: it decodes as an empty string.
        let bytes = encode_propose(EVENT_TYPE_PROPOSE, "eip155:42161", QM_HASH, "", 3);
        assert_eq!(
            decode_propose(Some(&bytes), "eip155:42161"),
            Err("missing or invalid indexed network caip2 id")
        );
    }

    #[test]
    fn rejects_a_malformed_indexed_network() {
        for indexed in ["cosmos:hub", "eip155:", "eip155:abc", "1"] {
            let bytes = encode_propose(EVENT_TYPE_PROPOSE, "eip155:42161", QM_HASH, indexed, 3);
            assert_eq!(
                decode_propose(Some(&bytes), "eip155:42161"),
                Err("missing or invalid indexed network caip2 id"),
                "indexed network {indexed:?} should be rejected"
            );
        }
    }

    #[test]
    fn rejects_a_negative_candidate_count() {
        let bytes = encode_propose(EVENT_TYPE_PROPOSE, "eip155:42161", QM_HASH, "eip155:1", -1);
        assert_eq!(
            decode_propose(Some(&bytes), "eip155:42161"),
            Err("negative indexing agreements requested")
        );
    }

    #[test]
    fn parses_eip155_caip2_ids() {
        assert_eq!(parse_eip155_caip2("eip155:1"), Some(1));
        assert_eq!(parse_eip155_caip2("eip155:42161"), Some(42161));
        assert_eq!(parse_eip155_caip2("eip155:"), None);
        assert_eq!(parse_eip155_caip2("eip155:1x"), None);
        assert_eq!(parse_eip155_caip2("solana:1"), None);
        assert_eq!(parse_eip155_caip2(""), None);
    }

    // -------- handle_record --------

    type SetTargetCall = (Address, DeploymentId, ChainId, usize);
    type ReassessCall = (IndexingRequestId, DeploymentId, ChainId, usize);

    /// A registry whose `set_indexing_target_candidates` pops the next scripted
    /// response (erroring once the script runs out) and records its arguments.
    #[derive(Clone)]
    struct MockRegistry {
        script: Arc<Mutex<Vec<Option<SetTargetOutcome>>>>,
        calls: Arc<Mutex<Vec<SetTargetCall>>>,
    }

    impl MockRegistry {
        /// `None` entries are errors; after the script is exhausted every call errors.
        fn scripted(script: Vec<Option<SetTargetOutcome>>) -> Self {
            Self {
                script: Arc::new(Mutex::new(script)),
                calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn returning(outcome: SetTargetOutcome) -> Self {
            Self::scripted(vec![Some(outcome)])
        }

        fn calls(&self) -> Vec<SetTargetCall> {
            self.calls.lock().expect("poisoned").clone()
        }
    }

    #[async_trait]
    impl IndexingRequestRegistry for MockRegistry {
        async fn set_indexing_target_candidates(
            &self,
            requested_by: Address,
            deployment_id: DeploymentId,
            deployment_chain_id: ChainId,
            num_candidates: usize,
        ) -> RegistryResult<SetTargetOutcome> {
            self.calls.lock().expect("poisoned").push((
                requested_by,
                deployment_id,
                deployment_chain_id,
                num_candidates,
            ));
            let mut script = self.script.lock().expect("poisoned");
            match if script.is_empty() {
                None
            } else {
                script.remove(0)
            } {
                Some(outcome) => Ok(outcome),
                None => Err(crate::registry::Error::NoRecordsUpdated),
            }
        }

        async fn get_all_indexing_requests(&self) -> RegistryResult<Vec<IndexingRequestRecord>> {
            unimplemented!()
        }

        async fn get_indexing_request_by_id(
            &self,
            _id: &IndexingRequestId,
        ) -> RegistryResult<Option<IndexingRequestRecord>> {
            unimplemented!()
        }

        async fn get_indexing_requests_by_deployment_id(
            &self,
            _deployment_id: &DeploymentId,
        ) -> RegistryResult<Vec<IndexingRequestRecord>> {
            unimplemented!()
        }

        async fn get_open_indexing_requests_for_reassessment(
            &self,
            _min_age_seconds: i64,
            _batch_size: i64,
        ) -> RegistryResult<Vec<IndexingRequestRecord>> {
            unimplemented!()
        }
    }

    /// A worker queue that records reassessment jobs, optionally failing the
    /// first N pushes to exercise the queue-retry path.
    #[derive(Clone)]
    struct MockWorker {
        reassessments: Arc<Mutex<Vec<ReassessCall>>>,
        failures_left: Arc<Mutex<usize>>,
    }

    impl MockWorker {
        fn new() -> Self {
            Self::failing_pushes(0)
        }

        fn failing_pushes(failures: usize) -> Self {
            Self {
                reassessments: Arc::new(Mutex::new(Vec::new())),
                failures_left: Arc::new(Mutex::new(failures)),
            }
        }

        fn reassessments(&self) -> Vec<ReassessCall> {
            self.reassessments.lock().expect("poisoned").clone()
        }
    }

    #[async_trait]
    impl WorkerQueue for MockWorker {
        async fn send_indexing_agreement_proposal(
            &self,
            _candidate_url: Url,
            _agreement_id: IndexingAgreementId,
            _indexing_request_id: IndexingRequestId,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
            _priority: crate::worker::queue::JobPriority,
        ) -> anyhow::Result<JobId> {
            unimplemented!()
        }

        async fn reassess_indexing_request(
            &self,
            indexing_request_id: IndexingRequestId,
            deployment_id: DeploymentId,
            deployment_chain_id: ChainId,
            num_candidates: usize,
            _priority: crate::worker::queue::JobPriority,
        ) -> anyhow::Result<JobId> {
            {
                let mut failures_left = self.failures_left.lock().expect("poisoned");
                if *failures_left > 0 {
                    *failures_left -= 1;
                    anyhow::bail!("scripted queue failure");
                }
            }
            self.reassessments.lock().expect("poisoned").push((
                indexing_request_id,
                deployment_id,
                deployment_chain_id,
                num_candidates,
            ));
            Ok(JobId::default())
        }

        async fn cancel_rejected_agreement_on_chain(
            &self,
            _agreement_id: IndexingAgreementId,
            _priority: crate::worker::queue::JobPriority,
        ) -> anyhow::Result<JobId> {
            unimplemented!()
        }

        async fn submit_offer(
            &self,
            _agreement_id: IndexingAgreementId,
            _indexing_request_id: IndexingRequestId,
            _indexer_url: Url,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
            _priority: crate::worker::queue::JobPriority,
        ) -> anyhow::Result<JobId> {
            unimplemented!()
        }
    }

    /// Drive `apply_target_with_retries` for the standard valid propose, with
    /// no shutdown pending, returning its result and the captured events.
    async fn run_apply(
        registry: &MockRegistry,
        worker: &MockWorker,
        num_candidates: usize,
    ) -> (bool, Vec<CapturedEvent>) {
        let events_capture = CapturingEventsProducer::new();
        let events: Arc<dyn SubgraphIndexingAgreementEventsProducer> =
            Arc::new(events_capture.clone());
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);

        let target = ProposedTarget {
            event_id: "01912345-6789-7abc-def0-123456789abc".to_string(),
            deployment_id: deployment_id!(QM_HASH),
            deployment_chain_id: INDEXED_CHAIN_ID,
            num_candidates,
        };

        let applied = apply_target_with_retries(
            registry,
            worker,
            &events,
            PROTOCOL_CHAIN_ID,
            requester(),
            TEST_MAX_CANDIDATES,
            &target,
            0,
            0,
            &mut shutdown_rx,
        )
        .await;

        (applied, events_capture.events())
    }

    #[tokio::test]
    async fn an_inserted_outcome_applies_emits_and_queues_reassessment() {
        let registry = MockRegistry::returning(SetTargetOutcome::Inserted {
            id: IndexingRequestId::new(),
        });
        let worker = MockWorker::new();

        let (applied, events) = run_apply(&registry, &worker, 3).await;

        assert!(applied);
        assert_eq!(
            registry.calls(),
            vec![(requester(), deployment_id!(QM_HASH), INDEXED_CHAIN_ID, 3)]
        );

        let reassessments = worker.reassessments();
        assert_eq!(reassessments.len(), 1);
        assert_eq!(reassessments[0].1, deployment_id!(QM_HASH));
        assert_eq!(reassessments[0].2, INDEXED_CHAIN_ID);
        assert_eq!(reassessments[0].3, 3);

        assert_eq!(events.len(), 1, "expected 1 request-received event");
        match &events[0] {
            CapturedEvent::RequestReceived {
                deployment,
                chain_id,
                event,
            } => {
                assert_eq!(*deployment, deployment_id!(QM_HASH));
                assert_eq!(
                    *chain_id, PROTOCOL_CHAIN_ID,
                    "the event carries the protocol chain id, not the indexed chain"
                );
                assert_eq!(event.agreements_requested, 3);
            }
            other => panic!("expected RequestReceived, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_noop_outcome_applies_without_events_or_reassessment() {
        let registry = MockRegistry::returning(SetTargetOutcome::NoOp {
            id: IndexingRequestId::new(),
        });
        let worker = MockWorker::new();

        let (applied, events) = run_apply(&registry, &worker, 3).await;

        assert!(applied);
        assert!(worker.reassessments().is_empty(), "no-op must not reassess");
        assert!(events.is_empty(), "no-op must not emit events");
    }

    #[tokio::test]
    async fn a_zero_count_cancellation_reassesses_to_zero_without_events() {
        let registry = MockRegistry::returning(SetTargetOutcome::Canceled {
            id: IndexingRequestId::new(),
        });
        let worker = MockWorker::new();

        let (applied, events) = run_apply(&registry, &worker, 0).await;

        assert!(applied);
        assert_eq!(registry.calls()[0].3, 0, "the registry saw the 0 count");

        let reassessments = worker.reassessments();
        assert_eq!(reassessments.len(), 1);
        assert_eq!(
            reassessments[0].3, 0,
            "reassessment with 0 drives the shrink that cancels agreements"
        );
        assert!(
            events.is_empty(),
            "cancellation must not emit request-received"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_registry_failure_retries_the_whole_apply_until_it_succeeds() {
        // 1st call errors (nothing committed), the retry lands the insert.
        let registry = MockRegistry::scripted(vec![
            None,
            Some(SetTargetOutcome::Inserted {
                id: IndexingRequestId::new(),
            }),
        ]);
        let worker = MockWorker::new();

        let (applied, events) = run_apply(&registry, &worker, 3).await;

        assert!(applied);
        assert_eq!(registry.calls().len(), 2, "the apply was retried once");
        assert_eq!(worker.reassessments().len(), 1);
        assert_eq!(events.len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_queue_failure_retries_only_the_push_never_the_registry() {
        // If the retry re-ran the registry call, the outcome would be a no-op
        // and the reassessment job would be silently lost.
        let registry = MockRegistry::returning(SetTargetOutcome::Inserted {
            id: IndexingRequestId::new(),
        });
        let worker = MockWorker::failing_pushes(2);

        let (applied, events) = run_apply(&registry, &worker, 3).await;

        assert!(applied);
        assert_eq!(
            registry.calls().len(),
            1,
            "the committed row change must not be re-applied"
        );
        assert_eq!(
            worker.reassessments().len(),
            1,
            "the push eventually landed"
        );
        assert_eq!(
            events.len(),
            1,
            "the lifecycle event is emitted exactly once"
        );
    }

    #[tokio::test]
    async fn an_oversized_count_is_clamped_to_the_configured_maximum() {
        let registry = MockRegistry::returning(SetTargetOutcome::Inserted {
            id: IndexingRequestId::new(),
        });
        let worker = MockWorker::new();

        let (applied, _) = run_apply(&registry, &worker, 5_000_000).await;

        assert!(applied);
        assert_eq!(
            registry.calls()[0].3,
            TEST_MAX_CANDIDATES,
            "the registry must see the clamped count, not the wire value"
        );
        assert_eq!(worker.reassessments()[0].3, TEST_MAX_CANDIDATES);
    }

    #[tokio::test]
    async fn a_pending_shutdown_stops_retrying_without_applying() {
        // Every registry call fails, so only shutdown can end the retry loop.
        let registry = MockRegistry::scripted(vec![]);
        let worker = MockWorker::new();

        let events_capture = CapturingEventsProducer::new();
        let events: Arc<dyn SubgraphIndexingAgreementEventsProducer> =
            Arc::new(events_capture.clone());
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        shutdown_tx.send(true).expect("send shutdown");

        let target = ProposedTarget {
            event_id: "e".to_string(),
            deployment_id: deployment_id!(QM_HASH),
            deployment_chain_id: INDEXED_CHAIN_ID,
            num_candidates: 3,
        };

        let applied = apply_target_with_retries(
            &registry,
            &worker,
            &events,
            PROTOCOL_CHAIN_ID,
            requester(),
            TEST_MAX_CANDIDATES,
            &target,
            0,
            0,
            &mut shutdown_rx,
        )
        .await;

        assert!(!applied, "shutdown must win over the retry loop");
        assert!(worker.reassessments().is_empty());
        assert!(events_capture.events().is_empty());
    }

    // -------- Redpanda-backed service test --------

    use dipper_producer::{
        events::SubgraphIndexingAgreementsEventsEmitter,
        kafka::{KafkaConfig, KafkaConsumerConfig, KafkaProducer},
    };

    use crate::registry::RegistryProvider;

    fn redpanda_brokers() -> Option<Vec<String>> {
        match std::env::var("REDPANDA_BROKERS") {
            Ok(value) if !value.trim().is_empty() => Some(
                value
                    .split(',')
                    .map(|broker| broker.trim().to_string())
                    .collect(),
            ),
            // REQUIRE_REDPANDA turns the silent skip into a failure, so CI
            // cannot go green while accidentally testing nothing.
            _ if std::env::var("REQUIRE_REDPANDA").is_ok() => {
                panic!("REQUIRE_REDPANDA is set but REDPANDA_BROKERS is not")
            }
            _ => {
                eprintln!("skipping Redpanda-backed test: REDPANDA_BROKERS is not set");
                None
            }
        }
    }

    fn unique_topic() -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock before unix epoch")
            .as_nanos();
        format!("dipper.test.consumer.{}.{nanos}", std::process::id())
    }

    async fn create_topic(brokers: &[String], topic: &str) {
        let client = rskafka::client::ClientBuilder::new(brokers.to_vec())
            .build()
            .await
            .expect("connect to broker");
        client
            .controller_client()
            .expect("controller client")
            .create_topic(topic, 1, 1, 5_000)
            .await
            .expect("create topic");
    }

    fn propose_bytes(qm_hash: &str, count: i32) -> Vec<u8> {
        studio::SubgraphIndexingRequestEvent {
            event_id: "01912345-6789-7abc-def0-123456789abc".to_string(),
            event_type: EVENT_TYPE_PROPOSE.to_string(),
            event_version: "1.0".to_string(),
            timestamp: "2026-08-24T10:30:00.123Z".to_string(),
            subgraph_deployment_qm_hash: qm_hash.to_string(),
            the_graph_network_caip2id: "eip155:42161".to_string(),
            payload: Some(
                studio::subgraph_indexing_request_event::Payload::SubgraphIndexingRequestPropose(
                    studio::SubgraphIndexingRequestPropose {
                        indexing_agreements_requested: count,
                        indexed_network_caip2id: "eip155:1".to_string(),
                    },
                ),
            ),
        }
        .encode_to_vec()
    }

    async fn wait_until(what: &str, mut check: impl AsyncFnMut() -> bool) {
        for _ in 0..300 {
            if check().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("timed out waiting for {what}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_consumer_resumes_from_persisted_offsets_across_restarts() {
        let Some(brokers) = redpanda_brokers() else {
            return;
        };

        let topic = unique_topic();
        create_topic(&brokers, &topic).await;

        let temp_db = pgtemp::PgTempDB::new();
        let db = sqlx::Pool::connect(&temp_db.connection_uri())
            .await
            .expect("connect to temp db");
        dipper_pgregistry::run_db_migrations(&db)
            .await
            .expect("run migrations");
        let provider = RegistryProvider::new(db.clone());

        let requested_by: Address = "0x8f8c426f956876325b1e037c6eae9b189952994c"
            .parse()
            .expect("valid address");
        let deployment_a = deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv");
        let deployment_b = deployment_id!("QmXbNL4EMkQ6DAPUcBjYSDXZJdpu1Kb1XkKvNvS8JdT7Hs");

        let producer_config: KafkaConfig = serde_json::from_value(serde_json::json!({
            "brokers": brokers,
            "topic": topic,
            "partitions": 1,
        }))
        .expect("valid producer config");
        let producer = KafkaProducer::new(&producer_config)
            .await
            .expect("producer connects");

        let consumer_config = IndexingRequestConsumerConfig {
            enabled: true,
            kafka: KafkaConsumerConfig {
                brokers: brokers.clone(),
                topic: topic.clone(),
                sasl_mechanism: None,
                sasl_username: None,
                sasl_password: None,
                tls_enabled: false,
                tls_ca_cert_path: None,
                connect_timeout_secs: 60,
            },
            requested_by,
            max_wait: Duration::from_secs(1),
            fetch_max_bytes: 1_048_576,
        };

        let run_service = |worker: MockWorker| {
            let events: Arc<dyn SubgraphIndexingAgreementEventsProducer> =
                Arc::new(SubgraphIndexingAgreementsEventsEmitter::disabled());
            let (handle, service) = new(Ctx {
                registry: provider.clone(),
                worker_queue: worker,
                events,
                protocol_chain_id: 42161,
                max_candidates: 10,
                config: consumer_config.clone(),
            });
            (handle, tokio::spawn(service))
        };

        // A propose published before the consumer ever ran must still be
        // picked up: a fresh partition starts from the earliest record.
        producer
            .send(
                "eip155:42161/QmUzRg.../request",
                &propose_bytes("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv", 2),
            )
            .await
            .expect("produce event A");

        let worker_run_1 = MockWorker::new();
        let (handle, task) = run_service(worker_run_1.clone());
        wait_until("request A to be registered", async || {
            !provider
                .get_indexing_requests_by_deployment_id(&deployment_a)
                .await
                .expect("query requests")
                .is_empty()
        })
        .await;
        wait_until("offset 1 to be persisted", async || {
            provider
                .get_kafka_consumer_offset(&topic, 0)
                .await
                .expect("query offset")
                == Some(1)
        })
        .await;
        handle.stop().await;
        task.await.expect("service task").expect("service result");

        assert_eq!(
            worker_run_1.reassessments().len(),
            1,
            "run 1 applied exactly the 1 produced event"
        );

        // Published while the consumer is down; run 2 must pick it up from the
        // persisted offset without re-applying event A.
        producer
            .send(
                "eip155:42161/QmXbNL.../request",
                &propose_bytes("QmXbNL4EMkQ6DAPUcBjYSDXZJdpu1Kb1XkKvNvS8JdT7Hs", 3),
            )
            .await
            .expect("produce event B");

        let worker_run_2 = MockWorker::new();
        let (handle, task) = run_service(worker_run_2.clone());
        wait_until("request B to be registered", async || {
            !provider
                .get_indexing_requests_by_deployment_id(&deployment_b)
                .await
                .expect("query requests")
                .is_empty()
        })
        .await;
        handle.stop().await;
        task.await.expect("service task").expect("service result");

        let run_2_reassessments = worker_run_2.reassessments();
        assert_eq!(
            run_2_reassessments.len(),
            1,
            "run 2 resumed past event A and applied only event B: {run_2_reassessments:?}"
        );
        assert_eq!(run_2_reassessments[0].1, deployment_b);
        assert_eq!(run_2_reassessments[0].3, 3);

        assert_eq!(
            provider
                .get_kafka_consumer_offset(&topic, 0)
                .await
                .expect("query offset"),
            Some(2),
            "both records are committed"
        );
        assert_eq!(
            provider
                .get_indexing_requests_by_deployment_id(&deployment_a)
                .await
                .expect("query requests")
                .len(),
            1,
            "event A was applied exactly once across both runs"
        );
    }
}
