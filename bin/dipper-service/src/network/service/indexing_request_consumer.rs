//! Consumes the subgraph indexing request events Studio produces on Redpanda
//! and applies each one as a set-indexing-target change, making the topic a
//! second front door to the same path the admin RPC serves.

use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use dipper_producer::{
    events::SubgraphIndexingAgreementEventsProducer,
    kafka::{ConsumerError, KafkaConsumer, OffsetAt, Reconnecting},
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
    time::Instant,
};

use crate::{
    config::{IndexingRequestConsumerConfig, MAX_NUM_CANDIDATES},
    log_text::with_causes,
    registry::IndexingRequestRegistry,
    request_chain_check::{ChainCheckError, RequestChainCheck},
    set_indexing_target::{ApplyError, SetIndexingTarget, apply_set_indexing_target, check_chain},
    worker::service::{JobPriority, WorkerQueue},
};

/// The propose event type Studio sends.
const EVENT_TYPE_PROPOSE: &str = "subgraph.indexing.request.propose";

/// Studio's producer also defines a terminate event, but nothing sends it:
/// cancellation is a propose with a count of 0. Logged and skipped if seen.
const EVENT_TYPE_TERMINATE: &str = "subgraph.indexing.agreements.terminate";

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

/// How long the same record, fetch or connect may be retried before it is alerted on.
const STALL_ALERT_AFTER: Duration = Duration::from_secs(10 * 60);

/// The `event` tag of the alert for a consumer that keeps retrying the same step.
const STALLED_EVENT: &str = "studio_request_consumer_stalled";

/// The `event` tag of the alert for a stored offset moved past records nobody read.
const OFFSET_JUMPED_EVENT: &str = "studio_request_offset_jumped";

/// The `event` tag of the alert for a Studio request dropped because dipper can't read it.
const DROPPED_EVENT: &str = "studio_request_dropped";

/// The `event` tag of the alert for a consumer that could not connect when dipper started.
const NOT_CONNECTED_EVENT: &str = "studio_request_consumer_not_connected";

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

/// One fetched record, reduced to what the consumer reads.
struct FetchedRecord {
    offset: i64,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
}

/// What a partition loop asks of the broker client. [`KafkaConsumer`] is the real one; tests
/// script their own, since the loop can't otherwise run without a broker.
#[async_trait]
trait PartitionSource: Send + Sync + 'static {
    /// Records from `offset` on, and the partition's high watermark.
    async fn fetch(
        &self,
        partition: i32,
        offset: i64,
        max_bytes: i32,
        max_wait_ms: i32,
    ) -> Result<(Vec<FetchedRecord>, i64), ConsumerError>;

    /// The partition's earliest or latest offset.
    async fn offset(&self, partition: i32, at: OffsetAt) -> Result<i64, ConsumerError>;
}

/// What the service asks of a connected client, on top of what each partition loop does.
#[async_trait]
trait TopicSource: PartitionSource {
    /// The partitions the topic had when this client connected, in ascending order.
    fn partitions(&self) -> Vec<i32>;

    /// The topic's partition count now, read again from the brokers.
    async fn current_partition_count(&self) -> Result<usize, ConsumerError>;
}

#[async_trait]
impl PartitionSource for KafkaConsumer {
    async fn fetch(
        &self,
        partition: i32,
        offset: i64,
        max_bytes: i32,
        max_wait_ms: i32,
    ) -> Result<(Vec<FetchedRecord>, i64), ConsumerError> {
        let (records, high_watermark) =
            KafkaConsumer::fetch(self, partition, offset, max_bytes, max_wait_ms).await?;
        let records = records
            .into_iter()
            .map(|fetched| FetchedRecord {
                offset: fetched.offset,
                key: fetched.record.key,
                value: fetched.record.value,
            })
            .collect();
        Ok((records, high_watermark))
    }

    async fn offset(&self, partition: i32, at: OffsetAt) -> Result<i64, ConsumerError> {
        KafkaConsumer::offset(self, partition, at).await
    }
}

#[async_trait]
impl TopicSource for KafkaConsumer {
    fn partitions(&self) -> Vec<i32> {
        KafkaConsumer::partitions(self)
    }

    async fn current_partition_count(&self) -> Result<usize, ConsumerError> {
        KafkaConsumer::current_partition_count(self).await
    }
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
    /// Refuses requests whose chain is not their subgraph manifest's
    pub chain_check: Arc<dyn RequestChainCheck>,
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
    let (tx_stop, rx_stop) = mpsc::channel(1);
    let kafka = ctx.config.kafka.clone();
    // `connect` enforces its own deadline, so each attempt allows a little longer and that
    // deadline's error is the one logged.
    let connect_timeout = Duration::from_secs(kafka.connect_timeout_secs.saturating_add(5));
    let connect = move || {
        let kafka = kafka.clone();
        async move { KafkaConsumer::connect(&kafka).await }
    };
    let service = serve(ctx, rx_stop, connect_timeout, connect);

    (Handle { tx_stop }, service)
}

/// The consumer service over any way of connecting, each attempt bounded by `connect_timeout`:
/// [`new`] passes the real brokers, and tests a script. Brokers that are down or a topic that
/// is missing never end it. A fault in a partition loop or a topic that gains partitions does.
async fn serve<C, R, W, F, Fut>(
    ctx: Ctx<R, W>,
    mut rx_stop: mpsc::Receiver<()>,
    connect_timeout: Duration,
    connect: F,
) -> anyhow::Result<()>
where
    C: TopicSource,
    R: IndexingRequestRegistry + KafkaConsumerOffsetRegistry + Clone + Send + Sync + 'static,
    W: WorkerQueue + Clone + Send + Sync + 'static,
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<C, ConsumerError>> + Send,
{
    let topic = ctx.config.kafka.topic.clone();
    let consumer = connecting(topic.clone(), connect_timeout, connect);
    let Some(partitions) = first_partitions(&consumer, &mut rx_stop).await else {
        tracing::info!("indexing request consumer stopped before it had connected");
        return Ok(());
    };
    tracing::info!(
        topic,
        partitions = partitions.len(),
        requested_by = %ctx.config.requested_by,
        "indexing request consumer started"
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut tasks = partition_loops(&ctx, &consumer, partitions, &shutdown_rx);
    let result = partition_loops_ended(&mut rx_stop, &mut tasks, &consumer, &topic).await;

    let _ = shutdown_tx.send(true);
    while tasks.join_next().await.is_some() {}

    tracing::info!("indexing request consumer stopped");
    result
}

/// Starts 1 loop for each of `partitions`, all stopped through `shutdown_rx`.
fn partition_loops<C, R, W>(
    ctx: &Ctx<R, W>,
    consumer: &Reconnecting<C>,
    partitions: Vec<i32>,
    shutdown_rx: &watch::Receiver<bool>,
) -> JoinSet<()>
where
    C: TopicSource,
    R: IndexingRequestRegistry + KafkaConsumerOffsetRegistry + Clone + Send + Sync + 'static,
    W: WorkerQueue + Clone + Send + Sync + 'static,
{
    let mut tasks = JoinSet::new();
    for partition in partitions {
        tasks.spawn(partition_loop(
            consumer.clone(),
            partition,
            ctx.registry.clone(),
            ctx.worker_queue.clone(),
            Arc::clone(&ctx.events),
            ctx.protocol_chain_id,
            Arc::clone(&ctx.chain_check),
            ctx.config.clone(),
            shutdown_rx.clone(),
        ));
    }
    tasks
}

/// The topic's partitions, once the first connect gets through: brokers that are down or a
/// topic that is not there yet are waited for, however long. `None` if asked to stop first.
async fn first_partitions<C: TopicSource>(
    consumer: &Reconnecting<C>,
    rx_stop: &mut mpsc::Receiver<()>,
) -> Option<Vec<i32>> {
    tokio::select! {
        _ = rx_stop.recv() => None,
        first = consumer.connected() => Some(first.partitions()),
    }
}

/// Waits for the reason to stop the partition loops: `Ok` when the service is asked to stop.
/// A loop only returns on shutdown, so one ending early panicked or hit a bug, and the service
/// ends with an error, which restarts dipper. So it does when the topic gains partitions.
async fn partition_loops_ended<C: TopicSource>(
    rx_stop: &mut mpsc::Receiver<()>,
    tasks: &mut JoinSet<()>,
    consumer: &Reconnecting<C>,
    topic: &str,
) -> anyhow::Result<()> {
    let serving = tasks.len();
    let mut metadata_check = tokio::time::interval(PARTITION_METADATA_CHECK_INTERVAL);
    metadata_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = rx_stop.recv() => return Ok(()),
            joined = tasks.join_next() => return Err(match joined {
                Some(Ok(())) => anyhow::anyhow!(
                    "an indexing request consumer partition loop exited unexpectedly"
                ),
                Some(Err(err)) => anyhow::anyhow!(
                    "an indexing request consumer partition loop panicked: {err}"
                ),
                None => anyhow::anyhow!(
                    "the indexing request consumer had no partition loops to run"
                ),
            }),
            _ = metadata_check.tick() => {
                if let Some(gained) = partitions_gained(consumer, serving, topic).await {
                    return Err(gained);
                }
            },
        }
    }
}

/// An error once the topic has more partitions than the `serving` this consumer reads, since
/// nobody would read the new ones. It costs 1 restart of dipper each time someone grows the
/// topic, which is a deliberate change and not an outage; the new process reads them all.
async fn partitions_gained<C: TopicSource>(
    consumer: &Reconnecting<C>,
    serving: usize,
    topic: &str,
) -> Option<anyhow::Error> {
    // While the client is being rebuilt there is nothing to ask; the next tick does.
    let client = consumer.current()?;
    let count = partition_count_now(client.as_ref()).await?;
    if count > serving {
        return Some(anyhow::anyhow!(
            "topic '{topic}' now has {count} partitions but this consumer serves {serving}; \
             restarting to consume the full set"
        ));
    }
    // Kafka partitions only ever grow, so a lower count is a stale or partial metadata read,
    // never a real change; restarting the process over it would be a false alarm.
    if count < serving {
        tracing::warn!(
            count,
            serving,
            "metadata reported fewer partitions than this consumer serves; ignoring the stale read"
        );
    }
    None
}

/// The topic's partition count as the brokers give it now, or `None` when it can't be read.
async fn partition_count_now<C: TopicSource>(client: &C) -> Option<usize> {
    let count = tokio::time::timeout(
        PARTITION_METADATA_CHECK_TIMEOUT,
        client.current_partition_count(),
    )
    .await;
    match count {
        Ok(Ok(count)) => Some(count),
        Ok(Err(err)) => {
            tracing::warn!(error = %err, "failed to re-check topic partition metadata");
            None
        }
        Err(_) => {
            tracing::warn!("topic partition metadata re-check timed out");
            None
        }
    }
}

/// A client connected in the background, however long that takes, and again whenever it has to
/// be rebuilt: rskafka never goes back to the bootstrap addresses while it knows any broker, even
/// after the cluster replaces them. Each attempt is noted, so failing ones are alerted on.
fn connecting<C, F, Fut>(topic: String, connect_timeout: Duration, connect: F) -> Reconnecting<C>
where
    C: TopicSource,
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<C, ConsumerError>> + Send,
{
    let connects = Arc::new(Mutex::new(Connects::default()));
    Reconnecting::spawn("indexing requests", connect_timeout, move || {
        let attempt = connect();
        let connects = Arc::clone(&connects);
        let topic = topic.clone();
        async move {
            let connected = attempt.await.and_then(|client| {
                // No partition means no loop to read it, so wait for the topic like a missing one.
                match client.partitions().is_empty() {
                    true => Err(ConsumerError::NoPartitions { topic }),
                    false => Ok(client),
                }
            });
            note_connect(&connects, &connected);
            connected
        }
    })
}

/// Consumes one partition in order: fetch from the stored offset, apply each record, store the
/// offset past it. Delivery is at-least-once: replaying a count is a no-op, but a replay past a
/// cancellation re-opens it until the cancel replays, so re-anchoring rewinds no more than forced.
#[expect(
    clippy::cognitive_complexity,
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "predates this lint; fix when next touched"
)]
async fn partition_loop<C, R, W>(
    consumer: Reconnecting<C>,
    partition: i32,
    registry: R,
    worker_queue: W,
    events: Arc<dyn SubgraphIndexingAgreementEventsProducer>,
    protocol_chain_id: ChainId,
    chain_check: Arc<dyn RequestChainCheck>,
    config: IndexingRequestConsumerConfig,
    mut shutdown_rx: watch::Receiver<bool>,
) where
    C: PartitionSource,
    R: IndexingRequestRegistry + KafkaConsumerOffsetRegistry + Send + Sync,
    W: WorkerQueue + Send + Sync,
{
    let topic = config.kafka.topic.clone();
    let fetch_max_bytes = config.fetch_max_bytes;
    let expected_network = format!("eip155:{protocol_chain_id}");
    let max_wait_ms = i32::try_from(config.max_wait.as_millis()).unwrap_or(i32::MAX);

    // Resume from the persisted offset; a partition never seen before starts at
    // the earliest available record so requests published before the consumer's
    // first deploy are not lost.
    let mut stall = Stall::default();
    let mut next_offset = loop {
        if stopping(&shutdown_rx) {
            return;
        }
        let restored = match registry.get_kafka_consumer_offset(&topic, partition).await {
            Ok(restored) => restored,
            Err(err) => {
                tracing::error!(partition, error = %err, "failed to load consumer offset, retrying");
                note_failure(
                    &mut stall,
                    partition,
                    None,
                    "loading its stored offset",
                    &err,
                );
                if sleep_or_shutdown(&mut shutdown_rx, RETRY_BACKOFF).await {
                    return;
                }
                continue;
            }
        };
        match restored {
            Some(offset) => break offset,
            None => match with_consumer(&consumer, &mut shutdown_rx, |c| async move {
                c.offset(partition, OffsetAt::Earliest).await
            })
            .await
            {
                None => return,
                Some(Ok(earliest)) => break earliest,
                Some(Err(err)) => {
                    tracing::error!(partition, error = %err, "failed to query earliest offset, retrying");
                    let step = "reading the partition's earliest offset";
                    note_failure(&mut stall, partition, None, step, &err);
                    if sleep_or_shutdown(&mut shutdown_rx, RETRY_BACKOFF).await {
                        return;
                    }
                }
            },
        }
    };
    stall.cleared();
    tracing::debug!(partition, next_offset, "partition consumer resuming");

    loop {
        let Some(fetch) = with_consumer(&consumer, &mut shutdown_rx, |c| async move {
            c.fetch(partition, next_offset, fetch_max_bytes, max_wait_ms)
                .await
        })
        .await
        else {
            return;
        };

        let (records, high_watermark) = match fetch {
            Ok((records, high_watermark)) => {
                stall.cleared();
                (records, high_watermark)
            }
            Err(err) if err.is_offset_out_of_range() => {
                // The broker calls the offset out of range: behind the log start after retention,
                // or past its end after a recreate or truncation. Move to the nearer edge, since
                // rewinding to the earliest would replay the whole topic when truncated.
                let Some(range) = with_consumer(&consumer, &mut shutdown_rx, |c| async move {
                    // Both edges through 1 client, so a rebuild between them can't mix 2 clusters.
                    let earliest = c.offset(partition, OffsetAt::Earliest).await?;
                    let latest = c.offset(partition, OffsetAt::Latest).await?;
                    Ok((earliest, latest))
                })
                .await
                else {
                    return;
                };
                let moved = match range {
                    Ok((earliest, latest)) => {
                        let moved = re_anchor(next_offset, earliest, latest);
                        if let Some(to) = moved {
                            alert_offset_jump(partition, next_offset, to, (earliest, latest));
                        } else {
                            tracing::warn!(
                                partition,
                                next_offset,
                                earliest,
                                latest,
                                "the broker refused an offset inside the range it reports; \
                                 fetching from it again"
                            );
                            let at = Some(next_offset);
                            note_failure(&mut stall, partition, at, "fetching records", &err);
                        }
                        moved
                    }
                    Err(err) => {
                        tracing::error!(partition, error = %err, "failed to query the retained offset range");
                        let (step, at) = ("reading the partition's retained offsets", next_offset);
                        note_failure(&mut stall, partition, Some(at), step, &err);
                        None
                    }
                };
                match moved {
                    Some(to) => {
                        next_offset = to;
                        persist_offset(&registry, &topic, partition, next_offset).await;
                    }
                    // Nothing to move to yet, so wait: a broker that keeps answering this way
                    // would otherwise be asked again at once, for ever.
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
                let at = Some(next_offset);
                note_failure(&mut stall, partition, at, "fetching records", &err);
                if sleep_or_shutdown(&mut shutdown_rx, RETRY_BACKOFF).await {
                    return;
                }
                continue;
            }
        };

        let records_was_empty = records.is_empty();
        for record in records {
            // A batch can hold thousands of records, so a stop is looked for before each one;
            // the rest are fetched again on the next run.
            if stopping(&shutdown_rx) {
                return;
            }
            let offset = record.offset;
            match decode_propose(record.value.as_deref(), &expected_network) {
                Err(skip) => {
                    log_skipped_record(skip, &topic, partition, offset, record.key.as_deref());
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
                        chain_check.as_ref(),
                        protocol_chain_id,
                        config.requested_by,
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

/// Where to move an offset the broker called out of range: the nearer edge of what it retains,
/// or `None` when the offset is inside that range after all and must stay where it is.
fn re_anchor(next_offset: i64, earliest: i64, latest: i64) -> Option<i64> {
    if next_offset < earliest {
        Some(earliest)
    } else if next_offset > latest {
        Some(latest)
    } else {
        None
    }
}

/// Raise an alert for a stored offset moved to an edge of what the broker retains. The records
/// passed over were never read, so a request among them was never applied, and Studio is not told.
fn alert_offset_jump(partition: i32, from: i64, to: i64, (earliest, latest): (i64, i64)) {
    let why = if to > from {
        "the broker no longer retains the records in between"
    } else {
        "the log now ends before it, as it does after the topic is recreated, and records \
         already in the new log are not read"
    };
    tracing::error!(
        event = OFFSET_JUMPED_EVENT,
        partition,
        stale_offset = from,
        re_anchored = to,
        earliest,
        latest,
        "The Studio indexing request consumer's offset was moved: {why}. A request passed \
         over this way is never applied"
    );
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

/// Runs `call` on the current client, waiting while one is being connected, and reports whether it
/// reached the brokers, so a client left dialling brokers that are gone gets rebuilt. Returns
/// `None` if shutdown is requested first.
async fn with_consumer<C, T, F, Fut>(
    consumer: &Reconnecting<C>,
    shutdown_rx: &mut watch::Receiver<bool>,
    call: F,
) -> Option<Result<T, ConsumerError>>
where
    C: PartitionSource,
    F: FnOnce(Arc<C>) -> Fut,
    Fut: Future<Output = Result<T, ConsumerError>>,
{
    tokio::select! {
        biased;
        () = stop_requested(shutdown_rx) => None,
        result = consumer.call(call) => Some(result),
    }
}

/// Wait out a backoff, returning `true` when shutdown was requested instead.
async fn sleep_or_shutdown(shutdown_rx: &mut watch::Receiver<bool>, delay: Duration) -> bool {
    tokio::select! {
        biased;
        () = stop_requested(shutdown_rx) => true,
        () = tokio::time::sleep(delay) => false,
    }
}

/// How long the consumer has been retrying 1 step, to say when that is worth an alert.
#[derive(Default)]
struct Stall(Option<(Instant, Instant)>);

impl Stall {
    /// Counts 1 more failure of the step. Returns how long it has been failing when an alert is
    /// due: once that reaches [`STALL_ALERT_AFTER`], and again each time as long passes.
    fn failed(&mut self) -> Option<Duration> {
        let now = Instant::now();
        let (since, alert_at) = self.0.get_or_insert((now, now + STALL_ALERT_AFTER));
        if now < *alert_at {
            return None;
        }
        *alert_at = now + STALL_ALERT_AFTER;
        Some(now - *since)
    }

    /// The step got through, so the next failure starts a new count.
    fn cleared(&mut self) {
        self.0 = None;
    }

    /// Whether the step's last attempt failed.
    fn failing(&self) -> bool {
        self.0.is_some()
    }
}

/// Raise an alert for a partition that has made no progress for a while. Nothing behind the
/// step it is stuck on is applied, cancels included, and Studio is never told.
fn alert_stalled(
    partition: i32,
    offset: Option<i64>,
    step: &str,
    stalled_for: Duration,
    error: &dyn std::fmt::Display,
) {
    tracing::error!(
        event = STALLED_EVENT,
        partition,
        offset,
        stalled_for_secs = stalled_for.as_secs(),
        %error,
        "The Studio indexing request consumer keeps failing at {step}; no later request on \
         this partition is applied until it gets through"
    );
}

/// Counts 1 more failed attempt at `step`, alerting once the step has failed for long enough.
fn note_failure(
    stall: &mut Stall,
    partition: i32,
    offset: Option<i64>,
    step: &str,
    error: &(dyn std::error::Error + 'static),
) {
    if let Some(stalled_for) = stall.failed() {
        alert_stalled(partition, offset, step, stalled_for, &with_causes(error));
    }
}

/// The retries of 1 record: where it sits, and how long it has been failing.
struct Retries {
    partition: i32,
    offset: i64,
    stall: Stall,
}

impl Retries {
    /// Logs a failed attempt at `step`, and alerts once the record has been stuck long enough.
    fn failed(&mut self, step: &str, error: &(dyn std::error::Error + 'static)) {
        let (partition, offset) = (self.partition, self.offset);
        tracing::error!(
            partition,
            offset,
            error = with_causes(error),
            "failed at {step} for a Studio indexing request, retrying"
        );
        note_failure(&mut self.stall, partition, Some(offset), step, error);
    }
}

/// What the attempts to connect have come to since dipper started.
#[derive(Default)]
struct Connects {
    /// Whether any of them has succeeded.
    connected_before: bool,
    /// How long they have been failing.
    stall: Stall,
}

/// Notes what an attempt to connect found. A failure before the first connect is alerted on at
/// once, since it may be a wrong broker or topic setting and no longer stops dipper. Any run of
/// failures is alerted on again once it has gone on long enough.
fn note_connect<T>(connects: &Mutex<Connects>, connected: &Result<T, ConsumerError>) {
    let mut connects = connects
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let error = match connected {
        Ok(_) => {
            connects.connected_before = true;
            return connects.stall.cleared();
        }
        Err(error) => error,
    };
    let first_failure = !connects.stall.failing();
    let stalled_for = connects.stall.failed();
    if !connects.connected_before {
        if first_failure {
            alert_not_connected(error);
        }
    } else if let ConsumerError::TopicNotFound { .. } = error {
        // Retried like any other failure, since stopping dipper would also stop its offers,
        // but no Studio request arrives until someone restores the topic.
        tracing::error!(
            %error,
            "the indexing request topic is gone from the brokers; no requests from \
             Studio arrive until it is back"
        );
    }
    if let Some(stalled_for) = stalled_for {
        tracing::error!(
            event = STALLED_EVENT,
            stalled_for_secs = stalled_for.as_secs(),
            error = with_causes(error),
            "The Studio indexing request consumer keeps failing to connect to the brokers; \
             no request from Studio is applied until it can"
        );
    }
}

/// Raise an alert for a consumer that could not connect when dipper started. Dipper carries on
/// without it, so this line is what tells an operator that Studio's requests are not being read.
fn alert_not_connected(error: &ConsumerError) {
    let check = match error {
        ConsumerError::TopicNotFound { .. } | ConsumerError::NoPartitions { .. } => {
            "this clears once the topic is created; if it already was, check \
             indexing_request_consumer.kafka.topic against the topic Studio produces on"
        }
        _ => "check the brokers and credentials in indexing_request_consumer.kafka",
    };
    tracing::error!(
        event = NOT_CONNECTED_EVENT,
        error = with_causes(error),
        "The Studio indexing request consumer could not connect when dipper started, so no \
         request from Studio is applied until it does. Dipper runs on without it and keeps \
         trying: {check}"
    );
}

/// Whether the consumer was asked to stop. Reads the value, not the change to it: a change is
/// reported to 1 wait only, so a loop that carried on after that wait would never stop.
fn stopping(shutdown_rx: &watch::Receiver<bool>) -> bool {
    *shutdown_rx.borrow()
}

/// Resolves once the consumer is asked to stop, at once if it already was.
async fn stop_requested(shutdown_rx: &mut watch::Receiver<bool>) {
    // An error means the sender is gone, which only happens once the service itself has ended.
    let _ = shutdown_rx.wait_for(|stop| *stop).await;
}

/// Apply a decoded propose until it takes effect; `false` if shutdown came first. A registry
/// failure or unreadable manifest retries the whole apply, and a wrong chain skips the record. A
/// queue failure retries only the job push: a cancel applied again finds nothing and queues none.
#[allow(clippy::too_many_arguments)]
async fn apply_target_with_retries<R, W>(
    registry: &R,
    worker_queue: &W,
    events: &Arc<dyn SubgraphIndexingAgreementEventsProducer>,
    chain_check: &dyn RequestChainCheck,
    protocol_chain_id: ChainId,
    requested_by: Address,
    target: &ProposedTarget,
    partition: i32,
    offset: i64,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> bool
where
    R: IndexingRequestRegistry + Send + Sync,
    W: WorkerQueue + Send + Sync,
{
    let num_candidates = target.num_candidates;
    let mut retries = Retries {
        partition,
        offset,
        stall: Stall::default(),
    };
    let queue_retry_id = loop {
        let request = SetIndexingTarget {
            requested_by,
            deployment_id: target.deployment_id,
            deployment_chain_id: target.deployment_chain_id,
            num_candidates,
            // Interactive: a developer just asked for this in Studio.
            priority: JobPriority::Interactive,
            // A record can be delivered twice, and the 1st delivery may have lost its job.
            reassess_unchanged: true,
        };
        let Some(result) = check_and_apply(
            registry,
            worker_queue,
            events,
            chain_check,
            protocol_chain_id,
            request,
            shutdown_rx,
        )
        .await
        else {
            return false;
        };

        match result {
            Ok(_) => return true,
            // The same record can never pass, so it is skipped rather than holding
            // up the partition, and flagged, since Studio is never told.
            Err(ApplyError::WrongChain(err)) => {
                alert_refused_record(partition, offset, target, &err);
                return true;
            }
            Err(err @ (ApplyError::Registry(_) | ApplyError::ChainCheckUnavailable(_))) => {
                retries.failed("applying it", &err);
                if sleep_or_shutdown(shutdown_rx, RETRY_BACKOFF).await {
                    return false;
                }
            }
            Err(ApplyError::QueueReassess { id, source }) => {
                retries.failed(QUEUING_ITS_JOB, source.as_ref());
                break id;
            }
        }
    };

    loop {
        // On a stop, 1 last immediate attempt: the row change is already committed. The record
        // is read again at the next start and queues the job then, unless it is a cancel, which
        // finds nothing to cancel the 2nd time and queues none.
        let stopping = sleep_or_shutdown(shutdown_rx, RETRY_BACKOFF).await;
        let Err(err) =
            retry_reassess_push(worker_queue, queue_retry_id, num_candidates, target).await
        else {
            return true;
        };
        retries.failed(QUEUING_ITS_JOB, err.as_ref());
        if stopping {
            return false;
        }
    }
}

/// The step named in the log when the reassessment job that follows a write can't be queued.
const QUEUING_ITS_JOB: &str = "queuing its reassessment job";

/// 1 attempt at a record: its chain check, then the apply. `None` if a stop came during the
/// check, which only reads and can take the whole IPFS timeout for 1 manifest. The apply is
/// never cut short, since it must not end between its write and the job that follows.
async fn check_and_apply<R, W>(
    registry: &R,
    worker_queue: &W,
    events: &Arc<dyn SubgraphIndexingAgreementEventsProducer>,
    chain_check: &dyn RequestChainCheck,
    protocol_chain_id: ChainId,
    request: SetIndexingTarget,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> Option<Result<Option<dipper_core::ids::IndexingRequestId>, ApplyError>>
where
    R: IndexingRequestRegistry + Send + Sync,
    W: WorkerQueue + Send + Sync,
{
    let checked = tokio::select! {
        biased;
        () = stop_requested(shutdown_rx) => return None,
        checked = check_chain(chain_check, request) => checked,
    };
    Some(match checked {
        Ok(request) => {
            apply_set_indexing_target(registry, worker_queue, events, protocol_chain_id, request)
                .await
        }
        Err(err) => Err(err),
    })
}

/// Raise an alert for a Studio request dropped because its chain is wrong: Studio
/// gets no reply, so without this the developer's request vanishes unseen.
fn alert_refused_record(
    partition: i32,
    offset: i64,
    target: &ProposedTarget,
    err: &ChainCheckError,
) {
    tracing::error!(
        event = "studio_request_chain_refused",
        partition,
        offset,
        event_id = %target.event_id,
        deployment_id = %target.deployment_id,
        chain_id = target.deployment_chain_id,
        error = %err,
        "Dropped a Studio indexing request whose chain does not match its subgraph manifest"
    );
}

/// One attempt at the reassessment push that failed inside the apply.
async fn retry_reassess_push<W>(
    worker_queue: &W,
    id: dipper_core::ids::IndexingRequestId,
    num_candidates: usize,
    target: &ProposedTarget,
) -> anyhow::Result<()>
where
    W: WorkerQueue + Send + Sync,
{
    worker_queue
        .reassess_indexing_request(
            id,
            target.deployment_id,
            target.deployment_chain_id,
            num_candidates,
            JobPriority::Interactive,
        )
        .await
        .map(|_| ())
}

/// A validated propose event, reduced to what the registry call needs.
#[derive(Debug, PartialEq, Eq)]
struct ProposedTarget {
    event_id: String,
    deployment_id: DeploymentId,
    deployment_chain_id: ChainId,
    num_candidates: usize,
}

/// Why a record is passed over for good.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Skip {
    /// Its envelope names another protocol network, so it is not this dipper's to apply.
    OtherNetwork,
    /// A request this dipper should have applied and can't read; the text says why.
    Unreadable(&'static str),
    /// It asks for this many indexers, more than a request may: the admin RPC refuses the same.
    TooManyIndexers(usize),
}

/// Logs a record that is passed over for good. Studio gets no reply, so one this dipper
/// should have applied raises an alert that says why it could not.
fn log_skipped_record(skip: Skip, topic: &str, partition: i32, offset: i64, key: Option<&[u8]>) {
    let key = key.map(String::from_utf8_lossy);
    match skip {
        Skip::OtherNetwork => tracing::warn!(
            topic,
            partition,
            offset,
            ?key,
            "Skipped a Studio indexing request for a different protocol network"
        ),
        Skip::Unreadable(reason) => tracing::error!(
            event = DROPPED_EVENT,
            topic,
            partition,
            offset,
            ?key,
            "Dropped a Studio indexing request dipper can't apply: {reason}"
        ),
        Skip::TooManyIndexers(asked) => tracing::error!(
            event = DROPPED_EVENT,
            topic,
            partition,
            offset,
            ?key,
            "Dropped a Studio indexing request that asks for {asked} indexers: a request may ask \
             for at most {MAX_NUM_CANDIDATES}, and nothing was changed"
        ),
    }
}

/// Decode a record value into a propose target, or the reason to skip it.
/// Unknown event types are tolerated by design: Studio may add types before
/// the dipper learns them, and they must not wedge the partition.
fn decode_propose(value: Option<&[u8]>, expected_network: &str) -> Result<ProposedTarget, Skip> {
    let Some(value) = value else {
        return Err(Skip::Unreadable("empty record value"));
    };

    let event = match studio::SubgraphIndexingRequestEvent::decode(value) {
        Ok(event) => event,
        Err(_) => return Err(Skip::Unreadable("undecodable protobuf")),
    };

    // Before the type: another network's record is not this dipper's, whatever its type.
    if event.the_graph_network_caip2id != expected_network {
        return Err(Skip::OtherNetwork);
    }

    match event.event_type.as_str() {
        EVENT_TYPE_PROPOSE => {}
        EVENT_TYPE_TERMINATE => {
            return Err(Skip::Unreadable(
                "terminate event (cancellation is a propose with 0)",
            ));
        }
        _ => return Err(Skip::Unreadable("unknown event type")),
    }

    let Some(studio::subgraph_indexing_request_event::Payload::SubgraphIndexingRequestPropose(
        propose,
    )) = event.payload
    else {
        return Err(Skip::Unreadable("propose event without a propose payload"));
    };

    let Ok(deployment_id) = event.subgraph_deployment_qm_hash.parse::<DeploymentId>() else {
        return Err(Skip::Unreadable("invalid subgraph deployment hash"));
    };

    let Some(deployment_chain_id) = parse_eip155_caip2(&propose.indexed_network_caip2id) else {
        // The field is the dipper's addition to Studio's schema; until Studio
        // sends it, every message lands here and is alerted on.
        return Err(Skip::Unreadable(
            "missing or invalid indexed network caip2 id",
        ));
    };

    let Ok(num_candidates) = usize::try_from(propose.indexing_agreements_requested) else {
        return Err(Skip::Unreadable("negative indexing agreements requested"));
    };
    // Dropped, not cut to the limit: each indexer is paid, so a bad count must not become the
    // most agreements a request can buy. Studio refuses such a count before it sends one.
    if num_candidates > MAX_NUM_CANDIDATES {
        return Err(Skip::TooManyIndexers(num_candidates));
    }

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
    use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
    use thegraph_core::{DeploymentId, deployment_id};
    use url::Url;

    use super::*;
    use crate::{
        registry::{
            IndexingRequest as IndexingRequestRecord, Result as RegistryResult, SetTargetOutcome,
        },
        request_chain_check::SkipChainCheck,
        test_support::{CapturedEvent, CapturingEventsProducer, RefusingChainCheck, logged},
        worker::queue::JobId,
    };

    /// The protocol (signer) chain id, distinct from the indexed chain below
    /// so assertions can tell the 2 apart.
    const PROTOCOL_CHAIN_ID: ChainId = 42161;

    /// The chain the test deployment indexes.
    const INDEXED_CHAIN_ID: ChainId = 1;

    const QM_HASH: &str = "QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv";

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
            Err(Skip::Unreadable("empty record value"))
        );
    }

    #[test]
    fn rejects_undecodable_bytes() {
        // 0xFF is a field-15 wire-type-7 tag; wire type 7 does not exist.
        let garbage = [0xFF, 0xFF, 0xFF];
        assert_eq!(
            decode_propose(Some(&garbage), "eip155:42161"),
            Err(Skip::Unreadable("undecodable protobuf"))
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
            Err(Skip::Unreadable("unknown event type"))
        );
    }

    #[test]
    fn rejects_a_terminate_event() {
        let bytes = encode_propose(EVENT_TYPE_TERMINATE, "eip155:42161", QM_HASH, "eip155:1", 3);
        assert!(matches!(
            decode_propose(Some(&bytes), "eip155:42161"),
            Err(Skip::Unreadable(reason)) if reason.contains("terminate")
        ));
    }

    #[test]
    fn rejects_an_event_for_another_protocol_network() {
        let bytes = encode_propose(EVENT_TYPE_PROPOSE, "eip155:421614", QM_HASH, "eip155:1", 3);
        assert_eq!(
            decode_propose(Some(&bytes), "eip155:42161"),
            Err(Skip::OtherNetwork)
        );
    }

    /// Another network's dipper answers for its own records, whatever their type, so none of
    /// them is counted as one this dipper failed to read.
    #[test]
    fn an_event_of_any_type_for_another_network_is_not_this_dippers() {
        for event_type in [EVENT_TYPE_TERMINATE, "subgraph.indexing.something.new"] {
            let bytes = encode_propose(event_type, "eip155:421614", QM_HASH, "eip155:1", 3);
            assert_eq!(
                decode_propose(Some(&bytes), "eip155:42161"),
                Err(Skip::OtherNetwork),
                "{event_type}"
            );
        }
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
            Err(Skip::Unreadable("propose event without a propose payload"))
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
            Err(Skip::Unreadable("invalid subgraph deployment hash"))
        );
    }

    #[test]
    fn rejects_a_missing_indexed_network() {
        // Studio has not added the field yet: it decodes as an empty string.
        let bytes = encode_propose(EVENT_TYPE_PROPOSE, "eip155:42161", QM_HASH, "", 3);
        assert_eq!(
            decode_propose(Some(&bytes), "eip155:42161"),
            Err(Skip::Unreadable(
                "missing or invalid indexed network caip2 id"
            ))
        );
    }

    #[test]
    fn rejects_a_malformed_indexed_network() {
        for indexed in ["cosmos:hub", "eip155:", "eip155:abc", "1"] {
            let bytes = encode_propose(EVENT_TYPE_PROPOSE, "eip155:42161", QM_HASH, indexed, 3);
            assert_eq!(
                decode_propose(Some(&bytes), "eip155:42161"),
                Err(Skip::Unreadable(
                    "missing or invalid indexed network caip2 id"
                )),
                "indexed network {indexed:?} should be rejected"
            );
        }
    }

    #[test]
    fn rejects_a_negative_candidate_count() {
        let bytes = encode_propose(EVENT_TYPE_PROPOSE, "eip155:42161", QM_HASH, "eip155:1", -1);
        assert_eq!(
            decode_propose(Some(&bytes), "eip155:42161"),
            Err(Skip::Unreadable("negative indexing agreements requested"))
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
        /// Every consumer offset stored, in order; the last is the one a restart resumes from.
        offsets: Arc<Mutex<Vec<i64>>>,
    }

    impl MockRegistry {
        /// `None` entries are errors; after the script is exhausted every call errors.
        fn scripted(script: Vec<Option<SetTargetOutcome>>) -> Self {
            Self {
                script: Arc::new(Mutex::new(script)),
                calls: Arc::new(Mutex::new(Vec::new())),
                offsets: Arc::default(),
            }
        }

        fn stored_offsets(&self) -> Vec<i64> {
            self.offsets.lock().expect("poisoned").clone()
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

        async fn get_indexing_requests_page(
            &self,
            _after: Option<IndexingRequestId>,
            _status: Option<crate::registry::IndexingRequestStatus>,
            _limit: u32,
        ) -> RegistryResult<crate::registry::RequestsPage> {
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

        async fn get_short_requests_on_deployments_with_ended_agreements(
            &self,
            _since_seconds: i64,
            _batch_size: i64,
        ) -> RegistryResult<Vec<IndexingRequestRecord>> {
            unimplemented!()
        }
    }

    #[async_trait]
    impl KafkaConsumerOffsetRegistry for MockRegistry {
        async fn get_kafka_consumer_offset(
            &self,
            _topic: &str,
            _partition_id: i32,
        ) -> RegistryResult<Option<i64>> {
            Ok(self.stored_offsets().last().copied())
        }

        async fn set_kafka_consumer_offset(
            &self,
            _topic: &str,
            _partition_id: i32,
            next_offset: i64,
        ) -> RegistryResult<()> {
            self.offsets.lock().expect("poisoned").push(next_offset);
            Ok(())
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
            _accept: Option<crate::worker::service::ChainAccept>,
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
        run_apply_checked(registry, worker, num_candidates, &SkipChainCheck).await
    }

    /// As [`run_apply`], with `chain_check` deciding which chains are accepted.
    async fn run_apply_checked(
        registry: &MockRegistry,
        worker: &MockWorker,
        num_candidates: usize,
        chain_check: &dyn RequestChainCheck,
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
            chain_check,
            PROTOCOL_CHAIN_ID,
            requester(),
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

    /// A record delivered twice lands on the no-op path the 2nd time. Queuing its job again
    /// repairs one lost between the 1st delivery's write and its push.
    #[tokio::test]
    async fn a_noop_outcome_queues_the_reassessment_again_without_events() {
        let id = IndexingRequestId::new();
        let registry = MockRegistry::returning(SetTargetOutcome::NoOp { id });
        let worker = MockWorker::new();

        let (applied, events) = run_apply(&registry, &worker, 3).await;

        assert!(applied);
        assert_eq!(
            worker.reassessments(),
            [(id, deployment_id!(QM_HASH), INDEXED_CHAIN_ID, 3)]
        );
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
        // The write is done, so only the push is repeated. A cancel written again would find
        // nothing to cancel and lose its job.
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

    /// Studio and the admin RPC are 2 doors to the same request, so both let a developer ask
    /// for the same number of indexers: up to 100, the count the RPC refuses anything above.
    #[test]
    fn a_studio_request_may_ask_for_as_many_indexers_as_an_rpc_request() {
        let asking_for = |count| {
            let bytes = encode_propose(
                EVENT_TYPE_PROPOSE,
                "eip155:42161",
                QM_HASH,
                "eip155:1",
                count,
            );
            decode_propose(Some(&bytes), "eip155:42161").map(|target| target.num_candidates)
        };

        assert_eq!(asking_for(100), Ok(MAX_NUM_CANDIDATES));
        assert_eq!(asking_for(101), Err(Skip::TooManyIndexers(101)));
        assert_eq!(asking_for(5_000_000), Err(Skip::TooManyIndexers(5_000_000)));
        assert_eq!(asking_for(0), Ok(0), "a cancel is not a count to refuse");
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
            &SkipChainCheck,
            PROTOCOL_CHAIN_ID,
            requester(),
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

    #[tokio::test]
    async fn a_record_on_another_chain_is_skipped_without_touching_the_registry() {
        let registry = MockRegistry::returning(SetTargetOutcome::Inserted {
            id: IndexingRequestId::new(),
        });
        let worker = MockWorker::new();
        let check = RefusingChainCheck::wrong_chain();

        let mut outcome = None;
        let logs = logged(async {
            outcome = Some(run_apply_checked(&registry, &worker, 3, &check).await);
        })
        .await;
        let (applied, events) = outcome.expect("the apply ran");

        assert!(
            logs.contains("studio_request_chain_refused"),
            "the dropped request raises an alert: {logs}"
        );
        assert!(applied, "the record is done with, so its offset moves on");
        assert_eq!(check.calls(), 1, "a wrong chain is not retried");
        assert!(registry.calls().is_empty());
        assert!(worker.reassessments().is_empty());
        assert!(events.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn an_unreadable_manifest_is_retried_not_skipped() {
        //* Arrange - the stop arrives during the wait after the 1st attempt, ending the loop
        let registry = MockRegistry::returning(SetTargetOutcome::Inserted {
            id: IndexingRequestId::new(),
        });
        let worker = MockWorker::new();
        let check = RefusingChainCheck::unavailable();
        let events: Arc<dyn SubgraphIndexingAgreementEventsProducer> =
            Arc::new(CapturingEventsProducer::new());
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let target = ProposedTarget {
            event_id: "e".to_string(),
            deployment_id: deployment_id!(QM_HASH),
            deployment_chain_id: INDEXED_CHAIN_ID,
            num_candidates: 3,
        };
        let stop = async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            shutdown_tx.send(true).expect("send shutdown");
        };

        //* Act
        let apply = apply_target_with_retries(
            &registry,
            &worker,
            &events,
            &check,
            PROTOCOL_CHAIN_ID,
            requester(),
            &target,
            0,
            0,
            &mut shutdown_rx,
        );
        let (applied, ()) = tokio::join!(apply, stop);

        //* Assert - unapplied, so the record is redelivered rather than lost
        assert!(!applied);
        assert_eq!(check.calls(), 1);
        assert!(registry.calls().is_empty());
    }

    #[tokio::test]
    async fn a_cancel_record_is_not_checked_against_the_manifest() {
        let registry = MockRegistry::returning(SetTargetOutcome::Canceled {
            id: IndexingRequestId::new(),
        });
        let worker = MockWorker::new();
        let check = RefusingChainCheck::wrong_chain();

        let (applied, _) = run_apply_checked(&registry, &worker, 0, &check).await;

        assert!(applied);
        assert_eq!(check.calls(), 0);
        assert_eq!(registry.calls().len(), 1);
    }

    // -------- partition_loop --------

    type Fetch = Result<(Vec<FetchedRecord>, i64), ConsumerError>;

    /// A partition whose fetches are answered from a script, in order. Once the script runs
    /// out a fetch waits for ever, like a partition with nothing new.
    struct ScriptedSource {
        fetches: Mutex<std::collections::VecDeque<Fetch>>,
        /// The offset each fetch asked for.
        asked: Arc<Mutex<Vec<i64>>>,
        earliest: i64,
        latest: i64,
        /// Once the script runs out, answer every fetch with an error the broker would keep
        /// giving, in place of waiting.
        fails_after_script: bool,
        /// The partitions the topic has.
        partitions: Vec<i32>,
    }

    impl ScriptedSource {
        fn new(fetches: Vec<Fetch>) -> Self {
            Self {
                fetches: Mutex::new(fetches.into()),
                asked: Arc::default(),
                earliest: 0,
                latest: 0,
                fails_after_script: false,
                partitions: vec![0],
            }
        }
    }

    #[async_trait]
    impl TopicSource for ScriptedSource {
        fn partitions(&self) -> Vec<i32> {
            self.partitions.clone()
        }

        async fn current_partition_count(&self) -> Result<usize, ConsumerError> {
            Ok(self.partitions.len())
        }
    }

    #[async_trait]
    impl PartitionSource for ScriptedSource {
        async fn fetch(
            &self,
            _partition: i32,
            offset: i64,
            _max_bytes: i32,
            _max_wait_ms: i32,
        ) -> Fetch {
            self.asked.lock().expect("poisoned").push(offset);
            let next = self.fetches.lock().expect("poisoned").pop_front();
            match next {
                Some(answer) => answer,
                None if self.fails_after_script => Err(ConsumerError::Fetch(
                    rskafka::client::error::Error::InvalidResponse("unreadable".to_string()),
                )),
                None => std::future::pending().await,
            }
        }

        async fn offset(&self, _partition: i32, at: OffsetAt) -> Result<i64, ConsumerError> {
            Ok(match at {
                OffsetAt::Earliest => self.earliest,
                _ => self.latest,
            })
        }
    }

    /// A fetch answered with 1 valid propose for each of `counts`, at offsets from `first` on.
    fn batch(first: i64, counts: &[i32]) -> Fetch {
        let records: Vec<FetchedRecord> = (first..)
            .zip(counts)
            .map(|(offset, count)| FetchedRecord {
                offset,
                key: None,
                value: Some(encode_propose(
                    EVENT_TYPE_PROPOSE,
                    "eip155:42161",
                    QM_HASH,
                    "eip155:1",
                    *count,
                )),
            })
            .collect();
        let high_watermark = records.last().map_or(first, |record| record.offset + 1);
        Ok((records, high_watermark))
    }

    fn test_config() -> IndexingRequestConsumerConfig {
        serde_json::from_value(serde_json::json!({
            "kafka": { "brokers": ["localhost:9092"], "topic": "t" },
            "requested_by": requester().to_string(),
        }))
        .expect("a valid consumer config")
    }

    /// The partition loop as the service runs it, for partition 0 of `source`.
    fn partition_loop_over(
        source: ScriptedSource,
        registry: &MockRegistry,
        worker: &MockWorker,
        chain_check: Arc<dyn RequestChainCheck>,
        shutdown_rx: watch::Receiver<bool>,
    ) -> impl Future<Output = ()> {
        let consumer =
            Reconnecting::with_client("test", source, Duration::from_secs(1), || async {
                Err::<ScriptedSource, &str>("these tests never rebuild the client")
            });
        partition_loop(
            consumer,
            0,
            registry.clone(),
            worker.clone(),
            Arc::new(CapturingEventsProducer::new()),
            PROTOCOL_CHAIN_ID,
            chain_check,
            test_config(),
            shutdown_rx,
        )
    }

    /// Lets `run` work until `done`, then asks it to stop and waits for it to return.
    async fn run_until(
        run: impl Future<Output = ()>,
        shutdown_tx: &watch::Sender<bool>,
        done: impl Fn() -> bool,
    ) {
        let stop = async {
            while !done() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let _ = shutdown_tx.send(true);
        };
        tokio::time::timeout(Duration::from_secs(3_600), async {
            tokio::join!(run, stop)
        })
        .await
        .expect("the partition loop stops once asked to");
    }

    #[tokio::test(start_paused = true)]
    async fn the_loop_applies_each_fetched_record_and_stores_the_offset_past_it() {
        let registry = MockRegistry::scripted(vec![
            Some(SetTargetOutcome::Inserted {
                id: IndexingRequestId::new(),
            }),
            Some(SetTargetOutcome::Updated {
                id: IndexingRequestId::new(),
                new_num_candidates: 5,
            }),
        ]);
        registry.offsets.lock().expect("poisoned").push(7);
        let worker = MockWorker::new();
        let source = ScriptedSource::new(vec![batch(7, &[3, 5])]);
        let asked = Arc::clone(&source.asked);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        run_until(
            partition_loop_over(
                source,
                &registry,
                &worker,
                Arc::new(SkipChainCheck),
                shutdown_rx,
            ),
            &shutdown_tx,
            || asked.lock().expect("poisoned").len() == 2,
        )
        .await;

        let counts: Vec<usize> = registry.calls().iter().map(|call| call.3).collect();
        assert_eq!(counts, [3, 5]);
        assert_eq!(registry.stored_offsets(), [7, 8, 9]);
        assert_eq!(*asked.lock().expect("poisoned"), [7, 9]);
    }

    /// A chain check that asks the consumer to stop each time it is asked, then passes.
    struct StopsTheConsumer(watch::Sender<bool>);

    #[async_trait]
    impl RequestChainCheck for StopsTheConsumer {
        async fn check(&self, _: &DeploymentId, _: ChainId) -> Result<(), ChainCheckError> {
            let _ = self.0.send(true);
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_stop_is_noticed_before_the_next_record_of_a_batch() {
        let inserted = || {
            Some(SetTargetOutcome::Inserted {
                id: IndexingRequestId::new(),
            })
        };
        let registry = MockRegistry::scripted(vec![inserted(), inserted(), inserted()]);
        registry.offsets.lock().expect("poisoned").push(0);
        let worker = MockWorker::new();
        let source = ScriptedSource::new(vec![batch(0, &[3, 4, 5])]);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let check = Arc::new(StopsTheConsumer(shutdown_tx));

        tokio::time::timeout(
            Duration::from_secs(600),
            partition_loop_over(source, &registry, &worker, check, shutdown_rx),
        )
        .await
        .expect("the partition loop stops once asked to");

        assert_eq!(
            registry.calls().len(),
            1,
            "the record in hand is applied and the rest are left for the next run"
        );
        assert_eq!(registry.stored_offsets(), [0, 1]);
    }

    /// A stop that lands while a failed job push waits to be retried is used up by that wait.
    /// The last try then succeeds, and the loop must still stop rather than carry on reading.
    #[tokio::test(start_paused = true)]
    async fn the_loop_still_stops_after_a_last_chance_push_succeeds() {
        let inserted = || {
            Some(SetTargetOutcome::Inserted {
                id: IndexingRequestId::new(),
            })
        };
        let registry = MockRegistry::scripted(vec![inserted(), inserted()]);
        registry.offsets.lock().expect("poisoned").push(0);
        let worker = MockWorker::failing_pushes(1);
        let failures_left = Arc::clone(&worker.failures_left);
        let source = ScriptedSource::new(vec![batch(0, &[3, 4])]);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        run_until(
            partition_loop_over(
                source,
                &registry,
                &worker,
                Arc::new(SkipChainCheck),
                shutdown_rx,
            ),
            &shutdown_tx,
            || *failures_left.lock().expect("poisoned") == 0,
        )
        .await;

        assert_eq!(
            worker.reassessments().len(),
            1,
            "the job that failed at first is queued on the last try"
        );
        assert_eq!(
            registry.calls().len(),
            1,
            "nothing more is applied once asked to stop"
        );
        assert_eq!(registry.stored_offsets(), [0, 1]);
    }

    /// A stop while the job queue is down ends the run with the request written and no job
    /// behind it. The record's offset is not stored, so the next run reads it again and
    /// queues the job, with no need for the daily reassignment pass.
    #[tokio::test(start_paused = true)]
    async fn a_job_lost_at_a_stop_is_queued_when_its_record_is_read_again() {
        let id = IndexingRequestId::new();
        let registry = MockRegistry::scripted(vec![
            Some(SetTargetOutcome::Inserted { id }),
            Some(SetTargetOutcome::NoOp { id }),
        ]);
        registry.offsets.lock().expect("poisoned").push(0);
        // The push after the write fails, and so does the last try made on the stop.
        let worker = MockWorker::failing_pushes(2);
        let failures_left = Arc::clone(&worker.failures_left);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        run_until(
            partition_loop_over(
                ScriptedSource::new(vec![batch(0, &[3])]),
                &registry,
                &worker,
                Arc::new(SkipChainCheck),
                shutdown_rx,
            ),
            &shutdown_tx,
            || *failures_left.lock().expect("poisoned") == 1,
        )
        .await;
        assert!(worker.reassessments().is_empty(), "the job is lost");
        assert_eq!(registry.stored_offsets(), [0], "and its record not passed");

        let stored = Arc::clone(&registry.offsets);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        run_until(
            partition_loop_over(
                ScriptedSource::new(vec![batch(0, &[3])]),
                &registry,
                &worker,
                Arc::new(SkipChainCheck),
                shutdown_rx,
            ),
            &shutdown_tx,
            || stored.lock().expect("poisoned").len() == 2,
        )
        .await;
        let queued: Vec<_> = worker
            .reassessments()
            .into_iter()
            .map(|(id, _, _, count)| (id, count))
            .collect();
        assert_eq!(queued, [(id, 3)], "the 2nd run queues it");
        assert_eq!(registry.stored_offsets(), [0, 1]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_stop_seen_once_already_still_ends_the_next_wait() {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        shutdown_tx.send(true).expect("send shutdown");

        let first = sleep_or_shutdown(&mut shutdown_rx, RETRY_BACKOFF).await;
        let second = sleep_or_shutdown(&mut shutdown_rx, RETRY_BACKOFF).await;

        assert!(first);
        assert!(second, "the stop was seen once before, and is still set");
    }

    /// A chain check that never answers, like a manifest read that hangs.
    struct NeverAnswers;

    #[async_trait]
    impl RequestChainCheck for NeverAnswers {
        async fn check(&self, _: &DeploymentId, _: ChainId) -> Result<(), ChainCheckError> {
            std::future::pending().await
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_stop_interrupts_a_manifest_read() {
        let registry = MockRegistry::returning(SetTargetOutcome::Inserted {
            id: IndexingRequestId::new(),
        });
        let worker = MockWorker::new();
        let events: Arc<dyn SubgraphIndexingAgreementEventsProducer> =
            Arc::new(CapturingEventsProducer::new());
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let target = ProposedTarget {
            event_id: "e".to_string(),
            deployment_id: deployment_id!(QM_HASH),
            deployment_chain_id: INDEXED_CHAIN_ID,
            num_candidates: 3,
        };
        let apply = apply_target_with_retries(
            &registry,
            &worker,
            &events,
            &NeverAnswers,
            PROTOCOL_CHAIN_ID,
            requester(),
            &target,
            0,
            0,
            &mut shutdown_rx,
        );
        let stop = async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            shutdown_tx.send(true).expect("send shutdown");
        };

        let (applied, ()) =
            tokio::time::timeout(Duration::from_secs(60), async { tokio::join!(apply, stop) })
                .await
                .expect("the stop is noticed while the manifest is being read");

        assert!(!applied, "unapplied, so the record is redelivered");
        assert!(registry.calls().is_empty());
    }

    // -------- records dropped for good --------

    /// Studio gets no reply to a record dipper drops, so each one it should have applied is
    /// alerted on. One for another protocol network's dipper is not this one's to act on.
    #[tokio::test(start_paused = true)]
    async fn a_dropped_record_is_alerted_on_unless_it_is_for_another_network() {
        let record = |offset, value| FetchedRecord {
            offset,
            key: Some(b"eip155:42161/QmUzRg/request".to_vec()),
            value: Some(value),
        };
        let propose =
            |network, indexed| encode_propose(EVENT_TYPE_PROPOSE, network, QM_HASH, indexed, 3);
        let records = vec![
            record(0, propose("eip155:421614", "eip155:1")),
            record(1, propose("eip155:42161", "")),
            record(2, vec![0xFF, 0xFF, 0xFF]),
            record(3, propose("eip155:42161", "solana:mainnet")),
        ];
        let registry = MockRegistry::scripted(vec![]);
        registry.offsets.lock().expect("poisoned").push(0);
        let worker = MockWorker::new();
        let source = ScriptedSource::new(vec![Ok((records, 4))]);
        let asked = Arc::clone(&source.asked);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let logs = logged(run_until(
            partition_loop_over(
                source,
                &registry,
                &worker,
                Arc::new(SkipChainCheck),
                shutdown_rx,
            ),
            &shutdown_tx,
            || asked.lock().expect("poisoned").len() == 2,
        ))
        .await;

        assert_eq!(alerts(&logs, DROPPED_EVENT), 3, "{logs}");
        let alert_lines: Vec<&str> = logs
            .lines()
            .filter(|line| line.contains(DROPPED_EVENT))
            .collect();
        assert!(alert_lines[0].contains("missing or invalid indexed network"));
        assert!(alert_lines[0].contains("offset=1"), "{}", alert_lines[0]);
        assert!(alert_lines[1].contains("undecodable protobuf"));
        assert!(alert_lines[2].contains("offset=3"), "{}", alert_lines[2]);
        assert!(logs.contains("different protocol network"), "{logs}");
        assert!(registry.calls().is_empty(), "none of them is applied");
        assert_eq!(registry.stored_offsets(), [0, 1, 2, 3, 4]);
    }

    /// The admin RPC refuses a request for more indexers than the limit and changes nothing.
    /// A record can't be refused, so the same request from Studio is dropped and alerted on:
    /// cutting it to the limit would turn a bad count into the most agreements a request can buy.
    #[tokio::test(start_paused = true)]
    async fn a_request_for_more_indexers_than_the_limit_is_dropped_not_cut_to_it() {
        let record = |offset, count| FetchedRecord {
            offset,
            key: Some(b"eip155:42161/QmUzRg/request".to_vec()),
            value: Some(encode_propose(
                EVENT_TYPE_PROPOSE,
                "eip155:42161",
                QM_HASH,
                "eip155:1",
                count,
            )),
        };
        let records = vec![record(0, 100), record(1, 101), record(2, i32::MAX)];
        // An answer for each, so that a record wrongly let through shows as applied.
        let inserted = || {
            Some(SetTargetOutcome::Inserted {
                id: IndexingRequestId::new(),
            })
        };
        let registry = MockRegistry::scripted(vec![inserted(), inserted(), inserted()]);
        registry.offsets.lock().expect("poisoned").push(0);
        let worker = MockWorker::new();
        let source = ScriptedSource::new(vec![Ok((records, 3))]);
        let asked = Arc::clone(&source.asked);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let logs = logged(run_until(
            partition_loop_over(
                source,
                &registry,
                &worker,
                Arc::new(SkipChainCheck),
                shutdown_rx,
            ),
            &shutdown_tx,
            || asked.lock().expect("poisoned").len() == 2,
        ))
        .await;

        let applied: Vec<usize> = registry.calls().iter().map(|call| call.3).collect();
        assert_eq!(
            applied,
            [100],
            "the limit itself is applied, nothing above it"
        );
        assert_eq!(alerts(&logs, DROPPED_EVENT), 2, "{logs}");
        let alert_lines: Vec<&str> = logs
            .lines()
            .filter(|line| line.contains(DROPPED_EVENT))
            .collect();
        assert!(alert_lines[0].contains("offset=1"), "{}", alert_lines[0]);
        assert!(
            alert_lines[0].contains("101 indexers"),
            "{}",
            alert_lines[0]
        );
        assert!(alert_lines[0].contains("at most 100"), "{}", alert_lines[0]);
        assert!(alert_lines[1].contains("2147483647 indexers"));
        assert_eq!(registry.stored_offsets(), [0, 1, 2, 3], "each is passed");
    }

    // -------- offsets out of range --------

    /// The broker's answer to a fetch from an offset outside what it retains.
    fn out_of_range() -> Fetch {
        use rskafka::client::error::{Error, ProtocolError, RequestContext};
        Err(ConsumerError::Fetch(Error::ServerError {
            protocol_error: ProtocolError::OffsetOutOfRange,
            error_message: None,
            request: RequestContext::Topic("t".to_string()),
            response: None,
            is_virtual: false,
        }))
    }

    /// Runs the loop from `stored`, over a partition retaining `earliest..latest` that answers
    /// `fetches`, for 1 minute. Returns the offsets fetched from, the registry and the logs.
    async fn run_from_offset(
        stored: i64,
        (earliest, latest): (i64, i64),
        fetches: Vec<Fetch>,
    ) -> (Vec<i64>, MockRegistry, String) {
        let registry = MockRegistry::returning(SetTargetOutcome::Inserted {
            id: IndexingRequestId::new(),
        });
        registry.offsets.lock().expect("poisoned").push(stored);
        let worker = MockWorker::new();
        let mut source = ScriptedSource::new(fetches);
        (source.earliest, source.latest) = (earliest, latest);
        let asked = Arc::clone(&source.asked);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let started = Instant::now();

        let logs = logged(run_until(
            partition_loop_over(
                source,
                &registry,
                &worker,
                Arc::new(SkipChainCheck),
                shutdown_rx,
            ),
            &shutdown_tx,
            || started.elapsed() > Duration::from_secs(60),
        ))
        .await;

        let asked = asked.lock().expect("poisoned").clone();
        (asked, registry, logs)
    }

    #[test]
    fn an_offset_is_only_moved_when_it_is_outside_what_the_broker_retains() {
        assert_eq!(re_anchor(5, 40, 100), Some(40), "behind the start");
        assert_eq!(re_anchor(5_000, 0, 20), Some(20), "past the end");
        assert_eq!(re_anchor(50, 0, 100), None, "inside the range");
        assert_eq!(
            re_anchor(100, 0, 100),
            None,
            "at the end, with nothing to skip"
        );
        assert_eq!(re_anchor(0, 0, 0), None, "an empty log");
    }

    /// A broker can call an offset out of range and then report a range that holds it. Moving
    /// to the end of that range would skip records nobody read.
    #[tokio::test(start_paused = true)]
    async fn an_offset_the_broker_refuses_yet_still_retains_is_fetched_again() {
        let (asked, registry, logs) =
            run_from_offset(50, (0, 100), vec![out_of_range(), batch(50, &[3])]).await;

        assert_eq!(asked, [50, 50, 51], "{logs}");
        assert_eq!(registry.calls().len(), 1, "the record at 50 is applied");
        assert_eq!(registry.stored_offsets(), [50, 51]);
        assert_eq!(alerts(&logs, OFFSET_JUMPED_EVENT), 0, "{logs}");
    }

    #[tokio::test(start_paused = true)]
    async fn an_offset_moved_to_an_edge_of_the_log_is_alerted_on() {
        let (past_the_end, registry, moved_back) =
            run_from_offset(5_000, (0, 20), vec![out_of_range()]).await;
        assert_eq!(past_the_end, [5_000, 20]);
        assert_eq!(registry.stored_offsets(), [5_000, 20]);
        assert_eq!(alerts(&moved_back, OFFSET_JUMPED_EVENT), 1, "{moved_back}");

        let (behind_the_start, registry, moved_on) =
            run_from_offset(5, (40, 100), vec![out_of_range()]).await;
        assert_eq!(behind_the_start, [5, 40]);
        assert_eq!(registry.stored_offsets(), [5, 40]);
        assert_eq!(alerts(&moved_on, OFFSET_JUMPED_EVENT), 1, "{moved_on}");
    }

    // -------- stalls --------

    fn alerts(logs: &str, event: &str) -> usize {
        logs.matches(&format!(r#"event="{event}""#)).count()
    }

    #[test]
    fn the_consumers_alert_tags_are_alerted_on_by_default() {
        let defaults = crate::config::AlertsConfig::default().events;

        for event in [
            "studio_request_chain_refused",
            STALLED_EVENT,
            OFFSET_JUMPED_EVENT,
            DROPPED_EVENT,
            NOT_CONNECTED_EVENT,
        ] {
            assert!(defaults.iter().any(|default| default == event), "{event}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_stall_is_due_an_alert_after_10_minutes_and_every_10_after() {
        let mut stall = Stall::default();

        assert_eq!(stall.failed(), None, "the 1st failure starts the count");
        tokio::time::advance(STALL_ALERT_AFTER - Duration::from_secs(1)).await;
        assert_eq!(stall.failed(), None);
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(stall.failed(), Some(STALL_ALERT_AFTER));
        assert_eq!(stall.failed(), None, "1 alert, not 1 for each retry");
        tokio::time::advance(STALL_ALERT_AFTER).await;
        assert_eq!(stall.failed(), Some(2 * STALL_ALERT_AFTER));

        stall.cleared();
        tokio::time::advance(STALL_ALERT_AFTER).await;
        assert_eq!(
            stall.failed(),
            None,
            "getting through starts the count again"
        );
    }

    /// Runs `apply_target_with_retries` for the standard propose until a stop `stop_after` later,
    /// returning what it logged.
    async fn logs_of_apply_stopped_after(
        registry: &MockRegistry,
        worker: &MockWorker,
        chain_check: &dyn RequestChainCheck,
        stop_after: Duration,
    ) -> String {
        let events: Arc<dyn SubgraphIndexingAgreementEventsProducer> =
            Arc::new(CapturingEventsProducer::new());
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let target = ProposedTarget {
            event_id: "e".to_string(),
            deployment_id: deployment_id!(QM_HASH),
            deployment_chain_id: INDEXED_CHAIN_ID,
            num_candidates: 3,
        };
        logged(async {
            let apply = apply_target_with_retries(
                registry,
                worker,
                &events,
                chain_check,
                PROTOCOL_CHAIN_ID,
                requester(),
                &target,
                4,
                17,
                &mut shutdown_rx,
            );
            let stop = async {
                tokio::time::sleep(stop_after).await;
                shutdown_tx.send(true).expect("send shutdown");
            };
            tokio::join!(apply, stop);
        })
        .await
    }

    #[tokio::test(start_paused = true)]
    async fn a_manifest_that_stays_unreadable_is_alerted_on_after_10_minutes() {
        let registry = MockRegistry::scripted(vec![]);
        let worker = MockWorker::new();
        let check = RefusingChainCheck::unavailable();

        let before = logs_of_apply_stopped_after(
            &registry,
            &worker,
            &check,
            STALL_ALERT_AFTER - Duration::from_secs(30),
        )
        .await;
        let after = logs_of_apply_stopped_after(
            &registry,
            &worker,
            &check,
            STALL_ALERT_AFTER + Duration::from_secs(30),
        )
        .await;

        assert_eq!(alerts(&before, STALLED_EVENT), 0, "{before}");
        assert_eq!(alerts(&after, STALLED_EVENT), 1, "{after}");
        assert!(
            after.contains("IPFS is down"),
            "the alert says why: {after}"
        );
        assert!(after.contains("offset=17"), "and which record: {after}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_job_push_that_keeps_failing_is_alerted_on_after_10_minutes() {
        let registry = MockRegistry::returning(SetTargetOutcome::Inserted {
            id: IndexingRequestId::new(),
        });
        let worker = MockWorker::failing_pushes(usize::MAX);

        let logs = logs_of_apply_stopped_after(
            &registry,
            &worker,
            &SkipChainCheck,
            STALL_ALERT_AFTER + Duration::from_secs(30),
        )
        .await;

        assert_eq!(alerts(&logs, STALLED_EVENT), 1, "{logs}");
        assert!(logs.contains("scripted queue failure"), "{logs}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_fetch_that_keeps_failing_is_alerted_on_after_10_minutes() {
        let registry = MockRegistry::scripted(vec![]);
        registry.offsets.lock().expect("poisoned").push(0);
        let worker = MockWorker::new();
        let mut source = ScriptedSource::new(vec![]);
        source.fails_after_script = true;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let started = Instant::now();

        let logs = logged(run_until(
            partition_loop_over(
                source,
                &registry,
                &worker,
                Arc::new(SkipChainCheck),
                shutdown_rx,
            ),
            &shutdown_tx,
            || started.elapsed() > STALL_ALERT_AFTER + Duration::from_secs(30),
        ))
        .await;

        assert_eq!(alerts(&logs, STALLED_EVENT), 1, "{logs}");
        assert!(logs.contains("unreadable"), "the alert says why: {logs}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_topic_that_stays_gone_is_alerted_on_after_10_minutes() {
        let connects = Mutex::new(Connects::default());
        note_connect(&connects, &Ok(()));
        let gone = || {
            Err::<(), _>(ConsumerError::TopicNotFound {
                topic: "requests".to_string(),
            })
        };

        let at_first = logged(async { note_connect(&connects, &gone()) }).await;
        tokio::time::advance(STALL_ALERT_AFTER).await;
        let later = logged(async { note_connect(&connects, &gone()) }).await;
        note_connect(&connects, &Ok(()));
        tokio::time::advance(STALL_ALERT_AFTER).await;
        let after_reconnecting = logged(async { note_connect(&connects, &gone()) }).await;

        assert!(at_first.contains("gone from the brokers"), "{at_first}");
        assert_eq!(alerts(&at_first, STALLED_EVENT), 0, "{at_first}");
        assert_eq!(
            alerts(&at_first, NOT_CONNECTED_EVENT),
            0,
            "an outage after it has connected is not a failed start: {at_first}"
        );
        assert_eq!(alerts(&later, STALLED_EVENT), 1, "{later}");
        assert_eq!(
            alerts(&after_reconnecting, STALLED_EVENT),
            0,
            "{after_reconnecting}"
        );
    }

    // -------- the service --------

    /// What 1 attempt to connect finds; `None` for brokers that never answer it.
    type Found = Option<Result<ScriptedSource, ConsumerError>>;

    /// The service as dipper runs it, over a topic that `found` stands in for: it is given
    /// the number of each attempt to connect, from 1, and says what that attempt finds.
    fn serve_over(
        found: impl Fn(u32) -> Found + Send + Sync + 'static,
        registry: &MockRegistry,
        worker: &MockWorker,
        rx_stop: mpsc::Receiver<()>,
    ) -> impl Future<Output = anyhow::Result<()>> {
        let ctx = Ctx {
            registry: registry.clone(),
            worker_queue: worker.clone(),
            events: Arc::new(CapturingEventsProducer::new()),
            protocol_chain_id: PROTOCOL_CHAIN_ID,
            chain_check: Arc::new(SkipChainCheck),
            config: test_config(),
        };
        let attempts = std::sync::atomic::AtomicU32::new(0);
        serve(ctx, rx_stop, Duration::from_secs(65), move || {
            let attempt = attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            let found = found(attempt);
            async move {
                match found {
                    Some(found) => found,
                    None => std::future::pending().await,
                }
            }
        })
    }

    /// Resolves once an offset has been stored, which is after a record was applied.
    async fn an_offset_is_stored(stored: Arc<Mutex<Vec<i64>>>) {
        while stored.lock().expect("poisoned").is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// A topic with 1 request waiting on it.
    fn topic_with_a_request() -> ScriptedSource {
        ScriptedSource::new(vec![batch(0, &[3])])
    }

    /// A registry that takes that request, and the offsets it stores.
    fn registry_for_a_request() -> (MockRegistry, Arc<Mutex<Vec<i64>>>) {
        let registry = MockRegistry::scripted(vec![Some(SetTargetOutcome::Inserted {
            id: IndexingRequestId::new(),
        })]);
        let stored = Arc::clone(&registry.offsets);
        (registry, stored)
    }

    /// Runs `service` until `then`, then asks it to stop as dipper's shutdown does, and
    /// returns how it ended. `None` if it ended by itself before that.
    async fn serve_until(
        service: impl Future<Output = anyhow::Result<()>>,
        tx_stop: mpsc::Sender<()>,
        then: impl Future<Output = ()>,
    ) -> Option<anyhow::Result<()>> {
        tokio::pin!(service);
        tokio::select! {
            ended = &mut service => {
                drop(ended);
                return None;
            }
            () = then => {}
        }
        let _ = tx_stop.send(()).await;
        let ended = tokio::time::timeout(Duration::from_secs(5), service).await;
        Some(ended.expect("the service stops within the 5 seconds dipper allows it"))
    }

    #[tokio::test(start_paused = true)]
    async fn the_service_reads_the_topic_it_connects_to_and_stops_when_asked() {
        let (registry, stored) = registry_for_a_request();
        let worker = MockWorker::new();
        let (tx_stop, rx_stop) = mpsc::channel(1);

        let ended = serve_until(
            serve_over(
                |_| Some(Ok(topic_with_a_request())),
                &registry,
                &worker,
                rx_stop,
            ),
            tx_stop,
            an_offset_is_stored(stored),
        )
        .await;

        assert!(matches!(ended, Some(Ok(()))), "{ended:?}");
        assert_eq!(worker.reassessments().len(), 1);
        assert_eq!(registry.stored_offsets(), [1]);
    }

    /// Dipper restarting while Redpanda is down must not end the consumer: the supervisor
    /// would take every other service down with it, again on each restart, for the whole outage.
    #[tokio::test(start_paused = true)]
    async fn brokers_that_cannot_be_reached_at_start_do_not_end_the_service() {
        let (registry, _) = registry_for_a_request();
        let worker = MockWorker::new();
        let (tx_stop, rx_stop) = mpsc::channel(1);
        let mut ended = None;

        let logs = logged(async {
            ended = serve_until(
                serve_over(
                    |_| Some(Err(ConsumerError::Timeout)),
                    &registry,
                    &worker,
                    rx_stop,
                ),
                tx_stop,
                tokio::time::sleep(Duration::from_secs(3_600)),
            )
            .await;
        })
        .await;

        assert!(
            matches!(ended, Some(Ok(()))),
            "still running after an hour, and stops when asked: {ended:?}"
        );
        assert_eq!(alerts(&logs, NOT_CONNECTED_EVENT), 1, "{logs}");
        assert!(logs.contains("Kafka operation timed out"), "{logs}");
        let stalled = alerts(&logs, STALLED_EVENT);
        assert!((5..=6).contains(&stalled), "1 every 10 minutes: {stalled}");
    }

    /// Studio creates the topic on its first send, so it may not be there when the consumer
    /// is first switched on, and a wrong topic name looks the same. Neither may end dipper.
    #[tokio::test(start_paused = true)]
    async fn a_topic_that_is_not_there_at_start_is_alerted_on_and_read_once_it_is() {
        let (registry, stored) = registry_for_a_request();
        let worker = MockWorker::new();
        let (tx_stop, rx_stop) = mpsc::channel(1);
        let found = |attempt| {
            Some(if attempt <= 3 {
                Err(ConsumerError::TopicNotFound {
                    topic: "requests".to_string(),
                })
            } else {
                Ok(topic_with_a_request())
            })
        };
        let mut ended = None;

        let logs = logged(async {
            ended = serve_until(
                serve_over(found, &registry, &worker, rx_stop),
                tx_stop,
                an_offset_is_stored(stored),
            )
            .await;
        })
        .await;

        assert!(matches!(ended, Some(Ok(()))), "{ended:?}");
        assert_eq!(worker.reassessments().len(), 1, "the request is applied");
        assert_eq!(alerts(&logs, NOT_CONNECTED_EVENT), 1, "{logs}");
        assert!(logs.contains("topic 'requests' does not exist"), "{logs}");
    }

    /// A stop that arrives while the first connect is still waiting on the brokers must not
    /// wait for it: dipper allows each service 5 seconds before it gives up on a clean exit.
    #[tokio::test(start_paused = true)]
    async fn a_stop_during_the_first_connect_ends_the_service_at_once() {
        let (registry, _) = registry_for_a_request();
        let worker = MockWorker::new();
        let (tx_stop, rx_stop) = mpsc::channel(1);

        let ended = serve_until(
            serve_over(|_| None, &registry, &worker, rx_stop),
            tx_stop,
            tokio::time::sleep(Duration::from_secs(1)),
        )
        .await;

        assert!(matches!(ended, Some(Ok(()))), "{ended:?}");
    }

    /// A topic the brokers list with no partitions yet has nothing to read. It is treated
    /// like a topic that is not there, and connected to again, where it once ended dipper.
    #[tokio::test(start_paused = true)]
    async fn a_topic_with_no_partitions_yet_is_connected_to_again() {
        let (registry, stored) = registry_for_a_request();
        let worker = MockWorker::new();
        let (tx_stop, rx_stop) = mpsc::channel(1);
        let found = |attempt| {
            let mut topic = topic_with_a_request();
            if attempt == 1 {
                topic.partitions = Vec::new();
            }
            Some(Ok(topic))
        };

        let ended = serve_until(
            serve_over(found, &registry, &worker, rx_stop),
            tx_stop,
            an_offset_is_stored(stored),
        )
        .await;

        assert!(matches!(ended, Some(Ok(()))), "{ended:?}");
        assert_eq!(worker.reassessments().len(), 1, "the request is applied");
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
    #[expect(
        clippy::too_many_lines,
        reason = "predates this lint; fix when next touched"
    )]
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
                chain_check: Arc::new(SkipChainCheck),
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
