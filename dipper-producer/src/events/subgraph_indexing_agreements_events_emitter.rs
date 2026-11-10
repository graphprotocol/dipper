//! Subgraph Indexing Agreement event emitter to Kafka topic utilities

use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use prost::Message;
use thegraph_core::{DeploymentId, alloy::primitives::ChainId};
use tokio::{
    sync::{Notify, mpsc},
    task::JoinHandle,
};

use crate::{
    kafka::{self, KafkaConfig, KafkaProducer, Reconnecting},
    proto,
};

/// Failure of a durable-tier emit (see [`SubgraphIndexingAgreementEventsProducer`]).
///
/// The caller must treat this as "not yet emitted" and leave its DB marker
/// unset so the event is retried on the next sweep.
#[derive(Debug, thiserror::Error)]
pub enum EmitError {
    #[error("failed to send lifecycle event to broker: {0}")]
    Send(String),
}

/// CAIP-2 identifier for an EVM (`eip155`) chain.
///
/// Wraps a numeric [`ChainId`] and renders to its CAIP-2 string form
/// `eip155:{chain_id}` (e.g. `eip155:42161`) when written to the wire. Construct
/// one from a chain id via [`From`] / `ChainId::into`, so call sites pass the raw
/// chain id without repeating the `eip155:` formatting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Caip2ChainId(ChainId);

impl From<ChainId> for Caip2ChainId {
    fn from(chain_id: ChainId) -> Self {
        Self(chain_id)
    }
}

impl std::fmt::Display for Caip2ChainId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "eip155:{}", self.0)
    }
}

/// Produces Subgraph Indexing Agreement lifecycle events.
///
/// Abstracts [`SubgraphIndexingAgreementsEventsEmitter`] so callers can depend on
/// the behavior rather than the concrete Kafka emitter, and tests can substitute a
/// capturing double. `the_graph_network` is the protocol network's numeric
/// [`ChainId`]; implementors render it to its CAIP-2 form (`eip155:{id}`) on the wire.
///
/// # Two delivery tiers
///
/// - **Diagnostic** (`request.received`, `proposed`, `n_indexers_unavailable`):
///   the `produce_*` methods enqueue onto a bounded in-memory channel and return
///   immediately. Best-effort; dropped on a full/closed queue.
/// - **Durable** (`terminated`, and later `accepted` / `expired`): the `emit_*`
///   methods send synchronously and report success, so the caller persists a DB
///   emission marker only after the broker confirms the send. A failure leaves
///   the marker unset and the event is re-derived and retried on the next sweep.
#[async_trait]
pub trait SubgraphIndexingAgreementEventsProducer: Send + Sync {
    /// Produces a subgraph.indexing.agreement.request.received event
    fn produce_subgraph_indexing_agreement_request_received(
        &self,
        subgraph_deployment_qm_hash: DeploymentId,
        the_graph_network: ChainId,
        event: proto::SubgraphIndexingAgreementRequestReceived,
    );

    /// Produces a subgraph.indexing.agreement.proposed event
    fn produce_subgraph_indexing_agreement_proposed(
        &self,
        subgraph_deployment_qm_hash: DeploymentId,
        the_graph_network: ChainId,
        event: proto::SubgraphIndexingAgreementProposed,
    );

    /// Emits a subgraph.indexing.agreement.accepted event durably.
    ///
    /// Same durability contract as
    /// [`emit_subgraph_indexing_agreement_terminated`](Self::emit_subgraph_indexing_agreement_terminated):
    /// synchronous send, `Ok(())` lets the caller stamp its
    /// `accepted_event_emitted_at` marker, `Err(_)` leaves it unset for the next
    /// sweep. At-least-once; consumers dedup on `event.agreement_id`.
    async fn emit_subgraph_indexing_agreement_accepted(
        &self,
        subgraph_deployment_qm_hash: DeploymentId,
        the_graph_network: ChainId,
        event: proto::SubgraphIndexingAgreementAccepted,
    ) -> Result<(), EmitError>;

    /// Emits a subgraph.indexing.agreement.request.expired event durably.
    ///
    /// Same durability contract as the other `emit_*` methods: synchronous send,
    /// `Ok(())` lets the caller stamp its `expired_event_emitted_at` marker,
    /// `Err(_)` leaves it unset for the next sweep. At-least-once; consumers dedup
    /// on `event.agreement_id`.
    async fn emit_subgraph_indexing_agreement_request_expired(
        &self,
        subgraph_deployment_qm_hash: DeploymentId,
        the_graph_network: ChainId,
        event: proto::SubgraphIndexingAgreementRequestExpired,
    ) -> Result<(), EmitError>;

    /// Produces a subgraph.indexing.agreement.n_indexers_unavailable event
    fn produce_subgraph_indexing_agreement_n_indexers_unavailable(
        &self,
        subgraph_deployment_qm_hash: DeploymentId,
        the_graph_network: ChainId,
        event: proto::SubgraphIndexingAgreementNIndexersUnavailable,
    );

    /// Emits a subgraph.indexing.agreement.terminated event durably.
    ///
    /// Sends synchronously and reports the outcome. `Ok(())` means the broker
    /// accepted the event (or emission is disabled, a no-op the caller can treat
    /// as success); the caller may then stamp its `terminated_event_emitted_at`
    /// marker. `Err(_)` means the send failed and the marker must stay unset so
    /// the next sweep re-derives and retries. Delivery is at-least-once;
    /// consumers dedup on `event.agreement_id`.
    async fn emit_subgraph_indexing_agreement_terminated(
        &self,
        subgraph_deployment_qm_hash: DeploymentId,
        the_graph_network: ChainId,
        event: proto::SubgraphIndexingAgreementTerminated,
    ) -> Result<(), EmitError>;

    /// Flush buffered diagnostic events on shutdown. Best-effort and bounded; the
    /// default is a no-op for implementations with nothing to flush.
    async fn flush(&self) {}
}

/// Kafka producer wrapper for Subgraph Indexing agreements lifecycle events.
///
/// `streaming == None` means emission is disabled by configuration -- durable
/// `emit_*` calls are a successful no-op so the caller stamps its marker, and
/// diagnostic `produce_*` calls are dropped. `streaming == Some` means emission
/// is on; the inner producer may still be `None` while the broker is being
/// (re)connected, in which case durable emits return `Err` (so markers are not
/// stamped) and diagnostic events wait in the bounded queue until it connects.
pub struct SubgraphIndexingAgreementsEventsEmitter {
    streaming: Option<Streaming>,
}

/// State for an emitter with event streaming enabled.
struct Streaming {
    /// Current broker handle. `None` while (re)connecting.
    producer: Reconnecting<KafkaProducer>,
    /// Bounded channel for the best-effort diagnostic tier.
    queue: mpsc::Sender<QueuedSubgraphIndexingAgreementEvent>,
    /// Cumulative diagnostic events dropped (queue full, or the broker was
    /// disconnected). Surfaced in the warn logs since there is no metrics stack.
    dropped: Arc<AtomicU64>,
    /// Signals the drain task to flush what is buffered and exit (used by `flush`
    /// on shutdown). `notify_one` stores a permit, so the signal is not lost if
    /// the drain task is mid-send when it fires.
    shutdown: Arc<Notify>,
    /// The drain task handle, awaited by `flush`.
    drain_handle: Mutex<Option<JoinHandle<()>>>,
}

impl SubgraphIndexingAgreementsEventsEmitter {
    /// Creates a disabled producer (emission off by configuration).
    pub fn disabled() -> Self {
        Self { streaming: None }
    }

    /// Creates a streaming producer. The broker is connected in the background, retrying while
    /// unreachable and connecting again when sends keep failing, so this never blocks and never
    /// stays disabled after a broker outage or a change of brokers.
    pub fn enabled(config: KafkaConfig, capacity: usize) -> Self {
        let dropped = Arc::new(AtomicU64::new(0));
        let shutdown = Arc::new(Notify::new());
        let (tx, mut rx) = mpsc::channel::<QueuedSubgraphIndexingAgreementEvent>(capacity);

        let producer = Reconnecting::spawn("lifecycle events", move || {
            let config = config.clone();
            async move { KafkaProducer::new(&config).await }
        });

        // Drain diagnostic events through the current producer, holding each one
        // until the brokers take it (the bounded queue is the backpressure). On
        // shutdown, flush what is buffered.
        let drain_handle = {
            let producer = producer.clone();
            let dropped = dropped.clone();
            let shutdown = shutdown.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        maybe = rx.recv() => match maybe {
                            Some(event) => {
                                let (event_type, key, envelope) = Self::prepare_event(event);
                                let Some(buf) = Self::encode(event_type, &envelope) else {
                                    continue;
                                };
                                let (key_ref, buf_ref) = (key.as_str(), buf.as_slice());
                                let send = move |client: Arc<KafkaProducer>| async move {
                                    client.send(key_ref, buf_ref).await
                                };
                                match deliver(&producer, &shutdown, event_type, &key, send).await {
                                    Delivery::Sent | Delivery::Refused => {}
                                    // Shutdown fired while the brokers were unreachable: drop
                                    // the held event and the rest, keeping the counter honest.
                                    Delivery::Shutdown => {
                                        let mut dropped_now: u64 = 1;
                                        while rx.try_recv().is_ok() {
                                            dropped_now += 1;
                                        }
                                        let total =
                                            dropped.fetch_add(dropped_now, Ordering::Relaxed)
                                                + dropped_now;
                                        tracing::warn!(
                                            dropped_now,
                                            dropped_total = total,
                                            "event broker unreachable at shutdown; dropping buffered diagnostic events"
                                        );
                                        break;
                                    }
                                }
                            }
                            None => break,
                        },
                        _ = shutdown.notified() => {
                            while let Ok(event) = rx.try_recv() {
                                Self::drain_one(&producer, &dropped, event).await;
                            }
                            break;
                        }
                    }
                }
            })
        };

        Self {
            streaming: Some(Streaming {
                producer,
                queue: tx,
                dropped,
                shutdown,
                drain_handle: Mutex::new(Some(drain_handle)),
            }),
        }
    }

    /// Send one diagnostic event through the current producer, or drop it (with a
    /// counter) if the broker is not connected. Only used by the shutdown flush,
    /// which must not wait for a connect.
    async fn drain_one(
        producer: &Reconnecting<KafkaProducer>,
        dropped: &AtomicU64,
        event: QueuedSubgraphIndexingAgreementEvent,
    ) {
        let Some(handle) = producer.current() else {
            let total = dropped.fetch_add(1, Ordering::Relaxed) + 1;
            tracing::warn!(
                dropped_total = total,
                "event broker not connected; dropping diagnostic event"
            );
            return;
        };
        let (event_type, key, envelope) = Self::prepare_event(event);
        // `send_event` already bounds the produce attempt via `KafkaProducer`'s
        // internal produce timeout, so no additional timeout is layered here.
        Self::send_event(producer, &handle, event_type, &key, envelope).await;
    }
}

#[async_trait]
impl SubgraphIndexingAgreementEventsProducer for SubgraphIndexingAgreementsEventsEmitter {
    fn produce_subgraph_indexing_agreement_request_received(
        &self,
        subgraph_deployment_qm_hash: DeploymentId,
        the_graph_network: ChainId,
        event: proto::SubgraphIndexingAgreementRequestReceived,
    ) {
        self.enqueue(
            subgraph_deployment_qm_hash,
            the_graph_network.into(),
            EventPayload::RequestReceived(event),
        );
    }

    fn produce_subgraph_indexing_agreement_proposed(
        &self,
        subgraph_deployment_qm_hash: DeploymentId,
        the_graph_network: ChainId,
        event: proto::SubgraphIndexingAgreementProposed,
    ) {
        self.enqueue(
            subgraph_deployment_qm_hash,
            the_graph_network.into(),
            EventPayload::Proposed(event),
        );
    }

    async fn emit_subgraph_indexing_agreement_accepted(
        &self,
        subgraph_deployment_qm_hash: DeploymentId,
        the_graph_network: ChainId,
        event: proto::SubgraphIndexingAgreementAccepted,
    ) -> Result<(), EmitError> {
        self.send_durable(
            subgraph_deployment_qm_hash,
            the_graph_network.into(),
            SubgraphIndexingAgreementEventType::Accepted,
            EventPayload::Accepted(event),
        )
        .await
    }

    async fn emit_subgraph_indexing_agreement_request_expired(
        &self,
        subgraph_deployment_qm_hash: DeploymentId,
        the_graph_network: ChainId,
        event: proto::SubgraphIndexingAgreementRequestExpired,
    ) -> Result<(), EmitError> {
        self.send_durable(
            subgraph_deployment_qm_hash,
            the_graph_network.into(),
            SubgraphIndexingAgreementEventType::RequestExpired,
            EventPayload::RequestExpired(event),
        )
        .await
    }

    fn produce_subgraph_indexing_agreement_n_indexers_unavailable(
        &self,
        subgraph_deployment_qm_hash: DeploymentId,
        the_graph_network: ChainId,
        event: proto::SubgraphIndexingAgreementNIndexersUnavailable,
    ) {
        self.enqueue(
            subgraph_deployment_qm_hash,
            the_graph_network.into(),
            EventPayload::NIndexersUnavailable(event),
        );
    }

    async fn emit_subgraph_indexing_agreement_terminated(
        &self,
        subgraph_deployment_qm_hash: DeploymentId,
        the_graph_network: ChainId,
        event: proto::SubgraphIndexingAgreementTerminated,
    ) -> Result<(), EmitError> {
        self.send_durable(
            subgraph_deployment_qm_hash,
            the_graph_network.into(),
            SubgraphIndexingAgreementEventType::Terminated,
            EventPayload::Terminated(event),
        )
        .await
    }

    async fn flush(&self) {
        self.flush_diagnostics().await;
    }
}

impl SubgraphIndexingAgreementsEventsEmitter {
    /// Send a durable-tier event synchronously and report the outcome. Shared by
    /// the `emit_*` methods.
    ///
    /// - Disabled by config: a successful no-op so the caller stamps its marker.
    /// - Streaming but not yet connected: `Err`, so the caller does NOT stamp its
    ///   marker and the event is retried on the next sweep once the broker is up.
    /// - Connected: send synchronously and report the broker's outcome.
    async fn send_durable(
        &self,
        subgraph_deployment_qm_hash: DeploymentId,
        the_graph_network: Caip2ChainId,
        event_type: SubgraphIndexingAgreementEventType,
        payload: EventPayload,
    ) -> Result<(), EmitError> {
        let Some(streaming) = &self.streaming else {
            return Ok(());
        };
        let Some(producer) = streaming.producer.current() else {
            return Err(EmitError::Send(
                "event broker not connected; will retry".to_string(),
            ));
        };

        let metadata = EventMetadata {
            subgraph_deployment_qm_hash,
            the_graph_network,
        };
        let key = metadata.partition_key();
        let envelope = Self::create_event_envelope(event_type, &metadata, payload.into_proto());

        let mut buf = Vec::with_capacity(envelope.encoded_len());
        envelope
            .encode(&mut buf)
            .map_err(|e| EmitError::Send(format!("encode failed: {e}")))?;

        let result = producer.send(&key, &buf).await;
        report(&streaming.producer, &producer, &result);
        result.map_err(|e| EmitError::Send(e.to_string()))
    }

    fn enqueue(
        &self,
        subgraph_deployment_qm_hash: DeploymentId,
        the_graph_network: Caip2ChainId,
        payload: EventPayload,
    ) {
        let Some(streaming) = &self.streaming else {
            return;
        };

        let event = QueuedSubgraphIndexingAgreementEvent {
            metadata: EventMetadata {
                subgraph_deployment_qm_hash,
                the_graph_network,
            },
            payload,
        };

        let event_type = event.payload.event_type();
        if let Err(err) = streaming.queue.try_send(event) {
            let total = streaming.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            let reason = match err {
                mpsc::error::TrySendError::Full(_) => "event queue full",
                mpsc::error::TrySendError::Closed(_) => "event queue closed",
            };
            tracing::warn!(
                event_type = %event_type,
                dropped_total = total,
                "{reason}; dropping diagnostic event"
            );
        }
    }

    /// Flush buffered diagnostic events and stop the drain task. Called once on
    /// shutdown. Bounded by a timeout so a slow/unreachable broker can't hang
    /// shutdown. Idempotent: the drain handle is taken, so a second call is a
    /// no-op.
    async fn flush_diagnostics(&self) {
        let Some(streaming) = &self.streaming else {
            return;
        };
        // Wake the drain task (permit is stored if it is mid-send).
        streaming.shutdown.notify_one();
        let handle = streaming
            .drain_handle
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(handle) = handle {
            const FLUSH_TIMEOUT: Duration = Duration::from_secs(5);
            if tokio::time::timeout(FLUSH_TIMEOUT, handle).await.is_err() {
                tracing::warn!("timed out flushing buffered lifecycle events on shutdown");
            }
        }
    }

    fn prepare_event(
        event: QueuedSubgraphIndexingAgreementEvent,
    ) -> (
        SubgraphIndexingAgreementEventType,
        String,
        proto::SubgraphIndexingAgreementEvent,
    ) {
        let event_type = event.payload.event_type();
        let key = event.metadata.partition_key();
        let envelope =
            Self::create_event_envelope(event_type, &event.metadata, event.payload.into_proto());

        (event_type, key, envelope)
    }

    /// Sends the event to the Kafka topic
    ///
    /// Errors are logged, but do not fail as it is a best-effort
    async fn send_event(
        conn: &Reconnecting<KafkaProducer>,
        producer: &Arc<KafkaProducer>,
        event_type: SubgraphIndexingAgreementEventType,
        key: &str,
        event: proto::SubgraphIndexingAgreementEvent,
    ) {
        let Some(buf) = Self::encode(event_type, &event) else {
            return;
        };
        let result = producer.send(key, &buf).await;
        report(conn, producer, &result);
        if let Err(e) = result {
            tracing::warn!(
                event_type = %event_type,
                key,
                error = %e,
                "failed to send Subgraph Indexing Agreement event to producer (event dropped)"
            );
        }
    }

    /// Encodes an event for the wire, logging the event type if it can't be encoded.
    fn encode(
        event_type: SubgraphIndexingAgreementEventType,
        event: &proto::SubgraphIndexingAgreementEvent,
    ) -> Option<Vec<u8>> {
        let mut buf = Vec::with_capacity(event.encoded_len());
        match event.encode(&mut buf) {
            Ok(()) => Some(buf),
            Err(e) => {
                tracing::error!(
                    event_type = %event_type,
                    error = %e,
                    "failed to encode Subgraph Indexing Agreement event"
                );
                None
            }
        }
    }

    /// Creates the event envelope with common metadata
    fn create_event_envelope(
        event_type: SubgraphIndexingAgreementEventType,
        metadata: &EventMetadata,
        payload: proto::subgraph_indexing_agreement_event::Payload,
    ) -> proto::SubgraphIndexingAgreementEvent {
        proto::SubgraphIndexingAgreementEvent {
            event_id: uuid::Uuid::now_v7().to_string(),
            event_type: event_type.to_string(),
            event_version: "1.0".to_string(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            subgraph_deployment_qm_hash: metadata.subgraph_deployment_qm_hash.to_string(),
            the_graph_network: metadata.the_graph_network.to_string(),
            payload: Some(payload),
        }
    }
}

/// What became of a diagnostic event handed to [`deliver`].
enum Delivery {
    Sent,
    /// The brokers received the event and refused it, so it is dropped.
    Refused,
    /// Shutdown fired before the event could be sent.
    Shutdown,
}

/// Pause before resending an event the brokers didn't get, so fast failures don't spin.
const RESEND_PAUSE: Duration = Duration::from_secs(1);

/// Sends a diagnostic event, resending it while the brokers are unreachable. Each such failure is
/// reported to `conn`, which rebuilds the client once they have gone on for a minute, so the event
/// goes out once the cluster's current brokers are found.
async fn deliver<T, S, Fut>(
    conn: &Reconnecting<T>,
    shutdown: &Notify,
    event_type: SubgraphIndexingAgreementEventType,
    key: &str,
    send: S,
) -> Delivery
where
    T: Send + Sync + 'static,
    S: Fn(Arc<T>) -> Fut,
    Fut: Future<Output = Result<(), kafka::Error>>,
{
    let mut attempt: u32 = 0;
    loop {
        let Some(client) = wait_for_client(conn, shutdown).await else {
            return Delivery::Shutdown;
        };
        let result = send(client.clone()).await;
        report(conn, &client, &result);
        match result {
            Ok(()) => return Delivery::Sent,
            Err(e) if e.is_unreachable() => {
                attempt = attempt.saturating_add(1);
                tracing::warn!(
                    event_type = %event_type,
                    key,
                    attempt,
                    error = %e,
                    "event broker unreachable; holding the event to resend"
                );
                tokio::select! {
                    _ = tokio::time::sleep(RESEND_PAUSE) => {}
                    _ = shutdown.notified() => return Delivery::Shutdown,
                }
            }
            Err(e) => {
                tracing::warn!(
                    event_type = %event_type,
                    key,
                    error = %e,
                    "event broker refused the Subgraph Indexing Agreement event (event dropped)"
                );
                return Delivery::Refused;
            }
        }
    }
}

/// Tells `conn` whether a send through `client` reached the brokers. Every send goes through here,
/// so none is left out of the count that decides when to reconnect.
fn report<T: Send + Sync + 'static>(
    conn: &Reconnecting<T>,
    client: &Arc<T>,
    result: &Result<(), kafka::Error>,
) {
    conn.report(client, result);
}

/// Waits until a client is available, checking every 250ms. Returns `None` if shutdown is
/// signalled first, so the caller can stop waiting on brokers that may never come up.
async fn wait_for_client<T: Send + Sync + 'static>(
    conn: &Reconnecting<T>,
    shutdown: &Notify,
) -> Option<Arc<T>> {
    const POLL: Duration = Duration::from_millis(250);
    loop {
        if let Some(client) = conn.current() {
            return Some(client);
        }
        tokio::select! {
            _ = tokio::time::sleep(POLL) => {}
            _ = shutdown.notified() => return None,
        }
    }
}

/// A queued event: shared routing metadata plus its type-specific payload.
struct QueuedSubgraphIndexingAgreementEvent {
    metadata: EventMetadata,
    payload: EventPayload,
}

/// Routing metadata common to every Subgraph Indexing Agreement event.
struct EventMetadata {
    /// The Subgraph deployment. Rendered to its `Qm...` hash representation
    /// when building the envelope and partition key.
    subgraph_deployment_qm_hash: DeploymentId,
    /// The Graph protocol network. Rendered to its CAIP-2 form
    /// (e.g. `eip155:42161`) when building the envelope and partition key.
    the_graph_network: Caip2ChainId,
}

impl EventMetadata {
    /// Creates the partition key for the Subgraph Indexing Agreement events
    ///
    /// Format: `{the_graph_network}/{subgraph_deployment_qm_hash}`
    /// e.g. `eip155:42161/QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9`
    fn partition_key(&self) -> String {
        format!(
            "{}/{}",
            self.the_graph_network, self.subgraph_deployment_qm_hash
        )
    }
}

/// The type-specific payload of a Subgraph Indexing Agreement event.
enum EventPayload {
    RequestReceived(proto::SubgraphIndexingAgreementRequestReceived),
    Proposed(proto::SubgraphIndexingAgreementProposed),
    Accepted(proto::SubgraphIndexingAgreementAccepted),
    RequestExpired(proto::SubgraphIndexingAgreementRequestExpired),
    NIndexersUnavailable(proto::SubgraphIndexingAgreementNIndexersUnavailable),
    Terminated(proto::SubgraphIndexingAgreementTerminated),
}

impl EventPayload {
    fn event_type(&self) -> SubgraphIndexingAgreementEventType {
        match self {
            Self::RequestReceived(_) => SubgraphIndexingAgreementEventType::RequestReceived,
            Self::Proposed(_) => SubgraphIndexingAgreementEventType::Proposed,
            Self::Accepted(_) => SubgraphIndexingAgreementEventType::Accepted,
            Self::RequestExpired(_) => SubgraphIndexingAgreementEventType::RequestExpired,
            Self::NIndexersUnavailable(_) => {
                SubgraphIndexingAgreementEventType::NIndexersUnavailable
            }
            Self::Terminated(_) => SubgraphIndexingAgreementEventType::Terminated,
        }
    }

    fn into_proto(self) -> proto::subgraph_indexing_agreement_event::Payload {
        use proto::subgraph_indexing_agreement_event::Payload;
        match self {
            Self::RequestReceived(e) => Payload::SubgraphIndexingAgreementRequestReceived(e),
            Self::Proposed(e) => Payload::SubgraphIndexingAgreementProposed(e),
            Self::Accepted(e) => Payload::SubgraphIndexingAgreementAccepted(e),
            Self::RequestExpired(e) => Payload::SubgraphIndexingAgreementRequestExpired(e),
            Self::NIndexersUnavailable(e) => {
                Payload::SubgraphIndexingAgreementNIndexersUnavailable(e)
            }
            Self::Terminated(e) => Payload::SubgraphIndexingAgreementTerminated(e),
        }
    }
}

/// Subgraph Indexing Agreement Event type discriminator for events
#[derive(Debug, Clone, Copy)]
enum SubgraphIndexingAgreementEventType {
    RequestReceived,
    Proposed,
    Accepted,
    RequestExpired,
    NIndexersUnavailable,
    Terminated,
}

impl std::fmt::Display for SubgraphIndexingAgreementEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::RequestReceived => "subgraph.indexing.agreement.request.received",
            Self::Proposed => "subgraph.indexing.agreement.proposed",
            Self::Accepted => "subgraph.indexing.agreement.accepted",
            Self::RequestExpired => "subgraph.indexing.agreement.request.expired",
            Self::NIndexersUnavailable => "subgraph.indexing.agreement.n_indexers_unavailable",
            Self::Terminated => "subgraph.indexing.agreement.terminated",
        };
        f.write_str(s)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU32;

    use proto::subgraph_indexing_agreement_event::Payload;

    use super::*;

    const HASH: &str = "QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9";
    const CHAIN_ID: ChainId = 42161;
    const NETWORK: &str = "eip155:42161";

    fn deployment_id() -> DeploymentId {
        HASH.parse().expect("HASH is a valid deployment id")
    }

    /// A config whose producer can never connect (`partitions == 0` fails
    /// `KafkaProducer::new` fast, before any network), so the emitter stays in the
    /// disconnected state for the test.
    fn never_connects_config() -> KafkaConfig {
        KafkaConfig {
            brokers: vec![],
            topic: "test".to_string(),
            partitions: 0,
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            tls_enabled: false,
            tls_ca_cert_path: None,
            connect_timeout_secs: 60,
        }
    }

    #[tokio::test]
    async fn disabled_emitter_durable_emit_is_ok_noop() {
        // Disabled by config: durable emit is a successful no-op, so the caller
        // stamps its marker (events are intentionally off). Diagnostic produce is
        // a no-op too (must not panic).
        let emitter = SubgraphIndexingAgreementsEventsEmitter::disabled();

        let res = emitter
            .emit_subgraph_indexing_agreement_terminated(
                deployment_id(),
                CHAIN_ID,
                proto::SubgraphIndexingAgreementTerminated::default(),
            )
            .await;
        assert!(res.is_ok(), "disabled durable emit must be Ok (no-op)");

        emitter.produce_subgraph_indexing_agreement_n_indexers_unavailable(
            deployment_id(),
            CHAIN_ID,
            proto::SubgraphIndexingAgreementNIndexersUnavailable::default(),
        );
        emitter.flush().await;
    }

    #[tokio::test]
    async fn streaming_but_disconnected_durable_emit_errors() {
        // Enabled but the broker never connects: durable emit must return Err so
        // the caller does NOT stamp its marker and the event is retried on the next
        // sweep. This is what prevents a broker-down-at-startup from silently
        // losing durable events.
        let emitter = SubgraphIndexingAgreementsEventsEmitter::enabled(never_connects_config(), 8);

        let res = emitter
            .emit_subgraph_indexing_agreement_terminated(
                deployment_id(),
                CHAIN_ID,
                proto::SubgraphIndexingAgreementTerminated::default(),
            )
            .await;
        assert!(
            res.is_err(),
            "disconnected durable emit must be Err so the sweep retries"
        );

        // Diagnostic produce + flush must not panic while disconnected.
        emitter.produce_subgraph_indexing_agreement_n_indexers_unavailable(
            deployment_id(),
            CHAIN_ID,
            proto::SubgraphIndexingAgreementNIndexersUnavailable::default(),
        );
        emitter.flush().await;
    }

    #[tokio::test]
    async fn flush_while_disconnected_exits_promptly() {
        // Diagnostic events wait in the queue for the broker to connect, so the
        // drain task can be mid-wait at shutdown. Flush must interrupt that wait
        // (dropping what is buffered), not hang on a broker that never comes up.
        let emitter = SubgraphIndexingAgreementsEventsEmitter::enabled(never_connects_config(), 8);

        emitter.produce_subgraph_indexing_agreement_request_received(
            deployment_id(),
            CHAIN_ID,
            proto::SubgraphIndexingAgreementRequestReceived::default(),
        );

        tokio::time::timeout(Duration::from_secs(2), emitter.flush())
            .await
            .expect("flush must not wait for a broker that never connects");
    }

    /// Stands in for the producer, numbered by the connect attempt that made it.
    struct FakeClient(u32);

    /// Clients connected through a [`Reconnecting`], counting the connects.
    fn fake_clients() -> (Arc<AtomicU32>, Reconnecting<FakeClient>) {
        let connects = Arc::new(AtomicU32::new(0));
        let counted = connects.clone();
        let conn = Reconnecting::spawn("test", move || {
            let n = counted.fetch_add(1, Ordering::Relaxed) + 1;
            std::future::ready(Ok::<_, String>(FakeClient(n)))
        });
        (connects, conn)
    }

    async fn connected(conn: &Reconnecting<FakeClient>) {
        while conn.current().is_none() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn an_event_the_brokers_never_got_is_resent_through_a_new_client() {
        let (connects, conn) = fake_clients();
        let sent_by = Mutex::new(None);
        let sent_by_ref = &sent_by;

        // Client 1 dials brokers that are gone, as after a cluster replaces every broker.
        let delivery = deliver(
            &conn,
            &Notify::new(),
            SubgraphIndexingAgreementEventType::Proposed,
            "key",
            move |client: Arc<FakeClient>| async move {
                if client.0 == 1 {
                    return Err(kafka::Error::Timeout);
                }
                *sent_by_ref.lock().unwrap() = Some(client.0);
                Ok(())
            },
        )
        .await;

        assert!(matches!(delivery, Delivery::Sent));
        assert_eq!(*sent_by.lock().unwrap(), Some(2));
        assert_eq!(connects.load(Ordering::Relaxed), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn an_event_the_brokers_refused_is_dropped_without_resending() {
        let (connects, conn) = fake_clients();
        let attempts = AtomicU32::new(0);
        let attempts_ref = &attempts;

        let delivery = deliver(
            &conn,
            &Notify::new(),
            SubgraphIndexingAgreementEventType::Proposed,
            "key",
            move |_client: Arc<FakeClient>| async move {
                attempts_ref.fetch_add(1, Ordering::Relaxed);
                Err(kafka::Error::refused())
            },
        )
        .await;

        assert!(matches!(delivery, Delivery::Refused));
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
        assert_eq!(connects.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_stops_resending_an_event_the_brokers_never_got() {
        let (_, conn) = fake_clients();
        connected(&conn).await;
        let shutdown = Notify::new();
        shutdown.notify_one();

        let delivery = deliver(
            &conn,
            &shutdown,
            SubgraphIndexingAgreementEventType::Proposed,
            "key",
            |_client: Arc<FakeClient>| async { Err(kafka::Error::Timeout) },
        )
        .await;

        assert!(matches!(delivery, Delivery::Shutdown));
    }

    fn metadata() -> EventMetadata {
        EventMetadata {
            subgraph_deployment_qm_hash: deployment_id(),
            the_graph_network: CHAIN_ID.into(),
        }
    }

    /// Every payload variant paired with the event-type string and proto payload
    /// variant it is contractually required to map to. Adding a new event variant
    /// without updating this table is a compile error (non-exhaustive match below).
    fn all_payloads() -> Vec<(EventPayload, &'static str)> {
        vec![
            (
                EventPayload::RequestReceived(Default::default()),
                "subgraph.indexing.agreement.request.received",
            ),
            (
                EventPayload::Proposed(Default::default()),
                "subgraph.indexing.agreement.proposed",
            ),
            (
                EventPayload::Accepted(Default::default()),
                "subgraph.indexing.agreement.accepted",
            ),
            (
                EventPayload::RequestExpired(Default::default()),
                "subgraph.indexing.agreement.request.expired",
            ),
            (
                EventPayload::NIndexersUnavailable(Default::default()),
                "subgraph.indexing.agreement.n_indexers_unavailable",
            ),
            (
                EventPayload::Terminated(Default::default()),
                "subgraph.indexing.agreement.terminated",
            ),
        ]
    }

    #[test]
    fn caip2_chain_id_from_chain_id_renders_eip155() {
        let network: Caip2ChainId = CHAIN_ID.into();
        assert_eq!(network.to_string(), NETWORK);
        assert_eq!(network.to_string(), "eip155:42161");
    }

    #[test]
    fn partition_key_uses_network_slash_hash_format() {
        assert_eq!(metadata().partition_key(), format!("{NETWORK}/{HASH}"));
    }

    #[test]
    fn event_type_string_matches_payload_for_every_variant() {
        for (payload, expected) in all_payloads() {
            assert_eq!(payload.event_type().to_string(), expected);
        }
    }

    #[test]
    fn into_proto_preserves_the_variant_for_every_payload() {
        for (payload, expected) in all_payloads() {
            // Guards against a copy-paste swap between two payload arms.
            let mapped = match payload.into_proto() {
                Payload::SubgraphIndexingAgreementRequestReceived(_) => {
                    "subgraph.indexing.agreement.request.received"
                }
                Payload::SubgraphIndexingAgreementProposed(_) => {
                    "subgraph.indexing.agreement.proposed"
                }
                Payload::SubgraphIndexingAgreementAccepted(_) => {
                    "subgraph.indexing.agreement.accepted"
                }
                Payload::SubgraphIndexingAgreementRequestExpired(_) => {
                    "subgraph.indexing.agreement.request.expired"
                }
                Payload::SubgraphIndexingAgreementNIndexersUnavailable(_) => {
                    "subgraph.indexing.agreement.n_indexers_unavailable"
                }
                Payload::SubgraphIndexingAgreementTerminated(_) => {
                    "subgraph.indexing.agreement.terminated"
                }
            };
            assert_eq!(mapped, expected);
        }
    }

    #[test]
    fn create_event_envelope_populates_common_metadata() {
        let payload =
            EventPayload::RequestReceived(proto::SubgraphIndexingAgreementRequestReceived {
                agreements_requested: 2,
            });
        let event_type = payload.event_type();
        let envelope = SubgraphIndexingAgreementsEventsEmitter::create_event_envelope(
            event_type,
            &metadata(),
            payload.into_proto(),
        );

        assert_eq!(
            envelope.event_type,
            "subgraph.indexing.agreement.request.received"
        );
        assert_eq!(envelope.event_version, "1.0");
        assert_eq!(envelope.subgraph_deployment_qm_hash, HASH);
        assert_eq!(envelope.the_graph_network, NETWORK);
        assert!(matches!(
            envelope.payload,
            Some(Payload::SubgraphIndexingAgreementRequestReceived(_))
        ));

        let id = uuid::Uuid::parse_str(&envelope.event_id).expect("event_id is a valid uuid");
        assert_eq!(id.get_version_num(), 7, "event_id should be a UUIDv7");
        chrono::DateTime::parse_from_rfc3339(&envelope.timestamp)
            .expect("timestamp is valid rfc3339");
    }

    #[test]
    fn deployment_id_renders_to_qm_hash_in_envelope_and_partition_key() {
        // The emitter takes a strongly-typed DeploymentId and is responsible for
        // rendering it to its Qm-hash string on the wire. Assert that rendering is
        // the canonical Qm hash (round-trips back to the same DeploymentId) and that
        // the partition key uses the same rendering.
        let dep = deployment_id();
        let envelope = SubgraphIndexingAgreementsEventsEmitter::create_event_envelope(
            SubgraphIndexingAgreementEventType::RequestReceived,
            &metadata(),
            EventPayload::RequestReceived(Default::default()).into_proto(),
        );

        assert_eq!(envelope.subgraph_deployment_qm_hash, HASH);
        let parsed: DeploymentId = envelope
            .subgraph_deployment_qm_hash
            .parse()
            .expect("envelope qm hash parses back into a DeploymentId");
        assert_eq!(parsed, dep, "qm-hash rendering must be lossless");

        assert_eq!(
            metadata().partition_key(),
            format!("eip155:42161/{dep}"),
            "partition key must use the caip2 network and the Qm-hash rendering"
        );
    }

    #[test]
    fn prepare_event_builds_key_type_and_wire_encodable_envelope() {
        let event = QueuedSubgraphIndexingAgreementEvent {
            metadata: metadata(),
            payload: EventPayload::Terminated(proto::SubgraphIndexingAgreementTerminated {
                indexer: "0xabc".to_string(),
                terminated_at: 42,
                terminated_by: "0xdef".to_string(),
                terminated_tx: "0x123".to_string(),
                ..Default::default()
            }),
        };

        let (event_type, key, envelope) =
            SubgraphIndexingAgreementsEventsEmitter::prepare_event(event);

        assert_eq!(
            event_type.to_string(),
            "subgraph.indexing.agreement.terminated"
        );
        assert_eq!(key, format!("{NETWORK}/{HASH}"));

        // Round-trips through the same prost encode path `send_event` uses.
        let mut buf = Vec::with_capacity(envelope.encoded_len());
        envelope.encode(&mut buf).expect("envelope encodes");
        let decoded =
            proto::SubgraphIndexingAgreementEvent::decode(&buf[..]).expect("envelope decodes");
        assert_eq!(decoded, envelope);
    }
}
