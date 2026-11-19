//! Prometheus metrics, served at `/metrics` on the health port. The code records them through the
//! [`metrics`] facade where things happen (jobs, RPC calls, transactions, IISA, Kafka), a sampler
//! reads what only the database knows (its reachability and the queue's depth), and a log layer
//! counts every tagged log line, which makes each alertable event a counter too.
//!
//! Labels hold values from bounded sets (a job kind, an operation, an outcome, a configured RPC
//! provider's host, an `event` tag dipper's code sets), never an address, a deployment or an ID,
//! so the number of series stays bounded.

use std::{future::Future, time::Duration};

use metrics_exporter_prometheus::{BuildError, Matcher, PrometheusBuilder, PrometheusHandle};
use sqlx::{Connection as _, PgConnection, Pool, Postgres, postgres::PgConnectOptions};
use tokio::sync::mpsc;
use tracing::{Event, Subscriber};
use tracing_subscriber::{Layer, layer::Context};

/// Jobs the worker finished, by `kind` and `outcome` (success, retry, gave_up, deferred, fatal,
/// or settle_failed when the queue could not record the outcome, so the job will run again).
pub const JOBS: &str = "dipper_jobs_total";
/// How long each job ran, by `kind`.
pub const JOB_DURATION: &str = "dipper_job_duration_seconds";
/// Jobs a worker loop is running now.
pub const JOBS_IN_FLIGHT: &str = "dipper_jobs_in_flight";
/// Times a worker loop lost its LISTEN connection and fell back to polling the queue.
pub const JOB_LISTENER_FALLBACKS: &str = "dipper_job_listener_fallbacks_total";
/// Queued jobs, by `state` (ready, scheduled, failed), as last sampled.
pub const QUEUE_JOBS: &str = "dipper_queue_jobs";
/// How long the longest-waiting ready job has been due, as last sampled; 0 with none ready. A
/// job a worker is running still counts as ready here, as only its own transaction sees it taken,
/// so 1 long job on an idle queue raises this up to the job timeout.
pub const QUEUE_OLDEST_READY_AGE: &str = "dipper_queue_oldest_ready_job_age_seconds";
/// 1 while the last sample's `SELECT 1` succeeded, 0 otherwise.
pub const DB_UP: &str = "dipper_db_up";
/// The database pool's connections, by `state` (idle, in_use), as last sampled.
pub const DB_POOL_CONNECTIONS: &str = "dipper_db_pool_connections";
/// RPC requests to 1 provider, by `operation`, `provider` (host:port) and `outcome` (ok, behind,
/// retryable_error, refused when the provider answered with an error such as a revert, error).
/// Retries against the same provider count once each.
pub const RPC_REQUESTS: &str = "dipper_rpc_requests_total";
/// RPC calls every configured provider failed, by `operation`. A call the chain refused, such as
/// a gas estimate that reverts, is an answer and does not count.
pub const RPC_CALLS_FAILED: &str = "dipper_rpc_calls_failed_total";
/// On-chain transactions dipper sent, by `call` (offer, cancel, reconcile_provider,
/// reconcile_agreement) and `outcome`: mined, reverted, dropped (not mined in the receipt window),
/// or unconfirmed (no receipt check got an answer, so it may have mined).
pub const TRANSACTIONS: &str = "dipper_transactions_total";
/// Transactions dipper could not send, by `call` and `reason` (contract_revert when the gas
/// estimate reverted, submit_failed, rpc_error, error). None of them reached the chain.
pub const TRANSACTION_SUBMIT_FAILURES: &str = "dipper_transaction_submit_failures_total";
/// Times the signer's nonce was read back from the chain after a nonce error.
pub const NONCE_RESYNCS: &str = "dipper_nonce_resyncs_total";
/// Log lines tagged with an `event`, by `event` and `level`. The events dipper alerts on are
/// among them.
pub const LOG_EVENTS: &str = "dipper_log_events_total";

/// 1 while this dipper holds the singleton lock on its database, 0 otherwise.
pub const SINGLETON_LOCK_HELD: &str = "dipper_singleton_lock_held";

/// How often the sampler reads the database.
pub const SAMPLE_INTERVAL: Duration = Duration::from_secs(15);

/// How long one sample may take before it counts as the database being down.
const SAMPLE_TIMEOUT: Duration = Duration::from_secs(5);

/// How long stopping the sampler may take before shutdown moves on without it.
const STOP_TIMEOUT: Duration = Duration::from_secs(5);

/// Buckets for job durations: most jobs take well under a second, and none may pass the worker's
/// 420s job timeout.
const JOB_DURATION_BUCKETS: &[f64] = &[
    0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 420.0,
];

/// Builds the Prometheus recorder with dipper's buckets, without installing it.
pub fn recorder() -> Result<metrics_exporter_prometheus::PrometheusRecorder, BuildError> {
    Ok(PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full(JOB_DURATION.to_string()),
            JOB_DURATION_BUCKETS,
        )?
        .build_recorder())
}

/// Installs the Prometheus recorder as the process's metrics recorder and describes every metric.
/// Returns the handle that renders them for `/metrics`.
pub fn install() -> anyhow::Result<PrometheusHandle> {
    let recorder = recorder()?;
    let handle = recorder.handle();
    metrics::set_global_recorder(recorder)
        .map_err(|err| anyhow::anyhow!("failed to install the metrics recorder: {err}"))?;
    describe();
    Ok(handle)
}

/// Gives each metric its help text, which `/metrics` shows.
pub fn describe() {
    use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};
    describe_counter!(JOBS, "Jobs the worker finished, by kind and outcome");
    describe_histogram!(
        JOB_DURATION,
        Unit::Seconds,
        "How long each job ran, by kind"
    );
    describe_gauge!(JOBS_IN_FLIGHT, "Jobs a worker loop is running now");
    metrics::gauge!(JOBS_IN_FLIGHT).set(0.0);
    describe_counter!(
        JOB_LISTENER_FALLBACKS,
        "Times a worker loop lost its LISTEN connection and fell back to polling"
    );
    describe_gauge!(QUEUE_JOBS, "Queued jobs by state, as last sampled");
    describe_gauge!(
        QUEUE_OLDEST_READY_AGE,
        Unit::Seconds,
        "How long the longest-waiting ready job has been due, as last sampled"
    );
    describe_gauge!(
        DB_UP,
        "1 while the database answers dipper's samples, 0 otherwise"
    );
    describe_gauge!(
        SINGLETON_LOCK_HELD,
        "1 while this dipper holds the singleton lock on its database"
    );
    describe_gauge!(DB_POOL_CONNECTIONS, "Database pool connections by state");
    describe_counter!(
        RPC_REQUESTS,
        "RPC requests to 1 provider, by operation, provider and outcome"
    );
    describe_counter!(
        RPC_CALLS_FAILED,
        "RPC calls every provider failed, by operation"
    );
    describe_counter!(
        TRANSACTIONS,
        "On-chain transactions sent, by call and outcome"
    );
    describe_counter!(
        TRANSACTION_SUBMIT_FAILURES,
        "Transactions that could not be sent, by call and reason"
    );
    describe_counter!(
        NONCE_RESYNCS,
        "Times the signer's nonce was read back from the chain"
    );
    describe_counter!(
        LOG_EVENTS,
        "Log lines tagged with an event, by event and level"
    );
    describe_counter!(
        crate::telemetry::IISA_REQUESTS,
        "IISA requests, by endpoint and outcome, after the client's own retries"
    );
    describe_counter!(
        crate::telemetry::KAFKA_SENDS,
        "Attempts to send agreement events to Kafka, by outcome; a resent event counts again"
    );
    describe_counter!(
        crate::telemetry::KAFKA_EVENTS_DROPPED,
        "Diagnostic agreement events dropped before sending, by reason"
    );
}

/// IISA requests, by `endpoint` and `outcome`. Recorded by the IISA client crate, which owns the
/// name, and described here with the rest.
pub const IISA_REQUESTS: &str = dipper_iisa::METRIC_REQUESTS;
/// Attempts to send agreement events to Kafka, by `outcome`. Recorded by the producer crate.
pub const KAFKA_SENDS: &str = dipper_producer::METRIC_KAFKA_SENDS;
/// Diagnostic agreement events dropped before sending, by `reason`. Recorded by the producer crate.
pub const KAFKA_EVENTS_DROPPED: &str = dipper_producer::METRIC_KAFKA_EVENTS_DROPPED;

/// A log layer counting every log line that carries an `event` tag, at every level, so an event
/// can be graphed and alerted on from metrics as well as from logs and Slack. Add it with
/// [`TaggedEvents`] as its filter.
pub struct EventCounter;

/// The filter for [`EventCounter`]: only call sites with an `event` field. Decided once per call
/// site, so the counter does not make every other log line and span in dipper and its libraries
/// pass the level check, as a layer without a filter does by asking to see all of them.
pub struct TaggedEvents;

impl TaggedEvents {
    fn is_tagged(metadata: &tracing::Metadata<'_>) -> bool {
        metadata.is_event() && metadata.fields().field("event").is_some()
    }
}

impl<S> tracing_subscriber::layer::Filter<S> for TaggedEvents {
    fn enabled(&self, metadata: &tracing::Metadata<'_>, _ctx: &Context<'_, S>) -> bool {
        Self::is_tagged(metadata)
    }

    fn callsite_enabled(
        &self,
        metadata: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        if Self::is_tagged(metadata) {
            tracing::subscriber::Interest::always()
        } else {
            tracing::subscriber::Interest::never()
        }
    }
}

impl<S: Subscriber> Layer<S> for EventCounter {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        if let Some(tag) = crate::alerts::EventTag::of(event) {
            let level = event.metadata().level().as_str();
            metrics::counter!(LOG_EVENTS, "event" => tag, "level" => level).increment(1);
        }
    }
}

/// Stops the database sampler.
#[derive(Clone)]
pub struct Handle {
    tx_stop: mpsc::Sender<()>,
}

impl Handle {
    /// Stops the sampler, waiting at most [`STOP_TIMEOUT`].
    pub async fn stop(&self) {
        if self.tx_stop.is_closed() {
            return;
        }
        let _ = self.tx_stop.send(()).await;
        let _ = tokio::time::timeout(STOP_TIMEOUT, self.tx_stop.closed()).await;
    }
}

/// The database sampler: every `interval`, records whether the database answers, how the pool's
/// connections are used, and how deep the job queue is, and runs the exporter's upkeep. It asks
/// the database on a connection of its own, so a busy pool neither reads as the database being
/// down nor gets a connection taken from it.
pub fn sampler(
    pool: Pool<Postgres>,
    handle: PrometheusHandle,
    interval: Duration,
) -> (Handle, impl Future<Output = anyhow::Result<()>>) {
    let (tx_stop, mut rx_stop) = mpsc::channel(1);
    let run = async move {
        let options = pool.connect_options().as_ref().clone();
        let mut conn: Option<PgConnection> = None;
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! { biased;
                _ = rx_stop.recv() => break,
                _ = ticker.tick() => {}
            }
            // A sample in progress is dropped when the stop comes, so stopping never waits on it.
            tokio::select! { biased;
                _ = rx_stop.recv() => break,
                () = sample(&pool, &options, &mut conn) => {}
            }
            handle.run_upkeep();
        }
        if let Some(conn) = conn {
            let _ = conn.close().await;
        }
        Ok(())
    };
    (Handle { tx_stop }, run)
}

/// Takes 1 sample of the pool, the database and the queue, (re)connecting `conn` as needed.
async fn sample(
    pool: &Pool<Postgres>,
    options: &PgConnectOptions,
    conn: &mut Option<PgConnection>,
) {
    let idle = pool.num_idle();
    let size = pool.size() as usize;
    metrics::gauge!(DB_POOL_CONNECTIONS, "state" => "idle").set(idle as f64);
    metrics::gauge!(DB_POOL_CONNECTIONS, "state" => "in_use").set(size.saturating_sub(idle) as f64);

    if conn.is_none() {
        *conn = tokio::time::timeout(SAMPLE_TIMEOUT, PgConnection::connect_with(options))
            .await
            .ok()
            .and_then(Result::ok);
    }
    let Some(db) = conn.as_mut() else {
        metrics::gauge!(DB_UP).set(0.0);
        return;
    };
    let up = matches!(
        tokio::time::timeout(SAMPLE_TIMEOUT, sqlx::query("SELECT 1").execute(&mut *db)).await,
        Ok(Ok(_))
    );
    metrics::gauge!(DB_UP).set(if up { 1.0 } else { 0.0 });
    if !up {
        // Reconnect next time; leave the queue's last sampled depth in place rather than report
        // an empty queue.
        *conn = None;
        return;
    }

    match tokio::time::timeout(SAMPLE_TIMEOUT, dipper_pgmq::queue_depth(&mut *db)).await {
        Ok(Ok(depth)) => {
            metrics::gauge!(QUEUE_JOBS, "state" => "ready").set(depth.ready as f64);
            metrics::gauge!(QUEUE_JOBS, "state" => "scheduled").set(depth.scheduled as f64);
            metrics::gauge!(QUEUE_JOBS, "state" => "failed").set(depth.failed as f64);
            metrics::gauge!(QUEUE_OLDEST_READY_AGE).set(depth.oldest_ready_age_secs.unwrap_or(0.0));
        }
        Ok(Err(err)) => tracing::debug!(error = %err, "failed to sample the job queue's depth"),
        Err(_) => {
            tracing::debug!("sampling the job queue's depth timed out");
            *conn = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use metrics_exporter_prometheus::PrometheusRecorder;
    use tracing_subscriber::layer::SubscriberExt;

    use super::*;
    use crate::test_support::metric_value;

    /// A recorder for this test's thread only, with the handle that renders what it holds.
    fn local_recorder() -> (PrometheusRecorder, PrometheusHandle) {
        let recorder = recorder().unwrap();
        let handle = recorder.handle();
        (recorder, handle)
    }

    /// 2 warnings and an info line tagged with events, and 1 warning without a tag.
    fn log_tagged_and_untagged_lines() {
        for _ in 0..2 {
            tracing::warn!(event = "agreement_cancel_stuck", "a stuck cancel");
        }
        tracing::info!(
            event = "nonce_gap_fill_submitted",
            "an info-level event counts too"
        );
        tracing::warn!("a line without a tag is not counted");
    }

    #[test]
    fn tagged_log_lines_are_counted_by_event_and_level() {
        let (recorder, handle) = local_recorder();
        let _local = metrics::set_default_local_recorder(&recorder);
        let subscriber =
            tracing_subscriber::registry().with(EventCounter.with_filter(TaggedEvents));
        tracing::subscriber::with_default(subscriber, log_tagged_and_untagged_lines);

        let rendered = handle.render();
        let events = |event, level| {
            metric_value(&rendered, LOG_EVENTS, &[("event", event), ("level", level)])
        };
        assert_eq!(
            events("agreement_cancel_stuck", "WARN"),
            Some(2.0),
            "{rendered}"
        );
        assert_eq!(
            events("nonce_gap_fill_submitted", "INFO"),
            Some(1.0),
            "{rendered}"
        );
        assert_eq!(
            rendered.matches("dipper_log_events_total{").count(),
            2,
            "untagged lines add no series: {rendered}"
        );
    }

    /// Records the metadata of every event it sees, to get real call sites for a test.
    struct CallSites(std::sync::Arc<std::sync::Mutex<Vec<&'static tracing::Metadata<'static>>>>);

    impl<S: Subscriber> Layer<S> for CallSites {
        fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
            self.0.lock().unwrap().push(event.metadata());
        }
    }

    #[test]
    fn the_counter_leaves_untagged_log_lines_to_the_level_filter() {
        // A layer without a filter asks to see every call site, so debug and trace lines in
        // dipper and its libraries would all pass the level check and be built. With
        // `TaggedEvents`, an untagged debug line under an info-level output is never wanted, so
        // its call site is switched off for good, while a tagged one still reaches the counter.
        // Asked of the dispatcher directly, as `tracing::enabled!` reads a cache that other
        // tests' subscribers share.
        let seen = std::sync::Arc::default();
        tracing::subscriber::with_default(
            tracing_subscriber::registry().with(CallSites(std::sync::Arc::clone(&seen))),
            || {
                tracing::debug!("an untagged debug line");
                tracing::debug!(event = "test_tagged", "a tagged debug line");
            },
        );
        let sites = seen.lock().unwrap().clone();
        assert_eq!(sites.len(), 2, "both call sites were seen");
        let (untagged, tagged) = (sites[0], sites[1]);

        let dispatch = tracing::Dispatch::new(
            tracing_subscriber::registry()
                .with(
                    tracing_subscriber::fmt::layer()
                        .with_filter(tracing_subscriber::filter::LevelFilter::INFO),
                )
                .with(EventCounter.with_filter(TaggedEvents)),
        );
        assert!(
            dispatch.register_callsite(untagged).is_never(),
            "nothing wants the untagged debug line"
        );
        assert!(
            !dispatch.register_callsite(tagged).is_never(),
            "the counter still wants a tagged debug line"
        );
    }

    #[tokio::test]
    async fn the_sampler_reports_an_unreachable_database_as_down() {
        let (recorder, handle) = local_recorder();
        let _local = metrics::set_default_local_recorder(&recorder);
        let port = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap().port()
        };
        let options: sqlx::postgres::PgConnectOptions =
            format!("postgres://dipper@127.0.0.1:{port}/dipper")
                .parse()
                .unwrap();
        let db = sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(200))
            .connect_lazy_with(options);

        let options = db.connect_options().as_ref().clone();
        sample(&db, &options, &mut None).await;

        let rendered = handle.render();
        assert_eq!(metric_value(&rendered, DB_UP, &[]), Some(0.0), "{rendered}");
        assert!(
            !rendered.contains("dipper_queue_jobs{"),
            "no queue depth is reported without a database: {rendered}"
        );
    }

    #[tokio::test]
    async fn it_the_sampler_reports_the_database_and_the_queue() {
        let (recorder, handle) = local_recorder();
        let _local = metrics::set_default_local_recorder(&recorder);
        let temp_db = pgtemp::PgTempDB::new();
        let db = Pool::<Postgres>::connect(&temp_db.connection_uri())
            .await
            .unwrap();
        dipper_pgmq::run_db_migrations(&db).await.unwrap();
        let queue = dipper_pgmq::PgQueue::new(db.clone());
        queue
            .push(serde_json::json!({"type": "Test"}))
            .await
            .unwrap();

        let options = db.connect_options().as_ref().clone();
        let mut conn = None;
        sample(&db, &options, &mut conn).await;
        assert!(
            conn.is_some(),
            "the sampler keeps its own connection for the next sample"
        );

        let rendered = handle.render();
        assert_eq!(metric_value(&rendered, DB_UP, &[]), Some(1.0), "{rendered}");
        let jobs = |state| metric_value(&rendered, QUEUE_JOBS, &[("state", state)]);
        assert_eq!(jobs("ready"), Some(1.0), "{rendered}");
        assert_eq!(jobs("failed"), Some(0.0), "{rendered}");
    }
}
