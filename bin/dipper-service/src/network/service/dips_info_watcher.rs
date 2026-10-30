//! DIPs info watcher: counts how long each indexer holding an accepted agreement goes without a
//! readable answer from its `/dips/info` endpoint, and alerts once that passes a limit. IISA keeps
//! such an indexer in its group but can't score it, so nothing else would ever replace it.

use std::{future::Future, sync::Arc, time::Duration};

use thegraph_core::IndexerId;
use tokio::{
    sync::{Semaphore, mpsc},
    task::JoinSet,
    time::{Instant, MissedTickBehavior},
};

use super::liveness_checker::{UNREACHABLE_CLEAR_CYCLES, unreachable_trips};
use crate::{
    config::DipsInfoWatcherConfig,
    network::provider::NetworkProviderService,
    registry::{DipsInfoChecks, DipsInfoRegistry},
};

/// Handle for controlling the DIPs info watcher's lifecycle.
#[derive(Clone)]
pub struct Handle {
    tx_stop: mpsc::Sender<()>,
}

impl Handle {
    /// Stop the DIPs info watcher gracefully.
    pub async fn stop(&self) {
        if self.tx_stop.is_closed() {
            return;
        }
        let _ = self.tx_stop.send(()).await;
        self.tx_stop.closed().await;
    }
}

/// Context required by the DIPs info watcher.
pub struct Ctx<R> {
    /// Registry for the indexers to check and their missing time.
    pub registry: R,
    /// Network provider for looking up fresh indexer URLs.
    pub network: NetworkProviderService,
    /// Service configuration.
    pub config: DipsInfoWatcherConfig,
}

/// Create a new DIPs info watcher. Returns a handle for stopping it and a future that must be
/// spawned on a runtime.
pub fn new<R>(ctx: Ctx<R>) -> (Handle, impl Future<Output = anyhow::Result<()>>)
where
    R: DipsInfoRegistry + Send + Sync,
{
    let (tx_stop, mut rx_stop) = mpsc::channel(1);
    let Ctx {
        registry,
        network,
        config,
    } = ctx;

    let service = async move {
        tracing::info!(
            interval_secs = config.interval.as_secs(),
            alert_after_hours = config.alert_after_hours,
            "DIPs info watcher started"
        );

        const DB_TIMEOUT: Duration = Duration::from_secs(30);

        let mut timer = tokio::time::interval(config.interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let http_client = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .build()
            .map_err(|e| anyhow::anyhow!("failed to build HTTP client: {e}"))?;
        let mut cycles = Cycles::default();

        loop {
            tokio::select! {
                _ = rx_stop.recv() => break,
                _ = timer.tick() => {},
            }
            let now = Instant::now();
            let elapsed = cycles.start(now, config.interval);

            let indexers = match tokio::time::timeout(
                DB_TIMEOUT,
                registry.get_indexers_with_accepted_agreements(),
            )
            .await
            {
                Ok(Ok(indexers)) => indexers,
                Ok(Err(err)) => {
                    tracing::error!(error = %err, "failed to query indexers with accepted agreements");
                    continue;
                }
                Err(_) => {
                    tracing::error!("timeout querying indexers with accepted agreements");
                    continue;
                }
            };

            let missing = missing_dips_info(
                &network,
                &http_client,
                &indexers,
                config.max_concurrent_requests,
            )
            .await;
            let added = cycles.judge(indexers.len(), missing.len(), elapsed, now, &config);

            let checks = match tokio::time::timeout(
                DB_TIMEOUT,
                registry.record_dips_info_checks(&missing, added),
            )
            .await
            {
                Ok(Ok(checks)) => checks,
                Ok(Err(err)) => {
                    tracing::error!(error = %err, "failed to record DIPs info checks");
                    continue;
                }
                Err(_) => {
                    tracing::error!("timeout recording DIPs info checks");
                    continue;
                }
            };
            report(&checks, added, config.alert_after());
        }

        tracing::debug!("DIPs info watcher stopped");
        Ok(())
    };

    (Handle { tx_stop }, service)
}

/// How often the pause alert repeats while counting stays paused.
const PAUSE_ALERT_REPEAT: Duration = Duration::from_secs(6 * 3600);

/// What the watcher remembers between cycles: when the last one started, and since when, and when
/// last alerted on, counting has been paused because too many indexers gave no readable answer.
#[derive(Default)]
struct Cycles {
    last_started: Option<Instant>,
    paused_since: Option<Instant>,
    pause_alerted_at: Option<Instant>,
    /// Cycles in a row, while paused, with too few indexers missing to pause it.
    quiet_cycles: u32,
}

impl Cycles {
    /// Note a cycle starting, returning the time since the last one, at most 2 intervals so a
    /// stall doesn't count as missing time. The first cycle after startup counts nothing.
    fn start(&mut self, now: Instant, interval: Duration) -> Duration {
        let elapsed = self
            .last_started
            .map_or(Duration::ZERO, |at| now.saturating_duration_since(at));
        self.last_started = Some(now);
        elapsed.min(interval.saturating_mul(2))
    }

    /// The missing time this cycle adds: none while so many of the indexers checked give no
    /// readable answer that the fault is likely dipper's, or a release that changed the endpoint.
    /// The pause has no time limit, so its alert repeats every `PAUSE_ALERT_REPEAT` while it lasts.
    fn judge(
        &mut self,
        checked: usize,
        missing: usize,
        elapsed: Duration,
        now: Instant,
        config: &DipsInfoWatcherConfig,
    ) -> Duration {
        let trips = unreachable_trips(
            checked,
            missing,
            config.unreachable_trip_fraction,
            config.unreachable_trip_min_indexers,
        );
        // Clearing takes several calm cycles in a row, so a share hovering around the trip
        // fraction doesn't pause and resume counting every cycle.
        if trips {
            self.quiet_cycles = 0;
        } else if self.paused_since.is_some() {
            self.quiet_cycles += 1;
            if self.quiet_cycles >= UNREACHABLE_CLEAR_CYCLES {
                self.paused_since = None;
                self.quiet_cycles = 0;
                tracing::info!(missing, checked, "Counting missing DIPs info again");
            }
        }
        if trips {
            self.paused_since.get_or_insert(now);
        }
        let Some(since) = self.paused_since else {
            return elapsed;
        };
        if self
            .pause_alerted_at
            .is_none_or(|at| at < since || now.saturating_duration_since(at) >= PAUSE_ALERT_REPEAT)
        {
            self.pause_alerted_at = Some(now);
            tracing::warn!(
                event = "dips_info_checks_paused",
                missing,
                checked,
                paused_hours = now.saturating_duration_since(since).as_secs() / 3600,
                "Too many indexers without readable DIPs info at once: a fault on dipper's side \
                 or an indexer-service release that changed /dips/info; not counting missing \
                 DIPs info while it lasts"
            );
        }
        Duration::ZERO
    }

    /// Whether counting is paused.
    fn paused(&self) -> bool {
        self.paused_since.is_some()
    }
}

/// Log what a recorded cycle means: 1 alert naming every indexer whose missing time passed
/// `alert_after` in it, since Slack posts 1 message per event in each throttle window and only
/// counts the rest, and a note for each one that had been alerted on and has answered again.
fn report(checks: &DipsInfoChecks, added: Duration, alert_after: Duration) {
    let mut reached: Vec<String> = checks
        .missing
        .iter()
        .filter(|(_, missing)| crossed(**missing, added, alert_after))
        .map(|(indexer_id, _)| indexer_id.to_string())
        .collect();
    if !reached.is_empty() {
        reached.sort();
        tracing::warn!(
            event = "dips_info_missing",
            indexer_ids = reached.join(", "),
            alert_after_hours = alert_after.as_secs() / 3600,
            "Indexers holding an accepted agreement have given no readable /dips/info answer for \
             alert_after_hours; IISA keeps them in their groups but can't score them"
        );
    }
    for (indexer_id, missing) in &checks.cleared {
        if *missing >= alert_after {
            tracing::info!(
                indexer_id = %indexer_id,
                missing_hours = missing.as_secs() / 3600,
                "Indexer answers /dips/info readably again, or holds no accepted agreement"
            );
        }
    }
}

/// Whether adding `added` took a total of `total` to `limit` or past it.
fn crossed(total: Duration, added: Duration, limit: Duration) -> bool {
    !added.is_zero() && total >= limit && total.saturating_sub(added) < limit
}

/// Ask each of `indexers` for its DIPs info, at most `max_concurrent` at a time, returning those
/// that gave no readable answer.
async fn missing_dips_info(
    network: &NetworkProviderService,
    http_client: &reqwest::Client,
    indexers: &[IndexerId],
    max_concurrent: usize,
) -> Vec<IndexerId> {
    let permits = Arc::new(Semaphore::new(max_concurrent.max(1)));
    let mut asks = JoinSet::new();
    for indexer_id in indexers {
        let indexer_id = *indexer_id;
        let url = network
            .get_indexer_by_id(&indexer_id)
            .map(|indexer| indexer.url);
        let http_client = http_client.clone();
        let permits = permits.clone();
        asks.spawn(async move {
            let _permit = permits.acquire_owned().await;
            let answer = match url {
                Some(url) => ask_dips_info(&http_client, &url).await,
                None => Err("not in the network registry".to_string()),
            };
            (indexer_id, answer)
        });
    }

    let mut missing = Vec::new();
    while let Some(asked) = asks.join_next().await {
        match asked {
            Ok((_, Ok(()))) => {}
            Ok((indexer_id, Err(why))) => {
                tracing::debug!(indexer_id = %indexer_id, why, "no readable DIPs info");
                missing.push(indexer_id);
            }
            Err(err) => tracing::error!(error = %err, "DIPs info check task failed"),
        }
    }
    missing
}

/// Ask 1 indexer for its DIPs info, returning why its answer isn't readable, if it isn't.
async fn ask_dips_info(
    http_client: &reqwest::Client,
    indexer_url: &url::Url,
) -> Result<(), String> {
    let url = indexer_url
        .join("dips/info")
        .map_err(|e| format!("can't build the /dips/info URL: {e}"))?;
    let response = http_client
        .get(url)
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|e| e.to_string())?;
    let body = response.bytes().await.map_err(|e| e.to_string())?;
    readable_dips_info(&body).map_err(str::to_string)
}

/// Check a `/dips/info` body the way IISA's score job does: a JSON object, with any per-entity
/// price, top-level or under `pricing`, a finite number of at least 0. IISA counts anything else
/// as no DIPs info, so an invalid per-entity price hides an indexer from scoring as well.
fn readable_dips_info(body: &[u8]) -> Result<(), &'static str> {
    let value: serde_json::Value =
        serde_json::from_slice(body).map_err(|_| "the body is not JSON")?;
    let object = value.as_object().ok_or("the body is not a JSON object")?;
    let pricing = object
        .get("pricing")
        .and_then(serde_json::Value::as_object)
        .unwrap_or(object);
    match pricing.get("min_grt_per_billion_entities_per_30_days") {
        None | Some(serde_json::Value::Null) => Ok(()),
        Some(price) if valid_price(price) => Ok(()),
        Some(_) => Err("the per-entity price is not a finite number of at least 0"),
    }
}

/// Whether an advertised price is a finite number of at least 0, given as a JSON number or as
/// text. Text is read as loosely as IISA reads it, ignoring surrounding spaces and underscores.
fn valid_price(price: &serde_json::Value) -> bool {
    let parsed = match price {
        serde_json::Value::Number(number) => number.as_f64(),
        serde_json::Value::String(text) => text.trim().replace('_', "").parse::<f64>().ok(),
        _ => None,
    };
    parsed.is_some_and(|price| price.is_finite() && price >= 0.0)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use thegraph_core::indexer_id;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::*;
    use crate::network::service::indexer_urls;

    const HOUR: Duration = Duration::from_secs(3600);

    #[test]
    fn readable_dips_info_takes_an_object_with_a_valid_or_no_per_entity_price() {
        for body in [
            r#"{}"#,
            r#"{"min_grt_per_30_days": {"arbitrum-one": "450"}}"#,
            r#"{"min_grt_per_billion_entities_per_30_days": 2000}"#,
            r#"{"min_grt_per_billion_entities_per_30_days": " 1_000.5 "}"#,
            r#"{"min_grt_per_billion_entities_per_30_days": null}"#,
            r#"{"pricing": {"min_grt_per_billion_entities_per_30_days": "0"}}"#,
        ] {
            assert_eq!(readable_dips_info(body.as_bytes()), Ok(()), "{body}");
        }
    }

    #[test]
    fn readable_dips_info_refuses_what_iisa_counts_as_no_dips_info() {
        for body in [
            "",
            "not json",
            "[]",
            "null",
            r#""text""#,
            r#"{"min_grt_per_billion_entities_per_30_days": -1}"#,
            r#"{"min_grt_per_billion_entities_per_30_days": "NaN"}"#,
            r#"{"min_grt_per_billion_entities_per_30_days": "inf"}"#,
            r#"{"min_grt_per_billion_entities_per_30_days": "cheap"}"#,
            r#"{"min_grt_per_billion_entities_per_30_days": true}"#,
            r#"{"min_grt_per_billion_entities_per_30_days": [1]}"#,
            r#"{"pricing": {"min_grt_per_billion_entities_per_30_days": -5}}"#,
        ] {
            assert!(readable_dips_info(body.as_bytes()).is_err(), "{body}");
        }
    }

    #[test]
    fn readable_dips_info_reads_the_top_level_price_when_pricing_is_not_an_object() {
        let body = r#"{"pricing": 1, "min_grt_per_billion_entities_per_30_days": -1}"#;
        assert!(readable_dips_info(body.as_bytes()).is_err());
    }

    #[test]
    fn crossed_fires_only_on_the_cycle_that_reaches_the_limit() {
        let limit = 24 * HOUR;
        assert!(!crossed(23 * HOUR, HOUR, limit));
        assert!(crossed(24 * HOUR, HOUR, limit));
        assert!(
            !crossed(25 * HOUR, HOUR, limit),
            "already past it last cycle"
        );
        assert!(
            !crossed(24 * HOUR, Duration::ZERO, limit),
            "a cycle that added nothing reached nothing"
        );
    }

    #[test]
    fn a_cycle_counts_the_time_since_the_last_up_to_2_intervals() {
        let interval = Duration::from_secs(300);
        let start = Instant::now();
        let mut cycles = Cycles::default();

        assert_eq!(cycles.start(start, interval), Duration::ZERO, "first cycle");
        assert_eq!(
            cycles.start(start + Duration::from_secs(310), interval),
            Duration::from_secs(310)
        );
        assert_eq!(
            cycles.start(start + Duration::from_secs(5000), interval),
            Duration::from_secs(600),
            "a stall counts as 2 intervals at most"
        );
    }

    #[test]
    fn a_cycle_with_too_many_indexers_missing_adds_no_time() {
        let config = DipsInfoWatcherConfig::default();
        let mut cycles = Cycles::default();
        let elapsed = Duration::from_secs(300);
        let now = Instant::now();

        assert_eq!(cycles.judge(4, 1, elapsed, now, &config), elapsed);
        assert_eq!(
            cycles.judge(4, 2, elapsed, now, &config),
            Duration::ZERO,
            "half missing"
        );
        assert!(cycles.paused());
        for _ in 0..2 {
            assert_eq!(
                cycles.judge(4, 1, elapsed, now, &config),
                Duration::ZERO,
                "still paused until 3 calm cycles in a row"
            );
        }
        assert_eq!(cycles.judge(4, 2, elapsed, now, &config), Duration::ZERO);
        for _ in 0..2 {
            cycles.judge(4, 1, elapsed, now, &config);
        }
        assert!(
            cycles.paused(),
            "a missing share back up starts the calm cycles again"
        );
        assert_eq!(
            cycles.judge(4, 1, elapsed, now, &config),
            elapsed,
            "counting resumes on the 3rd calm cycle"
        );
        assert!(!cycles.paused());
        assert_eq!(
            cycles.judge(2, 2, elapsed, now, &config),
            elapsed,
            "too few indexers checked for the share to count"
        );
    }

    #[tokio::test]
    async fn the_pause_alert_repeats_while_counting_stays_paused() {
        let config = DipsInfoWatcherConfig::default();
        let mut cycles = Cycles::default();
        let elapsed = Duration::from_secs(300);
        let start = Instant::now();

        let logs = crate::test_support::logged(async {
            for minutes in [0, 5, 10, 6 * 60 + 5] {
                cycles.judge(
                    4,
                    4,
                    elapsed,
                    start + Duration::from_secs(minutes * 60),
                    &config,
                );
            }
        })
        .await;

        assert_eq!(
            logs.matches("dips_info_checks_paused").count(),
            2,
            "once as it pauses, again 6 hours on: {logs}"
        );
    }

    #[tokio::test]
    async fn missing_dips_info_lists_the_indexers_without_a_readable_answer() {
        //* Given
        let readable = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/dips/info"))
            .respond_with(
                ResponseTemplate::new(200).set_body_string(r#"{"supported_networks": []}"#),
            )
            .mount(&readable)
            .await;
        let removed = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/dips/info"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&removed)
            .await;
        let bad_price = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/dips/info"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(r#"{"min_grt_per_billion_entities_per_30_days": "free"}"#),
            )
            .mount(&bad_price)
            .await;

        let answers = indexer_id!("1111111111111111111111111111111111111111");
        let gone = indexer_id!("2222222222222222222222222222222222222222");
        let invalid = indexer_id!("3333333333333333333333333333333333333333");
        let unregistered = indexer_id!("4444444444444444444444444444444444444444");
        let url = |server: &MockServer| format!("{}/", server.uri()).parse().unwrap();
        let snapshot: indexer_urls::Snapshot = BTreeMap::from([
            (answers, url(&readable)),
            (gone, url(&removed)),
            (invalid, url(&bad_price)),
        ]);
        let network = NetworkProviderService::new(indexer_urls::Handle::for_test(snapshot));

        //* When
        let mut missing = missing_dips_info(
            &network,
            &reqwest::Client::new(),
            &[answers, gone, invalid, unregistered],
            2,
        )
        .await;

        //* Then
        missing.sort();
        let mut expected = vec![gone, invalid, unregistered];
        expected.sort();
        assert_eq!(missing, expected);
    }

    #[tokio::test]
    async fn report_alerts_once_naming_every_indexer_that_reached_the_limit() {
        let reached = indexer_id!("1111111111111111111111111111111111111111");
        let also_reached = indexer_id!("3333333333333333333333333333333333333333");
        let short = indexer_id!("2222222222222222222222222222222222222222");
        let checks = DipsInfoChecks {
            missing: HashMap::from([
                (reached, 24 * HOUR),
                (also_reached, 24 * HOUR),
                (short, 23 * HOUR),
            ]),
            cleared: HashMap::new(),
        };

        let logs = crate::test_support::logged(async { report(&checks, HOUR, 24 * HOUR) }).await;

        assert_eq!(logs.matches("dips_info_missing").count(), 1, "{logs}");
        assert!(logs.contains(&reached.to_string()), "{logs}");
        assert!(logs.contains(&also_reached.to_string()), "{logs}");
        assert!(!logs.contains(&short.to_string()), "{logs}");
    }
}
