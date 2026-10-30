//! DIPs info watcher: counts how long each indexer holding an accepted agreement goes without a
//! readable answer from its `/dips/info` endpoint, alerts past 1 limit and denies it past a 2nd, so
//! IISA, which keeps but can't score such an indexer, drops it until it answers readably again.

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::Arc,
    time::Duration,
};

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
            // A stop ends a cycle midway, dropping its requests before anything is recorded, so
            // shutdown doesn't wait on slow indexers.
            tokio::select! {
                _ = rx_stop.recv() => break,
                () = run_cycle(&registry, &network, &http_client, &mut cycles, &config) => {},
            }
        }

        tracing::debug!("DIPs info watcher stopped");
        Ok(())
    };

    (Handle { tx_stop }, service)
}

/// Run 1 cycle: ask every agreement holder and every indexer dipper denied for its DIPs info,
/// record how long each without it has gone, alert on and deny those past the limits, and lift
/// the denials of those that answered. A failed registry call ends the cycle early.
async fn run_cycle<R: DipsInfoRegistry + Sync>(
    registry: &R,
    network: &NetworkProviderService,
    http_client: &reqwest::Client,
    cycles: &mut Cycles,
    config: &DipsInfoWatcherConfig,
) {
    let now = Instant::now();
    let elapsed = cycles.elapsed(now, config.interval);
    let Some(holders) = db(
        "query indexers with accepted agreements",
        registry.get_indexers_with_accepted_agreements(),
    )
    .await
    else {
        return;
    };
    let Some(denied) = db("query DIPs info denials", registry.get_dips_info_denials()).await else {
        return;
    };
    let holders: HashSet<IndexerId> = holders.into_iter().collect();
    let denied: HashSet<IndexerId> = denied.into_iter().collect();

    // Denied indexers are asked too, so their denial is lifted once they answer, but only
    // agreement holders count toward the share that pauses counting.
    let checked: Vec<IndexerId> = holders.union(&denied).copied().collect();
    let missing = missing_dips_info(
        network,
        http_client,
        &checked,
        config.max_concurrent_requests,
    )
    .await;
    let missing_holders = missing.iter().filter(|id| holders.contains(id)).count();
    let added = cycles.judge(holders.len(), missing_holders, elapsed, now, config);
    let Some(checks) = db(
        "record DIPs info checks",
        registry.record_dips_info_checks(&missing, added),
    )
    .await
    else {
        return;
    };
    cycles.recorded(now);
    report(&checks, added, config.alert_after());

    let (to_deny, to_lift) =
        plan_denials(&checks, &denied, cycles.paused(), config.denylist_after());
    if !to_deny.is_empty()
        && let Some(mut added) = db(
            "deny indexers missing DIPs info",
            registry.deny_for_missing_dips_info(&to_deny),
        )
        .await
        && !added.is_empty()
    {
        added.sort();
        let indexer_ids: Vec<String> = added.iter().map(ToString::to_string).collect();
        tracing::warn!(
            event = "dips_info_missing_denylisted",
            indexer_ids = indexer_ids.join(", "),
            denylist_after_hours = config.denylist_after_hours,
            "Indexers gave no readable /dips/info answer for denylist_after_hours, so dipper \
             denied them: IISA drops them and their agreements are replaced"
        );
    }
    if !to_lift.is_empty()
        && let Some(lifted) = db(
            "lift DIPs info denials",
            registry.lift_dips_info_denials(&to_lift),
        )
        .await
    {
        for indexer_id in lifted {
            tracing::info!(
                indexer_id = %indexer_id,
                "Indexer answers /dips/info readably again, so dipper lifted its denial"
            );
        }
    }
}

/// Which indexers to deny and which of dipper's denials to lift after a recorded cycle: deny each
/// missing its DIPs info for `limit` or longer that isn't denied yet, unless counting is paused,
/// and lift each denial whose indexer answered readably, as it is no longer missing.
fn plan_denials(
    checks: &DipsInfoChecks,
    denied: &HashSet<IndexerId>,
    paused: bool,
    limit: Duration,
) -> (Vec<IndexerId>, Vec<IndexerId>) {
    let to_deny = if paused {
        Vec::new()
    } else {
        checks
            .missing
            .iter()
            .filter(|(id, missing)| **missing >= limit && !denied.contains(id))
            .map(|(id, _)| *id)
            .collect()
    };
    let to_lift = denied
        .iter()
        .filter(|id| !checks.missing.contains_key(id))
        .copied()
        .collect();
    (to_deny, to_lift)
}

/// Run 1 registry call with a timeout, logging and returning nothing when it fails, so the cycle
/// can end early; the next one tries again.
async fn db<T>(what: &str, call: impl Future<Output = crate::registry::Result<T>>) -> Option<T> {
    const DB_TIMEOUT: Duration = Duration::from_secs(30);
    match tokio::time::timeout(DB_TIMEOUT, call).await {
        Ok(Ok(value)) => Some(value),
        Ok(Err(err)) => {
            tracing::error!(error = %err, "failed to {what}");
            None
        }
        Err(_) => {
            tracing::error!("timeout trying to {what}");
            None
        }
    }
}

/// How often the pause alert repeats while counting stays paused.
const PAUSE_ALERT_REPEAT: Duration = Duration::from_secs(6 * 3600);

/// What the watcher remembers between cycles: when the last recorded one started, and, while too
/// many indexers giving no readable answer pause counting, since when and when last alerted on.
#[derive(Default)]
struct Cycles {
    last_recorded: Option<Instant>,
    paused_since: Option<Instant>,
    pause_alerted_at: Option<Instant>,
    /// Cycles in a row, while paused, with too few indexers missing to pause it.
    quiet_cycles: u32,
}

impl Cycles {
    /// The time since the last recorded cycle, at most 2 intervals so a stall doesn't count as
    /// missing time. The first cycle after startup counts nothing.
    fn elapsed(&self, now: Instant, interval: Duration) -> Duration {
        self.last_recorded
            .map_or(Duration::ZERO, |at| now.saturating_duration_since(at))
            .min(interval.saturating_mul(2))
    }

    /// Note that the cycle started at `now` was recorded, so a cycle that fails before then
    /// leaves its time to the next one.
    fn recorded(&mut self, now: Instant) {
        self.last_recorded = Some(now);
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

/// Ask each of `indexers` for its DIPs info, at its current URL in the network registry, at most
/// `max_concurrent` at a time, returning those that gave no readable answer.
async fn missing_dips_info(
    network: &NetworkProviderService,
    http_client: &reqwest::Client,
    indexers: &[IndexerId],
    max_concurrent: usize,
) -> Vec<IndexerId> {
    let asks = indexers.iter().map(|indexer_id| {
        let url = network
            .get_indexer_by_id(indexer_id)
            .map(|indexer| indexer.url);
        let http_client = http_client.clone();
        let ask = async move {
            match url {
                Some(url) => ask_dips_info(&http_client, &url).await,
                None => Err("not in the network registry".to_string()),
            }
        };
        (*indexer_id, ask)
    });
    missing_answers(asks, max_concurrent).await
}

/// Run each indexer's `ask`, at most `max_concurrent` at a time, returning the indexers whose ask
/// failed. One that crashed counts as failed, so its indexer keeps its count rather than being
/// forgotten as if it had answered.
async fn missing_answers<A>(
    asks: impl IntoIterator<Item = (IndexerId, A)>,
    max_concurrent: usize,
) -> Vec<IndexerId>
where
    A: Future<Output = Result<(), String>> + Send + 'static,
{
    let permits = Arc::new(Semaphore::new(max_concurrent.max(1)));
    let mut tasks = JoinSet::new();
    let mut asked_by_task = HashMap::new();
    for (indexer_id, ask) in asks {
        let permits = permits.clone();
        let task = tasks.spawn(async move {
            let _permit = permits.acquire_owned().await;
            (indexer_id, ask.await)
        });
        asked_by_task.insert(task.id(), indexer_id);
    }

    let mut missing = Vec::new();
    while let Some(asked) = tasks.join_next().await {
        match asked {
            Ok((_, Ok(()))) => {}
            Ok((indexer_id, Err(why))) => {
                tracing::debug!(indexer_id = %indexer_id, why, "no readable DIPs info");
                missing.push(indexer_id);
            }
            Err(err) => {
                let indexer_id = asked_by_task.get(&err.id()).copied();
                tracing::error!(error = %err, indexer_id = ?indexer_id, "DIPs info check failed");
                missing.extend(indexer_id);
            }
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
    fn a_cycle_counts_the_time_since_the_last_recorded_up_to_2_intervals() {
        let interval = Duration::from_secs(300);
        let start = Instant::now();
        let mut cycles = Cycles::default();

        assert_eq!(
            cycles.elapsed(start, interval),
            Duration::ZERO,
            "first cycle"
        );
        cycles.recorded(start);
        let failed = start + Duration::from_secs(300);
        assert_eq!(cycles.elapsed(failed, interval), Duration::from_secs(300));
        let next = start + Duration::from_secs(590);
        assert_eq!(
            cycles.elapsed(next, interval),
            Duration::from_secs(590),
            "a cycle that failed to record leaves its time to the next"
        );
        cycles.recorded(next);
        assert_eq!(
            cycles.elapsed(next + Duration::from_secs(5000), interval),
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
    async fn a_check_that_crashes_counts_its_indexer_as_missing() {
        let answers = indexer_id!("1111111111111111111111111111111111111111");
        let crashes = indexer_id!("2222222222222222222222222222222222222222");
        let asks = [answers, crashes].map(|indexer_id| {
            (indexer_id, async move {
                assert_ne!(indexer_id, crashes, "the check crashes");
                Ok(())
            })
        });

        let missing = missing_answers(asks, 2).await;

        assert_eq!(missing, vec![crashes]);
    }

    /// A registry holding a fixed set of agreement holders, recording each cycle it is given.
    struct StubRegistry {
        holders: Vec<IndexerId>,
        recorded: Arc<std::sync::Mutex<Vec<Vec<IndexerId>>>>,
    }

    #[async_trait::async_trait]
    impl DipsInfoRegistry for StubRegistry {
        async fn get_indexers_with_accepted_agreements(
            &self,
        ) -> crate::registry::Result<Vec<IndexerId>> {
            Ok(self.holders.clone())
        }

        async fn record_dips_info_checks(
            &self,
            missing: &[IndexerId],
            _added: Duration,
        ) -> crate::registry::Result<DipsInfoChecks> {
            self.recorded.lock().unwrap().push(missing.to_vec());
            Ok(DipsInfoChecks::default())
        }

        async fn get_dips_info_denials(&self) -> crate::registry::Result<Vec<IndexerId>> {
            Ok(Vec::new())
        }

        async fn deny_for_missing_dips_info(
            &self,
            indexer_ids: &[IndexerId],
        ) -> crate::registry::Result<Vec<IndexerId>> {
            Ok(indexer_ids.to_vec())
        }

        async fn lift_dips_info_denials(
            &self,
            indexer_ids: &[IndexerId],
        ) -> crate::registry::Result<Vec<IndexerId>> {
            Ok(indexer_ids.to_vec())
        }
    }

    #[tokio::test]
    async fn stop_ends_a_cycle_waiting_on_a_slow_indexer() {
        //* Given
        let slow = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/dips/info"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(60)))
            .mount(&slow)
            .await;
        let indexer = indexer_id!("1111111111111111111111111111111111111111");
        let snapshot: indexer_urls::Snapshot =
            BTreeMap::from([(indexer, format!("{}/", slow.uri()).parse().unwrap())]);
        let recorded = Arc::new(std::sync::Mutex::new(Vec::new()));
        let registry = StubRegistry {
            holders: vec![indexer],
            recorded: recorded.clone(),
        };
        let (handle, service) = new(Ctx {
            registry,
            network: NetworkProviderService::new(indexer_urls::Handle::for_test(snapshot)),
            config: DipsInfoWatcherConfig {
                enabled: true,
                request_timeout: Duration::from_secs(60),
                ..DipsInfoWatcherConfig::default()
            },
        });
        let task = tokio::spawn(service);
        while slow
            .received_requests()
            .await
            .unwrap_or_default()
            .is_empty()
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        //* When
        let stopped = tokio::time::timeout(Duration::from_secs(5), handle.stop()).await;

        //* Then
        assert!(stopped.is_ok(), "stop waited on the slow indexer");
        assert!(task.await.unwrap().is_ok());
        assert!(
            recorded.lock().unwrap().is_empty(),
            "the interrupted cycle recorded nothing"
        );
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

    #[test]
    fn plan_denials_denies_at_the_limit_and_lifts_once_an_indexer_answers() {
        let at_limit = indexer_id!("1111111111111111111111111111111111111111");
        let short = indexer_id!("2222222222222222222222222222222222222222");
        let still_denied = indexer_id!("3333333333333333333333333333333333333333");
        let answered = indexer_id!("4444444444444444444444444444444444444444");
        let checks = DipsInfoChecks {
            missing: HashMap::from([
                (at_limit, 48 * HOUR),
                (short, 47 * HOUR),
                (still_denied, 60 * HOUR),
            ]),
            cleared: HashMap::from([(answered, 50 * HOUR)]),
        };
        let denied = HashSet::from([still_denied, answered]);

        let (to_deny, to_lift) = plan_denials(&checks, &denied, false, 48 * HOUR);

        assert_eq!(
            to_deny,
            vec![at_limit],
            "already denied indexers aren't denied again"
        );
        assert_eq!(to_lift, vec![answered]);
    }

    #[test]
    fn plan_denials_denies_nobody_while_counting_is_paused() {
        let at_limit = indexer_id!("1111111111111111111111111111111111111111");
        let answered = indexer_id!("4444444444444444444444444444444444444444");
        let checks = DipsInfoChecks {
            missing: HashMap::from([(at_limit, 72 * HOUR)]),
            cleared: HashMap::new(),
        };

        let (to_deny, to_lift) = plan_denials(&checks, &HashSet::from([answered]), true, 48 * HOUR);

        assert!(to_deny.is_empty());
        assert_eq!(
            to_lift,
            vec![answered],
            "a readable answer still lifts a denial"
        );
    }
}
