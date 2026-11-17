//! Keeps dipper's default price ceilings in step with the minimum prices indexer-rs suggests.
//! The table is fetched at start and then on an interval, or sooner after a fetch that failed.
//! One that fails a check, or a fetch that fails, leaves the table in use where it is: nothing
//! here ends dipper or holds it up.

use std::{future::Future, sync::Arc, time::Duration};

use time::format_description::well_known::Rfc3339;
use tokio::{sync::mpsc, time::Instant};
use url::Url;

use crate::price_ceilings::{BasePriceCeilings, CeilingChange, parse_minimums};

/// How long 1 fetch of the table may take.
const FETCH_TIMEOUT: Duration = Duration::from_secs(10);

/// The most a published table may weigh. The real one is under 2 KiB.
const MAX_TABLE_BYTES: usize = 64 * 1024;

/// How long after a fetch that failed the next one is made: twice as long for each failure
/// in a row, and never longer than the interval between fetches.
const FIRST_RETRY_AFTER: Duration = Duration::from_secs(60);

/// How often a failure that goes on is alerted on again, however often the table is fetched.
const ALERT_AGAIN_AFTER: Duration = Duration::from_secs(10 * 60);

/// The `event` tag of the alert for default price ceilings that changed.
const CHANGED_EVENT: &str = "price_ceilings_changed";

/// The `event` tag of the alert for a fetched table that failed a check and was not taken.
const REFUSED_EVENT: &str = "suggested_minimums_refused";

/// The `event` tag of the alert for a table that could not be fetched.
const FETCH_FAILED_EVENT: &str = "suggested_minimums_fetch_failed";

/// Handle for controlling the service.
#[derive(Clone)]
pub struct Handle {
    tx_stop: mpsc::Sender<()>,
}

impl Handle {
    /// Stops the service. A fetch in flight is dropped, so this does not wait for the network.
    pub async fn stop(&self) {
        if self.tx_stop.is_closed() {
            return;
        }
        let _ = self.tx_stop.send(()).await;
        self.tx_stop.closed().await;
    }
}

/// What the service works with.
pub struct Ctx {
    /// The ceilings whose defaults follow the fetched table.
    pub ceilings: Arc<BasePriceCeilings>,
    /// The most any agreement may pay, which no ceiling may exceed.
    pub max_agreement_grt_per_30_days: f64,
    /// Where indexer-rs publishes the table.
    pub url: Url,
    /// How long to wait between fetches.
    pub interval: Duration,
    /// A note for each `pricing_table` price above its ceiling, as the ceilings stand when
    /// it is called.
    pub table_prices_above_ceilings: Box<dyn Fn() -> Vec<String> + Send + Sync>,
}

/// Creates the service: a handle to stop it, and a future to spawn that fetches the table at
/// once and then every `interval`. The future only ends when the service is stopped.
pub fn new(ctx: Ctx) -> (Handle, impl Future<Output = anyhow::Result<()>>) {
    let (tx_stop, rx_stop) = mpsc::channel(1);
    let url = ctx.url.clone();
    let fetch = move || {
        let url = url.clone();
        async move { fetch_table(&url).await }
    };
    (Handle { tx_stop }, run(ctx, rx_stop, fetch))
}

/// Why the published table could not be fetched.
#[derive(Debug, thiserror::Error)]
pub enum FetchFailure {
    #[error("the request failed: {0}")]
    Request(String),
    #[error("the server answered {0}")]
    Status(reqwest::StatusCode),
    #[error("the answer is larger than {MAX_TABLE_BYTES} bytes")]
    TooLarge,
}

/// Fetches the published table from `url`, as bytes still to be read and checked.
async fn fetch_table(url: &Url) -> Result<Vec<u8>, FetchFailure> {
    // The address is left out of the error, since it may hold a secret.
    let failed = |error: reqwest::Error| {
        FetchFailure::Request(format!("{:#}", anyhow::Error::from(error.without_url())))
    };
    // Built for each fetch, hours apart, so that failing to build one is 1 failed fetch.
    let client = reqwest::Client::builder()
        .timeout(FETCH_TIMEOUT)
        .build()
        .map_err(failed)?;
    let mut response = client.get(url.clone()).send().await.map_err(failed)?;
    if !response.status().is_success() {
        return Err(FetchFailure::Status(response.status()));
    }
    let mut table = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(failed)? {
        if table.len() + chunk.len() > MAX_TABLE_BYTES {
            return Err(FetchFailure::TooLarge);
        }
        table.extend_from_slice(&chunk);
    }
    Ok(table)
}

/// Runs the service over any way of fetching the table: [`new`] passes the real one, and
/// tests a script. Returns `Ok` when asked to stop, and never before.
async fn run<F, Fut>(ctx: Ctx, mut rx_stop: mpsc::Receiver<()>, mut fetch: F) -> anyhow::Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Vec<u8>, FetchFailure>>,
{
    // The table is fetched as dipper starts.
    let mut next_fetch = Instant::now();
    let mut retry_after = FIRST_RETRY_AFTER;
    let mut alerted = Alerted::default();
    loop {
        tokio::select! { biased;
            _ = rx_stop.recv() => return Ok(()),
            () = tokio::time::sleep_until(next_fetch) => {}
        }
        let started = Instant::now();
        // A stop wins over a fetch in flight: dropping it loses nothing but 1 read.
        let fetched = tokio::select! { biased;
            _ = rx_stop.recv() => return Ok(()),
            fetched = fetch() => fetched,
        };
        let wait = match fetched {
            Ok(table) => {
                ctx.settle(&table, &mut alerted);
                retry_after = FIRST_RETRY_AFTER;
                ctx.interval
            }
            Err(failure) => {
                let wait = retry_after.min(ctx.interval);
                retry_after = retry_after.saturating_mul(2);
                ctx.note_fetch_failure(&failure, wait, &mut alerted);
                wait
            }
        };
        next_fetch = started + wait;
    }
}

impl Ctx {
    /// Takes the table 1 fetch brought if it passes every check, and otherwise nothing,
    /// which leaves the table in use, and so every ceiling, as it was.
    fn settle(&self, table: &[u8], alerted: &mut Alerted) {
        let above_before = (self.table_prices_above_ceilings)();
        // Only this task takes a table, so none is taken between this read and the take.
        let first_since_start = self.ceilings.fetched_at().is_none();
        let taken = parse_minimums(table).and_then(|minimums| {
            self.ceilings
                .follow(minimums, self.max_agreement_grt_per_30_days)
        });
        match taken {
            Ok(changes) => {
                *alerted = Alerted::default();
                note_changes(&changes, first_since_start);
                // Start-up warned of the table prices above a ceiling then, so this warns
                // only of those a ceiling that moved has left above it.
                let above = (self.table_prices_above_ceilings)();
                for note in above.iter().filter(|note| !above_before.contains(note)) {
                    tracing::warn!("{note}");
                }
            }
            Err(refusal) => {
                // Tagged for an alert only when one is due; logged every time.
                let event = alerted.due_for_refusal().then_some(REFUSED_EVENT);
                tracing::error!(
                    event,
                    url = %without_secrets(&self.url),
                    "Dipper refused the minimum prices it fetched from indexer-rs: {refusal}. \
                     Its default price ceilings stay on {}. If the published prices are \
                     right, set max_grt_per_30_days and fallback_max_grt_per_30_days in the \
                     config, or release a dipper whose built-in prices are close to them",
                    self.table_in_use()
                );
            }
        }
    }

    fn note_fetch_failure(
        &self,
        failure: &FetchFailure,
        retry_in: Duration,
        alerted: &mut Alerted,
    ) {
        let event = alerted
            .due_for_fetch_failure()
            .then_some(FETCH_FAILED_EVENT);
        tracing::warn!(
            event,
            url = %without_secrets(&self.url),
            retry_in_secs = retry_in.as_secs(),
            "Dipper could not fetch the minimum prices indexer-rs suggests: {failure}. Its \
             default price ceilings stay on {}",
            self.table_in_use()
        );
    }

    /// The table the default ceilings are worked out from, for a message about one that was
    /// not taken: a restart puts dipper back on the built-in one until a fetch succeeds.
    fn table_in_use(&self) -> String {
        let Some(fetched_at) = self.ceilings.fetched_at() else {
            return "the minimum prices built into this release, since it has taken no others \
                    since it started"
                .to_string();
        };
        let at = fetched_at.replace_nanosecond(0).unwrap_or(fetched_at);
        let at = at.format(&Rfc3339).unwrap_or_else(|_| at.to_string());
        format!("the minimum prices it fetched at {at}")
    }
}

/// Logs what a table that was taken changed: a line for each ceiling, and 1 alert for all.
/// The first table since dipper started replaces the built-in one, and the alert says so.
fn note_changes(changes: &[CeilingChange], first_since_start: bool) {
    if changes.is_empty() {
        tracing::info!("fetched the minimum prices indexer-rs suggests; no price ceiling moved");
        return;
    }
    for change in changes {
        tracing::info!(
            chain = change.chain.as_deref().unwrap_or("fallback"),
            old_grt_per_30_days = change.old,
            new_grt_per_30_days = change.new,
            "default price ceiling changed to follow indexer-rs"
        );
    }
    let changes = describe(changes);
    let what = if first_since_start {
        format!(
            "Since it started, dipper's default price ceilings follow the minimum prices \
             indexer-rs suggests, which differ from the figures built into this release, in \
             GRT per 30 days: {changes}. This is raised at each start while the 2 differ, and \
             need not mean that indexer-rs changed a price just now"
        )
    } else {
        format!(
            "Dipper's default price ceilings changed to follow the minimum prices indexer-rs \
             suggests, in GRT per 30 days: {changes}"
        )
    };
    tracing::warn!(
        event = CHANGED_EVENT,
        "{what}. An indexer asking more than its chain's ceiling is left out of selection, \
         and loses an agreement it holds at the next reassessment"
    );
}

/// When each kind of failure was last alerted on, so that one that goes on is alerted on
/// again only every [`ALERT_AGAIN_AFTER`]. A table that is taken clears both.
#[derive(Default)]
struct Alerted {
    fetch_failure: Option<Instant>,
    refusal: Option<Instant>,
}

impl Alerted {
    fn due_for_fetch_failure(&mut self) -> bool {
        due(&mut self.fetch_failure)
    }

    fn due_for_refusal(&mut self) -> bool {
        due(&mut self.refusal)
    }
}

/// Whether an alert last raised at `last` is due again, noting now as the last time if so.
fn due(last: &mut Option<Instant>) -> bool {
    let now = Instant::now();
    if last.is_some_and(|last| now.duration_since(last) < ALERT_AGAIN_AFTER) {
        return false;
    }
    *last = Some(now);
    true
}

/// The changes as 1 line: each chain, and last the fallback, with its old and new ceiling.
fn describe(changes: &[CeilingChange]) -> String {
    let changes: Vec<String> = changes
        .iter()
        .map(|change| {
            let chain = change.chain.as_deref().unwrap_or("the fallback");
            format!("{chain} from {} to {}", change.old, change.new)
        })
        .collect();
    changes.join(", ")
}

/// The address without any user name, password or query, which may hold a secret.
fn without_secrets(url: &Url) -> String {
    let host = url.host_str().unwrap_or_default();
    let port = url
        .port()
        .map(|port| format!(":{port}"))
        .unwrap_or_default();
    format!("{}://{host}{port}{}", url.scheme(), url.path())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, VecDeque},
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use thegraph_core::alloy::primitives::{ChainId, U256};
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::*;
    use crate::{
        config::{IndexingAgreementChainPrices, IndexingAgreementConfig},
        price_ceilings::Minimums,
        test_support::logged,
        worker,
    };

    const MINUTE: Duration = Duration::from_secs(60);
    const HOUR: Duration = Duration::from_secs(3_600);

    /// The table in use when each test starts, as if built into the release.
    fn built_in() -> Minimums {
        [("arbitrum-one", 450.0), ("mainnet", 45.0), ("sepolia", 5.0)]
            .into_iter()
            .map(|(chain, minimum)| (chain.to_string(), minimum))
            .collect()
    }

    /// The file indexer-rs publishes, holding `minimums`.
    fn published(minimums: &[(&str, f64)]) -> Vec<u8> {
        let minimums: serde_json::Map<String, serde_json::Value> = minimums
            .iter()
            .map(|(chain, minimum)| ((*chain).to_string(), serde_json::json!(minimum)))
            .collect();
        serde_json::json!({ "about": ["A test table."], "min_grt_per_30_days": minimums })
            .to_string()
            .into_bytes()
    }

    fn ctx(ceilings: &Arc<BasePriceCeilings>, interval: Duration) -> Ctx {
        Ctx {
            ceilings: Arc::clone(ceilings),
            max_agreement_grt_per_30_days: 20_000.0,
            url: "https://prices.example/table.json?token=secret"
                .parse()
                .expect("a URL"),
            interval,
            table_prices_above_ceilings: Box::new(Vec::new),
        }
    }

    fn default_ceilings() -> Arc<BasePriceCeilings> {
        Arc::new(BasePriceCeilings::new(None, None, built_in()))
    }

    type Fetched = Result<Vec<u8>, FetchFailure>;

    /// Runs the service for `lasting`, answering its fetches from `script` in order and
    /// repeating the last answer once the script runs out, then stops it as dipper does.
    /// Returns how many times it fetched and what it logged.
    async fn served(
        ceilings: &Arc<BasePriceCeilings>,
        interval: Duration,
        script: Vec<fn() -> Fetched>,
        lasting: Duration,
    ) -> (usize, String) {
        served_with(ctx(ceilings, interval), script, lasting).await
    }

    /// [`served`], for a service the test built itself.
    async fn served_with(
        ctx: Ctx,
        script: Vec<fn() -> Fetched>,
        lasting: Duration,
    ) -> (usize, String) {
        let fetches = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&fetches);
        let script = Mutex::new(VecDeque::from(script));
        let fetch = move || {
            counted.fetch_add(1, Ordering::SeqCst);
            let mut script = script.lock().expect("poisoned");
            let answer = if script.len() > 1 {
                script.pop_front()
            } else {
                script.front().copied()
            };
            let answer = answer.expect("a script with at least 1 answer");
            async move { answer() }
        };
        let (tx_stop, rx_stop) = mpsc::channel(1);
        let service = run(ctx, rx_stop, fetch);
        let logs = logged(async {
            tokio::pin!(service);
            tokio::select! {
                ended = &mut service => panic!("the service ended by itself: {ended:?}"),
                () = tokio::time::sleep(lasting) => {}
            }
            tx_stop.send(()).await.expect("the service is running");
            let ended = tokio::time::timeout(Duration::from_secs(5), service).await;
            assert!(matches!(ended, Ok(Ok(()))), "{ended:?}");
        })
        .await;
        (fetches.load(Ordering::SeqCst), logs)
    }

    fn alerts(logs: &str, event: &str) -> usize {
        logs.matches(&format!(r#"event="{event}""#)).count()
    }

    fn raised() -> Fetched {
        Ok(published(&[
            ("arbitrum-one", 500.0),
            ("mainnet", 30.0),
            ("sepolia", 5.0),
            ("base", 80.0),
        ]))
    }

    fn not_found() -> Fetched {
        Err(FetchFailure::Status(reqwest::StatusCode::NOT_FOUND))
    }

    fn unchanged() -> Fetched {
        Ok(published(&[
            ("arbitrum-one", 450.0),
            ("mainnet", 45.0),
            ("sepolia", 5.0),
        ]))
    }

    #[tokio::test(start_paused = true)]
    async fn fetches_the_table_at_start_and_then_on_each_interval() {
        let ceilings = default_ceilings();

        let (at_start, _) = served(&ceilings, 6 * HOUR, vec![unchanged], HOUR).await;
        let (in_a_day, logs) = served(&ceilings, 6 * HOUR, vec![unchanged], 25 * HOUR).await;

        assert_eq!(at_start, 1, "the first fetch does not wait for an interval");
        assert_eq!(in_a_day, 5, "at 0, 6, 12, 18 and 24 hours");
        for event in [CHANGED_EVENT, REFUSED_EVENT, FETCH_FAILED_EVENT] {
            assert_eq!(alerts(&logs, event), 0, "{event}: {logs}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_fetched_table_moves_the_default_ceilings_and_is_alerted_on_once() {
        let ceilings = default_ceilings();

        let (_, logs) = served(&ceilings, 6 * HOUR, vec![raised], 25 * HOUR).await;

        assert_eq!(ceilings.for_chain("arbitrum-one"), 2_000.0);
        assert_eq!(ceilings.for_chain("mainnet"), 120.0);
        assert_eq!(ceilings.for_chain("base"), 320.0);
        assert_eq!(ceilings.for_chain("gnosis"), 2_000.0, "the fallback");
        assert_eq!(
            alerts(&logs, CHANGED_EVENT),
            1,
            "the same table fetched 4 more times changes nothing: {logs}"
        );
        for change in [
            "arbitrum-one from 1800 to 2000",
            "base from 1800 to 320",
            "mainnet from 180 to 120",
            "the fallback from 1800 to 2000",
        ] {
            assert!(logs.contains(change), "the alert says {change}: {logs}");
        }
        assert!(!logs.contains("sepolia from"), "unchanged: {logs}");
    }

    /// Start-up warns about each `pricing_table` price above its ceiling as the ceilings
    /// stand then. A ceiling that a fetched table moves under a table price is warned about
    /// as it moves, once, and nothing start-up already said is said again.
    #[tokio::test(start_paused = true)]
    async fn a_table_price_left_above_a_ceiling_that_moved_is_warned_about_once() {
        let ceilings = default_ceilings();
        let conf = IndexingAgreementConfig {
            base_price_ceilings: Arc::clone(&ceilings),
            ..IndexingAgreementConfig::for_tests()
        };
        let price = |grt| IndexingAgreementChainPrices {
            tokens_per_second: worker::grt_per_30_days_to_wei_per_second(grt),
            tokens_per_entity_per_second: U256::ZERO,
        };
        // Against the ceilings at start of 1,800, 180 and 20: above, below and above.
        let table = BTreeMap::from([
            (42161, price(1_900.0)),
            (1, price(150.0)),
            (11155111, price(50.0)),
        ]);
        let chain_name = |chain: ChainId| {
            let name = match chain {
                42161 => "arbitrum-one",
                1 => "mainnet",
                _ => "sepolia",
            };
            Some(name.to_string())
        };
        let ctx = Ctx {
            table_prices_above_ceilings: Box::new(move || {
                worker::table_prices_above_ceilings(&conf, &table, chain_name)
            }),
            ..ctx(&ceilings, 6 * HOUR)
        };

        // `raised` moves the 3 ceilings to 2,000, 120 and 20.
        let (fetches, logs) = served_with(ctx, vec![raised], 25 * HOUR).await;

        let lowered_past = "chain 1 (mainnet) is worth 150 GRT per 30 days, above that chain's \
                            base-price ceiling of 120";
        assert_eq!(fetches, 5);
        assert_eq!(
            logs.matches(lowered_past).count(),
            1,
            "said as the ceiling moves, and not by the 4 fetches after: {logs}"
        );
        assert!(
            !logs.contains("chain 42161"),
            "a raised ceiling is above the table price: {logs}"
        );
        assert!(
            !logs.contains("chain 11155111"),
            "above a ceiling that did not move, which start-up said: {logs}"
        );
    }

    /// Each chain's change is also logged as a line of its own, with the ceiling before and
    /// after as fields, for whoever searches the logs for 1 chain.
    #[tokio::test(start_paused = true)]
    async fn each_changed_ceiling_is_logged_with_its_old_and_new_value() {
        let ceilings = default_ceilings();

        let (_, logs) = served(&ceilings, 6 * HOUR, vec![raised], HOUR).await;

        for line in [
            r#"chain="arbitrum-one" old_grt_per_30_days=1800.0 new_grt_per_30_days=2000.0"#,
            r#"chain="mainnet" old_grt_per_30_days=180.0 new_grt_per_30_days=120.0"#,
            r#"chain="fallback" old_grt_per_30_days=1800.0 new_grt_per_30_days=2000.0"#,
        ] {
            assert!(logs.contains(line), "{line}: {logs}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_table_that_fails_a_check_is_refused_and_the_ceilings_stay() {
        fn mainnet_100_times_up() -> Fetched {
            Ok(published(&[
                ("arbitrum-one", 450.0),
                ("mainnet", 4_500.0),
                ("sepolia", 5.0),
            ]))
        }
        let ceilings = default_ceilings();

        let (_, logs) = served(&ceilings, 6 * HOUR, vec![mainnet_100_times_up], HOUR).await;

        assert_eq!(*ceilings.minimums(), built_in());
        assert_eq!(ceilings.for_chain("mainnet"), 180.0);
        assert_eq!(alerts(&logs, REFUSED_EVENT), 1, "{logs}");
        assert_eq!(alerts(&logs, CHANGED_EVENT), 0, "{logs}");
        assert!(
            logs.contains("it moves the minimum for mainnet from 45 to 4500"),
            "the alert says why: {logs}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_table_that_cannot_be_read_is_refused_and_the_ceilings_stay() {
        fn a_web_page() -> Fetched {
            Ok(b"<html>Sign in to continue</html>".to_vec())
        }
        let ceilings = default_ceilings();

        let (_, logs) = served(&ceilings, 6 * HOUR, vec![a_web_page], HOUR).await;

        assert_eq!(*ceilings.minimums(), built_in());
        assert_eq!(alerts(&logs, REFUSED_EVENT), 1, "{logs}");
        assert!(logs.contains("it cannot be read"), "{logs}");
        assert!(
            logs.contains("stay on the minimum prices built into this release"),
            "it says which table is in use: {logs}"
        );
    }

    /// Until indexer-rs publishes the file the address answers 404. Dipper then starts on
    /// the table built into it, says so, and keeps asking.
    #[tokio::test(start_paused = true)]
    async fn a_fetch_that_fails_at_start_leaves_the_built_in_table_and_is_alerted_on() {
        let ceilings = default_ceilings();

        let (fetches, logs) = served(&ceilings, 6 * HOUR, vec![not_found], MINUTE / 2).await;

        assert_eq!(fetches, 1);
        assert_eq!(*ceilings.minimums(), built_in());
        assert_eq!(alerts(&logs, FETCH_FAILED_EVENT), 1, "{logs}");
        assert!(logs.contains("404 Not Found"), "{logs}");
        assert!(
            logs.contains("stay on the minimum prices built into this release"),
            "it says which table is in use: {logs}"
        );
        assert!(
            logs.contains("https://prices.example/table.json") && !logs.contains("secret"),
            "the address is named without its query: {logs}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_fetch_that_fails_later_keeps_the_last_table_that_was_taken() {
        let ceilings = default_ceilings();

        let lasting = 6 * HOUR + MINUTE / 2;
        let (_, logs) = served(&ceilings, 6 * HOUR, vec![raised, not_found], lasting).await;

        assert_eq!(ceilings.for_chain("arbitrum-one"), 2_000.0);
        assert_eq!(ceilings.for_chain("mainnet"), 120.0);
        assert_eq!(alerts(&logs, CHANGED_EVENT), 1, "{logs}");
        assert_eq!(alerts(&logs, FETCH_FAILED_EVENT), 1, "{logs}");
        assert!(
            logs.contains("stay on the minimum prices it fetched at 20"),
            "it says which table is in use, by when it was fetched: {logs}"
        );
        assert!(
            !logs.contains("stay on the minimum prices built into this release"),
            "{logs}"
        );
    }

    /// A fetch that fails is tried again after 1 minute, then 2, then 4, and not a whole
    /// interval later. A table that is taken puts the fetches an interval apart again, and
    /// the next failure starts again from 1 minute.
    #[tokio::test(start_paused = true)]
    async fn a_failed_fetch_is_tried_again_sooner_until_a_table_is_taken() {
        let ceilings = default_ceilings();
        let script: Vec<fn() -> Fetched> =
            vec![not_found, not_found, not_found, unchanged, not_found];

        // Fetches at 0, 1 and 3 minutes fail, the one at 7 minutes is taken, and those at
        // 6 hours 7 and 6 hours 8 fail.
        let lasting = 6 * HOUR + 9 * MINUTE + MINUTE / 2;
        let (fetches, logs) = served(&ceilings, 6 * HOUR, script, lasting).await;

        assert_eq!(fetches, 6, "{logs}");
        for (wait, times) in [
            ("retry_in_secs=60", 2),
            ("retry_in_secs=120", 2),
            ("retry_in_secs=240", 1),
        ] {
            assert_eq!(logs.matches(wait).count(), times, "{wait}: {logs}");
        }
    }

    /// However long the failures go on, 2 fetches are never further apart than the interval.
    #[tokio::test(start_paused = true)]
    async fn the_wait_after_a_failed_fetch_is_never_longer_than_the_interval() {
        let ceilings = default_ceilings();

        // At 0, 1, 3 and 7 minutes, and from then every 5: at 12, 17, 22 and 27.
        let lasting = 29 * MINUTE + MINUTE / 2;
        let (fetches, _) = served(&ceilings, 5 * MINUTE, vec![not_found], lasting).await;

        assert_eq!(fetches, 8);
    }

    /// The published file will not change by itself, so a table that is refused is not
    /// fetched again before the interval is up.
    #[tokio::test(start_paused = true)]
    async fn a_refused_table_is_fetched_again_only_after_the_interval() {
        fn a_web_page() -> Fetched {
            Ok(b"<html>Sign in to continue</html>".to_vec())
        }
        let ceilings = default_ceilings();

        let lasting = 6 * HOUR + MINUTE / 2;
        let (fetches, _) = served(&ceilings, 6 * HOUR, vec![a_web_page], lasting).await;

        assert_eq!(fetches, 2, "at 0 and at 6 hours");
    }

    /// A dipper that starts is on the figures built into its release until its first fetch.
    /// Once indexer-rs has moved a price, every start therefore moves the ceilings. The
    /// alert for that says it is the start, so it is not read as a change made upstream.
    #[tokio::test(start_paused = true)]
    async fn the_first_change_after_a_start_is_alerted_on_as_a_start_up_message() {
        fn raised_again() -> Fetched {
            Ok(published(&[
                ("arbitrum-one", 600.0),
                ("mainnet", 30.0),
                ("sepolia", 5.0),
                ("base", 80.0),
            ]))
        }
        let ceilings = default_ceilings();

        let (_, logs) = served(&ceilings, 6 * HOUR, vec![raised, raised_again], 7 * HOUR).await;

        assert_eq!(alerts(&logs, CHANGED_EVENT), 2, "{logs}");
        let at_start = "differ from the figures built into this release, in GRT per 30 days: \
                        arbitrum-one from 1800 to 2000";
        assert_eq!(logs.matches(at_start).count(), 1, "the first: {logs}");
        let upstream = "changed to follow the minimum prices indexer-rs suggests, in GRT per \
                        30 days: arbitrum-one from 2000 to 2400";
        assert_eq!(logs.matches(upstream).count(), 1, "the second: {logs}");
    }

    /// An operator may set a short interval. A fault that lasts must not then post an alert
    /// for every fetch: it is raised at once, and again every 10 minutes while it lasts.
    #[tokio::test(start_paused = true)]
    async fn a_failure_that_goes_on_is_alerted_on_every_10_minutes_at_most() {
        let minute = Duration::from_secs(60);
        fn refused() -> Fetched {
            Ok(published(&[("arbitrum-one", 450.0)]))
        }

        // Stopped between 2 fetches, so that the count does not hang on which comes first.
        let hour = 59 * minute + minute / 2;
        let ceilings = default_ceilings();
        let (fetches, failing) = served(&ceilings, minute, vec![not_found], hour).await;
        let ceilings = default_ceilings();
        let (_, refusing) = served(&ceilings, minute, vec![refused], hour).await;

        assert_eq!(fetches, 60, "1 a minute");
        assert_eq!(
            alerts(&failing, FETCH_FAILED_EVENT),
            6,
            "at 0, 10, 20, 30, 40 and 50 minutes"
        );
        assert_eq!(alerts(&refusing, REFUSED_EVENT), 6, "{refusing}");
        assert_eq!(
            failing.matches("could not fetch").count(),
            60,
            "every failure is still logged"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_failure_after_a_table_was_taken_is_alerted_on_at_once() {
        let minute = Duration::from_secs(60);
        let ceilings = default_ceilings();
        let script: Vec<fn() -> Fetched> = vec![not_found, unchanged, not_found];

        let (_, logs) = served(&ceilings, minute, script, 4 * minute + minute / 2).await;

        assert_eq!(
            alerts(&logs, FETCH_FAILED_EVENT),
            2,
            "the failure at 2 minutes is a new one, not the first going on: {logs}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_stop_does_not_wait_for_a_fetch_that_never_answers() {
        let ceilings = default_ceilings();
        let (tx_stop, rx_stop) = mpsc::channel(1);
        let handle = Handle { tx_stop };
        let service = tokio::spawn(run(ctx(&ceilings, 6 * HOUR), rx_stop, || {
            std::future::pending::<Fetched>()
        }));
        tokio::time::sleep(Duration::from_secs(1)).await;

        let stopped = tokio::time::timeout(Duration::from_secs(5), handle.stop()).await;

        assert!(stopped.is_ok(), "the stop waited for the fetch");
        let ended = service.await.expect("the service did not panic");
        assert!(ended.is_ok(), "{ended:?}");
    }

    #[test]
    fn the_alert_tags_are_alerted_on_by_default() {
        let defaults = crate::config::AlertsConfig::default().events;
        for event in [CHANGED_EVENT, REFUSED_EVENT, FETCH_FAILED_EVENT] {
            assert!(defaults.iter().any(|tag| tag == event), "{event}");
        }
    }

    // -------- the fetch itself, against a local server --------

    async fn serving(answer: ResponseTemplate) -> (MockServer, Url) {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/table.json"))
            .respond_with(answer)
            .mount(&server)
            .await;
        let url = format!("{}/table.json", server.uri())
            .parse()
            .expect("a URL");
        (server, url)
    }

    #[tokio::test]
    async fn the_fetch_returns_the_published_bytes() {
        let body = published(&[("mainnet", 45.0)]);
        let (_server, url) = serving(ResponseTemplate::new(200).set_body_bytes(body.clone())).await;

        let fetched = fetch_table(&url).await;

        assert_eq!(fetched.ok(), Some(body));
    }

    #[tokio::test]
    async fn the_fetch_fails_on_an_answer_that_is_not_a_success() {
        let (_server, url) = serving(ResponseTemplate::new(404).set_body_string("404")).await;

        let fetched = fetch_table(&url).await;

        assert!(
            matches!(&fetched, Err(FetchFailure::Status(status)) if status.as_u16() == 404),
            "{fetched:?}"
        );
    }

    #[tokio::test]
    async fn the_fetch_fails_on_an_answer_too_large_to_be_the_table() {
        let huge = vec![b' '; MAX_TABLE_BYTES + 1];
        let (_server, url) = serving(ResponseTemplate::new(200).set_body_bytes(huge)).await;

        let fetched = fetch_table(&url).await;

        assert!(
            matches!(fetched, Err(FetchFailure::TooLarge)),
            "{fetched:?}"
        );
    }

    #[tokio::test]
    async fn the_fetch_fails_when_nothing_answers() {
        // A port that was free a moment ago: nothing listens on it once the listener is gone.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a free port");
        let port = listener.local_addr().expect("its address").port();
        drop(listener);
        let url: Url = format!("http://127.0.0.1:{port}/table.json")
            .parse()
            .expect("a URL");

        let fetched = fetch_table(&url).await;

        assert!(
            matches!(fetched, Err(FetchFailure::Request(_))),
            "{fetched:?}"
        );
    }

    #[test]
    fn an_address_is_shown_without_what_may_be_secret() {
        let url: Url = "https://user:password@prices.example:8443/a/table.json?token=abc"
            .parse()
            .expect("a URL");

        assert_eq!(
            without_secrets(&url),
            "https://prices.example:8443/a/table.json"
        );
    }
}
