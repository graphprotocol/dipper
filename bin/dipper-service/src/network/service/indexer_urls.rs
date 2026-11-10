//! Periodic background service that polls the indexing-payments subgraph's
//! `Indexer` entities (URLs from SubgraphService registration) and exposes
//! an indexer ID to URL lookup for proposal sending and liveness checks.

use std::{collections::BTreeMap, future::Future, time::Duration};

use thegraph_core::IndexerId;
use tokio::{
    sync::{mpsc, watch},
    time::MissedTickBehavior,
};
use url::Url;

/// Timeout for subgraph queries.
const QUERY_TIMEOUT: Duration = Duration::from_secs(30);

/// A snapshot of the registered indexers' URLs, keyed by indexer ID.
pub type Snapshot = BTreeMap<IndexerId, Url>;

/// The longest registered URL dipper will use, counted as it would be sent. A real one is a few
/// dozen characters. Dipper logs an indexer's URL whenever a call to it fails, so without a limit
/// an indexer could register one of any length and have it logged in full on every failed call.
const MAX_INDEXER_URL_CHARS: usize = 2_048;

/// Parse and validate an indexer URL: `Some(Url)` if it parses, uses an
/// HTTP(S) scheme, has a host component and is no longer than
/// [`MAX_INDEXER_URL_CHARS`]; `None` otherwise.
fn parse_indexer_url(raw: &str) -> Option<Url> {
    let url = raw.parse::<Url>().ok()?;
    let usable = matches!(url.scheme(), "http" | "https")
        && url.has_host()
        && url.as_str().len() <= MAX_INDEXER_URL_CHARS;
    usable.then_some(url)
}

/// Handle for interacting with the indexer URLs service.
#[derive(Clone)]
pub struct Handle {
    /// The receiver for the latest snapshot
    rx_snapshot: watch::Receiver<Snapshot>,

    /// The stop signal for the service
    tx_stop: mpsc::Sender<()>,
}

impl Handle {
    /// Look up the registered URL for an indexer.
    pub fn get_indexer_url(&self, id: &IndexerId) -> Option<Url> {
        self.rx_snapshot.borrow().get(id).cloned()
    }

    /// Build a `Handle` directly from a seeded snapshot for tests. The stop
    /// channel's receiver is dropped immediately; there is no background
    /// service, the handle only serves `get_indexer_url` reads.
    #[cfg(test)]
    pub(crate) fn for_test(snapshot: Snapshot) -> Self {
        let (tx_stop, _rx_stop) = mpsc::channel(1);
        let (_tx_snapshot, rx_snapshot) = watch::channel(snapshot);
        Self {
            rx_snapshot,
            tx_stop,
        }
    }

    /// Signal the service to stop and wait for it to shut down; returns
    /// immediately if it is already stopped.
    pub async fn stop(&self) {
        if self.tx_stop.is_closed() {
            return;
        }

        let _ = self.tx_stop.send(()).await;

        // Wait for the channel to close
        self.tx_stop.closed().await;
    }
}

/// Configuration for the indexer URLs service.
#[derive(Clone)]
pub struct Ctx {
    /// The indexing-payments subgraph query endpoint.
    pub endpoint: Url,
    /// Bearer token for the endpoint (needed for gateway-served subgraphs).
    pub api_key: Option<String>,
    /// Refresh interval.
    pub update_interval: Duration,
}

/// Fetch a full snapshot of registered indexer URLs from the subgraph.
/// Fails on query or decode errors; an empty result is not an error (no
/// indexers have registered yet).
pub async fn fetch_snapshot(
    client: &reqwest::Client,
    endpoint: &Url,
    api_key: Option<&str>,
) -> anyhow::Result<Snapshot> {
    let mut snapshot = Snapshot::new();
    // `Indexer.id` is Bytes (an address); the all-zero address sorts below
    // any real id, so it works as the initial keyset cursor.
    let mut last_id = ZERO_ADDRESS.to_string();

    loop {
        let body = serde_json::json!({
            "query": PAGINATED_QUERY,
            "variables": {
                "lastId": last_id,
                "first": PAGE_SIZE,
            },
        });

        let mut builder = client.post(endpoint.as_str()).json(&body);
        if let Some(key) = api_key {
            builder = builder.bearer_auth(key);
        }

        let response = builder.send().await.map_err(|err| {
            anyhow::anyhow!("failed to query indexers page: {}", err.without_url())
        })?;
        let status = response.status();
        if !status.is_success() {
            anyhow::bail!("indexers page request failed: HTTP {status}");
        }
        let response: PageResponse = response.json().await.map_err(|err| {
            anyhow::anyhow!("failed to decode indexers page: {}", err.without_url())
        })?;

        let Some(data) = response.data else {
            anyhow::bail!("subgraph errors in indexers page: {:?}", response.errors);
        };

        let page_size = data.indexers.len();

        for entry in data.indexers {
            // Advance the cursor unconditionally so pagination continues
            // even if this entry fails to parse.
            last_id = entry.id.clone();
            if let Some((id, url)) = parse_indexer_entry(&entry) {
                snapshot.insert(id, url);
            } else {
                tracing::warn!(
                    indexer = %entry.id,
                    url = %crate::log_text::untrusted(&entry.url),
                    "skipping indexer with unparseable id or invalid URL"
                );
            }
        }

        if page_size < PAGE_SIZE {
            break;
        }
    }

    Ok(snapshot)
}

/// Parse a single indexer entry into an ID and validated URL.
/// Returns None if either field fails to parse (entry is skipped).
fn parse_indexer_entry(entry: &IndexerEntity) -> Option<(IndexerId, Url)> {
    let id: IndexerId = entry.id.parse().ok()?;
    let url = parse_indexer_url(&entry.url)?;
    Some((id, url))
}

/// Create a new indexer URLs service: refetches the full set of registered
/// indexers at regular intervals and publishes the result. Failed or empty
/// refreshes preserve the previous snapshot.
#[expect(
    clippy::cast_possible_wrap,
    reason = "predates this lint; fix when next touched"
)]
pub fn new(ctx: Ctx, init: Snapshot) -> (Handle, impl Future<Output = anyhow::Result<()>>) {
    let (tx_stop, mut rx_stop) = mpsc::channel(1);
    let (tx_snapshot, rx_snapshot) = watch::channel(init);

    let service = async move {
        let client = reqwest::Client::builder()
            .timeout(QUERY_TIMEOUT)
            .build()
            .unwrap_or_default();

        let mut timer = tokio::time::interval(ctx.update_interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        // Skip the first tick: startup already fetched the initial snapshot.
        timer.tick().await;

        let mut prev_count: usize = tx_snapshot.borrow().len();

        loop {
            tokio::select! {
                _ = rx_stop.recv() => break,
                _ = timer.tick() => {},
            }

            let snapshot =
                match fetch_snapshot(&client, &ctx.endpoint, ctx.api_key.as_deref()).await {
                    Ok(snapshot) => snapshot,
                    Err(err) => {
                        tracing::warn!(error = %err, "failed to fetch indexer URLs update");
                        continue;
                    }
                };

            // Guard against refreshes that produce zero indexers: a subgraph
            // serving a partially indexed dataset would otherwise wipe every
            // URL and stall proposal sending until the next good refresh.
            let count = snapshot.len();
            if count == 0 {
                tracing::warn!(
                    "indexer URLs refresh produced 0 indexers -- preserving previous snapshot"
                );
                continue;
            }

            if prev_count != count {
                tracing::info!(
                    "indexer URLs updated: {}->{} ({:+})",
                    prev_count,
                    count,
                    count as isize - prev_count as isize,
                );
            } else {
                tracing::debug!(indexers = count, "indexer URLs refresh completed");
            }
            prev_count = count;

            // Send the snapshot to the receiver; if no listener is available,
            // finish the service
            if let Err(err) = tx_snapshot.send(snapshot) {
                tracing::debug!(error = %err, "failed to send indexer URLs update");
                break;
            }
        }

        tracing::debug!("indexer URLs service stopped");

        Ok(())
    };

    (
        Handle {
            rx_snapshot,
            tx_stop,
        },
        service,
    )
}

const PAGE_SIZE: usize = 1000;

const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

const PAGINATED_QUERY: &str = r#"
    query RegisteredIndexers($lastId: Bytes!, $first: Int!) {
        indexers(
            first: $first
            where: { id_gt: $lastId }
            orderBy: id
            orderDirection: asc
        ) {
            id
            url
        }
    }
"#;

#[derive(Debug, serde::Deserialize)]
struct PageResponse {
    data: Option<PageData>,
    errors: Option<Vec<serde_json::Value>>,
}

#[derive(Debug, serde::Deserialize)]
struct PageData {
    indexers: Vec<IndexerEntity>,
}

#[derive(Debug, serde::Deserialize)]
struct IndexerEntity {
    id: String,
    url: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    const INDEXER: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn entry(id: &str, url: &str) -> IndexerEntity {
        IndexerEntity {
            id: id.to_string(),
            url: url.to_string(),
        }
    }

    #[test]
    fn test_parse_indexer_entry_valid() {
        let (id, url) = parse_indexer_entry(&entry(INDEXER, "https://indexer.example.com/"))
            .expect("entry should parse");
        assert_eq!(id, INDEXER.parse::<IndexerId>().unwrap());
        assert_eq!(url.as_str(), "https://indexer.example.com/");
    }

    #[test]
    fn test_parse_indexer_entry_invalid_id() {
        assert!(
            parse_indexer_entry(&entry("not-an-address", "https://indexer.example.com")).is_none()
        );
    }

    #[test]
    fn test_parse_indexer_entry_invalid_url() {
        assert!(parse_indexer_entry(&entry(INDEXER, "")).is_none());
        assert!(parse_indexer_entry(&entry(INDEXER, "not a url")).is_none());
        // Non-HTTP scheme
        assert!(parse_indexer_entry(&entry(INDEXER, "ftp://indexer.example.com")).is_none());
        // Scheme merely prefixed with "http" is not http(s)
        assert!(parse_indexer_entry(&entry(INDEXER, "httpsx://indexer.example.com")).is_none());
        // No host
        assert!(parse_indexer_entry(&entry(INDEXER, "http://")).is_none());
    }

    /// Answer every query with 1 registered indexer whose URL is `url`, and return what dipper
    /// logs while reading it.
    async fn logged_reading_url(url: &str) -> String {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "data": {"indexers": [{"id": INDEXER, "url": url}]}
                })),
            )
            .mount(&server)
            .await;
        let endpoint: Url = server.uri().parse().unwrap();
        crate::test_support::logged(async {
            let snapshot = fetch_snapshot(&reqwest::Client::new(), &endpoint, None)
                .await
                .unwrap();
            assert!(snapshot.is_empty(), "the URL is not one dipper can use");
        })
        .await
    }

    #[tokio::test]
    async fn a_line_break_in_a_registered_url_cannot_start_a_fake_log_line() {
        //* Given a registered URL holding a line break, then text made to look like a log line
        let forged = "2026-11-10T00:00:00.000000Z ERROR dipper_service: funds are lost";

        //* When
        let logs = logged_reading_url(&format!("not a url\n{forged}")).await;

        //* Then the forged text stays inside the line that reports the indexer
        assert!(logs.contains("skipping indexer"), "{logs}");
        assert!(!logs.lines().any(|line| line.starts_with(forged)), "{logs}");
    }

    #[tokio::test]
    async fn a_huge_registered_url_is_cut_short_in_the_log() {
        //* When
        let logs = logged_reading_url(&"x".repeat(100_000)).await;

        //* Then
        assert!(logs.contains("skipping indexer"), "{logs:.300}");
        assert!(logs.len() < 1_000, "logged {} bytes", logs.len());
    }

    #[test]
    fn a_registered_url_far_longer_than_a_real_one_is_refused() {
        let base = "https://indexer.example.com/";
        let longest_allowed = format!("{base}{}", "a".repeat(MAX_INDEXER_URL_CHARS - base.len()));
        let too_long = format!("{longest_allowed}a");

        assert!(parse_indexer_url(&longest_allowed).is_some());
        assert!(parse_indexer_url(&too_long).is_none());
    }

    #[test]
    fn a_registered_url_is_measured_as_it_would_be_sent() {
        // Each of these letters is sent as 6 characters, so 500 of them make a 3,000 character
        // path out of 500 characters of registered text.
        let short_as_written = format!("https://indexer.example.com/{}", "é".repeat(500));

        assert!(parse_indexer_url(&short_as_written).is_none());
    }

    #[test]
    fn test_handle_lookup() {
        let id: IndexerId = INDEXER.parse().unwrap();
        let url: Url = "https://indexer.example.com".parse().unwrap();
        let init = Snapshot::from([(id, url.clone())]);

        let (handle, _service) = new(
            Ctx {
                endpoint: "http://localhost:9999/subgraphs/name/test".parse().unwrap(),
                api_key: None,
                update_interval: Duration::from_secs(3600),
            },
            init,
        );

        assert_eq!(handle.get_indexer_url(&id), Some(url));
        let other: IndexerId = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            .parse()
            .unwrap();
        assert_eq!(handle.get_indexer_url(&other), None);
    }

    #[tokio::test]
    async fn test_stop_signal() {
        let (handle, fut) = new(
            Ctx {
                endpoint: "http://localhost:9999/subgraphs/name/test".parse().unwrap(),
                api_key: None,
                update_interval: Duration::from_secs(3600),
            },
            Snapshot::new(),
        );

        let task = tokio::spawn(fut);
        handle.stop().await;

        // Should complete without error
        let result = tokio::time::timeout(Duration::from_secs(2), task).await;
        assert!(result.is_ok());
    }
}
