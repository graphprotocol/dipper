//! Periodic background service that fetches entity counts from the
//! indexing-payments subgraph and maintains a shared in-memory cache.
//!
//! Worker jobs read from the cache for optimistic fee estimation.
//! The cache is refreshed on a configurable interval (default 1 hour).

use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};

use thegraph_core::{DeploymentId, IndexerId};
use tokio::sync::{RwLock, mpsc};
use url::Url;

/// Timeout for subgraph queries.
const QUERY_TIMEOUT: Duration = Duration::from_secs(30);

/// Composite key for the entity count cache.
pub type EntityCountKey = (IndexerId, DeploymentId);

/// Shared entity count cache, readable by worker jobs.
pub type EntityCountCache = Arc<RwLock<HashMap<EntityCountKey, u64>>>;

/// Create a new empty entity count cache.
pub fn new_cache() -> EntityCountCache {
    Arc::new(RwLock::new(HashMap::new()))
}

/// Handle for controlling the entity count cache service.
#[derive(Clone)]
pub struct Handle {
    tx_stop: mpsc::Sender<()>,
}

impl Handle {
    /// Stop the service gracefully.
    pub async fn stop(&self) {
        if self.tx_stop.is_closed() {
            return;
        }
        let _ = self.tx_stop.send(()).await;
        self.tx_stop.closed().await;
    }
}

/// Context for the entity count cache service.
pub struct Ctx {
    /// The shared cache to populate.
    pub cache: EntityCountCache,
    /// Subgraph endpoint URL.
    pub endpoint: Url,
    /// Refresh interval.
    pub interval: Duration,
}

/// Create a new entity count cache service.
///
/// Returns a handle for lifecycle control and a future to spawn. The
/// service fetches all `IndexerDeploymentLatest` entities from the
/// subgraph on a fixed interval and populates the shared cache.
pub fn new(ctx: Ctx) -> (Handle, impl Future<Output = anyhow::Result<()>>) {
    let (tx_stop, rx_stop) = mpsc::channel(1);
    let handle = Handle { tx_stop };

    let fut = run(ctx.cache, ctx.endpoint, ctx.interval, rx_stop);
    (handle, fut)
}

async fn run(
    cache: EntityCountCache,
    endpoint: Url,
    interval: Duration,
    mut rx_stop: mpsc::Receiver<()>,
) -> anyhow::Result<()> {
    tracing::info!(
        interval_secs = interval.as_secs(),
        endpoint = %endpoint,
        "entity count cache service starting"
    );

    // Fetch immediately on startup
    refresh_cache(&cache, &endpoint).await;

    let mut timer = tokio::time::interval(interval);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Skip the first tick (we already fetched above)
    timer.tick().await;

    loop {
        tokio::select! { biased;
            _ = rx_stop.recv() => {
                tracing::debug!("entity count cache service stopping");
                return Ok(());
            }
            _ = timer.tick() => {
                refresh_cache(&cache, &endpoint).await;
            }
        }
    }
}

#[expect(
    clippy::cognitive_complexity,
    reason = "predates this lint; fix when next touched"
)]
async fn refresh_cache(cache: &EntityCountCache, endpoint: &Url) {
    tracing::debug!("refreshing entity count cache");

    let counts = fetch_all_entity_counts(endpoint).await;

    if counts.is_empty() {
        let stale_count = cache.read().await.len();
        if stale_count > 0 {
            tracing::warn!(
                stale_entries = stale_count,
                "entity count cache refresh returned empty, retaining stale data"
            );
        }
        return;
    }

    let count = counts.len();
    let mut guard = cache.write().await;
    *guard = counts;
    drop(guard);

    tracing::info!(entries = count, "entity count cache refreshed");
}

/// Fetch all IndexerDeploymentLatest entities from the subgraph with
/// cursor-based pagination.
#[expect(
    clippy::cognitive_complexity,
    reason = "predates this lint; fix when next touched"
)]
async fn fetch_all_entity_counts(endpoint: &Url) -> HashMap<EntityCountKey, u64> {
    let mut result = HashMap::new();
    let mut last_id = String::new();
    let client = reqwest::Client::builder()
        .timeout(QUERY_TIMEOUT)
        .build()
        .unwrap_or_default();

    loop {
        let body = serde_json::json!({
            "query": PAGINATED_QUERY,
            "variables": {
                "lastId": last_id,
            },
        });

        let response = match client.post(endpoint.as_str()).json(&body).send().await {
            Ok(resp) => resp,
            Err(err) => {
                tracing::warn!(error = %err, "failed to fetch entity counts page");
                return result;
            }
        };

        let json: PageResponse = match response.json().await {
            Ok(j) => j,
            Err(err) => {
                tracing::warn!(error = %err, "failed to parse entity counts page");
                return result;
            }
        };

        let Some(data) = json.data else {
            if let Some(errors) = json.errors {
                tracing::warn!(errors = ?errors, "subgraph errors in entity count page");
            }
            return result;
        };

        let page_size = data.indexer_deployment_latests.len();

        for entry in data.indexer_deployment_latests {
            // Advance cursor unconditionally so pagination continues even
            // if this entry fails to parse.
            last_id = entry.id.clone();
            if let Some((key, entities)) = parse_entity_entry(&entry) {
                result.insert(key, entities);
            }
        }

        if page_size < 1000 {
            break;
        }
    }

    result
}

/// Parse a single entity entry into a cache key-value pair.
/// Returns None if any field fails to parse (entry is skipped).
fn parse_entity_entry(entry: &LatestEntity) -> Option<(EntityCountKey, u64)> {
    let indexer_id: IndexerId = entry.indexer.parse().ok()?;
    let deployment_id: DeploymentId = entry.subgraph_deployment_id.parse().ok()?;
    let entities: u64 = entry.entities.parse().ok()?;
    Some(((indexer_id, deployment_id), entities))
}

const PAGINATED_QUERY: &str = r#"
    query AllEntityCounts($lastId: ID!) {
        indexerDeploymentLatests(
            first: 1000
            where: { id_gt: $lastId }
            orderBy: id
            orderDirection: asc
        ) {
            id
            indexer
            subgraphDeploymentId
            entities
        }
    }
"#;

#[derive(Debug, serde::Deserialize)]
struct PageResponse {
    data: Option<PageData>,
    errors: Option<Vec<serde_json::Value>>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct PageData {
    indexer_deployment_latests: Vec<LatestEntity>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct LatestEntity {
    id: String,
    indexer: String,
    subgraph_deployment_id: String,
    entities: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_cache_is_empty() {
        let cache = new_cache();
        let guard = cache.try_read().unwrap();
        assert!(guard.is_empty());
    }

    #[test]
    fn test_parse_entity_entry_valid() {
        let entry = LatestEntity {
            id: "0x01-0x02".to_string(),
            indexer: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            subgraph_deployment_id:
                "0x0000000000000000000000000000000000000000000000000000000000000001".to_string(),
            entities: "5000".to_string(),
        };
        let result = parse_entity_entry(&entry);
        assert!(result.is_some());
        let ((indexer, _deployment), entities) = result.unwrap();
        assert_eq!(
            indexer,
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .parse::<IndexerId>()
                .unwrap()
        );
        assert_eq!(entities, 5000);
    }

    #[test]
    fn test_parse_entity_entry_invalid_indexer() {
        let entry = LatestEntity {
            id: "x".to_string(),
            indexer: "not-an-address".to_string(),
            subgraph_deployment_id:
                "0x0000000000000000000000000000000000000000000000000000000000000001".to_string(),
            entities: "100".to_string(),
        };
        assert!(parse_entity_entry(&entry).is_none());
    }

    #[test]
    fn test_parse_entity_entry_invalid_entities() {
        let entry = LatestEntity {
            id: "x".to_string(),
            indexer: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            subgraph_deployment_id:
                "0x0000000000000000000000000000000000000000000000000000000000000001".to_string(),
            entities: "not-a-number".to_string(),
        };
        assert!(parse_entity_entry(&entry).is_none());
    }

    #[tokio::test]
    async fn test_cache_stop_signal() {
        let cache = new_cache();
        let endpoint: Url = "http://localhost:9999/subgraphs/name/test".parse().unwrap();

        let (handle, fut) = new(Ctx {
            cache: cache.clone(),
            endpoint,
            interval: Duration::from_secs(3600),
        });

        let task = tokio::spawn(fut);
        handle.stop().await;

        // Should complete without error
        let result = tokio::time::timeout(Duration::from_secs(2), task).await;
        assert!(result.is_ok());
    }
}
