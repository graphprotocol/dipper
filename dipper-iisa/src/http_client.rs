//! HTTP Client for IISA (Indexing Indexer Selection Algorithm)
//!
//! This module implements a Rust HTTP client for communicating with the IISA container service.
//! The client sends indexer selection requests and receives the selected indexer IDs.
//!
//! The IISA container handles:
//! - Fetching performance data from BigQuery
//! - GeoIP resolution for geographic diversity
//! - Calculating weighted scores for each candidate
//! - Running the selection algorithm
//! - Candidate filtering (internally, using its own scores data)

use std::{collections::HashMap, time::Duration};

use async_trait::async_trait;
use rand::Rng;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use thegraph_core::{DeploymentId, IndexerId};

use crate::api::{
    CandidateSelection, DipsAcceptingSnapshot, SelectedIndexer, SelectionContext, SelectionError,
};

/// Configuration for the HTTP client.
#[derive(Debug, Clone)]
pub struct HttpClientConfig {
    /// Total timeout for request + response
    pub request_timeout: Duration,
    /// Timeout for TCP connection establishment
    pub connect_timeout: Duration,
    /// Maximum number of retry attempts for transient failures.
    ///
    /// This is the number of *additional* attempts after the initial request fails.
    /// For example, `max_retries = 3` means up to 4 total attempts (1 initial + 3 retries).
    pub max_retries: u32,

    /// Bearer token for the authenticated `GET /dips-indexers` endpoint. `None`
    /// omits the header (local dev where IISA auth is off).
    pub push_token: Option<String>,
}

impl Default for HttpClientConfig {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            max_retries: 3,
            push_token: None,
        }
    }
}

/// HTTP client for the IISA container service.
#[derive(Clone)]
pub struct HttpIisaClient {
    client: Client,
    endpoint: String,
    config: HttpClientConfig,
}

/// Request body for the /select-indexers endpoint.
#[derive(Debug, Clone, Serialize)]
struct SelectionRequest {
    /// The deployment ID to select indexers for
    deployment_id: String,

    /// List of existing indexer IDs already assigned to this deployment
    #[serde(skip_serializing_if = "Option::is_none")]
    existing_indexers: Option<Vec<String>>,

    /// Pending agreements: deployment ID -> list of indexer IDs
    #[serde(skip_serializing_if = "Option::is_none")]
    pending_agreements: Option<HashMap<String, Vec<String>>>,

    /// Target group size: number of indexers to select
    num_candidates: usize,

    /// Indexer IDs to exclude from selection entirely (maps from `indexer_denylist`)
    #[serde(skip_serializing_if = "Option::is_none")]
    blocklist: Option<Vec<String>>,

    /// Declined indexers: deployment ID -> list of indexer IDs that recently declined
    #[serde(skip_serializing_if = "Option::is_none")]
    declined_indexers: Option<HashMap<String, Vec<String>>>,

    /// Chain ID (e.g. "arbitrum-one") for filtering by supported networks
    #[serde(skip_serializing_if = "Option::is_none")]
    chain_id: Option<String>,

    /// Payment ceiling: maximum GRT per 30 days
    #[serde(skip_serializing_if = "Option::is_none")]
    max_grt_per_30_days: Option<f64>,

    /// Expected DIPs fees per indexer in GRT per 30 days from accepted agreements
    #[serde(skip_serializing_if = "Option::is_none")]
    optimistic_dips_fees: Option<HashMap<String, f64>>,
}

/// Response from the /select-indexers endpoint.
///
/// Supports both legacy format (flat list of indexer ID strings) and new format
/// (list of objects with pricing).
#[derive(Debug, Deserialize)]
struct SelectionResponse {
    /// The deployment ID (echoed back, not used by client)
    #[serde(rename = "deployment_id")]
    _deployment_id: String,

    /// Full list of selected indexers.
    ///
    /// Each entry is either a plain string (legacy) or an object with pricing fields.
    indexers: Vec<IndexerEntry>,
}

/// An indexer entry in the IISA response.
///
/// Supports both formats:
/// - Legacy: a plain string ID like `"0xABC..."`
/// - New: an object like `{"id": "0xABC...", "min_grt_per_30_days": 450.0}`
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum IndexerEntry {
    WithPricing {
        id: String,
        #[serde(default)]
        min_grt_per_30_days: Option<f64>,
        #[serde(default)]
        min_grt_per_billion_entities_per_30_days: Option<f64>,
    },
    LegacyId(String),
}

/// Response from the `GET /dips-indexers` endpoint (the `count` field is ignored).
#[derive(Debug, Deserialize)]
struct DipsIndexersResponse {
    #[serde(default)]
    computed_at: Option<String>,
    #[serde(default)]
    indexers: Vec<String>,
}

/// Check if an HTTP error is retryable.
///
/// Retries on:
/// - `is_timeout()`: Request timed out waiting for response
/// - `is_connect()`: Failed to establish TCP connection
/// - `is_body()`: Error reading response body (connection reset, chunked encoding errors)
/// - `is_request()`: Request building/sending failed. While these are often deterministic
///   (e.g., serialization errors), we include them defensively to handle transient cases
///   like system resource pressure. These errors are rare in practice.
fn is_retryable_error(err: &reqwest::Error) -> bool {
    err.is_timeout() || err.is_connect() || err.is_body() || err.is_request()
}

/// Check if an HTTP status code is retryable (5xx server errors).
fn is_retryable_status(status: reqwest::StatusCode) -> bool {
    status.is_server_error()
}

/// Calculate retry delay with exponential backoff and jitter.
///
/// Base delay is 100ms, capped at 5s. Jitter adds +/- 25% variance to prevent
/// thundering herd problems when multiple clients retry simultaneously.
fn calculate_retry_delay(attempt: u32) -> Duration {
    const BASE_MS: u64 = 100;
    const MAX_MS: u64 = 5000;

    let exponential = BASE_MS.saturating_mul(2u64.saturating_pow(attempt.saturating_sub(1)));
    let capped = exponential.min(MAX_MS);

    // Add jitter: +/- 25%
    let jitter_range = capped / 4;
    if jitter_range == 0 {
        return Duration::from_millis(capped);
    }

    let mut rng = rand::rng();
    let jitter: u64 = rng.random_range(0..(jitter_range * 2));
    let with_jitter = capped.saturating_sub(jitter_range).saturating_add(jitter);

    Duration::from_millis(with_jitter)
}

impl HttpIisaClient {
    /// Create a new HTTP client for the IISA service with default configuration.
    ///
    /// # Arguments
    /// * `endpoint` - Base URL of the IISA service (e.g., "http://iisa-service:8080")
    pub fn new(endpoint: String) -> Self {
        Self::with_config(endpoint, HttpClientConfig::default())
    }

    /// Create a new HTTP client for the IISA service with custom configuration.
    ///
    /// # Arguments
    /// * `endpoint` - Base URL of the IISA service (e.g., "http://iisa-service:8080")
    /// * `config` - Client configuration for timeouts and retries
    #[expect(
        clippy::expect_used,
        reason = "predates this lint; fix when next touched"
    )]
    pub fn with_config(endpoint: String, config: HttpClientConfig) -> Self {
        let endpoint = if endpoint.ends_with('/') {
            endpoint
        } else {
            format!("{}/", endpoint)
        };

        let client = Client::builder()
            .timeout(config.request_timeout)
            .connect_timeout(config.connect_timeout)
            .build()
            .expect("Failed to build HTTP client");

        Self {
            client,
            endpoint,
            config,
        }
    }

    /// Check if the IISA service is healthy.
    pub async fn health_check(&self) -> Result<bool, SelectionError> {
        let url = format!("{}health", self.endpoint);

        let response =
            self.client.get(&url).send().await.map_err(|e| {
                SelectionError::Error(anyhow::anyhow!("Health check failed: {}", e))
            })?;

        Ok(response.status().is_success())
    }

    /// GET the DIPs-accepting set from IISA, with retry/backoff and bearer auth.
    async fn get_dips_indexers(
        &self,
        chain: &str,
        max_grt_per_30_days: Option<f64>,
    ) -> Result<DipsIndexersResponse, SelectionError> {
        let url = format!("{}dips-indexers", self.endpoint);
        let mut last_error = SelectionError::IisaServiceUnavailable;

        for attempt in 0..=self.config.max_retries {
            if attempt > 0 {
                tokio::time::sleep(calculate_retry_delay(attempt)).await;
            }
            let mut req = self.client.get(&url).query(&[("chain", chain)]);
            if let Some(max_grt) = max_grt_per_30_days {
                req = req.query(&[("max_grt_per_30_days", max_grt.to_string().as_str())]);
            }
            if let Some(token) = &self.config.push_token {
                req = req.bearer_auth(token);
            }
            match req.send().await {
                Ok(response) if response.status().is_success() => {
                    return response.json().await.map_err(|e| {
                        SelectionError::Error(anyhow::anyhow!(
                            "Failed to parse /dips-indexers response: {}",
                            e
                        ))
                    });
                }
                Ok(response) if is_retryable_status(response.status()) => {
                    last_error = SelectionError::IisaServiceUnavailable;
                }
                Ok(response) => {
                    tracing::error!(
                        "IISA /dips-indexers returned client error status: {}",
                        response.status()
                    );
                    return Err(SelectionError::IisaServiceUnavailable);
                }
                Err(e) if is_retryable_error(&e) => {
                    last_error = SelectionError::IisaServiceUnavailable;
                }
                Err(e) => return Err(SelectionError::Error(e.into())),
            }
        }
        Err(last_error)
    }

    /// Execute an HTTP POST request with retry logic.
    ///
    /// Retries on:
    /// - Network errors (timeout, connection failed)
    /// - 5xx server errors
    ///
    /// Does not retry on:
    /// - 4xx client errors
    /// - Parse/deserialization errors
    #[expect(
        clippy::cognitive_complexity,
        reason = "predates this lint; fix when next touched"
    )]
    async fn post_with_retry<T: for<'de> Deserialize<'de>>(
        &self,
        url: &str,
        request: &SelectionRequest,
    ) -> Result<T, SelectionError> {
        let mut last_error = SelectionError::IisaServiceUnavailable;

        for attempt in 0..=self.config.max_retries {
            if attempt > 0 {
                let delay = calculate_retry_delay(attempt);
                tracing::debug!(
                    attempt = attempt,
                    delay_ms = delay.as_millis(),
                    "Retrying IISA request after delay"
                );
                tokio::time::sleep(delay).await;
            }

            match self.client.post(url).json(request).send().await {
                Ok(response) if response.status().is_success() => {
                    return response.json().await.map_err(|e| {
                        SelectionError::Error(anyhow::anyhow!("Failed to parse response: {}", e))
                    });
                }
                Ok(response) if is_retryable_status(response.status()) => {
                    tracing::debug!(
                        attempt = attempt,
                        status = %response.status(),
                        "Retryable HTTP status from IISA"
                    );
                    last_error = SelectionError::IisaServiceUnavailable;
                }
                Ok(response) => {
                    // Non-retryable status (4xx)
                    tracing::error!("IISA returned client error status: {}", response.status());
                    return Err(SelectionError::IisaServiceUnavailable);
                }
                Err(e) if is_retryable_error(&e) => {
                    tracing::debug!(
                        attempt = attempt,
                        error = %e,
                        "Retryable HTTP error from IISA"
                    );
                    last_error = SelectionError::IisaServiceUnavailable;
                }
                Err(e) => {
                    tracing::error!("IISA request failed with non-retryable error: {}", e);
                    return Err(SelectionError::Error(e.into()));
                }
            }
        }

        tracing::error!(
            max_retries = self.config.max_retries,
            "IISA request failed after all retries"
        );
        Err(last_error)
    }

    /// Format existing indexers from context for the HTTP request.
    ///
    /// Returns `None` if the list is empty to skip serialization.
    fn format_existing_indexers(context: &SelectionContext) -> Option<Vec<String>> {
        if context.existing_indexers.is_empty() {
            None
        } else {
            Some(
                context
                    .existing_indexers
                    .iter()
                    .map(|id| format!("{:#x}", id))
                    .collect(),
            )
        }
    }

    /// Format pending agreements from context for the HTTP request.
    ///
    /// Returns `None` if the map is empty to skip serialization.
    fn format_pending_agreements(
        context: &SelectionContext,
    ) -> Option<HashMap<String, Vec<String>>> {
        if context.pending_agreements.is_empty() {
            None
        } else {
            Some(
                context
                    .pending_agreements
                    .iter()
                    .map(|(deployment_id, indexer_ids)| {
                        (
                            deployment_id.to_string(),
                            indexer_ids.iter().map(|id| format!("{:#x}", id)).collect(),
                        )
                    })
                    .collect(),
            )
        }
    }

    /// Format indexer denylist from context as `blocklist` for the HTTP request.
    ///
    /// Returns `None` if the list is empty to skip serialization.
    fn format_blocklist(context: &SelectionContext) -> Option<Vec<String>> {
        if context.indexer_denylist.is_empty() {
            None
        } else {
            Some(
                context
                    .indexer_denylist
                    .iter()
                    .map(|id| format!("{:#x}", id))
                    .collect(),
            )
        }
    }

    /// Format optimistic DIPs fees from context for the HTTP request.
    ///
    /// Converts `IndexerId` keys to lowercase hex strings.
    /// Returns `None` if the map is empty to skip serialization.
    fn format_optimistic_dips_fees(context: &SelectionContext) -> Option<HashMap<String, f64>> {
        if context.optimistic_dips_fees.is_empty() {
            None
        } else {
            Some(
                context
                    .optimistic_dips_fees
                    .iter()
                    .map(|(id, fee)| (format!("{:#x}", id), *fee))
                    .collect(),
            )
        }
    }

    /// Format declined indexers from context for the HTTP request.
    ///
    /// Returns `None` if the map is empty to skip serialization.
    fn format_declined_indexers(
        context: &SelectionContext,
    ) -> Option<HashMap<String, Vec<String>>> {
        if context.declined_indexers.is_empty() {
            None
        } else {
            Some(
                context
                    .declined_indexers
                    .iter()
                    .map(|(deployment_id, indexer_ids)| {
                        (
                            deployment_id.to_string(),
                            indexer_ids.iter().map(|id| format!("{:#x}", id)).collect(),
                        )
                    })
                    .collect(),
            )
        }
    }
}

#[async_trait]
impl CandidateSelection for HttpIisaClient {
    async fn select_indexers(
        &self,
        deployment_id: DeploymentId,
        num_candidates: usize,
        context: &SelectionContext,
    ) -> Result<Vec<SelectedIndexer>, SelectionError> {
        if num_candidates == 0 {
            return Ok(Vec::new());
        }

        let request = SelectionRequest {
            deployment_id: deployment_id.to_string(),
            existing_indexers: Self::format_existing_indexers(context),
            pending_agreements: Self::format_pending_agreements(context),
            num_candidates,
            blocklist: Self::format_blocklist(context),
            declined_indexers: Self::format_declined_indexers(context),
            chain_id: context.chain_id.clone(),
            max_grt_per_30_days: context.max_grt_per_30_days,
            optimistic_dips_fees: Self::format_optimistic_dips_fees(context),
        };

        let url = format!("{}select-indexers", self.endpoint);
        let result: SelectionResponse = self.post_with_retry(&url, &request).await?;

        // Parse returned indexer entries into SelectedIndexer types
        let mut selected = Vec::with_capacity(result.indexers.len());
        for entry in result.indexers {
            let (id_str, min_grt, min_entity_grt) = match entry {
                IndexerEntry::WithPricing {
                    id,
                    min_grt_per_30_days,
                    min_grt_per_billion_entities_per_30_days,
                } => (
                    id,
                    min_grt_per_30_days,
                    min_grt_per_billion_entities_per_30_days,
                ),
                IndexerEntry::LegacyId(id) => (id, None, None),
            };
            match id_str.parse::<IndexerId>() {
                Ok(id) => selected.push(SelectedIndexer {
                    id,
                    min_grt_per_30_days: min_grt,
                    min_grt_per_billion_entities_per_30_days: min_entity_grt,
                }),
                Err(e) => {
                    tracing::warn!("Failed to parse indexer ID '{}': {}", id_str, e);
                }
            }
        }

        Ok(selected)
    }

    async fn dips_accepting_indexers(
        &self,
        chain: &str,
        max_grt_per_30_days: Option<f64>,
    ) -> Result<DipsAcceptingSnapshot, SelectionError> {
        let response = self.get_dips_indexers(chain, max_grt_per_30_days).await?;
        let mut indexers = Vec::with_capacity(response.indexers.len());
        for id_str in response.indexers {
            match id_str.parse::<IndexerId>() {
                Ok(id) => indexers.push(id),
                Err(e) => {
                    tracing::warn!(
                        "Failed to parse DIPs-accepting indexer ID '{}': {}",
                        id_str,
                        e
                    )
                }
            }
        }
        Ok(DipsAcceptingSnapshot {
            computed_at: response.computed_at,
            indexers,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::*;

    #[test]
    fn test_endpoint_normalization() {
        let client = HttpIisaClient::new("http://localhost:8080".to_string());
        assert_eq!(client.endpoint, "http://localhost:8080/");

        let client = HttpIisaClient::new("http://localhost:8080/".to_string());
        assert_eq!(client.endpoint, "http://localhost:8080/");
    }

    #[test]
    fn test_config_default() {
        let config = HttpClientConfig::default();
        assert_eq!(config.request_timeout, Duration::from_secs(30));
        assert_eq!(config.connect_timeout, Duration::from_secs(10));
        assert_eq!(config.max_retries, 3);
    }

    #[test]
    fn test_with_config_sets_timeouts() {
        let config = HttpClientConfig {
            request_timeout: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(5),
            max_retries: 5,
            push_token: None,
        };
        let client = HttpIisaClient::with_config("http://localhost:8080".to_string(), config);

        assert_eq!(client.config.request_timeout, Duration::from_secs(60));
        assert_eq!(client.config.connect_timeout, Duration::from_secs(5));
        assert_eq!(client.config.max_retries, 5);
    }

    #[test]
    fn test_calculate_retry_delay_exponential() {
        // Attempt 1: base delay ~100ms
        let delay1 = calculate_retry_delay(1);
        assert!(delay1.as_millis() >= 75 && delay1.as_millis() <= 125);

        // Attempt 2: ~200ms
        let delay2 = calculate_retry_delay(2);
        assert!(delay2.as_millis() >= 150 && delay2.as_millis() <= 250);

        // Attempt 3: ~400ms
        let delay3 = calculate_retry_delay(3);
        assert!(delay3.as_millis() >= 300 && delay3.as_millis() <= 500);

        // Attempt 4: ~800ms
        let delay4 = calculate_retry_delay(4);
        assert!(delay4.as_millis() >= 600 && delay4.as_millis() <= 1000);
    }

    #[test]
    fn test_calculate_retry_delay_capped() {
        // High attempt numbers should cap at 5000ms +/- 25%
        let delay = calculate_retry_delay(10);
        assert!(delay.as_millis() >= 3750 && delay.as_millis() <= 6250);
    }

    #[test]
    fn test_is_retryable_status() {
        assert!(is_retryable_status(
            reqwest::StatusCode::INTERNAL_SERVER_ERROR
        ));
        assert!(is_retryable_status(reqwest::StatusCode::BAD_GATEWAY));
        assert!(is_retryable_status(
            reqwest::StatusCode::SERVICE_UNAVAILABLE
        ));
        assert!(is_retryable_status(reqwest::StatusCode::GATEWAY_TIMEOUT));

        assert!(!is_retryable_status(reqwest::StatusCode::OK));
        assert!(!is_retryable_status(reqwest::StatusCode::BAD_REQUEST));
        assert!(!is_retryable_status(reqwest::StatusCode::NOT_FOUND));
        assert!(!is_retryable_status(reqwest::StatusCode::UNAUTHORIZED));
    }

    #[test]
    fn test_format_existing_indexers_empty() {
        let context = SelectionContext::default();
        assert_eq!(HttpIisaClient::format_existing_indexers(&context), None);
    }

    #[test]
    fn test_format_existing_indexers_with_data() {
        let indexer_id: IndexerId = "0x1234567890123456789012345678901234567890"
            .parse()
            .unwrap();
        let context = SelectionContext {
            existing_indexers: vec![indexer_id],
            ..Default::default()
        };

        let result = HttpIisaClient::format_existing_indexers(&context);
        assert!(result.is_some());
        let indexers = result.unwrap();
        assert_eq!(indexers.len(), 1);
        assert_eq!(indexers[0], "0x1234567890123456789012345678901234567890");
    }

    #[test]
    fn test_format_pending_agreements_empty() {
        let context = SelectionContext::default();
        assert_eq!(HttpIisaClient::format_pending_agreements(&context), None);
    }

    #[test]
    fn test_format_pending_agreements_with_data() {
        let indexer_id: IndexerId = "0x1234567890123456789012345678901234567890"
            .parse()
            .unwrap();
        let deployment_id: DeploymentId = "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"
            .parse()
            .unwrap();

        let mut pending = HashMap::new();
        pending.insert(deployment_id, vec![indexer_id]);

        let context = SelectionContext {
            pending_agreements: pending,
            ..Default::default()
        };

        let result = HttpIisaClient::format_pending_agreements(&context);
        assert!(result.is_some());
        let agreements = result.unwrap();
        assert_eq!(agreements.len(), 1);
        // Key should be deployment ID, value should be list of indexer IDs
        assert!(agreements.contains_key("QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"));
        let indexers = agreements
            .get("QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG")
            .unwrap();
        assert_eq!(indexers.len(), 1);
        assert_eq!(indexers[0], "0x1234567890123456789012345678901234567890");
    }

    #[test]
    fn test_selection_context_default() {
        let context = SelectionContext::default();
        assert!(context.existing_indexers.is_empty());
        assert!(context.pending_agreements.is_empty());
        assert!(context.indexer_denylist.is_empty());
        assert!(context.declined_indexers.is_empty());
    }

    #[test]
    fn test_format_blocklist_empty() {
        let context = SelectionContext::default();
        assert_eq!(HttpIisaClient::format_blocklist(&context), None);
    }

    #[test]
    fn test_format_blocklist_with_data() {
        let indexer_id: IndexerId = "0x1234567890123456789012345678901234567890"
            .parse()
            .unwrap();
        let context = SelectionContext {
            indexer_denylist: vec![indexer_id],
            ..Default::default()
        };

        let result = HttpIisaClient::format_blocklist(&context);
        assert!(result.is_some());
        let blocklist = result.unwrap();
        assert_eq!(blocklist.len(), 1);
        assert_eq!(blocklist[0], "0x1234567890123456789012345678901234567890");
    }

    #[test]
    fn test_format_declined_indexers_empty() {
        let context = SelectionContext::default();
        assert_eq!(HttpIisaClient::format_declined_indexers(&context), None);
    }

    #[test]
    fn test_format_declined_indexers_with_data() {
        let indexer_id: IndexerId = "0x1234567890123456789012345678901234567890"
            .parse()
            .unwrap();
        let deployment_id: DeploymentId = "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"
            .parse()
            .unwrap();

        let mut declined = HashMap::new();
        declined.insert(deployment_id, vec![indexer_id]);

        let context = SelectionContext {
            declined_indexers: declined,
            ..Default::default()
        };

        let result = HttpIisaClient::format_declined_indexers(&context);
        assert!(result.is_some());
        let declined_map = result.unwrap();
        assert_eq!(declined_map.len(), 1);
        assert!(declined_map.contains_key("QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"));
        let indexers = declined_map
            .get("QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG")
            .unwrap();
        assert_eq!(indexers.len(), 1);
        assert_eq!(indexers[0], "0x1234567890123456789012345678901234567890");
    }

    #[tokio::test]
    async fn test_health_check_returns_true_when_healthy() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/health"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let client = HttpIisaClient::new(mock_server.uri());
        let result = client.health_check().await;

        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_health_check_returns_false_when_unhealthy() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/health"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&mock_server)
            .await;

        let client = HttpIisaClient::new(mock_server.uri());
        let result = client.health_check().await;

        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[tokio::test]
    async fn test_health_check_returns_error_when_connection_fails() {
        // Use an endpoint that will refuse connections
        let client = HttpIisaClient::new("http://127.0.0.1:1".to_string());
        let result = client.health_check().await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, SelectionError::Error(_)));
    }

    #[tokio::test]
    async fn test_select_indexers_success() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/select-indexers"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "deployment_id": "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
                "indexers": [
                    "0x1234567890123456789012345678901234567890",
                    "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
                ]
            })))
            .mount(&mock_server)
            .await;

        let client = HttpIisaClient::new(mock_server.uri());
        let deployment_id: DeploymentId = "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"
            .parse()
            .unwrap();

        let result = client
            .select_indexers(deployment_id, 2, &SelectionContext::default())
            .await;

        assert!(result.is_ok());
        let indexers = result.unwrap();
        assert_eq!(indexers.len(), 2);
    }

    #[tokio::test]
    async fn test_select_indexers_zero_candidates_returns_empty() {
        let client = HttpIisaClient::new("http://localhost:1".to_string());
        let deployment_id: DeploymentId = "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"
            .parse()
            .unwrap();

        let result = client
            .select_indexers(deployment_id, 0, &SelectionContext::default())
            .await;

        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_retry_on_5xx_then_success() {
        let mock_server = MockServer::start().await;
        let call_count = std::sync::Arc::new(AtomicU32::new(0));
        let call_count_clone = call_count.clone();

        Mock::given(method("POST"))
            .and(path("/select-indexers"))
            .respond_with(move |_: &wiremock::Request| {
                let count = call_count_clone.fetch_add(1, Ordering::SeqCst);
                if count < 2 {
                    // First 2 calls return 503
                    ResponseTemplate::new(503)
                } else {
                    // Third call succeeds
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "deployment_id": "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
                        "indexers": ["0x1234567890123456789012345678901234567890"]
                    }))
                }
            })
            .mount(&mock_server)
            .await;

        let config = HttpClientConfig {
            request_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(2),
            max_retries: 3,
            push_token: None,
        };
        let client = HttpIisaClient::with_config(mock_server.uri(), config);

        let result = client
            .select_indexers(
                "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"
                    .parse()
                    .unwrap(),
                1,
                &SelectionContext::default(),
            )
            .await;

        // Should succeed after 2 retries (3 total calls)
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1);
        assert_eq!(call_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_no_retry_on_4xx() {
        let mock_server = MockServer::start().await;
        let call_count = std::sync::Arc::new(AtomicU32::new(0));
        let call_count_clone = call_count.clone();

        Mock::given(method("POST"))
            .and(path("/select-indexers"))
            .respond_with(move |_: &wiremock::Request| {
                call_count_clone.fetch_add(1, Ordering::SeqCst);
                ResponseTemplate::new(400)
            })
            .expect(1) // Should only be called once
            .mount(&mock_server)
            .await;

        let config = HttpClientConfig {
            request_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(2),
            max_retries: 3,
            push_token: None,
        };
        let client = HttpIisaClient::with_config(mock_server.uri(), config);

        let result = client
            .select_indexers(
                "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"
                    .parse()
                    .unwrap(),
                1,
                &SelectionContext::default(),
            )
            .await;

        // Should fail immediately without retry
        assert!(result.is_err());
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_exhausted_retries_returns_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/select-indexers"))
            .respond_with(ResponseTemplate::new(503))
            .expect(4) // Initial + 3 retries
            .mount(&mock_server)
            .await;

        let config = HttpClientConfig {
            request_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(2),
            max_retries: 3,
            push_token: None,
        };
        let client = HttpIisaClient::with_config(mock_server.uri(), config);

        let result = client
            .select_indexers(
                "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"
                    .parse()
                    .unwrap(),
                1,
                &SelectionContext::default(),
            )
            .await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SelectionError::IisaServiceUnavailable
        ));
    }

    #[tokio::test]
    async fn test_timeout_returns_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/select-indexers"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({
                        "deployment_id": "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
                        "indexers": ["0x1234567890123456789012345678901234567890"]
                    }))
                    .set_delay(Duration::from_secs(2)), // Delay longer than timeout
            )
            .mount(&mock_server)
            .await;

        let config = HttpClientConfig {
            request_timeout: Duration::from_millis(100), // Very short timeout
            connect_timeout: Duration::from_secs(2),
            max_retries: 0, // No retries to speed up test
            push_token: None,
        };
        let client = HttpIisaClient::with_config(mock_server.uri(), config);

        let result = client
            .select_indexers(
                "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"
                    .parse()
                    .unwrap(),
                1,
                &SelectionContext::default(),
            )
            .await;

        // Should fail due to timeout
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_malformed_json_response_returns_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/select-indexers"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not valid json {{{"))
            .mount(&mock_server)
            .await;

        let config = HttpClientConfig {
            request_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(2),
            max_retries: 0,
            push_token: None,
        };
        let client = HttpIisaClient::with_config(mock_server.uri(), config);

        let result = client
            .select_indexers(
                "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"
                    .parse()
                    .unwrap(),
                1,
                &SelectionContext::default(),
            )
            .await;

        // Should fail due to JSON parse error
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SelectionError::Error(_)));
    }

    #[tokio::test]
    async fn test_dips_accepting_indexers_sends_chain_and_ceiling() {
        let mock_server = MockServer::start().await;
        // The mock only matches when both query params are present, so a success
        // proves dipper sent the chain and the price ceiling.
        Mock::given(method("GET"))
            .and(path("/dips-indexers"))
            .and(wiremock::matchers::query_param("chain", "arbitrum-one"))
            .and(wiremock::matchers::query_param(
                "max_grt_per_30_days",
                "4500",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "computed_at": "2026-06-23T09:00:00+00:00",
                "count": 2,
                "indexers": [
                    "0x1234567890123456789012345678901234567890",
                    "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
                ]
            })))
            .mount(&mock_server)
            .await;

        let client = HttpIisaClient::new(mock_server.uri());
        let snap = client
            .dips_accepting_indexers("arbitrum-one", Some(4500.0))
            .await
            .unwrap();
        assert_eq!(
            snap.computed_at.as_deref(),
            Some("2026-06-23T09:00:00+00:00")
        );
        assert_eq!(snap.indexers.len(), 2);
    }

    #[tokio::test]
    async fn test_dips_accepting_sends_bearer_when_token_configured() {
        let mock_server = MockServer::start().await;
        // The mock only matches with the bearer header, so a success proves it was sent.
        Mock::given(method("GET"))
            .and(path("/dips-indexers"))
            .and(wiremock::matchers::header("authorization", "Bearer secret"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "computed_at": null, "count": 0, "indexers": []
            })))
            .mount(&mock_server)
            .await;

        let config = HttpClientConfig {
            request_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(2),
            max_retries: 0,
            push_token: Some("secret".to_string()),
        };
        let client = HttpIisaClient::with_config(mock_server.uri(), config);
        assert!(
            client
                .dips_accepting_indexers("mainnet", None)
                .await
                .unwrap()
                .indexers
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_dips_accepting_tolerates_null_computed_at_and_skips_bad_ids() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/dips-indexers"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "computed_at": null,
                "count": 2,
                "indexers": ["0x1234567890123456789012345678901234567890", "not-an-id"]
            })))
            .mount(&mock_server)
            .await;

        let client = HttpIisaClient::new(mock_server.uri());
        let snap = client
            .dips_accepting_indexers("mainnet", None)
            .await
            .unwrap();
        assert!(snap.computed_at.is_none());
        // The malformed id is warn-and-skipped, leaving only the valid one.
        assert_eq!(snap.indexers.len(), 1);
    }

    #[tokio::test]
    async fn test_dips_accepting_retries_on_5xx_then_succeeds() {
        let mock_server = MockServer::start().await;
        let call_count = std::sync::Arc::new(AtomicU32::new(0));
        let call_count_clone = call_count.clone();
        Mock::given(method("GET"))
            .and(path("/dips-indexers"))
            .respond_with(move |_: &wiremock::Request| {
                let count = call_count_clone.fetch_add(1, Ordering::SeqCst);
                if count < 1 {
                    ResponseTemplate::new(503)
                } else {
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "computed_at": "2026-06-23T09:00:00+00:00", "count": 0, "indexers": []
                    }))
                }
            })
            .mount(&mock_server)
            .await;

        let config = HttpClientConfig {
            request_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(2),
            max_retries: 3,
            push_token: None,
        };
        let client = HttpIisaClient::with_config(mock_server.uri(), config);
        assert!(
            client
                .dips_accepting_indexers("mainnet", None)
                .await
                .unwrap()
                .indexers
                .is_empty()
        );
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }
}
