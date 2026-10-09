//! RPC provider pool with automatic failover and retry.
//!
//! Ported from `rewards-eligibility-oracle/blockchain_client.py`.

use std::{
    future::Future,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use thegraph_core::alloy::{
    providers::{
        Provider, ProviderBuilder, RootProvider,
        fillers::{BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller},
    },
    transports::{RpcError, TransportError, TransportErrorKind},
};
use url::Url;

use crate::chain_client::ChainClientError;

/// Pull a `ChainClientError` back out of a `TransportError` if a closure
/// boxed it in via `TransportErrorKind::custom` (see `build_and_send_call`).
/// Returns `None` if the transport error came from elsewhere.
fn extract_chain_client_error(err: TransportError) -> Option<ChainClientError> {
    match err {
        RpcError::Transport(TransportErrorKind::Custom(boxed)) => {
            boxed.downcast::<ChainClientError>().ok().map(|b| *b)
        }
        _ => None,
    }
}

/// How an endpoint is named in logs and errors. Hosted RPC endpoints carry their API key in
/// the path or the query, so naming one by host says which endpoint it was without the key.
fn endpoint_name(url: &Url) -> String {
    match (url.host_str(), url.port()) {
        (Some(host), Some(port)) => format!("{host}:{port}"),
        (Some(host), None) => host.to_string(),
        (None, _) => "unnamed endpoint".to_string(),
    }
}

/// How a failure reads once the endpoint's URL is taken out of it. A connection that never
/// got a reply is described by the HTTP client, which names the URL it was reaching for, and
/// that is where a hosted endpoint carries the API key that gets us in.
fn describe_failure(url: &Url, error: &TransportError) -> String {
    let name = endpoint_name(url);
    // A URL is as likely to be printed with its trailing slash as without, so take out both.
    error
        .to_string()
        .replace(url.as_str(), &name)
        .replace(url.as_str().trim_end_matches('/'), &name)
}

/// 1 endpoint's latest block, asked once. A failure is described without the URL, since
/// hosted endpoints carry their API key in it.
async fn latest_block(http: reqwest::Client, url: &Url) -> Result<u64, String> {
    let provider = ProviderBuilder::new().connect_reqwest(http, url.clone());
    provider
        .get_block_number()
        .await
        .map_err(|err| describe_failure(url, &err))
}

/// Error text that indicates a transient failure worth retrying, used only for faults
/// that arrive as prose rather than as a status code or JSON-RPC error object.
const RETRYABLE_ERROR_PATTERNS: &[&str] = &[
    // A node behind the rest of its provider's fleet, asked for a block it hasn't reached.
    "header not found",
    "unknown block",
    "connection refused",
    "connection reset",
    "connection closed",
    "timeout",
    "timed out",
    "rate limit",
    "too many requests",
    "service unavailable",
    "bad gateway",
    "temporary internal error",
];

/// How a read refused by an endpoint behind a block dipper has already seen describes it. It
/// gets quick retries, since an endpoint a few blocks behind catches up within a second, then
/// the next endpoint, rather than the backoff for a failing one.
pub(super) const BEHIND_A_SEEN_BLOCK: &str = "behind a block already seen";

/// How long a read waits before asking an endpoint behind a block already seen again: 2 blocks.
const LAG_PAUSE: Duration = Duration::from_millis(500);

/// How many times an endpoint behind a block already seen is asked again: about 4 blocks of
/// lag in all, so the read just after a transaction mines can wait out a node a little behind.
const LAG_RETRIES: u32 = 2;

/// Type alias for the provider with default fillers.
pub type HttpProvider = FillProvider<
    JoinFill<
        thegraph_core::alloy::providers::Identity,
        JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>,
    >,
    RootProvider,
>;

/// Several RPC endpoints treated as one, retried with exponential backoff on the current
/// endpoint and rotated through on failure.
#[derive(Debug)]
pub struct RpcProviderPool {
    /// Provider URLs (primary first, then fallbacks)
    providers: Vec<Url>,
    /// Current provider index (atomic for thread-safety)
    current_index: AtomicUsize,
    /// Shared across every endpoint, which is what lets connections be pooled and reused
    /// rather than reopened per call. Built once, so a call can never fail for lack of one.
    http: reqwest::Client,
    /// Maximum retries per provider before rotating
    max_retries: u32,
    /// The longest one walk of the ring can take; computed once here because the pool is
    /// what knows the schedule. The submission deadline is derived from it.
    worst_case_walk: Duration,
}

impl RpcProviderPool {
    /// Create a new RPC provider pool. Errors if no providers are configured.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "predates this lint; fix when next touched"
    )]
    pub fn new(
        providers: Vec<Url>,
        request_timeout: Duration,
        max_retries: u32,
    ) -> Result<Self, ChainClientError> {
        if providers.is_empty() {
            return Err(ChainClientError::ConfigError(
                "At least one RPC provider URL is required".to_string(),
            ));
        }

        let http = reqwest::Client::builder()
            .timeout(request_timeout)
            .build()
            .map_err(|e| {
                ChainClientError::ConfigError(format!("Failed to build HTTP client: {e}"))
            })?;

        tracing::info!(
            provider_count = providers.len(),
            primary = %endpoint_name(&providers[0]),
            "RPC provider pool initialized"
        );

        // Every endpoint spending the full request timeout on every attempt, plus the
        // backoff waited out between attempts, across one visit to each endpoint.
        let backoff: Duration = (0..max_retries).map(Self::backoff_delay).sum();
        let worst_case_walk =
            (request_timeout * (max_retries + 1) + backoff) * providers.len() as u32;

        Ok(Self {
            providers,
            current_index: AtomicUsize::new(0),
            http,
            max_retries,
            worst_case_walk,
        })
    }

    /// The longest [`execute`](Self::execute) can spend before giving up: every retry the
    /// schedule allows, on every endpoint, with the backoff between them all waited out.
    pub fn worst_case_walk(&self) -> Duration {
        self.worst_case_walk
    }

    /// How many endpoints the pool has.
    pub fn endpoint_count(&self) -> usize {
        self.providers.len()
    }

    /// Each endpoint's latest block, all asked at once with no retries. Endpoints that fail, or
    /// don't answer within `deadline`, are left out, so dipper can see whether the rest agree.
    pub async fn latest_blocks(&self, deadline: Duration) -> Vec<u64> {
        let mut asks = tokio::task::JoinSet::new();
        for url in &self.providers {
            let (http, url) = (self.http.clone(), url.clone());
            asks.spawn(async move {
                let head = tokio::time::timeout(deadline, latest_block(http, &url))
                    .await
                    .unwrap_or_else(|_| Err(format!("no answer within {deadline:?}")));
                (endpoint_name(&url), head)
            });
        }
        let mut heads = Vec::with_capacity(self.providers.len());
        while let Some(answer) = asks.join_next().await {
            match answer {
                Ok((_, Ok(head))) => heads.push(head),
                Ok((endpoint, Err(reason))) => tracing::debug!(
                    provider = %endpoint,
                    error = %reason,
                    "RPC endpoint didn't give its latest block for a cross-check"
                ),
                Err(err) => tracing::warn!(error = %err, "Latest-block cross-check task failed"),
            }
        }
        heads
    }

    /// Rotate to the next provider.
    ///
    /// Returns the new provider URL after rotation.
    pub fn rotate(&self) -> &Url {
        let old_idx = self.current_index.fetch_add(1, Ordering::Relaxed);
        self.url_at(old_idx + 1)
    }

    /// The endpoint an unbounded ring position lands on.
    fn url_at(&self, position: usize) -> &Url {
        &self.providers[position % self.providers.len()]
    }

    /// Run an RPC call, retrying the current endpoint with backoff and then rotating on to
    /// the next. `operation` names the call for logging.
    pub async fn execute<F, Fut, T>(&self, operation: &str, f: F) -> Result<T, ChainClientError>
    where
        F: Fn(HttpProvider) -> Fut,
        Fut: Future<Output = Result<T, TransportError>>,
    {
        self.execute_with_retries(operation, self.max_retries, f)
            .await
    }

    async fn execute_with_retries<F, Fut, T>(
        &self,
        operation: &str,
        max_retries: u32,
        f: F,
    ) -> Result<T, ChainClientError>
    where
        F: Fn(HttpProvider) -> Fut,
        Fut: Future<Output = Result<T, TransportError>>,
    {
        // What each endpoint said, in the order they were tried. `sign_and_send` matches
        // this text to tell a stale nonce from a transport fault, so a rejection from the
        // first endpoint has to survive a different kind of failure on the next.
        let mut reasons: Vec<String> = Vec::with_capacity(self.providers.len());
        // The first endpoint to name its refusal precisely, such as a contract rejecting the
        // call during gas estimation. Callers act on a named refusal and only log a generic
        // one, so a later endpoint merely being unreachable must not bury it.
        let mut named_refusal: Option<ChainClientError> = None;
        let mut providers_tried = 0;

        // Walk the ring by local offset from wherever the pool points. Re-reading the shared
        // index each time lets a concurrent rotation send this call back to a provider it
        // already tried, so it can give up without ever reaching the healthy one.
        let start = self.current_index.load(Ordering::Relaxed);

        loop {
            let current_url = self.url_at(start + providers_tried).clone();
            let endpoint = endpoint_name(&current_url);

            let endpoint_error = match self
                .ask_endpoint(operation, &current_url, max_retries, &f)
                .await
            {
                Ok(result) => return Ok(result),
                Err(err) => err,
            };

            let lagging = Self::is_behind(&endpoint_error);
            let reason = describe_failure(&current_url, &endpoint_error);
            reasons.push(format!("{endpoint}: {reason}"));
            providers_tried += 1;

            // Structured errors reach us boxed in through `TransportErrorKind::custom`, which
            // is how gas estimation hands back a contract rejection. Keep the first one.
            named_refusal = named_refusal.or_else(|| extract_chain_client_error(endpoint_error));

            // Check if we've tried all providers
            if providers_tried >= self.providers.len() {
                if let Some(named) = named_refusal {
                    return Err(named);
                }

                return Err(ChainClientError::RpcError(anyhow::anyhow!(
                    "All {} RPC providers failed for '{}': {}",
                    self.providers.len(),
                    operation,
                    reasons.join("; "),
                )));
            }

            // Move the shared start on, so a call beginning after this one skips the endpoint
            // that just failed. Counting failures this way lets concurrent callers wind it
            // back round to a failing endpoint, costing them the one wasted first ask.
            self.rotate();
            let next_url = self.url_at(start + providers_tried);
            if lagging {
                tracing::debug!(
                    operation,
                    old_provider = %endpoint,
                    new_provider = %endpoint_name(next_url),
                    error = %reason,
                    "Rotating RPC provider past one behind a block already seen"
                );
            } else {
                tracing::warn!(
                    operation,
                    old_provider = %endpoint,
                    new_provider = %endpoint_name(next_url),
                    providers_tried,
                    total_providers = self.providers.len(),
                    error = %reason,
                    "Rotating RPC provider after failures"
                );
            }
        }
    }

    /// Run a call against one endpoint, retrying a fault worth another go, and return what it
    /// answered or why it last failed.
    async fn ask_endpoint<F, Fut, T>(
        &self,
        operation: &str,
        url: &Url,
        max_retries: u32,
        f: &F,
    ) -> Result<T, TransportError>
    where
        F: Fn(HttpProvider) -> Fut,
        Fut: Future<Output = Result<T, TransportError>>,
    {
        let endpoint = endpoint_name(url);
        // Reused across this endpoint's attempts, so a retry does not pay for a fresh
        // TLS handshake on the path that is already running out of time.
        let provider = ProviderBuilder::new().connect_reqwest(self.http.clone(), url.clone());
        let mut endpoint_error: Option<TransportError> = None;
        let mut lag_retries = 0;
        for attempt in 0..=max_retries {
            match f(provider.clone()).await {
                Ok(result) => return Ok(result),
                Err(e)
                    if Self::is_behind(&e)
                        && lag_retries < LAG_RETRIES
                        && attempt < max_retries =>
                {
                    tracing::debug!(
                        operation,
                        provider = %endpoint,
                        error = %describe_failure(url, &e),
                        "RPC endpoint behind a block already seen, asking again"
                    );
                    lag_retries += 1;
                    tokio::time::sleep(LAG_PAUSE).await;
                    endpoint_error = Some(e);
                }
                Err(e) if Self::is_retryable(&e) && attempt < max_retries => {
                    let delay = Self::backoff_delay(attempt);
                    tracing::warn!(
                        operation,
                        provider = %endpoint,
                        attempt = attempt + 1,
                        max_retries,
                        delay_ms = delay.as_millis(),
                        error = %describe_failure(url, &e),
                        "Retryable RPC error, backing off"
                    );
                    tokio::time::sleep(delay).await;
                    endpoint_error = Some(e);
                }
                Err(e) => return Err(e),
            }
        }
        // Every attempt records why it failed before stopping, so the fallback only
        // covers a configuration that somehow allows no attempt at all.
        Err(endpoint_error.unwrap_or_else(|| TransportErrorKind::custom_str("no attempt was made")))
    }

    fn is_behind(error: &TransportError) -> bool {
        error.to_string().contains(BEHIND_A_SEEN_BLOCK)
    }

    /// Whether an error is worth trying again rather than giving up on. Each check can only
    /// say yes, so a fault the status and the error code both miss still gets read as text.
    fn is_retryable(error: &TransportError) -> bool {
        // A 5xx is the server failing for its own reasons and 429 is it declining; either
        // can succeed on a retry or another endpoint. A status is worth reading before the
        // text, because the same digits inside a revert reason mean nothing.
        if let RpcError::Transport(kind) = error
            && let Some(http) = kind.as_http_error()
            && (http.status >= 500 || http.status == 429)
        {
            return true;
        }

        // Some providers answer 200 and report being overloaded in the JSON-RPC error
        // instead, each with its own code. Alloy knows those codes, and reports a genuine
        // execution error such as a revert as not worth retrying.
        if let RpcError::ErrorResp(payload) = error
            && payload.is_retry_err()
        {
            return true;
        }

        let error_str = error.to_string().to_lowercase();
        RETRYABLE_ERROR_PATTERNS
            .iter()
            .any(|p| error_str.contains(p))
    }

    /// Calculate backoff delay for a retry attempt.
    ///
    /// Uses exponential backoff: 1s, 2s, 4s, 8s, 16s, capped at 30s.
    pub fn backoff_delay(attempt: u32) -> Duration {
        const BASE_MS: u64 = 1000;
        const MAX_MS: u64 = 30_000;

        let delay_ms = BASE_MS.saturating_mul(1u64 << attempt.min(5));
        Duration::from_millis(delay_ms.min(MAX_MS))
    }
}

#[cfg(test)]
mod tests {
    use thegraph_core::alloy::providers::Provider;
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

    use super::*;

    /// Refuses every call for a reason no retry would clear, so the pool moves straight on.
    async fn server_refusing() -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0",
                "id": 0,
                "error": { "code": -32000, "message": "refused" },
            })))
            .mount(&server)
            .await;
        server
    }

    /// Answers a block-number lookup, which is the shape of the read calls the service makes.
    async fn server_answering_block(number: u64) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0",
                "id": 0,
                "result": format!("{number:#x}"),
            })))
            .mount(&server)
            .await;
        server
    }

    /// Every read goes through this path, so a read has to fail over the way a submission does,
    /// and a fault worth another go has to earn one before it rotates. One retry keeps the
    /// backoff short; real time, since pausing it jumps the clock to the request timeout.
    #[tokio::test]
    async fn a_read_retries_a_sick_endpoint_then_rotates() {
        let sick = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(503).set_body_string("service unavailable"))
            .mount(&sick)
            .await;
        let healthy = server_answering_block(0x2a).await;

        let pool = RpcProviderPool::new(
            vec![
                sick.uri().parse().expect("sick URL"),
                healthy.uri().parse().expect("healthy URL"),
            ],
            Duration::from_secs(5),
            1,
        )
        .expect("pool");

        let block = pool
            .execute("get_block_number", |provider| async move {
                provider.get_block_number().await
            })
            .await
            .expect("the read should succeed on the second endpoint");

        assert_eq!(block, 0x2a);
        assert_eq!(
            sick.received_requests().await.unwrap_or_default().len(),
            2,
            "a read should use its retry budget before rotating"
        );
        assert_eq!(
            healthy.received_requests().await.unwrap_or_default().len(),
            1,
            "the healthy endpoint should answer on the first ask"
        );
    }

    /// A call walks its own way round the ring, so it reaches every endpoint even while other
    /// calls move the shared starting point underneath it. Reading that shared point afresh
    /// each time let a call revisit one endpoint and give up without trying another.
    #[tokio::test]
    async fn a_call_reaches_every_endpoint_even_while_others_rotate() {
        let servers = [
            server_refusing().await,
            server_refusing().await,
            server_refusing().await,
        ];
        let pool = RpcProviderPool::new(
            servers
                .iter()
                .map(|s| s.uri().parse().expect("server URL"))
                .collect(),
            Duration::from_secs(5),
            0,
        )
        .expect("pool");

        let call = pool.execute("probe", |provider| async move {
            tokio::task::yield_now().await;
            provider.get_block_number().await
        });
        let others_rotating = async {
            for _ in 0..64 {
                pool.rotate();
                tokio::task::yield_now().await;
            }
        };
        let (result, ()) = tokio::join!(call, others_rotating);

        assert!(result.is_err(), "every endpoint refused, so the call fails");
        for (i, server) in servers.iter().enumerate() {
            assert_eq!(
                server.received_requests().await.unwrap_or_default().len(),
                1,
                "endpoint {i} should have been tried exactly once"
            );
        }
    }

    /// Hosted RPC endpoints carry their API key in the path, and this failure text reaches the
    /// logs and every error built from it, so it has to say which endpoint refused without
    /// repeating the key that gets it in.
    #[tokio::test]
    async fn a_failure_names_the_endpoint_without_its_api_key() {
        let refusing = server_refusing().await;
        let keyed: Url = format!("{}/v2/super-secret-key", refusing.uri())
            .parse()
            .expect("keyed endpoint URL");

        let pool = RpcProviderPool::new(vec![keyed], Duration::from_secs(5), 0).expect("pool");
        let err = pool
            .execute("probe", |provider| async move {
                provider.get_block_number().await
            })
            .await
            .expect_err("the endpoint refused, so the call fails");

        let text = err.to_string();
        assert!(
            !text.contains("super-secret-key"),
            "the API key must not appear in the failure: {text}"
        );
        assert!(
            text.contains("127.0.0.1"),
            "the failure should still say which endpoint refused: {text}"
        );
    }

    /// A contract refusing the call is the chain's answer, not one endpoint's, and the caller
    /// reads it to decide whether to give up rather than retry. A later endpoint being merely
    /// unreachable must not turn that into a generic fault that reads as worth another go.
    #[tokio::test]
    async fn a_named_refusal_outlives_a_later_endpoint_going_dark() {
        let pool = RpcProviderPool::new(
            vec![
                Url::parse("http://refusing.invalid").expect("refusing endpoint URL"),
                Url::parse("http://dark.invalid").expect("dark endpoint URL"),
            ],
            Duration::from_secs(5),
            0,
        )
        .expect("pool");

        let calls = AtomicUsize::new(0);
        let err = pool
            .execute("probe", |_provider| {
                let refusing = calls.fetch_add(1, Ordering::Relaxed) == 0;
                async move {
                    let outcome: Result<(), TransportError> = Err(if refusing {
                        TransportErrorKind::custom(ChainClientError::ContractRevert {
                            selector: [0xde, 0xad, 0xbe, 0xef],
                            data: Default::default(),
                        })
                    } else {
                        TransportErrorKind::custom_str("connection refused")
                    });
                    outcome
                }
            })
            .await
            .expect_err("both endpoints failed, so the call fails");

        assert!(
            matches!(err, ChainClientError::ContractRevert { .. }),
            "the contract's refusal should have survived, got {err}"
        );
    }

    /// The latest-block cross-check logs each endpoint's failure, so it must hide the key too.
    #[tokio::test]
    async fn a_cross_check_failure_hides_the_api_key() {
        let keyed: Url = "http://127.0.0.1:1/v2/super-secret-key"
            .parse()
            .expect("keyed endpoint URL");

        let reason = latest_block(reqwest::Client::new(), &keyed)
            .await
            .expect_err("nothing is listening, so the ask fails");

        assert!(
            !reason.contains("super-secret-key"),
            "the API key must not appear in the failure: {reason}"
        );
    }

    /// Reads wait on the cross-check, so 1 endpoint that never answers must not hold it for the
    /// whole request timeout. The healthy endpoint's head still counts.
    #[tokio::test]
    async fn a_cross_check_stops_waiting_for_a_hung_endpoint() {
        let healthy = server_answering_block(0x2a).await;
        let hung = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(30)))
            .mount(&hung)
            .await;
        let pool = RpcProviderPool::new(
            vec![
                healthy.uri().parse().expect("healthy URL"),
                hung.uri().parse().expect("hung URL"),
            ],
            Duration::from_secs(60),
            0,
        )
        .expect("pool");

        let heads = tokio::time::timeout(
            Duration::from_secs(2),
            pool.latest_blocks(Duration::from_millis(200)),
        )
        .await
        .expect("the cross-check should give up on the hung endpoint at its deadline");

        assert_eq!(heads, vec![0x2a]);
    }

    /// An endpoint that answers can only describe its own refusal, so it never repeats the
    /// URL. One that never answers is described by the HTTP client instead, which says which
    /// URL it was reaching for, and that is where the key sits. Nothing listens on port 1.
    #[tokio::test]
    async fn a_connection_that_gets_no_answer_hides_the_api_key() {
        let keyed: Url = "http://127.0.0.1:1/v2/super-secret-key"
            .parse()
            .expect("keyed endpoint URL");

        let pool = RpcProviderPool::new(vec![keyed], Duration::from_secs(5), 0).expect("pool");
        let err = pool
            .execute("probe", |provider| async move {
                provider.get_block_number().await
            })
            .await
            .expect_err("nothing is listening, so the call fails");

        let text = err.to_string();
        assert!(
            !text.contains("super-secret-key"),
            "the API key must not appear in the failure: {text}"
        );
        assert!(
            text.contains("127.0.0.1:1"),
            "the failure should still say which endpoint was unreachable: {text}"
        );
    }

    /// Reporting only the last endpoint's reason hid what the earlier ones said, and a caller
    /// reading this text to tell a chain rejection from a transport fault would then miss the
    /// rejection and skip the recovery it calls for.
    #[tokio::test]
    async fn a_failure_names_every_endpoint_that_was_tried() {
        let servers = [server_refusing().await, server_refusing().await];
        let pool = RpcProviderPool::new(
            servers
                .iter()
                .map(|s| s.uri().parse().expect("server URL"))
                .collect(),
            Duration::from_secs(5),
            0,
        )
        .expect("pool");

        let err = pool
            .execute("probe", |provider| async move {
                provider.get_block_number().await
            })
            .await
            .expect_err("every endpoint refused, so the call fails");

        let text = err.to_string();
        for server in &servers {
            let named = endpoint_name(&server.uri().parse().expect("server URL"));
            assert!(text.contains(&named), "{named} is missing from: {text}");
        }
        assert_eq!(
            text.matches("refused").count(),
            servers.len(),
            "every endpoint's own reason should appear: {text}"
        );
    }

    /// 500 is the status a provider answered on 2026-07-29 while an accepted agreement went
    /// unfunded. Reading it here is what earns a retry on that provider before rotating; the
    /// rotation itself is unconditional, so this decides attempts rather than failover.
    #[test]
    fn server_faults_are_retryable_by_status() {
        for status in [500, 502, 503, 504, 429] {
            let err = TransportErrorKind::http_error(status, "provider fault".to_string());
            assert!(
                RpcProviderPool::is_retryable(&err),
                "HTTP {status} should be retryable"
            );
        }
    }

    /// A 4xx other than 429 means the request itself is wrong, so resending it unchanged
    /// to the same or another provider cannot succeed.
    #[test]
    fn client_faults_are_not_retryable_by_status() {
        for status in [400, 401, 403, 404] {
            let err = TransportErrorKind::http_error(status, "bad request".to_string());
            assert!(
                !RpcProviderPool::is_retryable(&err),
                "HTTP {status} should not be retryable"
            );
        }
    }

    /// Some providers and the proxies in front of them report throttling under a 4xx rather
    /// than a 429. The status alone reads as "your request is wrong", so the wording is what
    /// tells this apart from a request that will be refused however often it is sent.
    #[test]
    fn throttling_described_in_a_client_fault_body_is_retryable() {
        let err = TransportErrorKind::http_error(403, "rate limit exceeded".to_string());
        assert!(
            RpcProviderPool::is_retryable(&err),
            "a 403 that explains it is throttling should be retryable"
        );
    }

    /// Alloy recognises the error codes providers use for throttling, but not every
    /// transient fault has one. A gateway that describes a timeout in an otherwise ordinary
    /// response is still worth another go, and only the wording says so.
    #[test]
    fn transient_faults_described_in_a_json_rpc_error_are_retryable() {
        let payload = serde_json::from_str(r#"{"code":-32603,"message":"connection reset"}"#)
            .expect("JSON-RPC error payload");
        let err: TransportError = RpcError::ErrorResp(payload);
        assert!(
            RpcProviderPool::is_retryable(&err),
            "a reset described in the error body should be retryable"
        );
    }

    /// The provider that stranded an accepted agreement on 2026-07-29 answered `code 19
    /// Temporary internal error. Please retry`, a code alloy does not know. Sent under a 200
    /// the status says nothing either, so the wording is the only thing left to read.
    #[test]
    fn a_temporary_internal_error_without_a_status_is_retryable() {
        let payload = serde_json::from_str(
            r#"{"code":19,"message":"Temporary internal error. Please retry"}"#,
        )
        .expect("JSON-RPC error payload");
        let err: TransportError = RpcError::ErrorResp(payload);
        assert!(
            RpcProviderPool::is_retryable(&err),
            "the fault behind the outage should be retryable however it is reported"
        );
    }

    /// Hosted endpoints spread calls across nodes, so one a block behind is routine: it is
    /// asked again after short pauses, then passed over, never backed off from.
    #[tokio::test]
    async fn an_endpoint_behind_a_block_already_seen_gets_quick_retries() {
        let lagging = server_answering_block(1).await;
        let current = server_answering_block(2).await;
        let pool = RpcProviderPool::new(
            vec![
                lagging.uri().parse().expect("lagging URL"),
                current.uri().parse().expect("current URL"),
            ],
            Duration::from_secs(5),
            3,
        )
        .expect("pool");
        let started = std::time::Instant::now();

        let block = pool
            .execute("get_block_number", |provider| async move {
                let block = provider.get_block_number().await?;
                if block < 2 {
                    return Err(TransportErrorKind::custom_str(BEHIND_A_SEEN_BLOCK));
                }
                Ok(block)
            })
            .await
            .expect("read from the endpoint that has the block");

        assert_eq!(block, 2);
        assert_eq!(
            lagging.received_requests().await.unwrap_or_default().len(),
            3
        );
        assert!(started.elapsed() < Duration::from_secs(2), "no backoff");
    }

    #[test]
    fn a_node_that_has_not_reached_a_block_yet_is_retryable() {
        let payload = serde_json::from_str(r#"{"code":-32000,"message":"header not found"}"#)
            .expect("JSON-RPC error payload");
        let err: TransportError = RpcError::ErrorResp(payload);
        assert!(RpcProviderPool::is_retryable(&err));
    }

    #[test]
    fn each_retry_waits_twice_as_long_up_to_a_ceiling() {
        // 1s, 2s, 4s, 8s, 16s, 32s->30s
        assert_eq!(RpcProviderPool::backoff_delay(0), Duration::from_secs(1));
        assert_eq!(RpcProviderPool::backoff_delay(1), Duration::from_secs(2));
        assert_eq!(RpcProviderPool::backoff_delay(2), Duration::from_secs(4));
        assert_eq!(RpcProviderPool::backoff_delay(3), Duration::from_secs(8));
        assert_eq!(RpcProviderPool::backoff_delay(4), Duration::from_secs(16));
        assert_eq!(RpcProviderPool::backoff_delay(5), Duration::from_secs(30)); // capped at 30s
        assert_eq!(RpcProviderPool::backoff_delay(10), Duration::from_secs(30)); // stays capped
    }

    /// Faults that arrive with no status and no error code, only a description, which is
    /// what a connection that never got a reply looks like.
    #[test]
    fn faults_described_only_in_words_are_read_from_the_text() {
        let retryable_errors = [
            "connection refused by remote host",
            "Connection Reset by peer",
            "request TIMEOUT exceeded",
            "Service Unavailable",
            "Bad Gateway",
            "rate limit exceeded",
            "too many requests, slow down",
        ];

        for err_str in retryable_errors {
            let err = TransportErrorKind::custom_str(err_str);
            assert!(
                RpcProviderPool::is_retryable(&err),
                "expected '{err_str}' to be retryable"
            );
        }

        let non_retryable_errors = [
            "nonce too low",
            "insufficient funds",
            "execution reverted",
            "invalid signature",
        ];

        for err_str in non_retryable_errors {
            let err = TransportErrorKind::custom_str(err_str);
            assert!(
                !RpcProviderPool::is_retryable(&err),
                "expected '{err_str}' to not be retryable"
            );
        }
    }

    #[test]
    fn a_pool_with_no_endpoints_is_refused() {
        let result = RpcProviderPool::new(vec![], Duration::from_secs(30), 3);
        assert!(result.is_err());

        let err = result.unwrap_err();
        match err {
            ChainClientError::ConfigError(msg) => {
                assert!(msg.contains("At least one RPC provider"));
            }
            _ => panic!("Expected ConfigError"),
        }
    }

    /// The submission deadline is derived from this figure, so it has to count every
    /// attempt the schedule allows and the backoff waited out between them.
    #[test]
    fn the_worst_case_walk_counts_every_attempt_and_backoff() {
        let providers = vec![
            Url::parse("https://rpc1.example.com").unwrap(),
            Url::parse("https://rpc2.example.com").unwrap(),
        ];
        let pool = RpcProviderPool::new(providers, Duration::from_secs(10), 3).unwrap();

        // Per endpoint: 4 attempts of 10s plus 1+2+4s of backoff, across 2 endpoints.
        assert_eq!(pool.worst_case_walk(), Duration::from_secs(94));
    }

    #[test]
    fn rotating_walks_the_endpoints_and_wraps_round() {
        let providers = vec![
            Url::parse("https://rpc1.example.com").unwrap(),
            Url::parse("https://rpc2.example.com").unwrap(),
            Url::parse("https://rpc3.example.com").unwrap(),
        ];

        let pool = RpcProviderPool::new(providers.clone(), Duration::from_secs(30), 3).unwrap();

        assert_eq!(pool.rotate().as_str(), "https://rpc2.example.com/");
        assert_eq!(pool.rotate().as_str(), "https://rpc3.example.com/");

        // Wraps back round rather than running off the end.
        assert_eq!(pool.rotate().as_str(), "https://rpc1.example.com/");
    }
}
