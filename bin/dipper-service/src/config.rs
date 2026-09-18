//! Dipper service configuration

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
    path::Path,
    sync::Arc,
    time::Duration,
};

use dipper_core::config::{Hidden, HiddenSecretKeyAsHexStr};
use dipper_producer::kafka::{KafkaConfig, KafkaConsumerConfig};
use serde_with::serde_as;
use thegraph_core::alloy::{
    primitives::{Address, ChainId, U256},
    signers::k256::SecretKey,
};
use url::Url;

/// The maximum number of candidates to select.
pub const DEFAULT_MAX_CANDIDATES: usize = 3;

/// Load the configuration from a JSON file.
pub fn load_from_file(path: &Path) -> Result<Config, Error> {
    let config_content = std::fs::read_to_string(path)?;
    let config = serde_json::from_str(&config_content)?;
    Ok(config)
}

/// An error that can occur when loading the configuration.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An error occurred while reading the configuration file.
    #[error("failed to read configuration file: {0}")]
    Io(#[from] std::io::Error),

    /// An error occurred while deserializing the configuration.
    #[error("failed to deserialize configuration: {0}")]
    Deserialize(#[from] serde_json::Error),
}

/// The configuration for the DIPs service
#[derive(custom_debug::CustomDebug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The DIPs agreement configuration
    pub dips: DipsAgreementConfig,
    /// The Admin RPC server configuration
    pub admin_rpc: AdminRpcConfig,
    /// The database configuration
    pub db: DbConfig,
    /// The indexer URLs service configuration
    pub indexer_urls: IndexerUrlsConfig,
    /// The signer configuration
    pub signer: SignerConfig,
    /// The IISA (Indexing Indexer Selection Algorithm) service configuration
    pub iisa: IisaConfig,
    /// The indexer gRPC client configuration (for sending RCA proposals)
    #[serde(default)]
    pub indexer_client: IndexerClientConfig,
    /// The reassignment service configuration
    #[serde(default)]
    pub reassignment: Option<ReassignmentConfig>,
    /// The expiration service configuration (marks stale Created agreements as Expired)
    #[serde(default)]
    pub expiration: Option<ExpirationConfig>,
    /// The chain listener service configuration (monitors on-chain events)
    #[serde(default)]
    pub chain_listener: Option<ChainListenerConfig>,
    /// The liveness checker service configuration (detects silent agreement abandonment)
    #[serde(default)]
    pub liveness_checker: Option<LivenessCheckerConfig>,
    /// The escrow reconciler service configuration (AgreementManager mode only)
    #[serde(default)]
    pub escrow_reconciler: Option<EscrowReconcilerConfig>,
    /// Additional chain ID to network name mappings for dev/test chains.
    ///
    /// Production chains are resolved via the graph-networks-registry crate.
    /// This map supplements the registry with chains that aren't in the official
    /// registry (e.g. `1337 = "hardhat"` for local development).
    #[serde(default)]
    pub additional_networks: BTreeMap<ChainId, String>,
    /// The chain client configuration (for sending on-chain transactions)
    #[serde(default)]
    pub chain_client: Option<ChainClientConfig>,
    /// Events configuration for sending dipper events on the configured topic for streaming
    #[serde(default)]
    pub event_streaming_config: Option<EventStreamingConfig>,
    /// The Studio indexing request consumer configuration (reads subgraph
    /// indexing requests from a Redpanda topic; absent means off)
    #[serde(default)]
    pub indexing_request_consumer: Option<IndexingRequestConsumerConfig>,
    /// Number of concurrent worker loops draining the job queue (default: 8).
    /// Each loop can hold up to three pooled DB connections at once and shares
    /// the pool with the registry and background services; size accordingly.
    #[serde(default = "default_worker_concurrency")]
    pub worker_concurrency: usize,
    /// The HTTP health endpoint configuration. The health server is on by
    /// default so the liveness probe always has an endpoint to hit; omitting
    /// this section keeps the defaults, and setting `enabled` to false turns
    /// the server off.
    #[serde(default)]
    pub health: HealthConfig,
}

/// Configuration for the HTTP health endpoint used by orchestrator liveness probes. Omitting the
/// section leaves a working server on 0.0.0.0:8546, so a pod that never configured one still
/// answers the probe; set `enabled` to false or move `listen_addr` wherever 8546 is not free.
#[serde_as]
#[derive(Debug, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HealthConfig {
    /// Whether to start the health server at all. On by default; set to false
    /// to disable the endpoint entirely.
    pub enabled: bool,

    /// The health server listen address (e.g. `0.0.0.0:8546`).
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub listen_addr: std::net::SocketAddr,

    /// Staleness threshold in seconds after which the worker is reported
    /// unhealthy. Defaults to [`crate::health::DEFAULT_HEALTH_THRESHOLD`].
    #[serde_as(as = "serde_with::DurationSeconds")]
    pub threshold: Duration,
}

impl HealthConfig {
    /// Rejects a health listener that cannot serve the orchestrator's probe.
    pub fn validate(&self, admin_rpc_listen_addr: std::net::SocketAddr) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }
        // Port 0 asks the OS for whatever is free, but the probe targets a fixed port, so every
        // check would be refused and the pod would restart forever.
        if self.listen_addr.port() == 0 {
            return Err(
                "health.listen_addr must name a fixed port: port 0 picks an arbitrary one that \
                 the liveness probe cannot reach"
                    .to_string(),
            );
        }
        // The health listener binds first and wins the port; the admin RPC server then fails to
        // bind, leaving a pod that reports healthy while serving no admin traffic at all. Ports
        // are compared without the interface, since a wildcard bind takes the port everywhere.
        if self.listen_addr.port() == admin_rpc_listen_addr.port() {
            return Err(format!(
                "health.listen_addr and admin_rpc.listen_addr both use port {}: give the health \
                 endpoint a port of its own",
                self.listen_addr.port()
            ));
        }
        Ok(())
    }
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            listen_addr: default_health_listen_addr(),
            threshold: crate::health::DEFAULT_HEALTH_THRESHOLD,
        }
    }
}

fn default_health_listen_addr() -> std::net::SocketAddr {
    std::net::SocketAddr::from(([0, 0, 0, 0], 8546))
}

/// The IISA (Indexing Indexer Selection Algorithm) service configuration
#[serde_as]
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IisaConfig {
    /// The IISA service endpoint URL (e.g., "http://iisa-service:8080")
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub endpoint: Url,

    /// Request timeout in seconds (default: 30)
    #[serde(default = "default_request_timeout")]
    #[serde_as(as = "serde_with::DurationSeconds")]
    pub request_timeout: Duration,

    /// Connection timeout in seconds (default: 10)
    #[serde(default = "default_connect_timeout")]
    #[serde_as(as = "serde_with::DurationSeconds")]
    pub connect_timeout: Duration,

    /// Maximum retry attempts for transient failures (default: 3).
    ///
    /// This is the number of *additional* attempts after the initial request fails.
    /// For example, `max_retries = 3` means up to 4 total attempts (1 initial + 3 retries).
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// Bearer token for IISA's authenticated `GET /dips-indexers` endpoint, used by
    /// the unresponsive breaker. Must match IISA_PUSH_TOKEN. None = no auth header.
    #[serde(default)]
    pub push_token: Option<Hidden<String>>,
}

fn default_request_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_connect_timeout() -> Duration {
    Duration::from_secs(10)
}

fn default_max_retries() -> u32 {
    3
}

/// Indexer gRPC client configuration (for sending RCA proposals to indexers)
#[serde_as]
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerClientConfig {
    /// Request timeout in seconds (default: 240).
    ///
    /// This must be long enough to cover indexer-rs IPFS retry worst case (190s)
    /// plus buffer. indexer-rs retries IPFS fetches up to 4 times with exponential
    /// backoff (30s timeout + 10s/20s/40s delays = 190s worst case).
    #[serde(default = "default_indexer_request_timeout")]
    #[serde_as(as = "serde_with::DurationSeconds")]
    pub request_timeout: Duration,

    /// Connection timeout in seconds (default: 10)
    #[serde(default = "default_indexer_connect_timeout")]
    #[serde_as(as = "serde_with::DurationSeconds")]
    pub connect_timeout: Duration,

    /// Maximum retry attempts for transient failures (default: 3).
    ///
    /// With `max_retries = 3`, up to 4 total attempts are made (1 initial + 3 retries).
    /// Retries use exponential backoff (1s, 2s, 4s, ...) and only occur on
    /// transient gRPC errors (UNAVAILABLE, RESOURCE_EXHAUSTED, ABORTED, DEADLINE_EXCEEDED).
    #[serde(default = "default_indexer_max_retries")]
    pub max_retries: u32,
}

fn default_indexer_request_timeout() -> Duration {
    Duration::from_secs(240) // 190s IPFS worst case + 50s buffer
}

fn default_indexer_connect_timeout() -> Duration {
    Duration::from_secs(10)
}

fn default_indexer_max_retries() -> u32 {
    3
}

impl Default for IndexerClientConfig {
    fn default() -> Self {
        Self {
            request_timeout: default_indexer_request_timeout(),
            connect_timeout: default_indexer_connect_timeout(),
            max_retries: default_indexer_max_retries(),
        }
    }
}

/// Configuration for the periodic reassignment service
#[serde_as]
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReassignmentConfig {
    /// Whether the reassignment service is enabled (default: true)
    #[serde(default = "default_reassignment_enabled")]
    pub enabled: bool,

    /// Interval between reassignment cycles in seconds (default: 86400s / 24 hours)
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(default = "default_reassignment_interval")]
    pub interval: Duration,

    /// Hour of day (UTC, 0-23) to run the reassignment cycle (default: 10, i.e., 10:00 UTC)
    ///
    /// The first cycle will be delayed until this hour, then subsequent cycles
    /// run at the configured interval. This allows alignment with upstream data
    /// refresh schedules (e.g., IISA score computation runs at 09:00 UTC).
    #[serde(
        default = "default_reassignment_run_at_utc_hour",
        deserialize_with = "deserialize_utc_hour"
    )]
    pub run_at_utc_hour: u8,

    /// Maximum number of requests to process per cycle (default: 100, 0 = unlimited)
    #[serde(default = "default_reassignment_batch_size")]
    pub batch_size: i64,

    /// Minimum age of requests to consider for reassessment in seconds (default: 86400s)
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(default = "default_reassignment_min_age")]
    pub min_request_age: Duration,
}

fn default_reassignment_enabled() -> bool {
    true
}

fn default_reassignment_interval() -> Duration {
    Duration::from_secs(86400) // 24 hours
}

fn deserialize_utc_hour<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<u8, D::Error> {
    let hour = <u8 as serde::Deserialize>::deserialize(deserializer)?;
    if hour > 23 {
        return Err(serde::de::Error::custom(format!(
            "run_at_utc_hour must be 0-23, got {hour}"
        )));
    }
    Ok(hour)
}

fn default_reassignment_run_at_utc_hour() -> u8 {
    10 // 10:00 UTC, 1 hour after IISA score computation at 09:00 UTC
}

fn default_reassignment_batch_size() -> i64 {
    100
}

fn default_reassignment_min_age() -> Duration {
    Duration::from_secs(86400)
}

impl Default for ReassignmentConfig {
    fn default() -> Self {
        Self {
            enabled: default_reassignment_enabled(),
            interval: default_reassignment_interval(),
            run_at_utc_hour: default_reassignment_run_at_utc_hour(),
            batch_size: default_reassignment_batch_size(),
            min_request_age: default_reassignment_min_age(),
        }
    }
}

/// Configuration for the deadline expiration service.
///
/// This service periodically scans for `Created` agreements whose RCA deadline
/// has passed, marks them as `Expired`, and triggers IISA reassessment to find
/// replacement indexers.
#[serde_as]
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExpirationConfig {
    /// Whether the expiration service is enabled (default: true)
    #[serde(default = "default_expiration_enabled")]
    pub enabled: bool,

    /// Interval between expiration scans in seconds (default: 90s)
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(default = "default_expiration_interval")]
    pub interval: Duration,

    /// Maximum agreements to process per cycle (default: 100)
    #[serde(default = "default_expiration_batch_size")]
    pub batch_size: i64,

    /// Grace period (chain seconds) held back past the deadline before an
    /// agreement is marked `Expired` (default: 300s).
    ///
    /// The local `Created` row lags the chain, so an indexer's on-chain accept
    /// within the deadline may not be reflected locally the instant the deadline
    /// passes. Waiting this margin past the deadline lets the chain_listener sync
    /// a within-deadline accept (flipping the row to `AcceptedOnChain`) before we
    /// consider it expired -- preventing a premature `expired` event that would
    /// contradict a subsequent `accepted`. Set to cover the worst-case subgraph
    /// sync lag.
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(default = "default_expiration_grace")]
    pub grace: Duration,
}

fn default_expiration_enabled() -> bool {
    true
}

fn default_expiration_interval() -> Duration {
    Duration::from_secs(90)
}

fn default_expiration_batch_size() -> i64 {
    100
}

fn default_expiration_grace() -> Duration {
    Duration::from_secs(300)
}

fn default_worker_concurrency() -> usize {
    8
}

impl Default for ExpirationConfig {
    fn default() -> Self {
        Self {
            enabled: default_expiration_enabled(),
            interval: default_expiration_interval(),
            batch_size: default_expiration_batch_size(),
            grace: default_expiration_grace(),
        }
    }
}

/// Escrow reconciler service config. Runs only in `AgreementManager` mode;
/// each tick calls the manager's permissionless `reconcileProvider` for
/// distinct providers with agreements needing escrow cleanup.
#[serde_as]
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EscrowReconcilerConfig {
    /// Whether the escrow reconciler is enabled (default: true).
    #[serde(default = "default_escrow_reconciler_enabled")]
    pub enabled: bool,

    /// Interval between reconciliation sweeps in seconds (default: 600s).
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(default = "default_escrow_reconciler_interval")]
    pub interval: Duration,

    /// Maximum distinct providers to reconcile per sweep (default: 500).
    #[serde(default = "default_escrow_reconciler_batch_size")]
    pub batch_size: i64,
}

fn default_escrow_reconciler_enabled() -> bool {
    true
}

fn default_escrow_reconciler_interval() -> Duration {
    Duration::from_secs(600)
}

fn default_escrow_reconciler_batch_size() -> i64 {
    500
}

impl Default for EscrowReconcilerConfig {
    fn default() -> Self {
        Self {
            enabled: default_escrow_reconciler_enabled(),
            interval: default_escrow_reconciler_interval(),
            batch_size: default_escrow_reconciler_batch_size(),
        }
    }
}

/// Configuration for the liveness checker service.
///
/// This service periodically polls each indexer's status endpoint to verify that
/// indexing is progressing. Agreements where no block height progress is observed
/// within the tolerance window are canceled as payer and reassigned.
#[serde_as]
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LivenessCheckerConfig {
    /// Whether the liveness checker is enabled (default: false).
    ///
    /// Disabled by default since it requires the chain client to be configured
    /// for on-chain cancellation.
    #[serde(default = "default_liveness_checker_enabled")]
    pub enabled: bool,

    /// Interval between liveness checks in seconds (default: 300s / 5 min).
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(default = "default_liveness_checker_interval")]
    pub interval: Duration,

    /// Maximum tolerance window in days (default: 4).
    ///
    /// The actual threshold scales with the number of active agreements on the
    /// deployment: `min(active_count, max_tolerance_days)` days.
    #[serde(default = "default_liveness_checker_max_tolerance_days")]
    pub max_tolerance_days: u32,

    /// Timeout per indexer status HTTP request in seconds (default: 10s).
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(default = "default_liveness_checker_request_timeout")]
    pub request_timeout: Duration,

    /// Maximum agreements to fetch per cycle (default: 500).
    #[serde(default = "default_liveness_checker_batch_size")]
    pub batch_size: i64,
}

fn default_liveness_checker_enabled() -> bool {
    false
}

fn default_liveness_checker_interval() -> Duration {
    Duration::from_secs(300)
}

fn default_liveness_checker_max_tolerance_days() -> u32 {
    4
}

fn default_liveness_checker_request_timeout() -> Duration {
    Duration::from_secs(10)
}

fn default_liveness_checker_batch_size() -> i64 {
    500
}

impl Default for LivenessCheckerConfig {
    fn default() -> Self {
        Self {
            enabled: default_liveness_checker_enabled(),
            interval: default_liveness_checker_interval(),
            max_tolerance_days: default_liveness_checker_max_tolerance_days(),
            request_timeout: default_liveness_checker_request_timeout(),
            batch_size: default_liveness_checker_batch_size(),
        }
    }
}

/// Configuration for the on-chain event listener service.
///
/// This service monitors the SubgraphService contract for `IndexingAgreementAccepted`
/// and `IndexingAgreementCanceled` events via a subgraph. When a `Rejected` agreement
/// is accepted on-chain, it triggers automatic cancellation via `cancelIndexingAgreementByPayer`.
#[serde_as]
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainListenerConfig {
    /// Whether the chain listener service is enabled (default: false)
    ///
    /// Disabled by default since it requires subgraph configuration.
    #[serde(default = "default_chain_listener_enabled")]
    pub enabled: bool,

    /// The subgraph endpoint URL for querying indexing agreement events.
    ///
    /// This should point to a subgraph that indexes the SubgraphService contract's
    /// IndexingAgreementAccepted and IndexingAgreementCanceled events.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub subgraph_endpoint: Url,

    /// API key for subgraph authentication (optional for local/test subgraphs).
    #[serde(default)]
    pub subgraph_api_key: Option<String>,

    /// Chain ID for state tracking (default: 42161 for Arbitrum One)
    #[serde(default = "default_chain_id")]
    pub chain_id: u64,

    /// Poll interval in seconds (default: 30s)
    ///
    /// How often to query the subgraph for new events. Since subgraphs have some
    /// indexing latency, polling more frequently than ~30s provides diminishing returns.
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(default = "default_chain_listener_poll_interval")]
    pub poll_interval: Duration,

    /// Request timeout in seconds (default: 30)
    #[serde(default = "default_chain_listener_request_timeout")]
    #[serde_as(as = "serde_with::DurationSeconds")]
    pub request_timeout: Duration,

    /// Maximum retry attempts for transient failures (default: 3)
    #[serde(default = "default_chain_listener_max_retries")]
    pub max_retries: u32,

    /// Number of blocks at the tail of the chain that every poll re-reads,
    /// so a reorg that moves a state change across the cursor boundary is
    /// still picked up. Set to 0 to disable.
    #[serde(default = "default_chain_listener_reorg_buffer_blocks")]
    pub reorg_buffer_blocks: u32,

    /// How far ahead of the host's wall clock the subgraph's reported
    /// chain timestamp may sit before the response is rejected as
    /// corrupt. Default 60s covers typical NTP drift; widen if the
    /// host clock is known to lag.
    #[serde(default = "default_chain_listener_wall_clock_skew_tolerance_secs")]
    pub wall_clock_skew_tolerance_secs: u64,

    /// How much faster than wall-clock the persisted chain timestamp
    /// may advance per poll before the listener caps the advance.
    /// Chain time legitimately moves at ~1s per wall second; the
    /// tolerance covers poll-cadence jitter and subgraph-side rounding.
    /// Widen for environments with choppy poll cadence.
    #[serde(default = "default_chain_listener_chain_ts_drift_tolerance_secs")]
    pub chain_ts_drift_tolerance_secs: u64,

    /// Bypass every defense that compares chain timestamps against
    /// the host's wall clock. Intended exclusively for local-network
    /// testing, where `evm_increaseTime` deliberately advances chain
    /// time by hours or days while wall-clock stays put.
    ///
    /// When true:
    ///   * the subgraph timestamp skew check is skipped
    ///   * the per-poll chain timestamp drift cap is skipped
    ///   * agreement deadlines are computed from chain time instead
    ///     of wall time, so freshly created agreements do not appear
    ///     born-expired against an advanced chain
    ///
    /// MUST be false in production. With the flag on, a hostile
    /// subgraph can poison the persisted chain timestamp without
    /// restraint, prematurely expiring real agreements.
    #[serde(default)]
    pub bypass_chain_clock_defenses: bool,
}

fn default_chain_listener_enabled() -> bool {
    false
}

fn default_chain_id() -> u64 {
    42161 // Arbitrum One
}

fn default_chain_listener_poll_interval() -> Duration {
    Duration::from_secs(30)
}

fn default_chain_listener_request_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_chain_listener_max_retries() -> u32 {
    3
}

fn default_chain_listener_reorg_buffer_blocks() -> u32 {
    20
}

fn default_chain_listener_wall_clock_skew_tolerance_secs() -> u64 {
    60
}

fn default_chain_listener_chain_ts_drift_tolerance_secs() -> u64 {
    10
}

fn default_gas_price_multiplier() -> f64 {
    1.2
}

fn default_max_gas_price_gwei() -> u64 {
    100
}

/// Configuration for the on-chain transaction client.
///
/// This client sends transactions to the blockchain, such as calling
/// `cancelIndexingAgreementByPayer` on the SubgraphService contract.
/// It supports multiple RPC providers with automatic failover and retry.
#[serde_as]
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainClientConfig {
    /// Whether the chain client is enabled (default: false). Required in practice: dipper
    /// fetches the RecurringCollector's EIP-712 domain on-chain at startup and refuses to start
    /// unless this is `true` with at least one RPC provider — there is no chain-less mode.
    #[serde(default = "default_chain_client_enabled")]
    pub enabled: bool,

    /// List of RPC provider URLs (first is primary, rest are fallbacks).
    ///
    /// At least one provider is required when enabled. Providers are tried in order,
    /// rotating to the next on persistent failures.
    pub providers: Vec<Url>,

    /// Request timeout per RPC call in seconds (default: 10s). Every call here is a small one,
    /// so this is headroom rather than a working limit, and it multiplies with `max_retries`.
    #[serde(default = "default_chain_client_request_timeout")]
    #[serde_as(as = "serde_with::DurationSeconds")]
    pub request_timeout: Duration,

    /// Maximum retry attempts before rotating to next provider (default: 3)
    ///
    /// Uses exponential backoff (1s, 2s, 4s...) between retries.
    #[serde(default = "default_chain_client_max_retries")]
    pub max_retries: u32,

    /// Seconds between background re-fetches of the RecurringCollector's
    /// EIP-712 signing domain (default: 3600). See `chain_client::eip5267`.
    #[serde(default = "default_domain_refresh_interval")]
    #[serde_as(as = "serde_with::DurationSeconds")]
    pub domain_refresh_interval: Duration,

    /// Gas price multiplier (default: 1.2)
    ///
    /// Applied to the estimated gas price to ensure timely inclusion.
    #[serde(default = "default_gas_price_multiplier")]
    pub gas_price_multiplier: f64,

    /// Maximum gas price in gwei (default: 100)
    ///
    /// Transactions will fail if the gas price exceeds this limit.
    #[serde(default = "default_max_gas_price_gwei")]
    pub max_gas_price_gwei: u64,

    /// Gas limit buffer multiplier (default: 2.0)
    ///
    /// The estimated gas is multiplied by this value, then bounded by
    /// floor and ceiling.
    #[serde(default = "default_gas_buffer_multiplier")]
    pub gas_buffer_multiplier: f64,

    /// Minimum gas limit floor (default: 100,000)
    ///
    /// Even if the estimate is lower, this floor is applied.
    #[serde(default = "default_gas_floor")]
    pub gas_floor: u64,

    /// Maximum gas addition above estimate (default: 200,000)
    ///
    /// The gas limit is capped at estimate + this value.
    #[serde(default = "default_gas_max_addition")]
    pub gas_max_addition: u64,
}

fn default_chain_client_enabled() -> bool {
    false
}

pub(crate) fn default_chain_client_request_timeout() -> Duration {
    Duration::from_secs(10)
}

fn default_domain_refresh_interval() -> Duration {
    Duration::from_secs(3600)
}

pub(crate) fn default_chain_client_max_retries() -> u32 {
    3
}

fn default_gas_buffer_multiplier() -> f64 {
    2.0
}

fn default_gas_floor() -> u64 {
    100_000
}

fn default_gas_max_addition() -> u64 {
    200_000
}

#[serde_as]
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DipsAgreementConfig {
    /// The data service address (SubgraphService contract).
    pub data_service: Address,
    /// The RecurringCollector contract address. Dipper posts on-chain offers
    /// here via `RecurringCollector.offer()` before dispatching gRPC proposals.
    pub recurring_collector: Address,
    /// The RecurringAgreementManager contract address. The manager is the
    /// on-chain payer; dipper routes every offer and cancel through it.
    pub recurring_agreement_manager: Address,
    /// Flat per-agreement payment ceiling (GRT per 30 days). Applied to every
    /// RCA regardless of chain. Drives the RCA's `maxOngoingTokensPerSecond`
    /// (as a rate). `maxInitialTokens` is hard-coded to zero in v1 of the
    /// pricing system, so this value alone determines the on-chain monthly
    /// ceiling.
    ///
    /// Per-chain variation is left to `max_grt_per_30_days` (selection filter
    /// on the indexer's advertised base price). The on-chain cap is flat
    /// because the entity-driven component of an indexer's actual claim
    /// dominates the per-chain base rate at large subgraph sizes anyway.
    #[serde(default = "default_max_agreement_grt_per_30_days")]
    pub max_agreement_grt_per_30_days: f64,
    /// Maximum seconds per collection.
    pub max_seconds_per_collection: u32,
    /// Minimum seconds per collection.
    pub min_seconds_per_collection: u32,
    /// Agreement duration in seconds (None = u64::MAX).
    pub duration_seconds: Option<u64>,
    /// Deadline duration in seconds (how long the indexer has to accept on-chain).
    #[serde(default = "default_deadline_seconds")]
    pub deadline_seconds: u64,

    /// Per-chain pricing table.
    ///
    /// Deprecated: When IISA returns per-indexer prices, this table is only used as
    /// fallback for indexers without advertised prices.
    #[serde(default)]
    pub pricing_table: BTreeMap<ChainId, ChainPrices>,

    /// Maximum GRT per 30 days Dipper will pay, per network (by chain name).
    ///
    /// Used as a ceiling when requesting indexers from IISA. Indexers asking
    /// more than the ceiling for their chain are excluded from selection.
    /// Keys are chain names (e.g. "arbitrum-one", "mainnet").
    #[serde(default = "default_max_grt_per_30_days")]
    pub max_grt_per_30_days: BTreeMap<String, f64>,

    /// Maximum GRT per billion entities per 30 days.
    #[serde(default = "default_max_grt_per_billion_entities_per_30_days")]
    pub max_grt_per_billion_entities_per_30_days: f64,

    /// Number of days to look back for declined indexers (standard exclusion). Covers
    /// CanceledByIndexer, expiries whose offer reached the chain, and structurally
    /// persistent rejections (UNSUPPORTED_NETWORK, MANIFEST_TOO_LARGE). Default: 30 days.
    #[serde(default = "default_declined_indexer_lookback_days")]
    pub declined_indexer_lookback_days: i32,

    /// Number of days to look back for PRICE_TOO_LOW rejections.
    ///
    /// Shorter window because IISA refreshes price data daily. Once new prices
    /// are available, the indexer should be reconsidered. Default: 1 day.
    #[serde(default = "default_price_rejection_lookback_days")]
    pub price_rejection_lookback_days: i32,

    /// Minutes to look back for transient rejections: reasons that clear on
    /// their own (capacity, availability) or dipper-side faults (invalid
    /// signature, replay). Default 5 minutes; the alias accepts the older key.
    #[serde(
        default = "default_transient_rejection_lookback_minutes",
        alias = "signer_rejection_lookback_minutes"
    )]
    pub transient_rejection_lookback_minutes: i32,

    /// Days to look back for uncertain rejections that may clear within a day but
    /// aren't a quick transient blip: the indexer not yet trusting the payer
    /// (SENDER_NOT_TRUSTED), or an unspecified/unknown/missing reason. Default: 1 day.
    #[serde(default = "default_uncertain_rejection_lookback_days")]
    pub uncertain_rejection_lookback_days: i32,

    /// Days to skip an indexer across ALL deployments after it failed to respond
    /// to a proposal (status `Unresponsive`). Default: 1 day.
    #[serde(default = "default_unresponsive_indexer_lookback_days")]
    pub unresponsive_indexer_lookback_days: i32,

    /// Fraction of the DIPs-accepting pool that must be unresponsive before the
    /// breaker trips and suppresses the network-wide exclusion. Default 0.50.
    #[serde(default = "default_mass_unresponsive_trip_fraction")]
    pub mass_unresponsive_trip_fraction: f64,

    /// Fraction the unresponsive pool must fall back under before exclusions resume
    /// (hysteresis dead-band with the trip fraction). Default 0.25.
    #[serde(default = "default_mass_unresponsive_reset_fraction")]
    pub mass_unresponsive_reset_fraction: f64,

    /// Max age (hours) of IISA's DIPs-accepting snapshot before it's too stale to
    /// drive the breaker; a stale snapshot never trips it. Default 48.
    #[serde(default = "default_dips_accepting_snapshot_max_age_hours")]
    pub dips_accepting_snapshot_max_age_hours: i64,

    /// How long (seconds) dipper caches IISA's DIPs-accepting set, so a burst of
    /// reassessments doesn't re-query the same daily snapshot. Default 300.
    #[serde(default = "default_dips_accepting_cache_ttl_seconds")]
    pub dips_accepting_cache_ttl_seconds: u64,

    /// Cap on agreements a single indexer may hold in-flight (created but not
    /// yet accepted on-chain) before dipper withholds new offers to it. Default
    /// 5; explicit null removes the cap; 0 pauses new offers to every indexer.
    #[serde(default = "default_max_in_flight_offers_per_indexer")]
    pub max_in_flight_offers_per_indexer: Option<u32>,

    /// Cap on agreements in-flight (created but not yet accepted on-chain)
    /// across all indexers before dipper withholds new offers. Default 100;
    /// explicit null removes the cap; 0 pauses all new offers.
    #[serde(default = "default_max_in_flight_offers_total")]
    pub max_in_flight_offers_total: Option<u32>,
}

/// Mirrors MIN_SECONDS_COLLECTION_WINDOW in RecurringCollector.sol, which has no
/// getter, so re-check it when bumping the contracts pin. Drift stays loud at
/// runtime: offers revert with the contract's live bound in the decoded reason.
const MIN_SECONDS_COLLECTION_WINDOW: u64 = 600;

impl DipsAgreementConfig {
    /// Reject a configuration the protocol-managed path cannot run with: dipper
    /// drives everything through the RecurringAgreementManager, so a zero manager
    /// address means nothing to call. Any future config reload must revalidate too.
    pub fn validate(&self) -> Result<(), String> {
        if self.recurring_agreement_manager == Address::ZERO {
            return Err(
                "recurring_agreement_manager must be set to a non-zero address".to_string(),
            );
        }
        if self.mass_unresponsive_reset_fraction >= self.mass_unresponsive_trip_fraction {
            return Err(format!(
                "mass_unresponsive_reset_fraction ({}) must be below mass_unresponsive_trip_fraction ({})",
                self.mass_unresponsive_reset_fraction, self.mass_unresponsive_trip_fraction
            ));
        }
        // A per-indexer cap above the global cap can never bind; reject the
        // contradiction. A total of 0 is a deliberate pause (nothing binds
        // beyond it), so the ordering check only applies to a positive total.
        if let (Some(per_indexer), Some(total)) = (
            self.max_in_flight_offers_per_indexer,
            self.max_in_flight_offers_total,
        ) && total > 0
            && per_indexer > total
        {
            return Err(format!(
                "max_in_flight_offers_per_indexer ({per_indexer}) must not exceed max_in_flight_offers_total ({total})"
            ));
        }
        // The RecurringCollector refuses terms that break its collection window
        // rules, so every offer built from such a config reverts at gas
        // estimation and no agreement can ever form; refuse to start instead.
        let min = u64::from(self.min_seconds_per_collection);
        let max = u64::from(self.max_seconds_per_collection);
        if max <= min || max - min < MIN_SECONDS_COLLECTION_WINDOW {
            return Err(format!(
                "max_seconds_per_collection ({max}) must exceed min_seconds_per_collection \
                 ({min}) by at least the RecurringCollector minimum collection window of \
                 {MIN_SECONDS_COLLECTION_WINDOW} seconds"
            ));
        }
        if let Some(duration) = self.duration_seconds {
            if duration <= self.deadline_seconds {
                return Err(format!(
                    "duration_seconds ({duration}) must exceed deadline_seconds ({}): the \
                     agreement would end before its acceptance deadline",
                    self.deadline_seconds
                ));
            }
            if duration - self.deadline_seconds < min + MIN_SECONDS_COLLECTION_WINDOW {
                return Err(format!(
                    "duration_seconds ({duration}) minus deadline_seconds ({}) must be at \
                     least min_seconds_per_collection ({min}) plus the \
                     {MIN_SECONDS_COLLECTION_WINDOW}-second minimum collection window, so one \
                     collection fits even when acceptance lands at the deadline",
                    self.deadline_seconds
                ));
            }
        }
        Ok(())
    }
}

fn default_deadline_seconds() -> u64 {
    600
}

/// Indexer-rs minimum GRT/30-days, per chain. Used by
/// `default_max_grt_per_30_days` to derive a per-chain
/// **selection-filter** ceiling.
///
/// The default `max_grt_per_30_days` map multiplies each value by 10:
/// an indexer is dropped from selection if its advertised base price
/// exceeds that ceiling on the relevant chain. This is a filter, not
/// a payment rate — actual payment per indexer is set by IISA's
/// reported price (or the fallback `pricing_table`), bounded above
/// by `max_agreement_grt_per_30_days`. Operators do not pay 10x by
/// default; they simply tolerate indexers asking up to 10x the
/// indexer-rs published minimum on a given chain.
///
/// **Scope**: only the chains in the initial DIPs rollout set are
/// listed here. Other chains carry no default filter — an indexer
/// can offer any base price on them and still pass selection.
/// Operators who want filter coverage on additional chains must
/// add explicit entries to `max_grt_per_30_days` in their config.
///
/// Synced from <https://github.com/graphprotocol/indexer-rs/blob/mb9/dips-signalling-endpoint/crates/config/maximal-config-example.toml#L201-L210>
/// (the rollout-trimmed `[dips.min_grt_per_30_days]` section).
///
/// To refresh: re-read the linked section and copy the value pairs.
/// Update the `mb9/dips-signalling-endpoint` ref to the merged commit
/// hash on `main` (or `main-dips`) once the PR lands.
const INDEXER_RS_MIN_GRT_PER_30_DAYS: &[(&str, f64)] = &[
    ("arbitrum-one", 450.0),
    ("matic", 300.0),
    ("avalanche", 225.0),
    ("bsc", 200.0),
    ("base", 80.0),
    ("mainnet", 45.0),
    ("optimism", 30.0),
    ("base-sepolia", 15.0),
    ("sepolia", 5.0),
];

/// Multiplier applied to indexer-rs minimums to derive dipper's max ceilings.
const PAYMENT_CEILING_MULTIPLIER: f64 = 10.0;

/// Default selection ceiling: 10x the indexer-rs minimum per chain.
fn default_max_grt_per_30_days() -> BTreeMap<String, f64> {
    INDEXER_RS_MIN_GRT_PER_30_DAYS
        .iter()
        .map(|(name, min)| ((*name).to_string(), min * PAYMENT_CEILING_MULTIPLIER))
        .collect()
}

fn default_max_grt_per_billion_entities_per_30_days() -> f64 {
    2000.0
}

/// Default per-agreement payment ceiling (GRT per 30 days).
///
/// 20,000 GRT/30d covers any subgraph up to roughly 30 billion entities at
/// the ~600 GRT/billion-entities indexer-pricing baseline (~0.72 KB per
/// entity, ~22 TB at 30B). Operators with subgraphs in the long tail
/// beyond that should bump this value in their own configmap.
fn default_max_agreement_grt_per_30_days() -> f64 {
    20_000.0
}

fn default_declined_indexer_lookback_days() -> i32 {
    30
}

fn default_price_rejection_lookback_days() -> i32 {
    1
}

fn default_transient_rejection_lookback_minutes() -> i32 {
    5
}

fn default_uncertain_rejection_lookback_days() -> i32 {
    1
}

fn default_unresponsive_indexer_lookback_days() -> i32 {
    1
}

fn default_mass_unresponsive_trip_fraction() -> f64 {
    0.50
}

fn default_mass_unresponsive_reset_fraction() -> f64 {
    0.25
}

fn default_dips_accepting_snapshot_max_age_hours() -> i64 {
    48
}

fn default_dips_accepting_cache_ttl_seconds() -> u64 {
    300
}

fn default_max_in_flight_offers_per_indexer() -> Option<u32> {
    Some(5)
}

fn default_max_in_flight_offers_total() -> Option<u32> {
    Some(100)
}

/// Per-chain pricing for indexing agreements.
#[serde_as]
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainPrices {
    /// Tokens per second (base rate) in wei GRT.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub tokens_per_second: U256,
    /// Tokens per entity per second in wei GRT.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub tokens_per_entity_per_second: U256,
}

/// Gateway operator API configuration. Authenticates via EIP-712 signatures.
#[serde_as]
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminRpcConfig {
    /// The RPC server listen address.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub listen_addr: std::net::SocketAddr,

    /// Authorized gateway operator addresses (e.g., Graph Studio).
    #[serde_as(as = "serde_with::SetLastValueWins<_>")]
    pub gateway_operator_allowlist: BTreeSet<Address>,
}

/// The database configuration
#[serde_as]
#[derive(custom_debug::CustomDebug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DbConfig {
    /// The PostgreSQL database URL
    ///
    /// The URL should be in the format `postgres://<host>:<port>/<database>`.
    #[debug(with = std::fmt::Display::fmt)]
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub url: Url,

    /// The database auth username
    pub username: String,

    /// The database auth password
    pub password: Hidden<String>,

    /// The maximum number of connections to the database
    #[serde(default)]
    pub max_connections: Option<u32>,
}

/// The indexer URLs service configuration
#[serde_as]
#[derive(custom_debug::CustomDebug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerUrlsConfig {
    /// The indexing-payments subgraph query endpoint used to look up
    /// registered indexer URLs. Same form as `chain_listener.subgraph_endpoint`:
    /// a full query URL, gateway-served or self-hosted.
    #[debug(with = std::fmt::Display::fmt)]
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub subgraph_endpoint: Url,

    /// Bearer token for the endpoint (needed for gateway-served subgraphs)
    #[serde(default)]
    pub api_key: Option<Hidden<String>>,

    /// The update interval for the indexer URL lookup service
    #[serde_as(as = "serde_with::DurationSeconds")]
    pub update_interval: Duration,

    /// Boot even if the subgraph reports 0 registered indexers, instead of
    /// exiting after the startup retries. For environments that come up before
    /// any indexer has registered (e.g. local networks); leave off elsewhere.
    #[serde(default)]
    pub allow_empty_at_startup: bool,
}

/// The configuration for the signer
#[serde_as]
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignerConfig {
    /// The signing key to use for authentication
    #[serde_as(as = "HiddenSecretKeyAsHexStr")]
    pub secret_key: Hidden<SecretKey>,

    /// The signer chain ID (protocol chain), e.g. `eip155:42161` (Arbitrum One)
    pub chain_id: ChainId,
}

/// Runtime indexing agreement configuration.
#[derive(Debug)]
pub struct IndexingAgreementConfig {
    /// The data service address (SubgraphService contract).
    pub data_service: Address,
    /// The RecurringCollector contract address.
    pub recurring_collector: Address,
    /// The RecurringAgreementManager address (the on-chain payer).
    pub recurring_agreement_manager: Address,
    /// Flat per-agreement payment ceiling (GRT per 30 days). Drives the RCA's
    /// `maxOngoingTokensPerSecond` (as a rate); `maxInitialTokens` is
    /// hard-coded to zero in v1. Applied to every agreement regardless of
    /// chain.
    pub max_agreement_grt_per_30_days: f64,
    /// Maximum seconds per collection.
    pub max_seconds_per_collection: u32,
    /// Minimum seconds per collection.
    pub min_seconds_per_collection: u32,
    /// Agreement duration in seconds.
    pub duration_seconds: u64,
    /// Deadline duration in seconds.
    pub deadline_seconds: u64,
    /// Payment ceiling per chain (GRT per 30 days).
    pub max_grt_per_30_days: BTreeMap<String, f64>,
    /// Payment ceiling for entity pricing (GRT per billion entities per 30 days).
    pub max_grt_per_billion_entities_per_30_days: f64,
    /// Number of days to look back for declined indexers (standard exclusion).
    pub declined_indexer_lookback_days: i32,
    /// Number of days to look back for PRICE_TOO_LOW rejections.
    pub price_rejection_lookback_days: i32,
    /// Number of minutes to look back for transient rejections.
    pub transient_rejection_lookback_minutes: i32,
    /// Number of days to look back for uncertain rejections (sender-not-trusted, unspecified).
    pub uncertain_rejection_lookback_days: i32,
    /// Number of days to skip an unresponsive indexer across all deployments.
    pub unresponsive_indexer_lookback_days: i32,
    /// Breaker trip fraction (see `DipsAgreementConfig`).
    pub mass_unresponsive_trip_fraction: f64,
    /// Breaker reset fraction.
    pub mass_unresponsive_reset_fraction: f64,
    /// Max age (hours) of the DIPs-accepting snapshot before it's too stale to trip.
    pub dips_accepting_snapshot_max_age_hours: i64,
    /// TTL (seconds) for caching the DIPs-accepting set.
    pub dips_accepting_cache_ttl_seconds: u64,
    /// Per-indexer in-flight (created but unaccepted) offer cap; None removes
    /// the cap and 0 pauses new offers.
    pub max_in_flight_offers_per_indexer: Option<u32>,
    /// Global in-flight (created but unaccepted) offer cap; None removes the
    /// cap and 0 pauses all new offers.
    pub max_in_flight_offers_total: Option<u32>,
}

/// Per-chain pricing for indexing agreements (runtime).
#[derive(Debug)]
pub struct IndexingAgreementChainPrices {
    /// Tokens per second (base rate) in wei GRT.
    pub tokens_per_second: U256,
    /// Tokens per entity per second in wei GRT.
    pub tokens_per_entity_per_second: U256,
}

impl IndexingAgreementConfig {
    pub fn data_service(&self) -> Address {
        self.data_service
    }

    pub fn recurring_collector(&self) -> Address {
        self.recurring_collector
    }

    pub fn recurring_agreement_manager(&self) -> Address {
        self.recurring_agreement_manager
    }

    pub fn max_agreement_grt_per_30_days(&self) -> f64 {
        self.max_agreement_grt_per_30_days
    }

    pub fn max_seconds_per_collection(&self) -> u32 {
        self.max_seconds_per_collection
    }

    pub fn min_seconds_per_collection(&self) -> u32 {
        self.min_seconds_per_collection
    }

    pub fn duration_seconds(&self) -> u64 {
        self.duration_seconds
    }

    pub fn deadline_seconds(&self) -> u64 {
        self.deadline_seconds
    }

    pub fn max_grt_per_30_days(&self) -> &BTreeMap<String, f64> {
        &self.max_grt_per_30_days
    }

    pub fn max_grt_per_billion_entities_per_30_days(&self) -> f64 {
        self.max_grt_per_billion_entities_per_30_days
    }

    pub fn declined_indexer_lookback_days(&self) -> i32 {
        self.declined_indexer_lookback_days
    }

    pub fn price_rejection_lookback_days(&self) -> i32 {
        self.price_rejection_lookback_days
    }

    pub fn transient_rejection_lookback_minutes(&self) -> i32 {
        self.transient_rejection_lookback_minutes
    }

    pub fn uncertain_rejection_lookback_days(&self) -> i32 {
        self.uncertain_rejection_lookback_days
    }

    pub fn unresponsive_indexer_lookback_days(&self) -> i32 {
        self.unresponsive_indexer_lookback_days
    }

    pub fn mass_unresponsive_trip_fraction(&self) -> f64 {
        self.mass_unresponsive_trip_fraction
    }

    pub fn mass_unresponsive_reset_fraction(&self) -> f64 {
        self.mass_unresponsive_reset_fraction
    }

    pub fn dips_accepting_snapshot_max_age_hours(&self) -> i64 {
        self.dips_accepting_snapshot_max_age_hours
    }

    pub fn dips_accepting_cache_ttl_seconds(&self) -> u64 {
        self.dips_accepting_cache_ttl_seconds
    }

    pub fn max_in_flight_offers_per_indexer(&self) -> Option<u32> {
        self.max_in_flight_offers_per_indexer
    }

    pub fn max_in_flight_offers_total(&self) -> Option<u32> {
        self.max_in_flight_offers_total
    }
}

impl From<DipsAgreementConfig>
    for (
        Arc<IndexingAgreementConfig>,
        Arc<BTreeMap<ChainId, IndexingAgreementChainPrices>>,
    )
{
    fn from(value: DipsAgreementConfig) -> Self {
        let config = IndexingAgreementConfig {
            data_service: value.data_service,
            recurring_collector: value.recurring_collector,
            recurring_agreement_manager: value.recurring_agreement_manager,
            max_agreement_grt_per_30_days: value.max_agreement_grt_per_30_days,
            max_seconds_per_collection: value.max_seconds_per_collection,
            min_seconds_per_collection: value.min_seconds_per_collection,
            duration_seconds: value.duration_seconds.unwrap_or(u64::MAX),
            deadline_seconds: value.deadline_seconds,
            max_grt_per_30_days: value.max_grt_per_30_days,
            max_grt_per_billion_entities_per_30_days: value
                .max_grt_per_billion_entities_per_30_days,
            declined_indexer_lookback_days: value.declined_indexer_lookback_days,
            price_rejection_lookback_days: value.price_rejection_lookback_days,
            transient_rejection_lookback_minutes: value.transient_rejection_lookback_minutes,
            uncertain_rejection_lookback_days: value.uncertain_rejection_lookback_days,
            unresponsive_indexer_lookback_days: value.unresponsive_indexer_lookback_days,
            mass_unresponsive_trip_fraction: value.mass_unresponsive_trip_fraction,
            mass_unresponsive_reset_fraction: value.mass_unresponsive_reset_fraction,
            dips_accepting_snapshot_max_age_hours: value.dips_accepting_snapshot_max_age_hours,
            dips_accepting_cache_ttl_seconds: value.dips_accepting_cache_ttl_seconds,
            max_in_flight_offers_per_indexer: value.max_in_flight_offers_per_indexer,
            max_in_flight_offers_total: value.max_in_flight_offers_total,
        };
        let prices = value
            .pricing_table
            .into_iter()
            .map(|(chain_id, prices)| {
                (
                    chain_id,
                    IndexingAgreementChainPrices {
                        tokens_per_second: prices.tokens_per_second,
                        tokens_per_entity_per_second: prices.tokens_per_entity_per_second,
                    },
                )
            })
            .collect();
        (Arc::new(config), Arc::new(prices))
    }
}

/// Runtime configuration for the event streaming.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventStreamingConfig {
    /// Enable/disable event emission
    #[serde(default)]
    pub enabled: bool,

    /// Maximum number of events to buffer before applying backpressure.
    #[serde(default = "default_event_queue_capacity")]
    pub event_queue_capacity: NonZeroUsize,

    /// Kafka-specific configuration.
    #[serde(default)]
    pub kafka: Option<KafkaConfig>,
}

impl Default for EventStreamingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            event_queue_capacity: default_event_queue_capacity(),
            kafka: None,
        }
    }
}

impl EventStreamingConfig {
    /// Reject a configuration that says events are on but gives the emitter no
    /// broker to send to. A disabled-emitter fallback would stamp every emission
    /// marker as sent (durable emits no-op Ok), permanently consuming the events.
    pub fn validate(&self) -> Result<(), String> {
        if self.enabled && self.kafka.is_none() {
            return Err(
                "event_streaming_config.enabled is true but no kafka section is configured"
                    .to_string(),
            );
        }
        Ok(())
    }
}

pub fn default_event_queue_capacity() -> NonZeroUsize {
    NonZeroUsize::new(1024).expect("default event queue capacity is non-zero")
}

/// Configuration for the Studio indexing request consumer, which reads
/// subgraph indexing request events from a Redpanda topic and applies them
/// through the same path as the admin RPC.
#[serde_as]
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexingRequestConsumerConfig {
    /// Whether the consumer is enabled (default: false). Leave off until
    /// Studio's propose events carry `indexed_network_caip2id`: without the
    /// field every consumed request is skipped and its offset committed, so
    /// enabling early permanently discards real requests.
    #[serde(default)]
    pub enabled: bool,

    /// Kafka connection settings and the topic Studio produces on. The topic
    /// is required with no default; a wrong or missing name fails at startup.
    pub kafka: KafkaConsumerConfig,

    /// The identity recorded as the requester on consumed requests, since
    /// Kafka messages carry no signature to recover one from. Use the address
    /// Studio signs the admin RPC with, so both doors share request rows.
    pub requested_by: Address,

    /// How long a fetch waits server-side for new records before returning
    /// empty, in seconds (default: 5).
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(default = "default_indexing_request_consumer_max_wait")]
    pub max_wait: Duration,

    /// Maximum bytes per fetch (default: 1,048,576).
    #[serde(default = "default_indexing_request_consumer_fetch_max_bytes")]
    pub fetch_max_bytes: i32,
}

fn default_indexing_request_consumer_max_wait() -> Duration {
    Duration::from_secs(5)
}

fn default_indexing_request_consumer_fetch_max_bytes() -> i32 {
    1_048_576
}

impl IndexingRequestConsumerConfig {
    /// Reject a configuration the consumer cannot run with. Checked even when
    /// disabled, so a broken value cannot lie dormant until the flag flips.
    pub fn validate(&self) -> Result<(), String> {
        if self.kafka.brokers.is_empty() {
            return Err(
                "indexing_request_consumer.kafka.brokers must list at least 1 broker".to_string(),
            );
        }
        // 0 makes every connect time out instantly, so startup would fail with
        // a message blaming the broker instead of the config typo.
        if self.kafka.connect_timeout_secs == 0 {
            return Err(
                "indexing_request_consumer.kafka.connect_timeout_secs must be at least 1"
                    .to_string(),
            );
        }
        // A zero requester is almost certainly an unset value, and it would
        // silently key every consumed request under the zero address.
        if self.requested_by == Address::ZERO {
            return Err(
                "indexing_request_consumer.requested_by must be a non-zero address".to_string(),
            );
        }
        // Below ~1 KB a fetch cannot hold a whole record, so the consumer
        // would poll forever without ever making progress.
        if self.fetch_max_bytes < 1_024 {
            return Err(format!(
                "indexing_request_consumer.fetch_max_bytes ({}) must be at least 1024",
                self.fetch_max_bytes
            ));
        }
        if self.max_wait.is_zero() || self.max_wait.as_millis() > i32::MAX as u128 {
            return Err(format!(
                "indexing_request_consumer.max_wait ({}s) must be between 1 second and {} seconds",
                self.max_wait.as_secs(),
                i32::MAX / 1_000
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn consumer_config(json: serde_json::Value) -> IndexingRequestConsumerConfig {
        serde_json::from_value(json).expect("deserializes")
    }

    #[test]
    fn indexing_request_consumer_config_defaults_and_validation() {
        let config = consumer_config(serde_json::json!({
            "kafka": { "brokers": ["localhost:9092"], "topic": "t" },
            "requested_by": "0x8f8c426f956876325b1e037c6eae9b189952994c",
        }));
        assert!(
            !config.enabled,
            "the consumer must be off unless opted into"
        );
        assert_eq!(config.max_wait, Duration::from_secs(5));
        assert_eq!(config.fetch_max_bytes, 1_048_576);
        assert!(config.validate().is_ok());

        // Validation runs even for a disabled section, so broken values are
        // caught at startup rather than the day the flag flips.
        let broken = consumer_config(serde_json::json!({
            "enabled": false,
            "kafka": { "brokers": ["localhost:9092"], "topic": "t" },
            "requested_by": "0x0000000000000000000000000000000000000000",
        }));
        assert!(broken.validate().unwrap_err().contains("requested_by"));

        let no_brokers = consumer_config(serde_json::json!({
            "kafka": { "brokers": [], "topic": "t" },
            "requested_by": "0x8f8c426f956876325b1e037c6eae9b189952994c",
        }));
        assert!(no_brokers.validate().unwrap_err().contains("brokers"));

        let zero_connect_timeout = consumer_config(serde_json::json!({
            "kafka": { "brokers": ["localhost:9092"], "topic": "t", "connect_timeout_secs": 0 },
            "requested_by": "0x8f8c426f956876325b1e037c6eae9b189952994c",
        }));
        assert!(
            zero_connect_timeout
                .validate()
                .unwrap_err()
                .contains("connect_timeout_secs")
        );

        let tiny_fetch = consumer_config(serde_json::json!({
            "kafka": { "brokers": ["localhost:9092"], "topic": "t" },
            "requested_by": "0x8f8c426f956876325b1e037c6eae9b189952994c",
            "fetch_max_bytes": 512,
        }));
        assert!(
            tiny_fetch
                .validate()
                .unwrap_err()
                .contains("fetch_max_bytes")
        );
    }

    #[test]
    fn test_dips_agreement_config_deserialization() {
        //* Arrange - JSON config with all new field names
        let json = r#"{
            "data_service": "0x1111111111111111111111111111111111111111",
            "recurring_collector": "0x2222222222222222222222222222222222222222",
            "recurring_agreement_manager": "0x3333333333333333333333333333333333333333",
            "max_agreement_grt_per_30_days": 20000.0,
            "min_seconds_per_collection": 60,
            "max_seconds_per_collection": 3600,
            "duration_seconds": 86400,
            "deadline_seconds": 300,
            "max_in_flight_offers_per_indexer": 7,
            "max_in_flight_offers_total": 42,
            "pricing_table": {
                "1": {
                    "tokens_per_second": "10",
                    "tokens_per_entity_per_second": "2"
                },
                "42161": {
                    "tokens_per_second": "5",
                    "tokens_per_entity_per_second": "1"
                }
            }
        }"#;

        //* Act - Deserialize
        let config: DipsAgreementConfig =
            serde_json::from_str(json).expect("deserialization failed");

        //* Assert - Verify all fields
        use thegraph_core::alloy::primitives::{U256, address};

        assert_eq!(
            config.data_service,
            address!("1111111111111111111111111111111111111111"),
            "data_service mismatch"
        );
        assert_eq!(
            config.recurring_collector,
            address!("2222222222222222222222222222222222222222"),
            "recurring_collector mismatch"
        );
        assert_eq!(
            config.recurring_agreement_manager,
            address!("3333333333333333333333333333333333333333"),
            "recurring_agreement_manager mismatch"
        );
        assert_eq!(
            config.max_agreement_grt_per_30_days, 20000.0,
            "max_agreement_grt_per_30_days mismatch"
        );
        assert_eq!(
            config.min_seconds_per_collection, 60,
            "min_seconds_per_collection mismatch"
        );
        assert_eq!(
            config.max_seconds_per_collection, 3600,
            "max_seconds_per_collection mismatch"
        );
        assert_eq!(
            config.duration_seconds,
            Some(86400),
            "duration_seconds mismatch"
        );
        assert_eq!(config.deadline_seconds, 300, "deadline_seconds mismatch");
        assert_eq!(
            config.max_in_flight_offers_per_indexer,
            Some(7),
            "max_in_flight_offers_per_indexer mismatch"
        );
        assert_eq!(
            config.max_in_flight_offers_total,
            Some(42),
            "max_in_flight_offers_total mismatch"
        );

        // Verify pricing table
        assert_eq!(
            config.pricing_table.len(),
            2,
            "pricing_table should have 2 entries"
        );

        let chain_1_prices = config.pricing_table.get(&1).expect("chain 1 not found");
        assert_eq!(
            chain_1_prices.tokens_per_second,
            U256::from(10u64),
            "chain 1 tokens_per_second mismatch"
        );
        assert_eq!(
            chain_1_prices.tokens_per_entity_per_second,
            U256::from(2u64),
            "chain 1 tokens_per_entity_per_second mismatch"
        );

        let chain_42161_prices = config
            .pricing_table
            .get(&42161)
            .expect("chain 42161 not found");
        assert_eq!(
            chain_42161_prices.tokens_per_second,
            U256::from(5u64),
            "chain 42161 tokens_per_second mismatch"
        );
        assert_eq!(
            chain_42161_prices.tokens_per_entity_per_second,
            U256::from(1u64),
            "chain 42161 tokens_per_entity_per_second mismatch"
        );
    }

    #[test]
    fn validate_rejects_a_zero_manager_address() {
        // tc-3: validate() is the only guard for the zero-manager-address
        // footgun that downstream `.expect()`s rely on. Pin both outcomes so a
        // later change cannot weaken the invariant unseen.
        fn conf(manager: &str) -> DipsAgreementConfig {
            let json = format!(
                r#"{{
                    "data_service": "0x1111111111111111111111111111111111111111",
                    "recurring_collector": "0x2222222222222222222222222222222222222222",
                    "recurring_agreement_manager": "{manager}",
                    "min_seconds_per_collection": 60,
                    "max_seconds_per_collection": 3600,
                    "pricing_table": {{}}
                }}"#
            );
            serde_json::from_str(&json).expect("deserialization")
        }

        assert!(
            conf("0x0000000000000000000000000000000000000000")
                .validate()
                .is_err(),
            "a zero manager address must be rejected"
        );
        assert!(
            conf("0x3333333333333333333333333333333333333333")
                .validate()
                .is_ok(),
            "a non-zero manager address is valid"
        );
    }

    #[test]
    fn validate_rejects_a_collection_window_the_contract_would_refuse() {
        // The RecurringCollector requires max - min >= 600; an offer built from
        // a narrower window reverts at gas estimation on every attempt, so the
        // config must be refused at startup instead.
        fn conf(min: u32, max: u32) -> DipsAgreementConfig {
            let json = format!(
                r#"{{
                    "data_service": "0x1111111111111111111111111111111111111111",
                    "recurring_collector": "0x2222222222222222222222222222222222222222",
                    "recurring_agreement_manager": "0x3333333333333333333333333333333333333333",
                    "min_seconds_per_collection": {min},
                    "max_seconds_per_collection": {max},
                    "pricing_table": {{}}
                }}"#
            );
            serde_json::from_str(&json).expect("deserialization")
        }

        assert!(
            conf(60, 240).validate().is_err(),
            "a window narrower than 600 seconds must be rejected"
        );
        assert!(
            conf(240, 60).validate().is_err(),
            "an inverted window must be rejected"
        );
        assert!(
            conf(60, 659).validate().is_err(),
            "a window 1 second under the bound must be rejected"
        );
        assert!(
            conf(60, 660).validate().is_ok(),
            "a window exactly at the bound is valid"
        );
        assert!(
            conf(60, 3600).validate().is_ok(),
            "a comfortably wide window is valid"
        );
    }

    #[test]
    fn validate_rejects_a_duration_too_short_for_its_deadline() {
        // With a bounded duration the agreement must outlive the acceptance
        // deadline by min_seconds_per_collection + 600, so a collection fits
        // even when acceptance lands exactly at the deadline.
        fn conf(duration: &str, deadline: u64) -> DipsAgreementConfig {
            let json = format!(
                r#"{{
                    "data_service": "0x1111111111111111111111111111111111111111",
                    "recurring_collector": "0x2222222222222222222222222222222222222222",
                    "recurring_agreement_manager": "0x3333333333333333333333333333333333333333",
                    "min_seconds_per_collection": 60,
                    "max_seconds_per_collection": 3600,
                    "duration_seconds": {duration},
                    "deadline_seconds": {deadline},
                    "pricing_table": {{}}
                }}"#
            );
            serde_json::from_str(&json).expect("deserialization")
        }

        assert!(
            conf("null", 600).validate().is_ok(),
            "an unbounded duration passes the duration checks"
        );
        assert!(
            conf("600", 600).validate().is_err(),
            "an agreement ending at its deadline must be rejected"
        );
        assert!(
            conf("1259", 600).validate().is_err(),
            "1 second short of a full collection window must be rejected"
        );
        assert!(
            conf("1260", 600).validate().is_ok(),
            "exactly one collection window past the deadline is valid"
        );
    }

    #[test]
    fn test_dips_agreement_config_defaults() {
        //* Arrange - Minimal JSON with defaults
        let json = r#"{
            "data_service": "0x1111111111111111111111111111111111111111",
            "recurring_collector": "0x2222222222222222222222222222222222222222",
            "recurring_agreement_manager": "0x3333333333333333333333333333333333333333",
            "min_seconds_per_collection": 60,
            "max_seconds_per_collection": 3600,
            "pricing_table": {}
        }"#;

        //* Act
        let config: DipsAgreementConfig =
            serde_json::from_str(json).expect("deserialization failed");

        //* Assert - Check defaults
        assert_eq!(
            config.duration_seconds, None,
            "duration_seconds should default to None"
        );
        assert_eq!(
            config.deadline_seconds, 600,
            "deadline_seconds should default to 600"
        );
        assert_eq!(
            config.max_agreement_grt_per_30_days, 20000.0,
            "max_agreement_grt_per_30_days default missing"
        );
        assert_eq!(config.mass_unresponsive_trip_fraction, 0.50);
        assert_eq!(config.mass_unresponsive_reset_fraction, 0.25);
        assert_eq!(config.dips_accepting_snapshot_max_age_hours, 48);
        assert_eq!(config.dips_accepting_cache_ttl_seconds, 300);
        assert_eq!(
            config.max_in_flight_offers_per_indexer,
            Some(5),
            "max_in_flight_offers_per_indexer should default to 5"
        );
        assert_eq!(
            config.max_in_flight_offers_total,
            Some(100),
            "max_in_flight_offers_total should default to 100"
        );

        // Test the From conversion - None should map to u64::MAX
        let (agreement_config, _) = <(
            Arc<IndexingAgreementConfig>,
            Arc<BTreeMap<u64, IndexingAgreementChainPrices>>,
        )>::from(config);
        assert_eq!(
            agreement_config.duration_seconds(),
            u64::MAX,
            "duration_seconds None should convert to u64::MAX"
        );
        assert_eq!(
            agreement_config.max_in_flight_offers_per_indexer(),
            Some(5),
            "per-indexer cap should survive the From conversion"
        );
        assert_eq!(
            agreement_config.max_in_flight_offers_total(),
            Some(100),
            "total cap should survive the From conversion"
        );
    }

    #[test]
    fn validate_rejects_per_indexer_cap_above_total() {
        fn conf(per: &str, total: &str) -> DipsAgreementConfig {
            let json = format!(
                r#"{{
                    "data_service": "0x1111111111111111111111111111111111111111",
                    "recurring_collector": "0x2222222222222222222222222222222222222222",
                    "recurring_agreement_manager": "0x3333333333333333333333333333333333333333",
                    "min_seconds_per_collection": 60,
                    "max_seconds_per_collection": 3600,
                    "max_in_flight_offers_per_indexer": {per},
                    "max_in_flight_offers_total": {total},
                    "pricing_table": {{}}
                }}"#
            );
            serde_json::from_str(&json).expect("deserialization")
        }

        assert!(
            conf("10", "5").validate().is_err(),
            "per-indexer cap above total cap must be rejected"
        );
        assert!(
            conf("5", "100").validate().is_ok(),
            "per-indexer cap below total cap is valid"
        );
        // 0 is a deliberate pause, not a disable: a paused total makes the
        // ordering irrelevant, and a paused per-indexer cap is always valid.
        assert!(
            conf("10", "0").validate().is_ok(),
            "a paused (0) total cap must not trigger the ordering check"
        );
        assert!(
            conf("0", "5").validate().is_ok(),
            "a paused (0) per-indexer cap is valid"
        );
        // null removes a cap entirely; the ordering check needs both present.
        assert!(
            conf("null", "5").validate().is_ok(),
            "an uncapped per-indexer side skips the ordering check"
        );
        assert!(
            conf("10", "null").validate().is_ok(),
            "an uncapped total side skips the ordering check"
        );
    }

    /// Guards against silent typos and accidental duplicates in the
    /// indexer-rs mirror. Failure modes the test catches:
    ///   * a chain name is mistyped on either side of the multiplier
    ///   * the multiplier itself drifts away from 10x
    ///   * two rows accidentally share a chain name (BTreeMap would mask the
    ///     duplicate by silently dropping the earlier value)
    #[test]
    fn test_default_max_grt_per_30_days_const() {
        let map = default_max_grt_per_30_days();

        // Spot-check three values: high-traffic mainnet chains and a small
        // testnet, picked to cover both ends of the value range.
        assert_eq!(map.get("arbitrum-one"), Some(&4500.0), "arbitrum-one");
        assert_eq!(map.get("mainnet"), Some(&450.0), "mainnet");
        assert_eq!(map.get("sepolia"), Some(&50.0), "sepolia");

        // The const should mirror indexer-rs's published minimum table.
        // Updates that change the row count are intentional — refresh
        // this number alongside the const.
        assert_eq!(
            INDEXER_RS_MIN_GRT_PER_30_DAYS.len(),
            9,
            "row count drifted from the indexer-rs initial DIPs rollout set"
        );

        // No duplicate keys hidden by BTreeMap's last-write-wins behaviour.
        let unique_count = INDEXER_RS_MIN_GRT_PER_30_DAYS
            .iter()
            .map(|(name, _)| *name)
            .collect::<std::collections::HashSet<_>>()
            .len();
        assert_eq!(
            unique_count,
            INDEXER_RS_MIN_GRT_PER_30_DAYS.len(),
            "duplicate chain name in INDEXER_RS_MIN_GRT_PER_30_DAYS"
        );

        // Every output entry is 10x its source entry — confirms the
        // multiplier wired through without rounding surprises.
        for (name, min) in INDEXER_RS_MIN_GRT_PER_30_DAYS {
            let want = *min * PAYMENT_CEILING_MULTIPLIER;
            assert_eq!(map.get(*name), Some(&want), "ceiling for {name}");
        }
    }

    /// Stale `max_initial_tokens` and `max_ongoing_tokens_per_second` keys
    /// from the pre-refactor config schema should fail deserialization
    /// rather than be silently ignored, so operators surface the migration
    /// instead of running with caps that no longer take effect.
    #[test]
    fn test_dips_agreement_config_rejects_stale_keys() {
        let stale_keys = [
            "max_initial_tokens",
            "max_ongoing_tokens_per_second",
            "completely_made_up_key",
        ];

        for stale_key in stale_keys {
            let json = format!(
                r#"{{
                    "data_service": "0x1111111111111111111111111111111111111111",
                    "recurring_collector": "0x2222222222222222222222222222222222222222",
                    "min_seconds_per_collection": 60,
                    "max_seconds_per_collection": 3600,
                    "pricing_table": {{}},
                    "{stale_key}": "anything"
                }}"#
            );
            let err = serde_json::from_str::<DipsAgreementConfig>(&json)
                .expect_err(&format!("expected rejection for key {stale_key}"));
            assert!(
                err.to_string().contains(stale_key),
                "error for {stale_key} should name the unknown key, got: {err}"
            );
        }
    }

    /// A minimal chain_client block with only current keys still parses.
    #[test]
    fn test_chain_client_config_accepts_current_keys() {
        let json = r#"{
            "enabled": true,
            "providers": ["http://chain:8545"]
        }"#;

        let config: ChainClientConfig = serde_json::from_str(json).expect("deserialization failed");

        assert!(config.enabled);
        assert_eq!(config.providers.len(), 1);
    }

    /// Stale `chain_id` and `recurring_collector_address` keys (now read from
    /// the signer and dips sections instead) should fail deserialization
    /// rather than be silently ignored, so operators surface the migration
    /// instead of editing keys that no longer take effect.
    #[test]
    fn test_chain_client_config_rejects_stale_keys() {
        let stale_keys = [
            r#""chain_id": 1337"#,
            r#""recurring_collector_address": "0x2222222222222222222222222222222222222222""#,
            r#""completely_made_up_key": "anything""#,
        ];

        for stale_key in stale_keys {
            let json = format!(
                r#"{{
                    "enabled": true,
                    "providers": ["http://chain:8545"],
                    {stale_key}
                }}"#
            );
            let key_name = stale_key.split('"').nth(1).unwrap();
            let err = serde_json::from_str::<ChainClientConfig>(&json)
                .expect_err(&format!("expected rejection for key {key_name}"));
            assert!(
                err.to_string().contains(key_name),
                "error for {key_name} should name the unknown key, got: {err}"
            );
        }
    }

    /// Mirrors the `event_streaming_config` field on `Config`: `#[serde(default)]`
    /// over `Option<EventStreamingConfig>`. Used to exercise the present/absent
    /// behavior without constructing a full `Config`.
    #[derive(serde::Deserialize)]
    struct EventStreamingWrapper {
        #[serde(default)]
        event_streaming_config: Option<EventStreamingConfig>,
    }

    /// When the section is absent entirely, the optional field stays `None` so
    /// event streaming is simply off rather than failing to parse.
    #[test]
    fn event_streaming_config_absent_is_none() {
        let wrapper: EventStreamingWrapper =
            serde_json::from_str("{}").expect("deserialization failed");
        assert!(
            wrapper.event_streaming_config.is_none(),
            "absent event_streaming_config should deserialize to None"
        );
    }

    /// A present-but-empty section falls back to every field default rather than
    /// requiring operators to spell out keys they don't care about.
    #[test]
    fn event_streaming_config_present_empty_uses_defaults() {
        let wrapper: EventStreamingWrapper =
            serde_json::from_str(r#"{ "event_streaming_config": {} }"#)
                .expect("deserialization failed");

        let config = wrapper
            .event_streaming_config
            .expect("event_streaming_config should be Some");

        assert!(!config.enabled, "enabled should default to false");
        assert_eq!(
            config.event_queue_capacity,
            default_event_queue_capacity(),
            "event_queue_capacity should default to 1024"
        );
        assert!(config.kafka.is_none(), "kafka should default to None");
    }

    /// A fully specified section round-trips every field, including the nested Kafka block.
    #[test]
    fn event_streaming_config_parses_full() {
        let json = r#"{
            "enabled": true,
            "event_queue_capacity": 2048,
            "kafka": {
                "brokers": ["broker-1:9092", "broker-2:9092"],
                "topic": "custom.topic",
                "partitions": 8,
                "sasl_mechanism": "PLAIN",
                "sasl_username": "user",
                "sasl_password": "pass",
                "tls_enabled": true,
                "tls_ca_cert_path": "/etc/ssl/ca.pem"
            }
        }"#;

        let config: EventStreamingConfig =
            serde_json::from_str(json).expect("deserialization failed");

        assert!(config.enabled, "enabled mismatch");
        assert_eq!(
            config.event_queue_capacity,
            NonZeroUsize::new(2048).unwrap(),
            "event_queue_capacity mismatch"
        );

        let kafka = config.kafka.expect("kafka should be Some");
        assert_eq!(
            kafka.brokers,
            vec!["broker-1:9092".to_string(), "broker-2:9092".to_string()],
            "brokers mismatch"
        );
        assert_eq!(kafka.topic, "custom.topic", "topic mismatch");
        assert_eq!(kafka.partitions, 8, "partitions mismatch");
        assert_eq!(
            kafka.sasl_mechanism.as_deref(),
            Some("PLAIN"),
            "sasl_mechanism mismatch"
        );
        assert_eq!(
            kafka.sasl_username.as_deref(),
            Some("user"),
            "sasl_username mismatch"
        );
        assert_eq!(
            kafka.sasl_password.as_deref(),
            Some("pass"),
            "sasl_password mismatch"
        );
        assert!(kafka.tls_enabled, "tls_enabled mismatch");
        assert_eq!(
            kafka.tls_ca_cert_path.as_deref(),
            Some(std::path::Path::new("/etc/ssl/ca.pem")),
            "tls_ca_cert_path mismatch"
        );
    }

    /// Enabled-without-kafka must fail validation: the fallback would be a
    /// disabled emitter whose no-op durable emits stamp markers, losing events.
    #[test]
    fn event_streaming_validate_rejects_enabled_without_kafka() {
        let config: EventStreamingConfig =
            serde_json::from_str(r#"{ "enabled": true }"#).expect("deserialization failed");
        assert!(
            config.validate().is_err(),
            "enabled without a kafka section must be rejected"
        );

        let disabled: EventStreamingConfig =
            serde_json::from_str(r#"{ "enabled": false }"#).expect("deserialization failed");
        assert!(
            disabled.validate().is_ok(),
            "disabled without a kafka section is fine"
        );

        let complete: EventStreamingConfig =
            serde_json::from_str(r#"{ "enabled": true, "kafka": { "brokers": ["broker:9092"] } }"#)
                .expect("deserialization failed");
        assert!(
            complete.validate().is_ok(),
            "enabled with a kafka section must pass"
        );
    }

    /// A Kafka block with only the required `brokers` falls back to the topic
    /// and partition defaults and leaves the optional auth/TLS fields unset.
    #[test]
    fn event_streaming_config_kafka_minimal_uses_defaults() {
        let json = r#"{
            "kafka": {
                "brokers": ["broker:9092"]
            }
        }"#;

        let config: EventStreamingConfig =
            serde_json::from_str(json).expect("deserialization failed");

        let kafka = config.kafka.expect("kafka should be Some");
        assert_eq!(
            kafka.topic, "dipper.subgraph.indexing.agreement.events",
            "topic should fall back to default"
        );
        assert_eq!(
            kafka.partitions, 16,
            "partitions should fall back to default"
        );
        assert!(
            kafka.sasl_mechanism.is_none(),
            "sasl_mechanism should be None"
        );
        assert!(
            kafka.sasl_username.is_none(),
            "sasl_username should be None"
        );
        assert!(
            kafka.sasl_password.is_none(),
            "sasl_password should be None"
        );
        assert!(!kafka.tls_enabled, "tls_enabled should default to false");
        assert!(
            kafka.tls_ca_cert_path.is_none(),
            "tls_ca_cert_path should be None"
        );
    }

    /// A config with no health section must still produce a working endpoint, since the liveness
    /// probe targets it on every pod whether or not that pod configured one.
    #[test]
    fn health_config_defaults_when_the_section_is_absent() {
        let health: HealthConfig = serde_json::from_str("{}").unwrap();

        assert!(health.enabled, "the health endpoint defaults to on");
        assert_eq!(health.listen_addr.port(), 8546);
        assert_eq!(health.threshold, crate::health::DEFAULT_HEALTH_THRESHOLD);
    }

    #[test]
    fn health_config_honours_an_explicit_opt_out() {
        let health: HealthConfig = serde_json::from_str(r#"{"enabled": false}"#).unwrap();

        assert!(!health.enabled, "enabled: false must turn the server off");
        assert_eq!(
            health.threshold,
            crate::health::DEFAULT_HEALTH_THRESHOLD,
            "the remaining fields keep their defaults"
        );
    }

    /// A misspelled key would otherwise be dropped in silence, leaving an operator who meant to
    /// change the endpoint with the defaults and no sign that the setting never took effect.
    #[test]
    fn health_config_rejects_an_unknown_key() {
        let err = serde_json::from_str::<HealthConfig>(r#"{"enable": false}"#)
            .expect_err("an unknown key must be rejected");

        assert!(
            err.to_string().contains("enable"),
            "the error should name the offending key, got: {err}"
        );
    }

    /// Every config section rejects an unknown key instead of silently dropping it, so
    /// an operator's typo surfaces at startup rather than reverting that setting to its
    /// default with no trace. Covers the background-service sections tuned in production.
    #[test]
    fn config_sections_reject_unknown_keys() {
        fn assert_rejects<T: serde::de::DeserializeOwned + std::fmt::Debug>(section: &str) {
            let err = serde_json::from_str::<T>(r#"{"totally_not_a_field": true}"#)
                .expect_err(&format!("{section} must reject an unknown key"));
            assert!(
                err.to_string().contains("totally_not_a_field"),
                "{section} error should name the unknown key, got: {err}"
            );
        }

        assert_rejects::<ReassignmentConfig>("reassignment");
        assert_rejects::<ExpirationConfig>("expiration");
        assert_rejects::<EscrowReconcilerConfig>("escrow_reconciler");
        assert_rejects::<LivenessCheckerConfig>("liveness_checker");
        assert_rejects::<IndexerClientConfig>("indexer_client");
        assert_rejects::<EventStreamingConfig>("event_streaming_config");
    }

    /// A mistyped top-level section name (`expiraton` for `expiration`) is rejected
    /// outright, so a whole block of settings can never silently fall back to defaults.
    #[test]
    fn top_level_config_rejects_an_unknown_section() {
        let err = serde_json::from_str::<Config>(r#"{"expiraton": {}}"#)
            .expect_err("an unknown top-level section must be rejected");
        assert!(
            err.to_string().contains("expiraton"),
            "the error should name the unknown section, got: {err}"
        );
    }

    #[test]
    fn health_validate_rejects_a_port_shared_with_the_admin_rpc() {
        let health = HealthConfig {
            listen_addr: "0.0.0.0:8545".parse().unwrap(),
            ..HealthConfig::default()
        };
        let admin_rpc = "127.0.0.1:8545".parse().unwrap();

        let err = health
            .validate(admin_rpc)
            .expect_err("a shared port must be rejected even across interfaces");
        assert!(
            err.contains("8545"),
            "the error should name the port: {err}"
        );

        assert!(
            HealthConfig::default().validate(admin_rpc).is_ok(),
            "distinct ports are fine"
        );
    }

    #[test]
    fn health_validate_rejects_an_ephemeral_port() {
        let health = HealthConfig {
            listen_addr: "0.0.0.0:0".parse().unwrap(),
            ..HealthConfig::default()
        };
        let admin_rpc = "0.0.0.0:8545".parse().unwrap();

        assert!(
            health.validate(admin_rpc).is_err(),
            "port 0 must be rejected: the probe targets a fixed port"
        );
        assert!(
            HealthConfig {
                enabled: false,
                ..health
            }
            .validate(admin_rpc)
            .is_ok(),
            "a disabled endpoint binds nothing, so its address does not matter"
        );
    }
}
