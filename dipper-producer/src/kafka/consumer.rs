//! Kafka consumer for the subgraph indexing request events Studio produces.
//! Deliberately a thin fetch layer: rskafka has no consumer groups, so offset
//! tracking belongs to the caller (the dipper persists offsets in its own DB).

use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};

use rskafka::{
    client::{
        Client,
        partition::{OffsetAt, PartitionClient, UnknownTopicHandling},
    },
    record::RecordAndOffset,
};

use super::connection::{self, ConnectOptions, ConnectionError};

/// Kafka consumer configuration.
#[derive(Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaConsumerConfig {
    /// Kafka broker addresses.
    pub brokers: Vec<String>,
    /// Kafka topic to consume. Deliberately has no default: a consumer pointed
    /// at a missing or misnamed topic must fail at startup, not idle on nothing.
    pub topic: String,
    /// SASL authentication mechanism (e.g., "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512").
    #[serde(default)]
    pub sasl_mechanism: Option<String>,
    /// SASL username.
    #[serde(default)]
    pub sasl_username: Option<String>,
    /// SASL password.
    #[serde(default)]
    pub sasl_password: Option<String>,
    /// Enable TLS encryption.
    #[serde(default)]
    pub tls_enabled: bool,
    /// Path to a PEM-encoded CA certificate file for TLS verification.
    #[serde(default)]
    pub tls_ca_cert_path: Option<PathBuf>,
    /// Seconds allowed for the initial connect and topic discovery (default:
    /// 60). Load-bearing: the underlying client retries an unreachable broker
    /// forever, so without this bound `connect` would never return.
    #[serde(default = "default_connect_timeout_secs")]
    pub connect_timeout_secs: u64,
}

/// Default number of seconds allowed for connect and topic discovery.
pub fn default_connect_timeout_secs() -> u64 {
    60
}

// Manual impl instead of derive: the service logs the whole config with Debug
// formatting at startup, so the SASL password must never reach the output.
impl std::fmt::Debug for KafkaConsumerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaConsumerConfig")
            .field("brokers", &self.brokers)
            .field("topic", &self.topic)
            .field("sasl_mechanism", &self.sasl_mechanism)
            .field("sasl_username", &self.sasl_username)
            .field(
                "sasl_password",
                &self.sasl_password.as_ref().map(|_| "<redacted>"),
            )
            .field("tls_enabled", &self.tls_enabled)
            .field("tls_ca_cert_path", &self.tls_ca_cert_path)
            .field("connect_timeout_secs", &self.connect_timeout_secs)
            .finish()
    }
}

/// Kafka consumer bound to a single topic, with a partition client per
/// discovered partition. Thread-safe; share across tasks via `Arc`.
pub struct KafkaConsumer {
    client: Client,
    topic: String,
    partition_clients: BTreeMap<i32, Arc<PartitionClient>>,
}

impl KafkaConsumer {
    /// Margin added on top of a fetch's `max_wait_ms` before the whole call is
    /// abandoned, covering broker round-trip and retry time.
    const FETCH_TIMEOUT_MARGIN: Duration = Duration::from_secs(30);

    /// Timeout for offset queries, which carry no server-side wait.
    const OFFSET_TIMEOUT: Duration = Duration::from_secs(30);

    /// Connects to the brokers and binds to the configured topic. Partitions
    /// are discovered from broker metadata, so there is no partition count to
    /// configure; a topic the credentials cannot see is an error. The whole
    /// sequence is bounded by `connect_timeout_secs`, since the underlying
    /// client would otherwise retry an unreachable broker forever.
    pub async fn connect(config: &KafkaConsumerConfig) -> Result<Self, ConsumerError> {
        tokio::time::timeout(
            Duration::from_secs(config.connect_timeout_secs),
            Self::connect_inner(config),
        )
        .await
        .map_err(|_| ConsumerError::Timeout)?
    }

    async fn connect_inner(config: &KafkaConsumerConfig) -> Result<Self, ConsumerError> {
        let client = connection::connect(ConnectOptions {
            brokers: &config.brokers,
            sasl_mechanism: config.sasl_mechanism.as_deref(),
            sasl_username: config.sasl_username.as_deref(),
            sasl_password: config.sasl_password.as_deref(),
            tls_enabled: config.tls_enabled,
            tls_ca_cert_path: config.tls_ca_cert_path.as_deref(),
        })
        .await?;

        let partitions = discover_partitions(&client, &config.topic).await?;

        let mut partition_clients = BTreeMap::new();
        for partition in partitions {
            let partition_client = client
                .partition_client(&config.topic, partition, UnknownTopicHandling::Error)
                .await
                .map_err(ConsumerError::PartitionClient)?;
            partition_clients.insert(partition, Arc::new(partition_client));
        }

        Ok(Self {
            client,
            topic: config.topic.clone(),
            partition_clients,
        })
    }

    /// The topic this consumer is bound to.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// The partition ids discovered for the topic, in ascending order.
    pub fn partitions(&self) -> Vec<i32> {
        self.partition_clients.keys().copied().collect()
    }

    /// Re-reads broker metadata and returns the topic's current partition
    /// count, so callers can notice a topic growing partitions after connect
    /// (this consumer keeps serving only the set discovered at connect).
    pub async fn current_partition_count(&self) -> Result<usize, ConsumerError> {
        tokio::time::timeout(
            Self::OFFSET_TIMEOUT,
            discover_partitions(&self.client, &self.topic),
        )
        .await
        .map_err(|_| ConsumerError::Timeout)?
        .map(|partitions| partitions.len())
    }

    /// Fetches records from one partition starting at `offset`, waiting up to
    /// `max_wait_ms` for data to arrive. Returns the records (with their
    /// offsets) and the partition's current high watermark.
    pub async fn fetch(
        &self,
        partition: i32,
        offset: i64,
        max_bytes: i32,
        max_wait_ms: i32,
    ) -> Result<(Vec<RecordAndOffset>, i64), ConsumerError> {
        let partition_client = self.partition_client(partition)?;
        let timeout = Duration::from_millis(max_wait_ms.max(0) as u64) + Self::FETCH_TIMEOUT_MARGIN;

        // rskafka encodes the range's exclusive end minus 1 as the wire-level
        // max_bytes, so widen by 1 to request the full `max_bytes` budget.
        tokio::time::timeout(
            timeout,
            partition_client.fetch_records(offset, 1..max_bytes.saturating_add(1), max_wait_ms),
        )
        .await
        .map_err(|_| ConsumerError::Timeout)?
        .map_err(ConsumerError::Fetch)
    }

    /// Queries one partition's earliest or latest offset.
    pub async fn offset(&self, partition: i32, at: OffsetAt) -> Result<i64, ConsumerError> {
        let partition_client = self.partition_client(partition)?;

        tokio::time::timeout(Self::OFFSET_TIMEOUT, partition_client.get_offset(at))
            .await
            .map_err(|_| ConsumerError::Timeout)?
            .map_err(ConsumerError::Offset)
    }

    fn partition_client(&self, partition: i32) -> Result<&Arc<PartitionClient>, ConsumerError> {
        self.partition_clients
            .get(&partition)
            .ok_or(ConsumerError::UnknownPartition {
                partition,
                topic: self.topic.clone(),
            })
    }
}

/// Lists the topic's partitions from broker metadata; a topic the broker does
/// not report (missing, or invisible to the credentials) is an error.
async fn discover_partitions(
    client: &Client,
    topic: &str,
) -> Result<std::collections::BTreeSet<i32>, ConsumerError> {
    let topics = client
        .list_topics()
        .await
        .map_err(ConsumerError::Metadata)?;
    topics
        .into_iter()
        .find(|t| t.name == topic)
        .map(|t| t.partitions)
        .ok_or_else(|| ConsumerError::TopicNotFound {
            topic: topic.to_string(),
        })
}

/// Errors that can occur when working with the Kafka consumer.
#[derive(Debug, thiserror::Error)]
pub enum ConsumerError {
    /// Failed to establish the broker connection (SASL, TLS, or bootstrap)
    #[error(transparent)]
    Connection(#[from] ConnectionError),

    /// Failed to list topics from the broker
    #[error("failed to list topics from the broker")]
    Metadata(#[source] rskafka::client::error::Error),

    /// The configured topic does not exist (or is not visible to the credentials)
    #[error("topic '{topic}' does not exist on the broker or is not visible to the credentials")]
    TopicNotFound { topic: String },

    /// Failed to get partition client
    #[error("failed to get partition client")]
    PartitionClient(#[source] rskafka::client::error::Error),

    /// The requested partition is not part of the bound topic
    #[error("partition {partition} is not part of topic '{topic}'")]
    UnknownPartition { partition: i32, topic: String },

    /// Failed to fetch records from Kafka
    #[error("failed to fetch records from Kafka")]
    Fetch(#[source] rskafka::client::error::Error),

    /// Failed to query a partition offset
    #[error("failed to query partition offset")]
    Offset(#[source] rskafka::client::error::Error),

    /// Kafka operation timed out
    #[error("Kafka operation timed out")]
    Timeout,
}

impl ConsumerError {
    /// Whether this is the broker refusing a fetch offset outside its retained
    /// range. Callers recover by re-anchoring to the earliest available offset
    /// (records below it were deleted by retention).
    pub fn is_offset_out_of_range(&self) -> bool {
        matches!(
            self,
            Self::Fetch(rskafka::client::error::Error::ServerError {
                protocol_error: rskafka::client::error::ProtocolError::OffsetOutOfRange,
                ..
            })
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_deserializes_with_topic_and_defaults() {
        let json = r#"{
            "brokers": ["localhost:9092"],
            "topic": "studio.subgraph.indexing.requests"
        }"#;

        let config: KafkaConsumerConfig = serde_json::from_str(json).expect("valid config");
        assert_eq!(config.brokers, vec!["localhost:9092".to_string()]);
        assert_eq!(config.topic, "studio.subgraph.indexing.requests");
        assert_eq!(config.sasl_mechanism, None);
        assert_eq!(config.sasl_username, None);
        assert_eq!(config.sasl_password, None);
        assert!(!config.tls_enabled);
        assert_eq!(config.tls_ca_cert_path, None);
        assert_eq!(config.connect_timeout_secs, 60);
    }

    #[tokio::test]
    async fn connect_gives_up_after_the_configured_timeout() {
        // Port 1 refuses connections, which the underlying client retries
        // forever; the configured bound must turn that into an error.
        let config = KafkaConsumerConfig {
            brokers: vec!["127.0.0.1:1".to_string()],
            topic: "test".to_string(),
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            tls_enabled: false,
            tls_ca_cert_path: None,
            connect_timeout_secs: 1,
        };
        let started = std::time::Instant::now();
        let result = KafkaConsumer::connect(&config).await;
        assert!(
            matches!(result, Err(ConsumerError::Timeout)),
            "expected a timeout, got {:?}",
            result.err()
        );
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "connect must give up promptly"
        );
    }

    #[tokio::test]
    async fn connect_rejects_an_empty_brokers_list() {
        let config = KafkaConsumerConfig {
            brokers: Vec::new(),
            topic: "test".to_string(),
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            tls_enabled: false,
            tls_ca_cert_path: None,
            connect_timeout_secs: 60,
        };
        assert!(matches!(
            KafkaConsumer::connect(&config).await,
            Err(ConsumerError::Connection(
                super::super::connection::ConnectionError::MissingBrokers
            ))
        ));
    }

    #[test]
    fn config_without_topic_is_rejected() {
        // No default topic on purpose: a missing name must fail configuration
        // loading, not silently consume from nowhere.
        let json = r#"{ "brokers": ["localhost:9092"] }"#;

        let err = serde_json::from_str::<KafkaConsumerConfig>(json).unwrap_err();
        assert!(
            err.to_string().contains("topic"),
            "error should name the missing field: {err}"
        );
    }

    #[test]
    fn config_with_unknown_fields_is_rejected() {
        let json = r#"{
            "brokers": ["localhost:9092"],
            "topic": "t",
            "partitions": 16
        }"#;

        let err = serde_json::from_str::<KafkaConsumerConfig>(json).unwrap_err();
        assert!(
            err.to_string().contains("partitions"),
            "error should name the unknown field: {err}"
        );
    }

    #[test]
    fn debug_output_redacts_the_sasl_password() {
        let config = KafkaConsumerConfig {
            brokers: vec!["localhost:9092".to_string()],
            topic: "test".to_string(),
            sasl_mechanism: Some("PLAIN".to_string()),
            sasl_username: Some("user".to_string()),
            sasl_password: Some("hunter2".to_string()),
            tls_enabled: false,
            tls_ca_cert_path: None,
            connect_timeout_secs: 60,
        };
        let rendered = format!("{config:?}");
        assert!(
            !rendered.contains("hunter2"),
            "debug output must not contain the password: {rendered}"
        );
        assert!(
            rendered.contains("<redacted>"),
            "debug output should mark the password as redacted: {rendered}"
        );
    }
}
