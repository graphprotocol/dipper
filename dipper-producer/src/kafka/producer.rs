//! Kafka producer for sending dipper events on a configured topic

use std::{path::PathBuf, sync::Arc, time::Duration};

use rskafka::{
    client::partition::{Compression, PartitionClient, UnknownTopicHandling},
    record::Record,
};

use super::connection::{self, ConnectOptions, ConnectionError};

/// Kafka producer configuration.
#[derive(Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaConfig {
    /// Kafka broker addresses.
    pub brokers: Vec<String>,
    /// Kafka topic name.
    #[serde(default = "default_kafka_topic")]
    pub topic: String,
    /// Number of partitions used for key-based partition hashing. Must match the
    /// topic's real partition count (a mismatch makes connecting fail and retry
    /// forever), and changing it re-shuffles keys, breaking per-key ordering.
    #[serde(default = "default_kafka_partitions")]
    pub partitions: u32,
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
    /// Seconds allowed for the initial connect and partition binding (default:
    /// 60). Load-bearing: the underlying client retries an unreachable broker
    /// forever, so without this bound `new` would never return.
    #[serde(default = "super::consumer::default_connect_timeout_secs")]
    pub connect_timeout_secs: u64,
}

// Manual impl instead of derive: the service logs the whole config with Debug
// formatting at startup, so the SASL password must never reach the output.
impl std::fmt::Debug for KafkaConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaConfig")
            .field("brokers", &self.brokers)
            .field("topic", &self.topic)
            .field("partitions", &self.partitions)
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

/// Default Kafka topic for subgraph indexing agreement events.
pub fn default_kafka_topic() -> String {
    "dipper.subgraph.indexing.agreement.events".to_string()
}

/// Default number of Kafka partitions used for key-based partition hashing.
pub fn default_kafka_partitions() -> u32 {
    16
}

/// Kafka producer for sending worker events.
///
/// The producer is thread-safe and can be shared across tasks via `Arc`.
pub struct KafkaProducer {
    partitions: u32,
    partition_clients: Vec<Arc<PartitionClient>>,
}

impl KafkaProducer {
    const PRODUCE_TIMEOUT: Duration = Duration::from_secs(30);

    /// Creates a new Kafka producer with the given configuration. Bounded by
    /// `connect_timeout_secs`, since the underlying client retries an
    /// unreachable broker forever.
    pub async fn new(config: &KafkaConfig) -> Result<Self, Error> {
        tokio::time::timeout(
            Duration::from_secs(config.connect_timeout_secs),
            Self::new_inner(config),
        )
        .await
        .map_err(|_| Error::Timeout)?
    }

    async fn new_inner(config: &KafkaConfig) -> Result<Self, Error> {
        if config.partitions == 0 {
            return Err(Error::InvalidPartitionCount);
        }

        let client = connection::connect(ConnectOptions {
            brokers: &config.brokers,
            sasl_mechanism: config.sasl_mechanism.as_deref(),
            sasl_username: config.sasl_username.as_deref(),
            sasl_password: config.sasl_password.as_deref(),
            tls_enabled: config.tls_enabled,
            tls_ca_cert_path: config.tls_ca_cert_path.as_deref(),
        })
        .await?;
        let client = Arc::new(client);
        let mut partition_clients = Vec::with_capacity(config.partitions as usize);

        for partition in 0..config.partitions {
            let partition_client = Arc::new(
                client
                    .partition_client(&config.topic, partition as i32, UnknownTopicHandling::Error)
                    .await
                    .map_err(Error::PartitionClient)?,
            );
            partition_clients.push(partition_client);
        }

        Ok(Self {
            partitions: config.partitions,
            partition_clients,
        })
    }

    /// Sends an event to Kafka, partitioned by the partition key (table
    /// discriminator). The produce attempt times out after 30 seconds.
    pub async fn send(&self, partition_key: &str, payload: &[u8]) -> Result<(), Error> {
        let partition = self.partition_for_key(partition_key);
        let partition_client = &self.partition_clients[partition as usize];

        let record = Record {
            key: Some(partition_key.as_bytes().to_vec()),
            value: Some(payload.to_vec()),
            headers: Default::default(),
            timestamp: chrono::Utc::now(),
        };

        tokio::time::timeout(
            Self::PRODUCE_TIMEOUT,
            partition_client.produce(vec![record], Compression::Gzip),
        )
        .await
        .map_err(|_| Error::Timeout)?
        .map_err(Error::Send)
        .map(|_| ())
    }

    /// Computes the partition for a key: deterministic FNV-1a hash modulo
    /// `KafkaConfig::partitions`, so a key maps to the same partition across
    /// restarts and instances, preserving per-key ordering.
    fn partition_for_key(&self, key: &str) -> i32 {
        // FNV-1a (32-bit): order-dependent and well-distributed, unlike a byte sum.
        const FNV_OFFSET_BASIS: u32 = 0x811c_9dc5;
        const FNV_PRIME: u32 = 0x0100_0193;
        let hash = key.bytes().fold(FNV_OFFSET_BASIS, |hash, b| {
            (hash ^ u32::from(b)).wrapping_mul(FNV_PRIME)
        });
        // `partitions` is guaranteed non-zero by `KafkaProducer::new`.
        (hash % self.partitions) as i32
    }
}

/// Errors that can occur when working with the Kafka producer.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to establish the broker connection (SASL, TLS, or bootstrap)
    #[error(transparent)]
    Connection(#[from] ConnectionError),

    /// Failed to get partition client
    #[error("failed to get partition client")]
    PartitionClient(#[source] rskafka::client::error::Error),

    /// Failed to send event to Kafka
    #[error("failed to send event to Kafka")]
    Send(#[source] rskafka::client::error::Error),

    /// Kafka operation timed out
    #[error("Kafka operation timed out")]
    Timeout,

    /// Partition count must be greater than zero
    #[error("partitions must be greater than zero")]
    InvalidPartitionCount,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_output_redacts_the_sasl_password() {
        let config = make_kafka_config(Some("PLAIN"), Some("user".into()), Some("hunter2".into()));
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

    #[tokio::test]
    async fn should_reject_zero_partitions() {
        let mut config = make_kafka_config(None, None, None);
        config.partitions = 0;
        assert!(matches!(
            KafkaProducer::new(&config).await,
            Err(Error::InvalidPartitionCount)
        ));
    }

    // -------- Test helpers --------

    /// Creates a test Kafka config with optional SASL credentials.
    fn make_kafka_config(
        mechanism: Option<&str>,
        sasl_user: Option<String>,
        sasl_pass: Option<String>,
    ) -> KafkaConfig {
        KafkaConfig {
            brokers: vec!["localhost:9092".to_string()],
            topic: "test".to_string(),
            partitions: 1,
            sasl_mechanism: mechanism.map(String::from),
            sasl_username: sasl_user,
            sasl_password: sasl_pass,
            tls_enabled: false,
            tls_ca_cert_path: None,
            connect_timeout_secs: 60,
        }
    }
}
