//! Kafka producer for sending dipper events on a configured topic

use std::{
    path::{Path, PathBuf},
    sync::{Arc, Once},
    time::Duration,
};

use rskafka::{
    client::{
        ClientBuilder, Credentials, SaslConfig,
        partition::{Compression, PartitionClient, UnknownTopicHandling},
    },
    record::Record,
};
use rustls::ClientConfig;

static RUSTLS_CRYPTO_PROVIDER: Once = Once::new();

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

    /// Creates a new Kafka producer with the given configuration.
    #[expect(
        clippy::cast_possible_wrap,
        reason = "predates this lint; fix when next touched"
    )]
    pub async fn new(config: &KafkaConfig) -> Result<Self, Error> {
        if config.partitions == 0 {
            return Err(Error::InvalidPartitionCount);
        }

        let mut builder = ClientBuilder::new(config.brokers.clone());

        // Configure SASL authentication if mechanism is specified
        if let Some(mechanism_str) = &config.sasl_mechanism {
            let mechanism: SaslMechanism = mechanism_str.parse()?;
            let sasl_config = Self::build_sasl_config(mechanism, config)?;
            builder = builder.sasl_config(sasl_config);
        }

        // Configure TLS if enabled
        if config.tls_enabled {
            let tls_config = Self::build_tls_config(config.tls_ca_cert_path.as_deref())?;
            builder = builder.tls_config(tls_config);
        }

        let client = builder.build().await.map_err(Error::Connection)?;
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

    /// Builds SASL configuration from the provided mechanism and credentials.
    fn build_sasl_config(
        mechanism: SaslMechanism,
        config: &KafkaConfig,
    ) -> Result<SaslConfig, Error> {
        let username = config
            .sasl_username
            .clone()
            .ok_or(Error::MissingSaslUsername)?;
        let password = config
            .sasl_password
            .clone()
            .ok_or(Error::MissingSaslPassword)?;

        let credentials = Credentials::new(username, password);

        Ok(match mechanism {
            SaslMechanism::Plain => SaslConfig::Plain(credentials),
            SaslMechanism::ScramSha256 => SaslConfig::ScramSha256(credentials),
            SaslMechanism::ScramSha512 => SaslConfig::ScramSha512(credentials),
        })
    }

    /// Builds TLS configuration.
    ///
    /// If a custom CA certificate path is provided, the client will trust that CA
    /// for verifying broker connections. Otherwise, system root certificates are used.
    fn build_tls_config(ca_cert_path: Option<&Path>) -> Result<Arc<ClientConfig>, Error> {
        install_rustls_crypto_provider();

        let root_store = match ca_cert_path {
            Some(path) => {
                let ca_pem = fs_err::read(path).map_err(|e| Error::TlsCaCert { source: e })?;
                let mut reader = std::io::BufReader::new(&ca_pem[..]);
                let certs: Vec<_> = rustls_pemfile::certs(&mut reader)
                    .collect::<Result<_, _>>()
                    .map_err(|e| Error::TlsCaCert { source: e })?;

                let mut store = rustls::RootCertStore::empty();
                for cert in certs {
                    store.add(cert).map_err(|e| Error::TlsCaCert {
                        source: std::io::Error::new(std::io::ErrorKind::InvalidData, e),
                    })?;
                }
                store
            }
            None => rustls::RootCertStore {
                roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
            },
        };

        let tls_config = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        Ok(Arc::new(tls_config))
    }

    /// Sends an event to Kafka.
    ///
    /// Events are partitioned by the partition key (table discriminator) before being written to
    /// Kafka. The produce attempt times out after 30 seconds.
    #[expect(
        clippy::cast_sign_loss,
        reason = "predates this lint; fix when next touched"
    )]
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

    /// Computes the partition for a given key.
    ///
    /// Uses a deterministic FNV-1a hash modulo the partition count so that a given
    /// key always maps to the same partition across restarts and instances,
    /// preserving per-key ordering. The partition count is configured via
    /// `KafkaConfig::partitions`.
    #[expect(
        clippy::cast_possible_wrap,
        reason = "predates this lint; fix when next touched"
    )]
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

fn install_rustls_crypto_provider() {
    RUSTLS_CRYPTO_PROVIDER.call_once(|| {
        // Necessary for the Kafka client: it builds a Rustls TLS config directly,
        // so install a provider before `ClientConfig::builder()` tries to infer one.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

/// Errors that can occur when working with the Kafka producer.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to connect to Kafka brokers
    #[error("failed to connect to Kafka brokers")]
    Connection(#[source] rskafka::client::error::Error),

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

    /// Unsupported SASL mechanism
    #[error("unsupported SASL mechanism '{0}', supported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512")]
    UnsupportedSaslMechanism(String),

    /// Missing SASL username
    #[error("sasl_username is required when sasl_mechanism is set")]
    MissingSaslUsername,

    /// Missing SASL password
    #[error("sasl_password is required when sasl_mechanism is set")]
    MissingSaslPassword,

    /// Failed to load TLS CA certificate
    #[error("failed to load TLS CA certificate")]
    TlsCaCert {
        #[source]
        source: std::io::Error,
    },
}

/// Supported SASL authentication mechanisms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SaslMechanism {
    Plain,
    ScramSha256,
    ScramSha512,
}

impl std::str::FromStr for SaslMechanism {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "PLAIN" => Ok(Self::Plain),
            "SCRAM-SHA-256" => Ok(Self::ScramSha256),
            "SCRAM-SHA-512" => Ok(Self::ScramSha512),
            _ => Err(Error::UnsupportedSaslMechanism(s.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_parse_sasl_mechanism_correctly() {
        // Case-insensitive parsing
        assert_eq!(
            "PLAIN".parse::<SaslMechanism>().unwrap(),
            SaslMechanism::Plain
        );
        assert_eq!(
            "plain".parse::<SaslMechanism>().unwrap(),
            SaslMechanism::Plain
        );
        assert_eq!(
            "SCRAM-SHA-256".parse::<SaslMechanism>().unwrap(),
            SaslMechanism::ScramSha256
        );
        assert_eq!(
            "scram-sha-256".parse::<SaslMechanism>().unwrap(),
            SaslMechanism::ScramSha256
        );
        assert_eq!(
            "SCRAM-SHA-512".parse::<SaslMechanism>().unwrap(),
            SaslMechanism::ScramSha512
        );

        // Unsupported mechanism
        assert!("GSSAPI".parse::<SaslMechanism>().is_err());
    }

    #[test]
    fn should_build_sasl_plain_sasl_config() {
        let config = make_kafka_config(Some("PLAIN"), Some("user".into()), Some("pass".into()));
        let result = KafkaProducer::build_sasl_config(SaslMechanism::Plain, &config);
        assert!(matches!(result, Ok(SaslConfig::Plain(_))));
    }

    #[test]
    fn should_build_scram_sha_256_sasl_config() {
        let config = make_kafka_config(
            Some("SCRAM-SHA-256"),
            Some("user".into()),
            Some("pass".into()),
        );
        let result = KafkaProducer::build_sasl_config(SaslMechanism::ScramSha256, &config);
        assert!(matches!(result, Ok(SaslConfig::ScramSha256(_))));
    }

    #[test]
    fn should_build_sasl_sha_512_sasl_config() {
        let config = make_kafka_config(
            Some("SCRAM-SHA-512"),
            Some("user".into()),
            Some("pass".into()),
        );
        let result = KafkaProducer::build_sasl_config(SaslMechanism::ScramSha512, &config);
        assert!(matches!(result, Ok(SaslConfig::ScramSha512(_))));
    }

    #[test]
    fn should_throw_err_on_missing_credentials() {
        // Missing username
        let config = make_kafka_config(Some("PLAIN"), None, Some("pass".into()));
        assert!(matches!(
            KafkaProducer::build_sasl_config(SaslMechanism::Plain, &config),
            Err(Error::MissingSaslUsername)
        ));

        // Missing password
        let config = make_kafka_config(Some("PLAIN"), Some("user".into()), None);
        assert!(matches!(
            KafkaProducer::build_sasl_config(SaslMechanism::Plain, &config),
            Err(Error::MissingSaslPassword)
        ));
    }

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
        }
    }
}
