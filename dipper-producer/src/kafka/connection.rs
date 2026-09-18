//! Broker connection helpers shared by the Kafka producer and consumer:
//! SASL/TLS setup and the client bootstrap built from them.

use std::{
    path::Path,
    sync::{Arc, Once},
};

use rskafka::client::{Client, ClientBuilder, Credentials, SaslConfig};
use rustls::ClientConfig;

static RUSTLS_CRYPTO_PROVIDER: Once = Once::new();

/// Broker connection parameters, borrowed from the producer or consumer config.
pub(crate) struct ConnectOptions<'a> {
    pub brokers: &'a [String],
    pub sasl_mechanism: Option<&'a str>,
    pub sasl_username: Option<&'a str>,
    pub sasl_password: Option<&'a str>,
    pub tls_enabled: bool,
    pub tls_ca_cert_path: Option<&'a Path>,
}

/// Connects a Kafka client with the given SASL/TLS settings.
pub(crate) async fn connect(opts: ConnectOptions<'_>) -> Result<Client, ConnectionError> {
    if opts.brokers.is_empty() {
        return Err(ConnectionError::MissingBrokers);
    }

    let mut builder = ClientBuilder::new(opts.brokers.to_vec());

    if let Some(mechanism_str) = opts.sasl_mechanism {
        let mechanism: SaslMechanism = mechanism_str.parse()?;
        let sasl_config = build_sasl_config(mechanism, opts.sasl_username, opts.sasl_password)?;
        builder = builder.sasl_config(sasl_config);
    }

    if opts.tls_enabled {
        let tls_config = build_tls_config(opts.tls_ca_cert_path)?;
        builder = builder.tls_config(tls_config);
    }

    builder.build().await.map_err(ConnectionError::Connection)
}

/// Builds SASL configuration from the provided mechanism and credentials.
pub(crate) fn build_sasl_config(
    mechanism: SaslMechanism,
    username: Option<&str>,
    password: Option<&str>,
) -> Result<SaslConfig, ConnectionError> {
    let username = username.ok_or(ConnectionError::MissingSaslUsername)?;
    let password = password.ok_or(ConnectionError::MissingSaslPassword)?;

    let credentials = Credentials::new(username.to_string(), password.to_string());

    Ok(match mechanism {
        SaslMechanism::Plain => SaslConfig::Plain(credentials),
        SaslMechanism::ScramSha256 => SaslConfig::ScramSha256(credentials),
        SaslMechanism::ScramSha512 => SaslConfig::ScramSha512(credentials),
    })
}

/// Builds TLS configuration. A custom CA certificate path makes the client
/// trust that CA for broker verification; otherwise system roots are used.
pub(crate) fn build_tls_config(
    ca_cert_path: Option<&Path>,
) -> Result<Arc<ClientConfig>, ConnectionError> {
    install_rustls_crypto_provider();

    let root_store = match ca_cert_path {
        Some(path) => {
            let ca_pem =
                fs_err::read(path).map_err(|e| ConnectionError::TlsCaCert { source: e })?;
            let mut reader = std::io::BufReader::new(&ca_pem[..]);
            let certs: Vec<_> = rustls_pemfile::certs(&mut reader)
                .collect::<Result<_, _>>()
                .map_err(|e| ConnectionError::TlsCaCert { source: e })?;

            let mut store = rustls::RootCertStore::empty();
            for cert in certs {
                store.add(cert).map_err(|e| ConnectionError::TlsCaCert {
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

fn install_rustls_crypto_provider() {
    RUSTLS_CRYPTO_PROVIDER.call_once(|| {
        // Necessary for the Kafka client: it builds a Rustls TLS config directly,
        // so install a provider before `ClientConfig::builder()` tries to infer one.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

/// Errors that can occur while establishing a broker connection.
#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    /// Failed to connect to Kafka brokers
    #[error("failed to connect to Kafka brokers")]
    Connection(#[source] rskafka::client::error::Error),

    /// The brokers list is empty
    #[error("brokers must list at least 1 broker address")]
    MissingBrokers,

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
pub(crate) enum SaslMechanism {
    Plain,
    ScramSha256,
    ScramSha512,
}

impl std::str::FromStr for SaslMechanism {
    type Err = ConnectionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "PLAIN" => Ok(Self::Plain),
            "SCRAM-SHA-256" => Ok(Self::ScramSha256),
            "SCRAM-SHA-512" => Ok(Self::ScramSha512),
            _ => Err(ConnectionError::UnsupportedSaslMechanism(s.to_string())),
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
        let result = build_sasl_config(SaslMechanism::Plain, Some("user"), Some("pass"));
        assert!(matches!(result, Ok(SaslConfig::Plain(_))));
    }

    #[test]
    fn should_build_scram_sha_256_sasl_config() {
        let result = build_sasl_config(SaslMechanism::ScramSha256, Some("user"), Some("pass"));
        assert!(matches!(result, Ok(SaslConfig::ScramSha256(_))));
    }

    #[test]
    fn should_build_sasl_sha_512_sasl_config() {
        let result = build_sasl_config(SaslMechanism::ScramSha512, Some("user"), Some("pass"));
        assert!(matches!(result, Ok(SaslConfig::ScramSha512(_))));
    }

    #[test]
    fn should_throw_err_on_missing_credentials() {
        // Missing username
        assert!(matches!(
            build_sasl_config(SaslMechanism::Plain, None, Some("pass")),
            Err(ConnectionError::MissingSaslUsername)
        ));

        // Missing password
        assert!(matches!(
            build_sasl_config(SaslMechanism::Plain, Some("user"), None),
            Err(ConnectionError::MissingSaslPassword)
        ));
    }
}
