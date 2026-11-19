use std::time::Duration;

use anyhow::Context;
use sqlx::{
    Pool, Postgres,
    postgres::{PgConnectOptions, PgPoolOptions},
};

use crate::config::DbConfig;

/// Default max connections
pub const DEFAULT_MAX_CONNECTIONS: u32 = 32;

/// How long a caller waits for a free pooled connection before erroring. Set
/// explicitly so connection-pool starvation surfaces as a bounded error
/// instead of relying on the driver default.
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);

/// Connect to the database
#[expect(
    clippy::expect_used,
    reason = "predates this lint; fix when next touched"
)]
pub async fn connect(conf: &DbConfig) -> anyhow::Result<Pool<Postgres>> {
    // sqlx checks a `verify-ca` certificate with rustls's process-wide provider.
    crate::tls::install_crypto_provider();
    let mut conn_options: PgConnectOptions = conf.url.as_str().parse().expect("Invalid DB URL");
    conn_options = conn_options.username(&conf.username);
    // Without a password, dipper only gets in where the server asks for none.
    match &conf.password {
        Some(password) => conn_options = conn_options.password(password),
        None => tracing::info!(
            "db.password is not set: connecting without a password of dipper's own, as through \
             the Cloud SQL Auth Proxy with automatic IAM authentication"
        ),
    }

    // Try to connect to the DB
    let pool = PgPoolOptions::new()
        .max_connections(conf.max_connections.unwrap_or(DEFAULT_MAX_CONNECTIONS))
        .acquire_timeout(ACQUIRE_TIMEOUT)
        .connect_with(conn_options)
        .await
        .map_err(|err| explain_refusal(err, conf.password.is_none()))
        .context("failed to connect to DB")?;

    Ok(pool)
}

/// Postgres's code for a failed password check (`invalid_password`).
const INVALID_PASSWORD: &str = "28P01";

/// Adds to a refused password check that dipper had no password to give, which the server's own
/// message ("password authentication failed") does not say.
fn explain_refusal(err: sqlx::Error, no_password: bool) -> anyhow::Error {
    let refused_password = matches!(
        &err,
        sqlx::Error::Database(db) if db.code().as_deref() == Some(INVALID_PASSWORD)
    );
    if no_password && refused_password {
        anyhow::Error::new(err).context(
            "the database asked for a password, but db.password is not set; set it, or connect \
             through something that authenticates for dipper, such as the Cloud SQL Auth Proxy",
        )
    } else {
        err.into()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use super::*;

    /// Postgres's SSLRequest: the message length, 8, then the request code 80877103.
    const SSL_REQUEST: [u8; 8] = [0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f];

    /// The first byte of a TLS record holding a handshake message, which a ClientHello is.
    const TLS_HANDSHAKE_RECORD: u8 = 0x16;

    fn db_config(url: &str) -> DbConfig {
        serde_json::from_value(serde_json::json!({
            "url": url,
            "username": "dipper",
            "password": "unused",
        }))
        .expect("a valid db config")
    }

    #[tokio::test]
    async fn sslmode_require_starts_a_tls_handshake() {
        // A server that agrees to TLS and records what the client sends next. It never completes
        // the handshake, so the connect itself fails; only what reached the server matters.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let mut request = [0u8; 8];
            socket.read_exact(&mut request).await?;
            socket.write_all(b"S").await?;
            let mut next = [0u8; 1];
            socket.read_exact(&mut next).await?;
            Ok::<_, std::io::Error>((request, next[0]))
        });

        let conf = db_config(&format!("postgres://{addr}/dipper?sslmode=require"));
        let (_, seen) = tokio::join!(
            tokio::time::timeout(Duration::from_secs(5), connect(&conf)),
            tokio::time::timeout(Duration::from_secs(5), server),
        );
        let (request, next) = seen
            .expect("dipper never asked the server for TLS")
            .unwrap()
            .expect("the server read the SSLRequest and the next byte");
        assert_eq!(request, SSL_REQUEST, "dipper opens with an SSLRequest");
        assert_eq!(
            next, TLS_HANDSHAKE_RECORD,
            "after the server agrees, dipper starts a TLS handshake"
        );
    }

    /// A throwaway CA, and a server certificate it signed for `localhost`.
    struct TestCerts {
        ca_pem: String,
        server_chain: Vec<rustls::pki_types::CertificateDer<'static>>,
        server_key: rustls::pki_types::PrivateKeyDer<'static>,
    }

    fn test_certs() -> TestCerts {
        use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair, KeyUsagePurpose};
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let ca_key = KeyPair::generate().unwrap();
        let ca = ca_params.self_signed(&ca_key).unwrap();
        let server_key = KeyPair::generate().unwrap();
        let server = CertificateParams::new(vec!["localhost".to_string()])
            .unwrap()
            .signed_by(&server_key, &ca, &ca_key)
            .unwrap();
        TestCerts {
            ca_pem: ca.pem(),
            server_chain: vec![server.der().clone()],
            server_key: rustls::pki_types::PrivatePkcs8KeyDer::from(server_key.serialize_der())
                .into(),
        }
    }

    /// A server that agrees to TLS with `certs` and reports whether the client completed the
    /// handshake, which it only does once it accepts the server's certificate. It then drops the
    /// connection, so the connect fails either way; only the handshake matters.
    async fn tls_server(
        certs: &TestCerts,
    ) -> (std::net::SocketAddr, tokio::task::JoinHandle<bool>) {
        let config = rustls::ServerConfig::builder_with_provider(std::sync::Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(certs.server_chain.clone(), certs.server_key.clone_key())
        .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(config));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handshake = tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return false;
            };
            let mut request = [0u8; 8];
            if socket.read_exact(&mut request).await.is_err() || request != SSL_REQUEST {
                return false;
            }
            if socket.write_all(b"S").await.is_err() {
                return false;
            }
            acceptor.accept(socket).await.is_ok()
        });
        (addr, handshake)
    }

    /// Connects to a TLS server for `localhost` through `host`, with `params` in the URL, and says
    /// whether dipper completed the handshake, that is, accepted the certificate.
    async fn accepts_certificate(host: &str, params: &str, trust_ca: bool) -> bool {
        let certs = test_certs();
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("dipper-tls-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let ca = dir.join("ca.pem");
        std::fs::write(&ca, &certs.ca_pem).unwrap();
        let (addr, handshake) = tls_server(&certs).await;
        let root = if trust_ca {
            format!("&sslrootcert={}", ca.display())
        } else {
            String::new()
        };
        let url = format!("postgres://{host}:{}/dipper?{params}{root}", addr.port());

        let _ = tokio::time::timeout(Duration::from_secs(10), connect(&db_config(&url))).await;
        let accepted = tokio::time::timeout(Duration::from_secs(10), handshake)
            .await
            .map(|joined| joined.unwrap_or(false))
            .unwrap_or(false);
        let _ = std::fs::remove_dir_all(&dir);
        accepted
    }

    #[tokio::test]
    async fn verify_full_accepts_a_certificate_from_the_given_ca_for_the_host() {
        assert!(accepts_certificate("localhost", "sslmode=verify-full", true).await);
    }

    #[tokio::test]
    async fn verify_full_refuses_a_certificate_for_another_host() {
        // The certificate names `localhost`; dipper reaches the server as 127.0.0.1.
        assert!(!accepts_certificate("127.0.0.1", "sslmode=verify-full", true).await);
    }

    #[tokio::test]
    async fn verify_full_refuses_a_certificate_from_an_unknown_ca() {
        assert!(!accepts_certificate("localhost", "sslmode=verify-full", false).await);
    }

    #[tokio::test]
    async fn verify_ca_accepts_a_certificate_from_the_given_ca() {
        // sqlx checks a `verify-ca` certificate through rustls's process-wide crypto provider;
        // with 2 backends linked and none installed, that panicked.
        assert!(accepts_certificate("localhost", "sslmode=verify-ca", true).await);
    }

    #[tokio::test]
    async fn verify_ca_still_checks_the_hostname() {
        // sqlx 0.8 skips the hostname check for `verify-ca` only on the error rustls used to
        // return for a name mismatch; rustls 0.23 returns a newer one, so the check stays. Should
        // a later sqlx skip it as libpq does, this fails, and the docs on `db.url` need updating.
        assert!(!accepts_certificate("127.0.0.1", "sslmode=verify-ca", true).await);
    }

    /// A temporary Postgres that, unlike pgtemp's default, checks passwords on TCP connections.
    fn password_checking_db() -> (pgtemp::PgTempDB, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("dipper-scram-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let hba = dir.join("pg_hba.conf");
        std::fs::write(
            &hba,
            "local all all trust\nhost all all 127.0.0.1/32 scram-sha-256\nhost all all ::1/128 scram-sha-256\n",
        )
        .unwrap();
        let db = pgtemp::PgTempDBBuilder::new()
            .with_password("s3cret")
            .with_config_param("hba_file", hba.to_str().unwrap())
            .start();
        (db, dir)
    }

    fn pgtemp_db_config(temp_db: &pgtemp::PgTempDB, password: Option<&str>) -> DbConfig {
        let mut json = serde_json::json!({
            "url": format!("postgres://127.0.0.1:{}/{}", temp_db.db_port(), temp_db.db_name()),
            "username": temp_db.db_user(),
        });
        if let Some(password) = password {
            json["password"] = password.into();
        }
        serde_json::from_value(json).unwrap()
    }

    #[tokio::test]
    async fn it_connects_without_a_password_where_the_server_asks_for_none() {
        // pgtemp's default server trusts local connections, as a database behind the Cloud SQL
        // Auth Proxy with automatic IAM authentication asks the client for no password either.
        let temp_db = pgtemp::PgTempDB::new();
        let pool = connect(&pgtemp_db_config(&temp_db, None))
            .await
            .expect("connects without a password");
        let one: i32 = sqlx::query_scalar("SELECT 1")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(one, 1);
    }

    #[tokio::test]
    async fn it_says_so_when_a_server_wants_the_password_that_is_not_set() {
        let (temp_db, dir) = password_checking_db();

        let without = connect(&pgtemp_db_config(&temp_db, None)).await;
        let with = connect(&pgtemp_db_config(&temp_db, Some("s3cret"))).await;
        drop(temp_db);
        let _ = std::fs::remove_dir_all(dir);

        let err = format!("{:#}", without.expect_err("the server checks passwords"));
        assert!(err.contains("db.password is not set"), "{err}");
        with.expect("the right password gets in");
    }
}
