use std::time::Duration;

use anyhow::Context;
use dipper_pgregistry::writer_fence;
use sqlx::{
    Connection as _, PgConnection, Pool, Postgres,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

use crate::config::DbConfig;

/// Default max connections
pub const DEFAULT_MAX_CONNECTIONS: u32 = 32;

/// How long a caller waits for a free pooled connection before erroring. Set
/// explicitly so connection-pool starvation surfaces as a bounded error
/// instead of relying on the driver default.
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);

/// How to reach the database as configured, credentials included.
fn connect_options(conf: &DbConfig) -> anyhow::Result<PgConnectOptions> {
    // sqlx checks a `verify-ca` certificate with rustls's process-wide provider.
    crate::tls::install_crypto_provider();
    let conn_options: PgConnectOptions = conf
        .url
        .as_str()
        .parse()
        .context("db.url is not a Postgres connection URL")?;
    let conn_options = conn_options.username(&conf.username);
    Ok(match &conf.password {
        Some(password) => conn_options.password(password),
        None => conn_options,
    })
}

/// Connects the pool every database call of this process goes through, once the database
/// answers. Each of its sessions belongs to the process with `writer` as its token: the writer
/// fence lets its writes through only while that process is the writer, and a takeover by
/// another process ends them (see [`crate::writer`]). The connections opened from the pool's
/// options, such as the queue's listener and the Studio consumer's lock, belong to it too.
pub async fn connect(conf: &DbConfig, writer: Uuid) -> anyhow::Result<Pool<Postgres>> {
    let conn_options = connect_options(conf)?;
    // Without a password, dipper only gets in where the server asks for none.
    if conf.password.is_none() {
        tracing::info!(
            "db.password is not set: connecting without a password of dipper's own, as through \
             the Cloud SQL Auth Proxy with automatic IAM authentication"
        );
    }

    // Wait for the database with 1 connection at a time, each cut off at the time left of the
    // wait, then open the pool. Asking the pool instead would block every attempt for its whole
    // acquire timeout and report only that it timed out, not why.
    let probe = crate::startup::retry_within(
        "database connection",
        conf.connect_wait,
        ConnectFailure::worth_retrying,
        |remaining| {
            let options = conn_options.clone();
            async move {
                let limit = remaining.min(CONNECT_ATTEMPT_TIMEOUT);
                match tokio::time::timeout(limit, PgConnection::connect_with(&options)).await {
                    Ok(Ok(conn)) => {
                        let _ = conn.close().await;
                        Ok(())
                    }
                    Ok(Err(err)) => Err(ConnectFailure::Refused(err)),
                    Err(_) => Err(ConnectFailure::TimedOut(limit)),
                }
            }
        },
    )
    .await;
    if let Err(gave_up) = probe {
        let waited = format!(
            "failed to connect to DB after {} tries over {}s",
            gave_up.tries,
            gave_up.elapsed.as_secs()
        );
        return Err(match gave_up.error {
            ConnectFailure::Refused(err) => explain_refusal(err, conf.password.is_none()),
            timed_out @ ConnectFailure::TimedOut(_) => anyhow::Error::new(timed_out),
        }
        .context(waited));
    }

    let pool = PgPoolOptions::new()
        .max_connections(conf.max_connections.unwrap_or(DEFAULT_MAX_CONNECTIONS))
        .acquire_timeout(ACQUIRE_TIMEOUT)
        .connect_with(writer_fence::writer_options(conn_options, writer))
        .await
        .map_err(|err| explain_refusal(err, conf.password.is_none()))
        .context("failed to connect to DB")?;

    Ok(pool)
}

/// The pool of 1 connection a chain broadcast's writer check runs on, beside `pool`: a check holds
/// its connection for as long as its broadcast lasts, so it gets one of its own rather than 1 the
/// worker loops are counted on. Broadcasts take turns, so 1 is enough. Its session belongs to the
/// same process as `pool`'s, as `dipper/<token>/broadcast`, and it connects when first used.
pub fn broadcast_check_pool(pool: &Pool<Postgres>) -> Pool<Postgres> {
    let options = writer_fence::named(pool.connect_options().as_ref().clone(), "broadcast");
    PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(ACQUIRE_TIMEOUT)
        .connect_lazy_with(options)
}

/// Opens 1 session for work done on the database as a whole rather than as this process: the
/// migrations, and fencing the tables they add. The writer fence lets its writes through, and no
/// takeover ends it. Call it once [`connect`] has found the database answering.
pub async fn connect_operator(conf: &DbConfig) -> anyhow::Result<PgConnection> {
    let options = writer_fence::operator_options(connect_options(conf)?);
    PgConnection::connect_with(&options)
        .await
        .map_err(|err| explain_refusal(err, conf.password.is_none()))
        .context("failed to open the DB session for migrations")
}

/// The longest 1 connection attempt may take while waiting for the database at startup.
const CONNECT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);

/// Why 1 attempt to reach the database at startup failed.
#[derive(Debug, thiserror::Error)]
enum ConnectFailure {
    #[error("{0}")]
    Refused(sqlx::Error),
    #[error("no answer within {}s", .0.as_secs())]
    TimedOut(Duration),
}

impl ConnectFailure {
    /// Whether the attempt may succeed if made again. A database that cannot be reached, drops
    /// the connection, does not answer, is still starting up or has no connection to spare may
    /// be back soon. Anything else fails the same way each time, and ends the wait at once: a
    /// refused password or a missing database, a certificate the TLS check rejects (rustls
    /// reports those as invalid data), something other than Postgres answering (a protocol
    /// error), or the connection set up wrongly.
    fn worth_retrying(&self) -> bool {
        match self {
            Self::TimedOut(_) => true,
            Self::Refused(sqlx::Error::Io(err)) => err.kind() != std::io::ErrorKind::InvalidData,
            Self::Refused(sqlx::Error::Database(err)) => err.code().is_some_and(|code| {
                code == CANNOT_CONNECT_NOW || code == TOO_MANY_CONNECTIONS || code.starts_with("08")
            }),
            Self::Refused(_) => false,
        }
    }
}

/// Postgres's code for a server still starting up or shutting down (`cannot_connect_now`).
const CANNOT_CONNECT_NOW: &str = "57P03";

/// Postgres's code for a server with no connection slot to spare (`too_many_connections`).
const TOO_MANY_CONNECTIONS: &str = "53300";

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
    use std::{
        sync::{
            Arc,
            atomic::{AtomicU32, Ordering},
        },
        time::Duration,
    };

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
            // The TLS stub drops each connection after its handshake: do not wait it out.
            "connect_wait": 1,
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
            tokio::time::timeout(Duration::from_secs(5), connect(&conf, Uuid::now_v7())),
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

    /// A throwaway CA, and a server certificate it signed for one name.
    struct TestCerts {
        ca_pem: String,
        server_chain: Vec<rustls::pki_types::CertificateDer<'static>>,
        server_key: rustls::pki_types::PrivateKeyDer<'static>,
    }

    fn test_certs(name: &str) -> TestCerts {
        use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair, KeyUsagePurpose};
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let ca_key = KeyPair::generate().unwrap();
        let ca = ca_params.self_signed(&ca_key).unwrap();
        let server_key = KeyPair::generate().unwrap();
        let server = CertificateParams::new(vec![name.to_string()])
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
        accepts_certificate_for("localhost", host, params, trust_ca).await
    }

    /// As [`accepts_certificate`], for a server whose certificate names `cert_name` alone.
    async fn accepts_certificate_for(
        cert_name: &str,
        host: &str,
        params: &str,
        trust_ca: bool,
    ) -> bool {
        let certs = test_certs(cert_name);
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

        let _ = tokio::time::timeout(
            Duration::from_secs(10),
            connect(&db_config(&url), Uuid::now_v7()),
        )
        .await;
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
    async fn verify_ca_skips_the_hostname_check() {
        // As libpq does. sqlx 0.8 skipped it only on the name error rustls used to return, so
        // with rustls 0.23.24 and later `verify-ca` behaved as `verify-full`; sqlx 0.9 skips both.
        assert!(accepts_certificate("127.0.0.1", "sslmode=verify-ca", true).await);
    }

    #[tokio::test]
    async fn verify_ca_still_refuses_a_certificate_from_an_unknown_ca() {
        assert!(!accepts_certificate("127.0.0.1", "sslmode=verify-ca", false).await);
    }

    /// The only name in a Cloud SQL private-IP instance's server certificate: its DNS name, with
    /// a trailing dot.
    const CLOUD_SQL_NAME: &str = "d0b150ab5022.76ws9odrdkfn.us-central1.sql-psa.goog.";

    #[tokio::test]
    async fn verify_ca_accepts_a_cloud_sql_certificate() {
        assert!(
            accepts_certificate_for(CLOUD_SQL_NAME, "127.0.0.1", "sslmode=verify-ca", true).await
        );
    }

    #[tokio::test]
    async fn verify_full_refuses_a_cloud_sql_certificate_even_by_its_name() {
        // rustls accepts no presented name with a trailing dot, so such a server is reachable
        // with `verify-ca` only.
        assert!(
            !accepts_certificate_for(
                CLOUD_SQL_NAME,
                CLOUD_SQL_NAME.trim_end_matches('.'),
                "sslmode=verify-full",
                true
            )
            .await
        );
    }

    /// A temporary Postgres that, unlike pgtemp's default, checks passwords.
    fn password_checking_db() -> (pgtemp::PgTempDB, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("dipper-scram-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let hba = dir.join("pg_hba.conf");
        std::fs::write(&hba, "local all all scram-sha-256\n").unwrap();
        let db = crate::test_support::temp_postgres_from(
            pgtemp::PgTempDBBuilder::new()
                .with_password("s3cret")
                .with_config_param("hba_file", hba.to_str().unwrap()),
        );
        (db, dir)
    }

    fn pgtemp_db_config(temp_db: &pgtemp::PgTempDB, password: Option<&str>) -> DbConfig {
        let mut json = serde_json::json!({
            "url": crate::test_support::temp_postgres_url(temp_db),
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
        let temp_db = crate::test_support::temp_postgres();
        let pool = connect(&pgtemp_db_config(&temp_db, None), Uuid::now_v7())
            .await
            .expect("connects without a password");
        let one: i32 = sqlx::query_scalar("SELECT 1")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(one, 1);
    }

    /// The writer fence tells processes apart by what their sessions carry: a session of the pool
    /// belongs to this process, and the migrations' session to no process at all.
    #[tokio::test]
    async fn it_marks_every_session_with_whose_it_is() {
        let temp_db = crate::test_support::temp_postgres();
        let conf = pgtemp_db_config(&temp_db, None);
        let token = Uuid::now_v7();
        let carried =
            "SELECT current_setting('dipper.writer'), current_setting('application_name')";

        let pool = connect(&conf, token).await.expect("connect the pool");
        let (setting, name): (String, String) = sqlx::query_as(carried)
            .fetch_one(&pool)
            .await
            .expect("read the pool's session");
        assert_eq!(setting, token.to_string());
        assert_eq!(name, format!("dipper/{token}"));

        let mut operator = connect_operator(&conf)
            .await
            .expect("connect as an operator");
        let (setting, name): (String, String) = sqlx::query_as(carried)
            .fetch_one(&mut operator)
            .await
            .expect("read the operator's session");
        assert_eq!(setting, "operator");
        assert_eq!(name, "dipper-operator");
    }

    #[tokio::test]
    async fn it_says_so_when_a_server_wants_the_password_that_is_not_set() {
        let (temp_db, dir) = password_checking_db();

        let without = connect(&pgtemp_db_config(&temp_db, None), Uuid::now_v7()).await;
        let with = connect(&pgtemp_db_config(&temp_db, Some("s3cret")), Uuid::now_v7()).await;
        drop(temp_db);
        let _ = std::fs::remove_dir_all(dir);

        let err = format!("{:#}", without.expect_err("the server checks passwords"));
        assert!(err.contains("db.password is not set"), "{err}");
        with.expect("the right password gets in");
    }

    fn waiting_db_config(url: &str, connect_wait_secs: u64) -> DbConfig {
        serde_json::from_value(serde_json::json!({
            "url": url,
            "username": "dipper",
            "password": "unused",
            "connect_wait": connect_wait_secs,
        }))
        .expect("a valid db config")
    }

    #[tokio::test]
    async fn connect_retries_a_server_that_drops_the_connection() {
        // A server that accepts and at once drops each connection, as one restarting or failing
        // over can. The pool gives up on that at the first try; dipper must keep trying.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = Arc::new(AtomicU32::new(0));
        let server = {
            let accepted = Arc::clone(&accepted);
            tokio::spawn(async move {
                while let Ok((socket, _)) = listener.accept().await {
                    accepted.fetch_add(1, Ordering::Relaxed);
                    drop(socket);
                }
            })
        };

        let result = connect(
            &waiting_db_config(&format!("postgres://{addr}/dipper"), 2),
            Uuid::now_v7(),
        )
        .await;
        server.abort();

        assert!(result.is_err(), "the server never lets dipper in");
        assert!(
            accepted.load(Ordering::Relaxed) >= 2,
            "dipper tried {} time(s) within its 2s wait",
            accepted.load(Ordering::Relaxed)
        );
    }

    #[tokio::test]
    async fn connect_gives_up_on_a_silent_server_within_its_wait() {
        // A server that accepts and then says nothing. Each attempt is cut off at the time left,
        // so the wait ends on time instead of after an attempt's own timeout.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        });

        let started = std::time::Instant::now();
        let result = connect(
            &waiting_db_config(&format!("postgres://{addr}/dipper"), 2),
            Uuid::now_v7(),
        )
        .await;
        server.abort();

        let err = format!("{:#}", result.expect_err("the server never answers"));
        assert!(
            started.elapsed() < Duration::from_secs(4),
            "took {:?}",
            started.elapsed()
        );
        assert!(err.contains("no answer within"), "{err}");
    }

    fn io(kind: std::io::ErrorKind) -> ConnectFailure {
        ConnectFailure::Refused(sqlx::Error::Io(std::io::Error::from(kind)))
    }

    #[test]
    fn only_failures_that_may_clear_up_are_retried() {
        use std::io::ErrorKind;
        // A database out of reach, dropping connections or not answering may be back soon.
        assert!(io(ErrorKind::ConnectionRefused).worth_retrying());
        assert!(io(ErrorKind::ConnectionReset).worth_retrying());
        assert!(io(ErrorKind::UnexpectedEof).worth_retrying());
        assert!(ConnectFailure::TimedOut(Duration::from_secs(1)).worth_retrying());
        // rustls reports a rejected certificate as invalid data; it will be rejected every time.
        assert!(!io(ErrorKind::InvalidData).worth_retrying());
        // Something other than Postgres answering, or the connection set up wrongly.
        assert!(
            !ConnectFailure::Refused(sqlx::Error::Protocol("unexpected response".into()))
                .worth_retrying()
        );
        assert!(
            !ConnectFailure::Refused(sqlx::Error::Configuration("bad option".into()))
                .worth_retrying()
        );
    }
}
