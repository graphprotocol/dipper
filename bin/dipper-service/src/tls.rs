//! The TLS crypto provider for the process.
//!
//! dipper links rustls with both of its crypto backends (ring through some dependencies, aws-lc-rs
//! through others), so rustls cannot pick a process-wide default by itself and panics wherever a
//! library asks for one. sqlx asks for it when checking a certificate without its hostname
//! (`sslmode=verify-ca`). Installing aws-lc-rs, the backend dipper's own TLS uses, settles it.

/// Installs aws-lc-rs as the process's default rustls crypto provider. Safe to call any number of
/// times and from any task: only the first call installs it, and the Kafka producer installs the
/// same provider.
pub fn install_crypto_provider() {
    // An error only says a default is already installed, which is what this asks for.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}
