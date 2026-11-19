//! Database advisory locks, each held on a connection of its own: what a lock connection offers
//! and how a lock's key is derived from its name.

use async_trait::async_trait;

/// A database connection the lock is taken and held on. The lock lasts exactly as long as the
/// connection, so the database must be reached directly: a pooler that lends 1 connection to
/// many clients would hand the lock to others, or keep it after this dipper has gone.
#[async_trait]
pub trait LockConnection: Send {
    /// What the database said if it would not end this connection once idle, as one older
    /// than Postgres 14 will not. The lock is held on it all the same.
    fn idle_timeout_refused(&self) -> Option<&str>;

    /// Tries to take the lock without waiting; `false` while another connection holds it.
    async fn try_lock(&mut self) -> Result<bool, crate::registry::Error>;

    /// Whether the connection still answers, and so still holds a lock taken on it.
    async fn is_alive(&mut self) -> bool;

    /// Closes the connection, which gives the lock up at once.
    async fn close(self: Box<Self>);
}

/// The key of the database lock named `name`. It must come out the same in every build, since 2
/// dipper versions side by side (the 2 pods of a deploy) exclude each other only if they ask for
/// the same key, and always negative: the only other advisory lock dipper's database sees is the
/// one sqlx holds while it runs migrations, whose key is never negative.
pub fn key(name: &str) -> i64 {
    let hash = fnv1a_64(name.as_bytes()) | (1 << 63);
    i64::from_ne_bytes(hash.to_ne_bytes())
}

/// The FNV-1a hash, 64 bits. Written out here because Rust's own hashers may change between
/// releases, which a lock key must not.
pub(crate) fn fnv1a_64(bytes: &[u8]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    bytes.iter().fold(OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(PRIME)
    })
}
