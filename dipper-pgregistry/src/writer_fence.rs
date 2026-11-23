//! The writer fence, from a dipper process's side.
//!
//! Dipper's flows assume a single process writes to its database. The migration that adds the
//! fence makes Postgres enforce it: 1 row names the process allowed to write, by a token every
//! process picks at start, and a trigger on every table refuses a write from any session that
//! doesn't carry that token. A process that has been replaced is refused however late it learns
//! of it, so 2 processes overlapping behave like the older one crashing.
//!
//! This module is what a process asks of the fence: to take over ([`take`]), to wait for the
//! statements the old writer had started ([`drain`]) and end its sessions ([`cut`]), to say it is
//! still alive ([`beat`]) and to hand over at a clean stop ([`release`]). A chain broadcast isn't
//! a database write, so it asks through a [`BroadcastGuard`] instead.

use std::time::Duration;

use sqlx::{Acquire as _, PgConnection, PgPool, Postgres, Transaction, postgres::PgConnectOptions};
use uuid::Uuid;

/// The session setting that says which dipper process a session belongs to: its token, or
/// [`OPERATOR`].
pub const SETTING: &str = "dipper.writer";

/// The value of [`SETTING`] for manual work and migrations, which the fence lets through.
pub const OPERATOR: &str = "operator";

/// The application name of an [`OPERATOR`] session. It doesn't start with [`SESSION_PREFIX`], so
/// no takeover ends it.
pub const OPERATOR_SESSION: &str = "dipper-operator";

/// What every session of a dipper process is named, in `pg_stat_activity`, before its token. A
/// takeover ends every session named so for another token; see [`cut`].
pub const SESSION_PREFIX: &str = "dipper/";

/// The SQLSTATE of a write the fence refused: the session isn't the current writer's.
pub const REFUSED: &str = "DP001";

/// The SQLSTATE of a statement that waited longer than its lock timeout (`lock_not_available`).
const LOCK_NOT_AVAILABLE: &str = "55P03";

/// The name of the sessions of the process with `token`: `dipper/<token>`.
pub fn session_name(token: Uuid) -> String {
    format!("{SESSION_PREFIX}{token}")
}

/// `options` for a session of the process with `token`: the fence lets its writes through while
/// that process is the writer, and a takeover by another process ends it.
pub fn writer_options(options: PgConnectOptions, token: Uuid) -> PgConnectOptions {
    options
        .options([(SETTING, token)])
        .application_name(&session_name(token))
}

/// `options` for an [`OPERATOR`] session, which the fence lets through and no takeover ends.
pub fn operator_options(options: PgConnectOptions) -> PgConnectOptions {
    options
        .options([(SETTING, OPERATOR)])
        .application_name(OPERATOR_SESSION)
}

/// `options` under a name of their own, `<their name>/<part>`, so the session can be told apart
/// in `pg_stat_activity` and still counts as the same process's. Postgres keeps the first 63
/// bytes of a name.
pub fn named(options: PgConnectOptions, part: &str) -> PgConnectOptions {
    let name = match options.get_application_name() {
        Some(base) => format!("{base}/{part}"),
        None => part.to_owned(),
    };
    options.application_name(&name)
}

/// Whether `err` is the fence refusing a write, because the session isn't the current writer's.
pub fn is_refusal(err: &sqlx::Error) -> bool {
    has_code(err, REFUSED)
}

fn has_code(err: &sqlx::Error, code: &str) -> bool {
    matches!(err, sqlx::Error::Database(db) if db.code().as_deref() == Some(code))
}

/// The tables without an enabled fence, by name. Every start fences them: see [`attach`].
pub async fn unfenced(conn: &mut PgConnection) -> sqlx::Result<Vec<String>> {
    sqlx::query_scalar("SELECT tbl::text FROM dipper_fence_unfenced() AS tbl ORDER BY 1")
        .fetch_all(conn)
        .await
}

/// Fences `table`, in a transaction of its own. False when a transaction kept the table busy for
/// longer than `lock_timeout`, which leaves it as it was: ask again.
pub async fn attach(
    conn: &mut PgConnection,
    table: &str,
    lock_timeout: Duration,
) -> sqlx::Result<bool> {
    let mut tx = conn.begin().await?;
    set_lock_timeout(&mut tx, lock_timeout).await?;
    let attached = sqlx::query("SELECT dipper_fence_attach($1::regclass)")
        .bind(table)
        .execute(&mut *tx)
        .await;
    finish_within_lock_timeout(tx, attached).await
}

/// Whether the fence is on. `SELECT dipper_fence_set(false)` turns it off until it is turned on
/// again, across restarts.
pub async fn enforced(conn: &mut PgConnection) -> sqlx::Result<bool> {
    sqlx::query_scalar("SELECT enforced FROM dipper_writer")
        .fetch_one(conn)
        .await
}

/// Takes over as the writer if the current one released or has been silent for `stale_after`;
/// `holder` names this process for operators. False means a healthy writer exists: ask again.
///
/// From the moment this returns true, every new statement of the old writer is refused. It may
/// still have statements in flight: [`drain`] waits for them. Asking again after a true, as after
/// a lost connection, answers true again.
pub async fn take(
    conn: &mut PgConnection,
    token: Uuid,
    holder: &str,
    stale_after: Duration,
) -> sqlx::Result<bool> {
    sqlx::query_scalar("SELECT dipper_writer_take($1, $2, make_interval(secs => $3))")
        .bind(token)
        .bind(holder)
        .bind(stale_after.as_secs_f64())
        .fetch_one(conn)
        .await
}

/// Waits until every write and broadcast that passed the fence before the takeover has ended.
/// False when that took longer than `lock_timeout`: a session of the old writer is stuck, and
/// [`cut`] ends it.
pub async fn drain(conn: &mut PgConnection, lock_timeout: Duration) -> sqlx::Result<bool> {
    let mut tx = conn.begin().await?;
    set_lock_timeout(&mut tx, lock_timeout).await?;
    let drained = sqlx::query("SELECT dipper_writer_drain()")
        .execute(&mut *tx)
        .await;
    finish_within_lock_timeout(tx, drained).await
}

/// What a [`drain`] is waiting for: the sessions holding the fence's lock, by name, and any
/// prepared transaction holding it, which no session holds and no [`cut`] can end.
pub async fn drain_blockers(conn: &mut PgConnection) -> sqlx::Result<Vec<String>> {
    sqlx::query_scalar(
        "SELECT coalesce(nullif(a.application_name, ''), 'pid ' || l.pid, \
                         'a prepared transaction (roll it back with ROLLBACK PREPARED)') \
         FROM pg_locks l LEFT JOIN pg_stat_activity a ON a.pid = l.pid \
         WHERE l.locktype = 'advisory' AND l.granted \
           AND l.classid = 1684631664 AND l.objid = 1 AND l.objsubid = 2 \
           AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database()) \
         ORDER BY 1",
    )
    .fetch_all(conn)
    .await
}

/// Ends every session named for another dipper process, which rolls back what it had open and
/// frees the jobs and locks it held; returns how many it ended. It acts only while `token` is the
/// writer, and no process can take over while it runs, so it never ends the current writer's.
/// Sessions of another database role are skipped: their role alone may end them.
pub async fn cut(conn: &mut PgConnection, token: Uuid) -> sqlx::Result<i32> {
    sqlx::query_scalar("SELECT dipper_writer_cut($1)")
        .bind(token)
        .fetch_one(conn)
        .await
}

/// The writer's heartbeat. False means another process has taken over: stop at once.
pub async fn beat(conn: &mut PgConnection, token: Uuid) -> sqlx::Result<bool> {
    sqlx::query_scalar("SELECT dipper_writer_beat($1)")
        .bind(token)
        .fetch_one(conn)
        .await
}

/// A clean stop's last step: the next process takes over without waiting for this one to go
/// silent. False if another process had already taken over.
pub async fn release(conn: &mut PgConnection, token: Uuid) -> sqlx::Result<bool> {
    sqlx::query_scalar("SELECT dipper_writer_release($1)")
        .bind(token)
        .fetch_one(conn)
        .await
}

/// Lets 1 chain broadcast through only while this process is the writer. It holds a pooled
/// connection in a transaction for as long as the broadcast lasts, and the fence's shared lock
/// from the first check on, so a new writer's [`drain`] waits for the broadcast. Ask
/// [`BroadcastGuard::still_writer`] before each attempt, and [`BroadcastGuard::end`] it when the
/// broadcast ends.
pub struct BroadcastGuard {
    tx: Transaction<'static, Postgres>,
    token: Uuid,
}

impl BroadcastGuard {
    /// Starts the guard's transaction, on a connection from `pool`, under READ COMMITTED whatever
    /// the database's default: each check must read the token afresh to see a takeover.
    pub async fn begin(pool: &PgPool, token: Uuid) -> sqlx::Result<Self> {
        Ok(Self {
            tx: pool
                .begin_with("BEGIN ISOLATION LEVEL READ COMMITTED")
                .await?,
            token,
        })
    }

    /// Whether this process is still the writer, or the fence is off. Once it says false the
    /// process has been replaced, and no attempt should start.
    pub async fn still_writer(&mut self) -> sqlx::Result<bool> {
        sqlx::query_scalar("SELECT dipper_writer_guard($1)")
            .bind(self.token)
            .fetch_one(&mut *self.tx)
            .await
    }

    /// Ends the guard's transaction, which lets go of the fence's lock at once. Dropping the
    /// guard instead leaves that to whenever the pool next flushes the connection.
    pub async fn end(self) -> sqlx::Result<()> {
        self.tx.rollback().await
    }
}

async fn set_lock_timeout(
    tx: &mut Transaction<'_, Postgres>,
    lock_timeout: Duration,
) -> sqlx::Result<()> {
    sqlx::query("SELECT set_config('lock_timeout', $1, true)")
        .bind(format!("{}ms", lock_timeout.as_millis().max(1)))
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Commits `tx` if `outcome` succeeded; rolls it back and answers false if it ran out of its lock
/// timeout.
async fn finish_within_lock_timeout<T>(
    tx: Transaction<'_, Postgres>,
    outcome: sqlx::Result<T>,
) -> sqlx::Result<bool> {
    match outcome {
        Ok(_) => {
            tx.commit().await?;
            Ok(true)
        }
        Err(err) if has_code(&err, LOCK_NOT_AVAILABLE) => {
            tx.rollback().await?;
            Ok(false)
        }
        Err(err) => Err(err),
    }
}
