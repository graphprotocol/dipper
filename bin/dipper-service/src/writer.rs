//! Dipper's 1 writer, as this process takes part in it.
//!
//! Dipper's flows assume a single process acts. The writer fence makes the database enforce that
//! (see [`dipper_pgregistry::writer_fence`]): every session of this process carries its token,
//! and the database refuses their writes unless that token is the current writer's. A process
//! that has been replaced is therefore refused however late it finds out, and 2 processes
//! overlapping behave like the older one crashing, which dipper's flows already survive.
//!
//! This module decides when this process asks to be the writer ([`fence_every_table`] and
//! [`Takeover`] at start), keeps it the writer while it runs and hands over at a clean stop
//! ([`heartbeat`]), lets its chain broadcasts through only while it is the writer
//! ([`WriterGate`]), and stops it once another process has taken over. None of this decides
//! safety, which the database's check alone gives: the timings only decide how soon a handover
//! happens.

use std::{
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

use anyhow::Context as _;
use async_trait::async_trait;
use dipper_pgregistry::writer_fence;
use sqlx::{
    ConnectOptions as _, Connection as _, PgConnection, PgPool, postgres::PgConnectOptions,
};
use tokio::{
    sync::{Mutex, Notify, mpsc, oneshot},
    time::MissedTickBehavior,
};
use uuid::Uuid;

use crate::chain_client::{BroadcastGate, BroadcastTurn};

/// How long the writer may go without a heartbeat before a starting process takes over from it.
pub const STALE_AFTER: Duration = Duration::from_secs(20);

/// How often the writer says it is alive: a quarter of [`STALE_AFTER`], so 3 heartbeats in a row
/// can fail before it looks silent.
const BEAT_EVERY: Duration = Duration::from_secs(5);

/// The longest 1 heartbeat may take. A slower one counts as missed, and the next one is made on a
/// new connection.
const BEAT_TIMEOUT: Duration = Duration::from_secs(5);

/// How often a starting process asks again while the writer is healthy.
const ASK_EVERY: Duration = Duration::from_secs(2);

/// How often a starting process says it is still waiting for the writer.
const WAITING_LOG_EVERY: Duration = Duration::from_secs(30);

/// How long a wait for the writer is ordinary: the old pod's 90 seconds to stop, then the
/// 20 seconds of silence a crash costs. Longer, and another dipper is running that shouldn't be.
const WAITING_TOO_LONG: Duration = Duration::from_secs(120);

/// How long a takeover waits for the statements and broadcast attempts the old writer had in
/// flight before it ends that writer's sessions. Above the chain client's 10-second request
/// timeout, so a broadcast attempt in flight normally finishes first.
const DRAIN_WAIT: Duration = Duration::from_secs(15);

/// How long fencing 1 table may wait for a transaction that keeps it busy.
const ATTACH_WAIT: Duration = Duration::from_secs(5);

/// A token for a new process: random, and unique to it. Version 7 also sorts by start time.
pub fn new_token() -> Uuid {
    Uuid::now_v7()
}

/// The name this process goes by in `dipper_writer.holder`, for operators: its host name, which
/// in Kubernetes is the pod's name.
pub fn holder() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "unknown".to_owned())
}

/// Fences every table that isn't, as a start does once its migrations have run on `operator`, a
/// session marked as an operator's. A table a migration added is fenced this way without anyone
/// having to remember to. A table busy with a long transaction is tried again until it is free,
/// each time in a transaction of its own, so the wait never holds the other tables.
pub async fn fence_every_table(operator: &mut PgConnection) -> anyhow::Result<()> {
    let mut waiting_since: Option<Instant> = None;
    loop {
        let busy = fence_unfenced(operator).await?;
        if busy.is_empty() {
            break;
        }
        let since = *waiting_since.get_or_insert_with(Instant::now);
        tracing::warn!(
            event = "writer_fence_attach_waiting",
            tables = ?busy,
            waited_secs = since.elapsed().as_secs(),
            "could not put the writer fence on these tables yet: a transaction keeps them busy, \
             as a process of a dipper release without the fence may while it stops. Trying again"
        );
        tokio::time::sleep(ASK_EVERY).await;
    }
    warn_if_the_fence_is_off(operator).await
}

/// Fences each table that isn't, until none is left or only busy ones are. Returns the busy ones.
async fn fence_unfenced(operator: &mut PgConnection) -> anyhow::Result<Vec<String>> {
    loop {
        let unfenced = writer_fence::unfenced(operator)
            .await
            .context("failed to list the tables without the writer fence")?;
        if unfenced.is_empty() {
            return Ok(Vec::new());
        }
        let mut busy = Vec::new();
        for table in unfenced {
            let fenced = writer_fence::attach(operator, &table, ATTACH_WAIT)
                .await
                .with_context(|| format!("failed to put the writer fence on {table}"))?;
            if fenced {
                tracing::info!(table, "put the writer fence on the table");
            } else {
                busy.push(table);
            }
        }
        if !busy.is_empty() {
            return Ok(busy);
        }
    }
}

async fn warn_if_the_fence_is_off(conn: &mut PgConnection) -> anyhow::Result<()> {
    let on = writer_fence::enforced(conn)
        .await
        .context("failed to read whether the writer fence is on")?;
    if !on {
        tracing::warn!(
            event = "writer_fence_off",
            "the writer fence is switched off: the database lets any process write, so nothing \
             stops a 2nd dipper from acting at once with this one. Switch it on again with \
             SELECT dipper_fence_set(true)"
        );
    }
    Ok(())
}

/// Becoming the writer, at start: wait while another process is the healthy writer, take over,
/// then wait for what the old writer still had in flight and end its sessions. Do it as late as
/// startup allows, just before the services start: until then, any write of this process is
/// refused.
pub struct Takeover {
    options: PgConnectOptions,
    token: Uuid,
    holder: String,
}

impl Takeover {
    /// For the process whose sessions `pool` opens: they carry its token.
    pub fn new(pool: &PgPool, token: Uuid, holder: String) -> Self {
        Self {
            options: pool.connect_options().as_ref().clone(),
            token,
            holder,
        }
    }

    /// Waits until this process may take over, and takes over. That happens once the writer
    /// has handed over at a clean stop, or has been silent for [`STALE_AFTER`]. From when this
    /// returns, the database refuses every statement the process it replaced starts; start the
    /// [`heartbeat`] at once, then [`Taken::settle`].
    pub async fn take(&self) -> anyhow::Result<Taken> {
        let started = Instant::now();
        let mut last_said = started;
        let mut conn: Option<PgConnection> = None;
        loop {
            match self.ask(&mut conn).await {
                Ok(true) => break,
                Ok(false) => {
                    if last_said.elapsed() >= WAITING_LOG_EVERY {
                        last_said = Instant::now();
                        say_waiting(started.elapsed());
                    }
                }
                Err(err) if worth_asking_again(&err) => {
                    conn = None;
                    tracing::warn!(error = %err, "could not ask to become the writer; asking again");
                }
                Err(err) => return Err(err).context("failed to ask to become the writer"),
            }
            tokio::time::sleep(ASK_EVERY).await;
        }
        let conn = match conn {
            Some(conn) => conn,
            None => self.connect().await?,
        };
        Ok(Taken {
            conn,
            token: self.token,
            waited: started.elapsed(),
        })
    }

    /// Asks once, on `conn`, connecting it first if it isn't.
    async fn ask(&self, conn: &mut Option<PgConnection>) -> sqlx::Result<bool> {
        let conn = match conn {
            Some(conn) => conn,
            None => conn.insert(
                writer_fence::named(self.options.clone(), "takeover")
                    .connect()
                    .await?,
            ),
        };
        writer_fence::take(conn, self.token, &self.holder, STALE_AFTER).await
    }

    async fn connect(&self) -> anyhow::Result<PgConnection> {
        writer_fence::named(self.options.clone(), "takeover")
            .connect()
            .await
            .context("failed to connect to finish taking over as the writer")
    }
}

fn say_waiting(waited: Duration) {
    let waited_secs = waited.as_secs();
    if waited >= WAITING_TOO_LONG {
        tracing::warn!(
            event = "writer_waiting",
            waited_secs,
            "still waiting to become the writer: another dipper process is the writer and \
             answers its heartbeat. A stopping pod hands over well before this, so another \
             dipper is running that shouldn't be; see dipper_writer.holder for its host"
        );
    } else {
        tracing::info!(
            event = "writer_waiting",
            waited_secs,
            "waiting to become the writer: another dipper process is the writer, and hands over \
             when it stops or after 20 seconds without a heartbeat"
        );
    }
}

/// Whether asking failed in a way asking again may not: the database answered with an error of
/// its own rather than the connection failing or being ended.
fn worth_asking_again(err: &sqlx::Error) -> bool {
    match err {
        // Connection exceptions (08) and operator intervention (57), such as another starting
        // process's takeover ending this session, or a statement timeout.
        sqlx::Error::Database(db) => db
            .code()
            .is_some_and(|code| code.starts_with("08") || code.starts_with("57")),
        _ => true,
    }
}

/// A takeover that has happened, with what the replaced process had in flight still to settle.
pub struct Taken {
    conn: PgConnection,
    token: Uuid,
    waited: Duration,
}

impl Taken {
    /// Waits for the statements and broadcast attempts the replaced process had in flight, then
    /// ends every session of it that is left, which rolls back what it had open and frees the
    /// jobs and session locks it held. A session that blocks the wait is ended, and the wait
    /// asked again, until nothing blocks it. Once this returns, this process may start working.
    pub async fn settle(mut self) -> anyhow::Result<()> {
        let conn = &mut self.conn;
        writer_fence::drain(conn, DRAIN_WAIT)
            .await
            .context("failed to wait for the replaced writer's statements")?;
        let mut sessions_cut = cut(conn, self.token).await?;
        while !writer_fence::drain(conn, DRAIN_WAIT)
            .await
            .context("failed to wait for the replaced writer's statements")?
        {
            let blockers = writer_fence::drain_blockers(conn)
                .await
                .context("failed to list what the takeover is waiting for")?;
            tracing::warn!(
                event = "writer_drain_blocked",
                ?blockers,
                "a takeover keeps waiting for sessions of the dipper it replaced; ending them \
                 and waiting again. A prepared transaction among them holds the takeover back \
                 until an operator rolls it back"
            );
            sessions_cut += cut(conn, self.token).await?;
        }
        tracing::info!(
            event = "writer_taken",
            waited_secs = self.waited.as_secs(),
            sessions_cut,
            "this process is now dipper's writer"
        );
        let _ = self.conn.close().await;
        Ok(())
    }
}

async fn cut(conn: &mut PgConnection, token: Uuid) -> anyhow::Result<i32> {
    writer_fence::cut(conn, token)
        .await
        .context("failed to end the replaced writer's sessions")
}

/// Stops the [`heartbeat`] at the end of a stop.
pub struct Handle {
    finish: mpsc::Sender<(bool, oneshot::Sender<bool>)>,
}

impl Handle {
    /// Stops the heartbeat. With `release` it then hands over, so the next process takes over
    /// without waiting for this one to go silent: pass it only once every service has stopped.
    /// Returns whether it handed over.
    pub async fn finish(&self, release: bool) -> bool {
        let (reply, answer) = oneshot::channel();
        if self.finish.send((release, reply)).await.is_err() {
            // The heartbeat has stopped already: this process was replaced.
            return false;
        }
        answer.await.unwrap_or(false)
    }
}

/// Why a [`heartbeat`] stopped the process.
#[derive(Debug, thiserror::Error)]
#[error("another dipper process has taken over as the writer")]
pub struct Superseded;

/// Keeps this process the writer from its takeover until its stop: every few seconds it says it
/// is alive, on a connection of its own opened from `options` (the pool's), so a starting
/// process waits. Run it from the moment [`Takeover::take`] returns, and stop it through the
/// [`Handle`] as the last step of a stop.
///
/// It ends with [`Superseded`] as soon as it finds another process has taken over, which stops
/// this one: when a heartbeat says so, or when told through `refusals` that the database refused
/// a write ([`note_refusal`]) and the heartbeat it then makes at once confirms it. A database
/// that can't be reached changes nothing: the writer stays the writer, and beats again once the
/// database answers.
pub fn heartbeat(
    options: PgConnectOptions,
    token: Uuid,
    refusals: Arc<Notify>,
) -> (Handle, impl Future<Output = anyhow::Result<()>>) {
    let (finish, finishes) = mpsc::channel(1);
    let options = writer_fence::named(options, "heartbeat")
        .options([("statement_timeout", BEAT_TIMEOUT.as_millis().to_string())]);
    let beats = Beats {
        options,
        token,
        conn: None,
        failing_since: None,
    };
    (Handle { finish }, run_heartbeat(beats, refusals, finishes))
}

async fn run_heartbeat(
    mut beats: Beats,
    refusals: Arc<Notify>,
    mut finishes: mpsc::Receiver<(bool, oneshot::Sender<bool>)>,
) -> anyhow::Result<()> {
    let mut ticks = tokio::time::interval(BEAT_EVERY);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        let refused = tokio::select! {
            finish = finishes.recv() => {
                let Some((release, reply)) = finish else {
                    return Ok(());
                };
                let released = release && beats.release().await;
                beats.close().await;
                let _ = reply.send(released);
                return Ok(());
            }
            _ = ticks.tick() => false,
            () = refusals.notified() => true,
        };
        match beats.beat().await {
            Some(true) if refused => tracing::error!(
                event = "writer_refused_while_writer",
                "the database refused a write of this process although it is the writer: a \
                 session of it lacks the process's token"
            ),
            Some(true) | None => {}
            Some(false) => {
                tracing::error!(
                    event = "writer_superseded",
                    "another dipper process has taken over as the writer; this one stops. The \
                     database refuses its writes from the takeover on"
                );
                return Err(Superseded.into());
            }
        }
    }
}

/// What the heartbeat asks of the database.
#[derive(Clone, Copy)]
enum Call {
    Beat,
    Release,
}

/// The heartbeat's connection, and what it last made of the database.
struct Beats {
    options: PgConnectOptions,
    token: Uuid,
    conn: Option<PgConnection>,
    failing_since: Option<Instant>,
}

impl Beats {
    /// Whether this process is still the writer, or `None` when the database couldn't be asked.
    async fn beat(&mut self) -> Option<bool> {
        match self.ask(Call::Beat).await {
            Ok(still) => {
                if let Some(since) = self.failing_since.take() {
                    tracing::info!(
                        failed_for_secs = since.elapsed().as_secs(),
                        "the writer's heartbeat reaches the database again"
                    );
                }
                Some(still)
            }
            Err(err) => {
                if self.failing_since.is_none() {
                    self.failing_since = Some(Instant::now());
                    tracing::warn!(
                        error = %err,
                        "the writer's heartbeat could not reach the database. It goes on \
                         trying; past 20 seconds of this, another dipper may take over"
                    );
                }
                None
            }
        }
    }

    /// Hands over. False if another process had taken over already, or the database couldn't
    /// be asked: the next process then waits out the 20 seconds of silence.
    async fn release(&mut self) -> bool {
        match self.ask(Call::Release).await {
            Ok(true) => {
                tracing::info!(
                    event = "writer_released",
                    "handed over: the next dipper process takes over without waiting"
                );
                true
            }
            Ok(false) => {
                tracing::warn!("did not hand over: another dipper process had taken over already");
                false
            }
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "could not hand over; the next dipper process takes over once this one has \
                     been silent for 20 seconds"
                );
                false
            }
        }
    }

    /// Makes `call` on the heartbeat's connection, connecting it first if it isn't. A connection
    /// that fails is given up and the call made once more on a new one: a takeover ends this
    /// process's sessions, so a lost connection is the first sign of one.
    async fn ask(&mut self, call: Call) -> anyhow::Result<bool> {
        let had_connection = self.conn.is_some();
        match self.ask_once(call).await {
            Err(_) if had_connection => self.ask_once(call).await,
            asked => asked,
        }
    }

    /// Makes `call` on the heartbeat's connection, connecting it first if it isn't, within
    /// [`BEAT_TIMEOUT`]. A failure drops the connection, so the next call connects afresh.
    async fn ask_once(&mut self, call: Call) -> anyhow::Result<bool> {
        let (options, token) = (&self.options, self.token);
        let conn = &mut self.conn;
        let asked = tokio::time::timeout(BEAT_TIMEOUT, async move {
            let conn = match conn {
                Some(conn) => conn,
                None => conn.insert(options.connect().await?),
            };
            match call {
                Call::Beat => writer_fence::beat(conn, token).await,
                Call::Release => writer_fence::release(conn, token).await,
            }
        })
        .await;
        match asked {
            Ok(Ok(answer)) => Ok(answer),
            Ok(Err(err)) => {
                self.conn = None;
                Err(err.into())
            }
            Err(_elapsed) => {
                self.conn = None;
                Err(anyhow::anyhow!(
                    "no answer within {}s",
                    BEAT_TIMEOUT.as_secs()
                ))
            }
        }
    }

    async fn close(&mut self) {
        if let Some(conn) = self.conn.take() {
            let _ = conn.close().await;
        }
    }
}

/// Where [`note_refusal`] tells the heartbeat of a refused write.
static REFUSALS: OnceLock<Arc<Notify>> = OnceLock::new();

/// What the [`heartbeat`] of this process listens on for refused writes.
pub fn refusals() -> Arc<Notify> {
    REFUSALS.get_or_init(|| Arc::new(Notify::new())).clone()
}

/// Tells this process's heartbeat if `err` is the database refusing a write because this
/// process isn't the writer, so it checks at once instead of at its next beat, and the process
/// stops seconds sooner. Anything else is left alone.
pub fn note_refusal(err: &anyhow::Error) {
    if refusal_in(err) {
        refused();
    }
}

/// Whether `err`, or an error it wraps, is the database refusing a write by the fence.
fn refusal_in(err: &anyhow::Error) -> bool {
    err.chain().any(is_refusal)
}

/// [`note_refusal`] for a database error.
pub fn note_db_refusal(err: &sqlx::Error) {
    if writer_fence::is_refusal(err) {
        refused();
    }
}

fn refused() {
    if let Some(refusals) = REFUSALS.get() {
        refusals.notify_one();
    }
}

/// Whether `err` is a refusal by the fence, as the database, the registry or the queue reports it.
fn is_refusal(err: &(dyn std::error::Error + 'static)) -> bool {
    if let Some(err) = err.downcast_ref::<sqlx::Error>() {
        return writer_fence::is_refusal(err);
    }
    if let Some(dipper_pgregistry::Error::DbError(err)) = err.downcast_ref() {
        return writer_fence::is_refusal(err);
    }
    if let Some(crate::registry::Error::BackendError(dipper_pgregistry::Error::DbError(err))) =
        err.downcast_ref()
    {
        return writer_fence::is_refusal(err);
    }
    false
}

/// Lets a chain broadcast through only while this process is the writer. Each broadcast holds a
/// pooled connection in a transaction for as long as it lasts, which a new writer's takeover
/// waits for.
pub struct WriterGate {
    pool: PgPool,
    token: Uuid,
}

impl WriterGate {
    pub fn new(pool: PgPool, token: Uuid) -> Self {
        Self { pool, token }
    }
}

#[async_trait]
impl BroadcastGate for WriterGate {
    async fn open(&self) -> anyhow::Result<Box<dyn BroadcastTurn>> {
        let guard = writer_fence::BroadcastGuard::begin(&self.pool, self.token)
            .await
            .context("failed to start the broadcast's check that this process is the writer")?;
        Ok(Box::new(WriterTurn(Mutex::new(guard))))
    }
}

struct WriterTurn(Mutex<writer_fence::BroadcastGuard>);

#[async_trait]
impl BroadcastTurn for WriterTurn {
    async fn still_writer(&self) -> anyhow::Result<bool> {
        let still = self
            .0
            .lock()
            .await
            .still_writer()
            .await
            .context("failed to check that this process is still the writer")?;
        if !still {
            // Another process took over: the heartbeat confirms it and stops this one.
            refused();
        }
        Ok(still)
    }

    async fn end(self: Box<Self>) {
        if let Err(err) = self.0.into_inner().end().await {
            // The transaction goes with its connection either way; a takeover then ends it.
            tracing::debug!(error = %err, "could not end a broadcast's writer check cleanly");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use dipper_pgmq::{JobBuilder, PgQueue};
    use dipper_pgregistry::writer_fence;
    use sqlx::{
        ConnectOptions as _, PgPool,
        postgres::{PgConnectOptions, PgPoolOptions},
    };
    use tokio::{sync::Notify, task::JoinHandle};
    use uuid::Uuid;

    use super::*;
    use crate::registry::IndexingRequestRegistry as _;

    /// A database as dipper's start leaves it before any takeover: both migrations run and every
    /// table fenced, on an operator's session.
    async fn started_db() -> (pgtemp::PgTempDB, PgConnectOptions) {
        let temp_db = pgtemp::PgTempDB::new();
        let base: PgConnectOptions = temp_db
            .connection_uri()
            .parse()
            .expect("pgtemp's URI is a Postgres URL");
        let mut operator = operator(&base).await;
        dipper_pgmq::run_db_migrations(&mut operator)
            .await
            .expect("run the queue's migrations");
        dipper_pgregistry::run_db_migrations(&mut operator)
            .await
            .expect("run the registry's migrations");
        fence_every_table(&mut operator)
            .await
            .expect("fence every table");
        (temp_db, base)
    }

    async fn operator(base: &PgConnectOptions) -> PgConnection {
        writer_fence::operator_options(base.clone())
            .connect()
            .await
            .expect("open an operator session")
    }

    /// The pool of the process with `token`, as `db::connect` opens it.
    async fn pool_of(base: &PgConnectOptions, token: Uuid) -> PgPool {
        PgPoolOptions::new()
            .max_connections(4)
            .connect_with(writer_fence::writer_options(base.clone(), token))
            .await
            .expect("open the pool")
    }

    /// Makes the process whose pool is `pool` the writer, as its start does: takes over, starts
    /// the heartbeat, and settles what the process it replaced had in flight.
    async fn start(
        pool: &PgPool,
        token: Uuid,
        refusals: Arc<Notify>,
    ) -> (Handle, JoinHandle<anyhow::Result<()>>) {
        let taken = Takeover::new(pool, token, "test".to_owned())
            .take()
            .await
            .expect("take over");
        let (handle, beating) = heartbeat(pool.connect_options().as_ref().clone(), token, refusals);
        let beating = tokio::spawn(beating);
        taken.settle().await.expect("settle the takeover");
        (handle, beating)
    }

    /// Hands over for the writer, as an operator does for one that won't stop.
    async fn force_handover(base: &PgConnectOptions) {
        sqlx::query("UPDATE dipper_writer_heartbeat SET released = true")
            .execute(&mut operator(base).await)
            .await
            .expect("hand over by hand");
    }

    async fn write(pool: &PgPool) -> sqlx::Result<()> {
        sqlx::query(
            "INSERT INTO dipper_kafka_consumer_offsets (topic, partition_id, next_offset) \
             VALUES (gen_random_uuid()::text, 0, 1)",
        )
        .execute(pool)
        .await
        .map(drop)
    }

    fn refused(result: sqlx::Result<()>) -> bool {
        result.is_err_and(|err| writer_fence::is_refusal(&err))
    }

    #[tokio::test]
    async fn it_fences_every_table_and_a_new_database_is_taken_over_at_once() {
        //* Given
        let (_temp_db, base) = started_db().await;
        let mut operator = operator(&base).await;
        let token = new_token();
        let pool = pool_of(&base, token).await;

        //* Then - every table is fenced, the queue's without the lock a job would hold
        assert!(
            writer_fence::unfenced(&mut operator)
                .await
                .unwrap()
                .is_empty()
        );
        let queue_check: String = sqlx::query_scalar(
            "SELECT p.proname::text FROM pg_trigger t JOIN pg_proc p ON p.oid = t.tgfoid \
             WHERE t.tgrelid = 'pgmq_queue'::regclass AND t.tgname = 'dipper_fence'",
        )
        .fetch_one(&mut operator)
        .await
        .unwrap();
        assert_eq!(queue_check, "dipper_fence_nolock");
        assert!(refused(write(&pool).await), "refused until it takes over");

        //* When
        let started = Instant::now();
        let (handle, _beating) = start(&pool, token, Arc::new(Notify::new())).await;

        //* Then
        assert!(
            started.elapsed() < ASK_EVERY,
            "a new database has no writer to wait for"
        );
        assert!(write(&pool).await.is_ok());
        assert!(handle.finish(true).await, "it hands over at its stop");
    }

    #[tokio::test]
    async fn it_waits_while_the_writer_is_healthy_and_takes_over_once_it_hands_over() {
        //* Given
        let (_temp_db, base) = started_db().await;
        let (old, new) = (new_token(), new_token());
        let old_pool = pool_of(&base, old).await;
        let (old_handle, _old_beating) = start(&old_pool, old, Arc::new(Notify::new())).await;
        let new_pool = pool_of(&base, new).await;
        let takeover = Takeover::new(&new_pool, new, "new".to_owned());

        //* When
        let waiting = tokio::spawn(async move { takeover.take().await });
        tokio::time::sleep(ASK_EVERY + Duration::from_secs(1)).await;

        //* Then
        assert!(!waiting.is_finished(), "a healthy writer is waited for");

        //* When - the old writer stops, and hands over
        assert!(old_handle.finish(true).await);

        //* Then
        let taken = tokio::time::timeout(ASK_EVERY * 3, waiting)
            .await
            .expect("it takes over at its next ask")
            .expect("the takeover task")
            .expect("take over");
        taken.settle().await.expect("settle the takeover");
        assert!(write(&new_pool).await.is_ok());
        assert!(refused(write(&old_pool).await));
    }

    #[tokio::test]
    async fn it_stops_a_replaced_writer_as_soon_as_a_write_of_it_is_refused() {
        //* Given - a writer that won't stop, and an operator forcing a handover
        let (_temp_db, base) = started_db().await;
        let (old, new) = (new_token(), new_token());
        let old_pool = pool_of(&base, old).await;
        let refusals = Arc::new(Notify::new());
        let (_old_handle, old_beating) = start(&old_pool, old, Arc::clone(&refusals)).await;
        force_handover(&base).await;
        let new_pool = pool_of(&base, new).await;
        let (_new_handle, _new_beating) = start(&new_pool, new, Arc::new(Notify::new())).await;

        //* When - the old writer's next write is refused, which tells its heartbeat
        assert!(refused(write(&old_pool).await));
        refusals.notify_one();

        //* Then - it stops well before its next heartbeat was due
        let stopped = tokio::time::timeout(Duration::from_secs(2), old_beating)
            .await
            .expect("the heartbeat stops the process at once")
            .expect("the heartbeat task");
        assert!(
            stopped.is_err_and(|err| err.downcast_ref::<Superseded>().is_some()),
            "it stops as replaced"
        );
    }

    #[tokio::test]
    async fn it_ends_the_replaced_writers_sessions_which_puts_its_running_job_back() {
        //* Given - the old writer is running a job when an operator forces a handover
        let (_temp_db, base) = started_db().await;
        let (old, new) = (new_token(), new_token());
        let old_pool = pool_of(&base, old).await;
        let (_old_handle, _old_beating) = start(&old_pool, old, Arc::new(Notify::new())).await;
        let old_queue = PgQueue::with_max_retries(old_pool.clone(), 2);
        old_queue
            .push(JobBuilder::new("a job".to_owned()))
            .await
            .expect("queue a job");
        let running = old_queue
            .pop::<String>()
            .await
            .expect("pop")
            .expect("the job");
        force_handover(&base).await;

        //* When
        let new_pool = pool_of(&base, new).await;
        let (_new_handle, _new_beating) = start(&new_pool, new, Arc::new(Notify::new())).await;

        //* Then - the new writer gets the job, and the old one can't settle it
        let new_queue = PgQueue::with_max_retries(new_pool.clone(), 2);
        let job = new_queue
            .pop::<String>()
            .await
            .expect("pop")
            .expect("the job is free once the takeover has settled");
        assert_eq!(job.id(), running.id());
        assert!(running.remove().await.is_err());
        job.remove().await.expect("the new writer settles it");
    }

    #[tokio::test]
    async fn it_lets_a_broadcast_through_only_while_this_process_is_the_writer() {
        //* Given
        let (_temp_db, base) = started_db().await;
        let (old, new) = (new_token(), new_token());
        let old_pool = pool_of(&base, old).await;
        let (_old_handle, _old_beating) = start(&old_pool, old, Arc::new(Notify::new())).await;
        let gate = WriterGate::new(old_pool.clone(), old);

        //* Then
        let turn = gate.open().await.expect("open a turn");
        assert!(turn.still_writer().await.expect("ask"));
        turn.end().await;

        //* When
        force_handover(&base).await;
        let new_pool = pool_of(&base, new).await;
        let (_new_handle, _new_beating) = start(&new_pool, new, Arc::new(Notify::new())).await;

        //* Then
        let turn = gate.open().await.expect("open a turn");
        assert!(!turn.still_writer().await.expect("ask"));
        turn.end().await;
    }

    #[tokio::test]
    async fn it_recognises_a_refusal_however_it_reaches_the_process() {
        //* Given - a process that never became the writer
        let (_temp_db, base) = started_db().await;
        let pool = pool_of(&base, new_token()).await;

        //* When
        let from_queue = PgQueue::with_max_retries(pool.clone(), 2)
            .push(JobBuilder::new("a job".to_owned()))
            .await
            .expect_err("the queue refuses it");
        let from_registry = crate::registry::RegistryProvider::new(pool)
            .set_indexing_target_candidates(
                thegraph_core::alloy::primitives::Address::ZERO,
                thegraph_core::deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv"),
                1,
                1,
            )
            .await
            .expect_err("the registry refuses it");

        //* Then
        assert!(refusal_in(&from_queue));
        assert!(refusal_in(
            &anyhow::Error::from(from_registry).context("in a job")
        ));
        assert!(!refusal_in(&anyhow::anyhow!("an error of another kind")));
    }
}
