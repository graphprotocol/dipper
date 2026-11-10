//! PostgreSQL message queue queries

use sqlx::{
    Postgres,
    migrate::{Migrate, MigrateError},
    types::Json,
};
use time::OffsetDateTime;
use uuid::Uuid;

use super::id::JobId;

/// Run the DB migrations.
///
/// It is used to ensure that the database is up to date with the latest migrations.
pub async fn run_db_migrations<'a, A>(conn: A) -> Result<(), MigrateError>
where
    A: sqlx::Acquire<'a>,
    <A::Connection as std::ops::Deref>::Target: Migrate,
{
    let mut migrator = sqlx::migrate!("./migrations");
    migrator.set_ignore_missing(true);
    migrator.run(conn).await?;
    Ok(())
}

/// Push a job into the queue
pub async fn push<'q, E, T>(
    executor: E,
    desc: T,
    max_attempts: i32,
    priority: JobPriority,
) -> anyhow::Result<JobId>
where
    E: sqlx::Executor<'q, Database = Postgres>,
    T: serde::Serialize,
{
    let id = Uuid::now_v7();

    sqlx::query(
        r#"INSERT INTO pgmq_queue (
                id,
                status,
                max_attempts,
                priority,
                descriptor
            ) VALUES (
                $1, $2, $3, $4, $5
            )"#,
    )
    .bind(id)
    .bind(PgJobStatus::default())
    .bind(max_attempts)
    .bind(priority)
    .bind(Json(desc))
    .execute(executor)
    .await?;

    Ok(JobId::from_uuid(id))
}

/// Push a scheduled job into the queue
pub async fn push_scheduled<'q, E, T>(
    executor: E,
    desc: T,
    max_attempts: i32,
    scheduled_for: OffsetDateTime,
    priority: JobPriority,
) -> anyhow::Result<JobId>
where
    E: sqlx::Executor<'q, Database = Postgres>,
    T: serde::Serialize,
{
    let id = JobId::from_uuid(Uuid::now_v7());
    sqlx::query(
        r#"INSERT INTO pgmq_queue (
                id,
                scheduled_for,
                status,
                max_attempts,
                priority,
                descriptor
            ) VALUES (
                $1, $2, $3, $4, $5, $6
            )"#,
    )
    .bind(id)
    .bind(scheduled_for)
    .bind(PgJobStatus::default())
    .bind(max_attempts)
    .bind(priority)
    .bind(Json(desc))
    .execute(executor)
    .await?;

    Ok(id)
}

/// Pop a job from the queue. Its descriptor comes back as stored, so a job this release can't
/// read is still taken off the head of the queue rather than failing the pop.
pub async fn pop<'q, E>(executor: E) -> anyhow::Result<Option<PgJob<serde_json::Value>>>
where
    E: sqlx::Executor<'q, Database = Postgres>,
{
    // MATERIALIZED is required for correctness, not perf: without it Postgres
    // re-runs the `FOR UPDATE SKIP LOCKED LIMIT 1` subquery per outer row and
    // marks several rows Running in one UPDATE, orphaning all but the first.
    let res = sqlx::query_as(
        r#"WITH next_job AS MATERIALIZED (
                SELECT id
                FROM pgmq_queue
                WHERE status = $2 AND scheduled_for <= timezone('UTC', now())
                -- Highest priority first, then insertion time, not scheduled_for:
                -- a deferred job is re-queued at now()+delay, so ordering by
                -- scheduled_for would sort it behind fresh jobs and could starve
                -- it. The monotonic v7 id breaks same-timestamp ties for FIFO.
                ORDER BY priority DESC, created_at, id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE pgmq_queue
            SET
                updated_at = timezone('UTC', now()),
                status = $1
            FROM next_job
            WHERE pgmq_queue.id = next_job.id
            RETURNING pgmq_queue.*"#,
    )
    .bind(PgJobStatus::Running)
    .bind(PgJobStatus::Queued)
    .fetch_optional(executor)
    .await?;
    Ok(res)
}

/// The serialized descriptor of every job not yet done: queued, waiting to retry, or running. A job
/// that failed for good isn't listed.
pub async fn unfinished_descriptors<'q, E>(executor: E) -> anyhow::Result<Vec<String>>
where
    E: sqlx::Executor<'q, Database = Postgres>,
{
    let rows: Vec<(String,)> =
        sqlx::query_as("SELECT descriptor::text FROM pgmq_queue WHERE status IN ($1, $2)")
            .bind(PgJobStatus::Queued)
            .bind(PgJobStatus::Running)
            .fetch_all(executor)
            .await?;
    Ok(rows.into_iter().map(|(descriptor,)| descriptor).collect())
}

/// Clear the queue
pub async fn clear<'q, E>(executor: E) -> anyhow::Result<()>
where
    E: sqlx::Executor<'q, Database = Postgres>,
{
    sqlx::query("DELETE FROM pgmq_queue")
        .execute(executor)
        .await?;
    Ok(())
}

/// Mark a job that can't be read as failed for good, leaving its descriptor as stored so an
/// operator can look at it.
pub async fn mark_as_unreadable<'q, E>(executor: E, id: &JobId) -> anyhow::Result<()>
where
    E: sqlx::Executor<'q, Database = Postgres>,
{
    sqlx::query(
        "UPDATE pgmq_queue SET status = $2, updated_at = timezone('UTC', now()) WHERE id = $1",
    )
    .bind(id)
    .bind(PgJobStatus::Failed)
    .execute(executor)
    .await?;
    Ok(())
}

/// Remove a job from the queue
pub async fn remove<'q, E>(executor: E, id: &JobId) -> anyhow::Result<()>
where
    E: sqlx::Executor<'q, Database = Postgres>,
{
    sqlx::query("DELETE FROM pgmq_queue WHERE id = $1")
        .bind(id)
        .execute(executor)
        .await?;
    Ok(())
}

/// Shared re-queue UPDATE for both the failure-retry and deferral paths, so the
/// touched columns can't drift. `count_attempt` set: bump `attempt_count`, go
/// `Failed` at `max_attempts`. Clear: leave the count, always re-queue. Returns
/// the status the job was left in.
async fn requeue<'q, E>(
    executor: E,
    id: &JobId,
    scheduled_for: OffsetDateTime,
    count_attempt: bool,
) -> anyhow::Result<PgJobStatus>
where
    E: sqlx::Executor<'q, Database = Postgres>,
{
    let increment: i32 = if count_attempt { 1 } else { 0 };
    let (status,) = sqlx::query_as(
        r#"UPDATE pgmq_queue
           SET
               updated_at = timezone('UTC', now()),
               attempt_count = attempt_count + $5,
               status = (CASE
                   WHEN $5 = 1 AND attempt_count + 1 >= max_attempts THEN $2 ELSE $3
               END),
               scheduled_for = (CASE
                   WHEN $5 = 0 OR attempt_count + 1 < max_attempts THEN $4 ELSE scheduled_for
               END)
           WHERE id = $1
           RETURNING status"#,
    )
    .bind(id)
    .bind(PgJobStatus::Failed)
    .bind(PgJobStatus::Queued)
    .bind(scheduled_for)
    .bind(increment)
    .fetch_one(executor)
    .await?;
    Ok(status)
}

/// What recording a failed attempt left the job as.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RetryOutcome {
    /// It is queued to run again.
    WillRetry,
    /// That was its last allowed attempt, so it stays failed and never runs again.
    GaveUp,
}

/// Mark a job as failed; see `requeue` for the attempt/backoff/give-up rules.
pub async fn mark_as_failed<'q, E>(
    executor: E,
    id: &JobId,
    scheduled_for: Option<OffsetDateTime>,
) -> anyhow::Result<RetryOutcome>
where
    E: sqlx::Executor<'q, Database = Postgres>,
{
    let scheduled_for = scheduled_for.unwrap_or_else(OffsetDateTime::now_utc);
    match requeue(executor, id, scheduled_for, true).await? {
        PgJobStatus::Queued => Ok(RetryOutcome::WillRetry),
        PgJobStatus::Failed => Ok(RetryOutcome::GaveUp),
        PgJobStatus::Running => {
            anyhow::bail!("job {id} was left running after its failed attempt was recorded")
        }
    }
}

/// Re-queue a job for a later time without recording a failed attempt, so a
/// deferred job (e.g. a contended lock) never advances toward its max-attempt
/// ceiling. See `requeue`.
pub async fn reschedule<'q, E>(
    executor: E,
    id: &JobId,
    scheduled_for: OffsetDateTime,
) -> anyhow::Result<()>
where
    E: sqlx::Executor<'q, Database = Postgres>,
{
    requeue(executor, id, scheduled_for, false).await?;
    Ok(())
}

/// Send a job available notification to the `pgmq_jobs_available` channel
pub async fn send_job_available_notification<'q, E>(executor: E, id: &JobId) -> anyhow::Result<()>
where
    E: sqlx::Executor<'q, Database = Postgres>,
{
    sqlx::query("SELECT pg_notify('pgmq_jobs_available', $1::text)")
        .bind(id)
        .execute(executor)
        .await?;
    Ok(())
}

#[derive(sqlx::FromRow)]
pub struct PgJob<T> {
    /// The job ID.
    pub(crate) id: JobId,
    /// The job creation timestamp.
    pub(crate) created_at: OffsetDateTime,
    /// The job last update timestamp.
    pub(crate) updated_at: OffsetDateTime,

    /// The maximum number of execution attempts.
    pub(crate) max_attempts: i32,
    /// The number of execution attempts made so far.
    pub(crate) attempt_count: i32,

    /// The job descriptor (serialized).
    pub(crate) descriptor: Json<T>,
}

/// The scheduling priority of a job; `pop()` serves higher priority first.
/// Policy: Interactive = a human or Studio waits on first assessment; Sweep = periodic passes
/// over every request, which yield even to the offers their own jobs create; everything else is
/// remediation or hygiene and runs Background.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Default)]
#[repr(i16)]
pub enum JobPriority {
    /// The reassignment service's daily and hourly passes.
    Sweep = -1,
    /// Expiration, liveness remediation, follow-up jobs.
    /// Downstream proposal/offer jobs also run Background by design for now.
    #[default]
    Background = 0,
    /// Admin RPC set-target and the Studio Kafka listener wait on this.
    Interactive = 1,
}

impl From<i16> for JobPriority {
    fn from(value: i16) -> Self {
        match value {
            i16::MIN..=-1 => Self::Sweep,
            0 => Self::Background,
            1..=i16::MAX => Self::Interactive,
        }
    }
}

/// Stored as the column's own `SMALLINT`, so reading it back doesn't rest on matching type names.
impl sqlx::Type<Postgres> for JobPriority {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <i16 as sqlx::Type<Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <i16 as sqlx::Type<Postgres>>::compatible(ty)
    }
}

impl sqlx::Encode<'_, Postgres> for JobPriority {
    fn encode_by_ref(
        &self,
        buf: &mut <Postgres as sqlx::Database>::ArgumentBuffer<'_>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        sqlx::Encode::<Postgres>::encode_by_ref(&(*self as i16), buf)
    }
}

impl sqlx::Decode<'_, Postgres> for JobPriority {
    fn decode(
        value: <Postgres as sqlx::Database>::ValueRef<'_>,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        let value: i16 = sqlx::Decode::<Postgres>::decode(value)?;
        Ok(value.into())
    }
}

/// The job status.
///
/// We use a postgres `INT` representation as an optimization.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
#[repr(i32)]
enum PgJobStatus {
    /// The job is queued.
    #[default]
    Queued = 0,

    /// The job is being executed by a worker.
    Running = -1,

    /// The job execution has failed. It is retried until `max_attempts` is
    /// reached, after which it stays `Failed` and is not rescheduled.
    Failed = 1,
}

impl From<i32> for PgJobStatus {
    fn from(value: i32) -> Self {
        match value {
            i32::MIN..=-1_i32 => Self::Running,
            0 => Self::Queued,
            1_i32..=i32::MAX => Self::Failed,
        }
    }
}

/// Stored as the column's own `INT`, so reading it back doesn't rest on matching type names.
impl sqlx::Type<Postgres> for PgJobStatus {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <i32 as sqlx::Type<Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <i32 as sqlx::Type<Postgres>>::compatible(ty)
    }
}

impl sqlx::Encode<'_, Postgres> for PgJobStatus {
    fn encode_by_ref(
        &self,
        buf: &mut <Postgres as sqlx::Database>::ArgumentBuffer<'_>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        sqlx::Encode::<Postgres>::encode_by_ref(&(*self as i32), buf)
    }
}

impl sqlx::Decode<'_, Postgres> for PgJobStatus {
    fn decode(
        value: <Postgres as sqlx::Database>::ValueRef<'_>,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        let value: i32 = sqlx::Decode::<Postgres>::decode(value)?;
        Ok(value.into())
    }
}

#[cfg(test)]
mod tests {
    use sqlx::{Postgres, Type};

    use super::{JobPriority, PgJobStatus};
    use crate::id::JobId;

    /// How a type declares itself to the database library.
    fn declared<T: Type<Postgres>>() -> String {
        format!("{:?}", T::type_info())
    }

    /// The job id, status and priority are read back as well as written, so each declares the
    /// built-in type of its column rather than a name, whose match to the column's type would
    /// rest on how the database library compares names.
    #[test]
    fn stored_job_fields_declare_their_columns_built_in_types() {
        assert_eq!(declared::<PgJobStatus>(), declared::<i32>());
        assert_eq!(declared::<JobPriority>(), declared::<i16>());
        assert_eq!(declared::<JobId>(), declared::<uuid::Uuid>());
    }

    /// A stored priority reads back as the level it was written at, and anything below
    /// Background, as an older release could never have written, reads as Sweep.
    #[test]
    fn stored_priorities_read_back_in_order() {
        for priority in [
            JobPriority::Sweep,
            JobPriority::Background,
            JobPriority::Interactive,
        ] {
            assert_eq!(JobPriority::from(priority as i16), priority);
        }
        assert_eq!(JobPriority::from(i16::MIN), JobPriority::Sweep);
        assert!(JobPriority::Sweep < JobPriority::Background);
        assert!(JobPriority::Background < JobPriority::Interactive);
    }
}
