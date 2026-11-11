use sqlx::{Pool, Postgres};
use time::OffsetDateTime;

use super::{
    id::JobId,
    job::{JobGuard, JobInner},
    postgres::{self, RetryOutcome},
};
pub use super::{listener::PgQueueListener, postgres::JobPriority};

/// The default maximum number of attempts before a job is considered as failed.
const DEFAULT_MAX_ATTEMPTS: i32 = 3;

/// A PostgreSQL message queue
#[derive(Debug, Clone)]
pub struct PgQueue {
    /// The DB connection pool.
    pool: Pool<Postgres>,
    /// The maximum number of attempts before a job is considered failed
    max_attempts: i32,
}

impl PgQueue {
    /// Creates a new PostgreSQL message queue.
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self {
            pool,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
        }
    }

    /// Creates a new PostgreSQL message queue with a custom maximum number of retries.
    ///
    /// The number of retries specifies how many times a job will be retried after the initial attempt fails.
    /// For example:
    /// - `max_retries = 0`: Job runs once, no retries (total attempts = 1)
    /// - `max_retries = 1`: Job runs once, then retried once if it fails (total attempts = 2)
    /// - `max_retries = 2`: Job runs once, then retried up to 2 times if it fails (total attempts = 3)
    pub fn with_max_retries(pool: Pool<Postgres>, value: u32) -> Self {
        let max_attempts = value.saturating_add(1).try_into().unwrap_or(i32::MAX);
        Self { pool, max_attempts }
    }
}

impl PgQueue {
    /// Pushes a job into the queue
    ///
    /// If the job is scheduled for immediate execution, the job will be available for processing
    /// immediately and a job available notification will be sent over the `pgmq_jobs_available`
    /// channel.
    pub async fn push<J, T>(&self, job: J) -> anyhow::Result<JobId>
    where
        J: Into<JobBuilder<T>>,
        T: serde::Serialize,
    {
        let JobBuilder {
            desc,
            max_attempts,
            scheduled_for,
            priority,
        } = job.into();

        let max_attempts = max_attempts.unwrap_or(self.max_attempts);
        match scheduled_for {
            None => {
                // Push the job and send the notification in a single transaction
                let mut tx = self.pool.begin().await?;
                let id = postgres::push(&mut *tx, desc, max_attempts, priority).await?;
                postgres::send_job_available_notification(&mut *tx, &id).await?;
                tx.commit().await?;
                Ok(id)
            }

            Some(scheduled_for) => {
                postgres::push_scheduled(&self.pool, desc, max_attempts, scheduled_for, priority)
                    .await
            }
        }
    }

    /// Pulls the next job this release can read from the queue. Each job ahead of it that can't
    /// be read is put off or failed first, as [`Self::take_next`] does, and logged.
    pub async fn pop<T>(&self) -> anyhow::Result<Option<JobGuard<'_, T>>>
    where
        T: for<'de> serde::Deserialize<'de> + Send + Unpin + 'static,
    {
        loop {
            match self.take_next().await? {
                Some(Popped::Job(job)) => return Ok(Some(job)),
                Some(Popped::Unreadable(job)) if job.outcome == RetryOutcome::WillRetry => {
                    tracing::warn!(
                        job_id = %job.id,
                        error = %job.error,
                        failed_attempts = job.failed_attempts,
                        "Put off a job that can't be read, to be tried again later"
                    );
                }
                Some(Popped::Unreadable(job)) => tracing::error!(
                    job_id = %job.id,
                    error = %job.error,
                    stored = %job.stored_excerpt(),
                    "Gave up on a job that can't be read; it is kept as failed"
                ),
                None => return Ok(None),
            }
        }
    }

    /// Takes the job at the head of the queue. A stored job that can't be read as `T`, such as
    /// 1 queued by a release whose job types differ, counts as a failed attempt: it is put off for
    /// a while, or failed for good once its attempts run out, its stored form kept either way, and
    /// handed back as [`Popped::Unreadable`]. Either way it leaves the head of the queue, so it
    /// can't hold up the jobs behind it. A database error is returned as an error, leaving the
    /// queue as it was.
    #[expect(
        clippy::cast_sign_loss,
        reason = "predates this lint; fix when next touched"
    )]
    pub async fn take_next<T>(&self) -> anyhow::Result<Option<Popped<'_, T>>>
    where
        T: for<'de> serde::Deserialize<'de> + Send + Unpin + 'static,
    {
        let mut tx = self.pool.begin().await?;

        let Some(job) = postgres::pop(tx.as_mut()).await? else {
            return Ok(None);
        };
        let desc = match T::deserialize(&job.descriptor.0) {
            Ok(desc) => desc,
            Err(error) => {
                let outcome = put_off_unreadable(&mut tx, &job).await?;
                tx.commit().await?;
                return Ok(Some(Popped::Unreadable(UnreadableJob {
                    id: job.id,
                    error,
                    stored: job.descriptor.0.to_string(),
                    failed_attempts: job.attempt_count,
                    max_attempts: job.max_attempts,
                    outcome,
                })));
            }
        };
        let job = JobInner {
            id: job.id,
            created_at: job.created_at,
            updated_at: job.updated_at,
            desc,
            max_attempts: job.max_attempts as u32,
            failed_attempts: job.attempt_count as u32,
        };
        Ok(Some(Popped::Job(JobGuard::new(tx, job))))
    }

    /// Subscribes to the `pgmq_jobs_available` channel
    pub async fn subscribe(&self) -> anyhow::Result<PgQueueListener> {
        PgQueueListener::new(&self.pool).await
    }

    /// The serialized descriptor of every job not yet done: queued, waiting to retry, or running.
    pub async fn unfinished_descriptors(&self) -> anyhow::Result<Vec<String>> {
        postgres::unfinished_descriptors(&self.pool).await
    }

    /// Clears the queue
    pub async fn clear(&self) -> anyhow::Result<()> {
        postgres::clear(&self.pool).await
    }
}

/// How long a job that can't be read is put off before it is tried again. A job this release
/// can't read may be 1 a newer release, running alongside it during a rollout, can; this is long
/// enough for the rollout to finish and the newer release to take the job.
const UNREADABLE_RETRY_DELAY: time::Duration = time::Duration::minutes(5);

/// Record a failed attempt at a job that can't be read, putting it off by
/// [`UNREADABLE_RETRY_DELAY`], or failing it for good once its attempts run out. Its stored form is
/// left as it is either way. A job whose attempt count can't be trusted, being negative or already
/// at its limit though queued, can't be counted towards giving up, so it is failed for good at once
/// rather than risk an update that can't be made, which would leave it at the head of the queue.
async fn put_off_unreadable(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    job: &postgres::PgJob<serde_json::Value>,
) -> anyhow::Result<RetryOutcome> {
    if job.attempt_count < 0 || job.attempt_count >= job.max_attempts {
        postgres::mark_as_unreadable(tx.as_mut(), &job.id).await?;
        return Ok(RetryOutcome::GaveUp);
    }
    let retry_at = OffsetDateTime::now_utc() + UNREADABLE_RETRY_DELAY;
    postgres::mark_as_failed(tx.as_mut(), &job.id, Some(retry_at)).await
}

/// What taking the job at the head of the queue found.
pub enum Popped<'c, T> {
    /// A job to run.
    Job(JobGuard<'c, T>),
    /// A stored job that can't be read, with its failed attempt recorded.
    Unreadable(UnreadableJob),
}

/// A stored job that couldn't be read, and what became of that failed attempt.
#[derive(Debug)]
pub struct UnreadableJob {
    /// The job's id.
    pub id: JobId,
    /// Why it couldn't be read.
    pub error: serde_json::Error,
    /// The job as stored.
    pub stored: String,
    /// The attempts that had failed before this 1, as stored.
    pub failed_attempts: i32,
    /// The attempts it is allowed, as stored.
    pub max_attempts: i32,
    /// Whether it was put off to be tried again, or failed for good with its stored form kept.
    pub outcome: RetryOutcome,
}

/// Most characters of a stored job a log line carries; the full form stays in the queue.
const STORED_EXCERPT_CHARS: usize = 1_000;

impl UnreadableJob {
    /// The stored job, cut to a length a log line or an alert can carry.
    pub fn stored_excerpt(&self) -> std::borrow::Cow<'_, str> {
        match self.stored.char_indices().nth(STORED_EXCERPT_CHARS) {
            None => self.stored.as_str().into(),
            Some((cut, _)) => format!(
                "{}... (cut; {} characters in all)",
                &self.stored[..cut],
                self.stored.chars().count()
            )
            .into(),
        }
    }
}

pub struct JobBuilder<T> {
    /// The job descriptor
    desc: T,
    /// The maximum number of attempts before a job is considered failed
    max_attempts: Option<i32>,
    /// The scheduled time for the job
    scheduled_for: Option<OffsetDateTime>,
    /// The scheduling priority; defaults to `Background`.
    priority: JobPriority,
}

impl<T> JobBuilder<T> {
    /// Creates a new job input
    pub fn new(desc: T) -> Self {
        Self {
            desc,
            max_attempts: None,
            scheduled_for: None,
            priority: JobPriority::default(),
        }
    }

    /// Sets the scheduling priority; `pop()` serves higher priority first.
    pub fn priority(mut self, priority: JobPriority) -> Self {
        self.priority = priority;
        self
    }

    /// Sets the maximum number of retries before a job is considered failed.
    ///
    /// The number of retries specifies how many times a job will be retried after the initial attempt fails.
    /// For example:
    /// - `max_retries = 0`: Job runs once, no retries (total attempts = 1)
    /// - `max_retries = 1`: Job runs once, then retried once if it fails (total attempts = 2)
    /// - `max_retries = 2`: Job runs once, then retried up to 2 times if it fails (total attempts = 3)
    pub fn max_retries(mut self, value: u32) -> Self {
        self.max_attempts = Some(value.saturating_add(1).try_into().unwrap_or(i32::MAX));
        self
    }

    /// Sets the scheduled time for the job
    pub fn schedule_at(mut self, schedule: OffsetDateTime) -> Self {
        self.scheduled_for = Some(schedule);
        self
    }
}

impl<T> From<T> for JobBuilder<T>
where
    T: serde::Serialize,
{
    fn from(desc: T) -> Self {
        Self::new(desc)
    }
}

#[cfg(test)]
mod tests {
    use super::{JobId, RetryOutcome, STORED_EXCERPT_CHARS, UnreadableJob};

    fn unreadable(stored: String) -> UnreadableJob {
        UnreadableJob {
            id: JobId::default(),
            error: serde_json::from_str::<u8>("\"x\"").unwrap_err(),
            stored,
            failed_attempts: 0,
            max_attempts: 3,
            outcome: RetryOutcome::WillRetry,
        }
    }

    /// A short stored job is logged whole; a long one is cut, on a character boundary, with a note
    /// of how long it was, so it can't swamp a log line or an alert.
    #[test]
    fn a_stored_job_is_cut_to_a_length_a_log_line_can_carry() {
        let short = "{\"type\":\"RemovedJobType\"}".to_string();
        assert_eq!(unreadable(short.clone()).stored_excerpt(), short);

        let long = "é".repeat(STORED_EXCERPT_CHARS + 5);
        let excerpt = unreadable(long).stored_excerpt().into_owned();
        assert!(
            excerpt.starts_with(&"é".repeat(STORED_EXCERPT_CHARS)),
            "{excerpt}"
        );
        assert!(
            excerpt.ends_with(&format!(
                "... (cut; {} characters in all)",
                STORED_EXCERPT_CHARS + 5
            )),
            "{excerpt}"
        );
    }
}
