use sqlx::{Pool, Postgres};
use time::OffsetDateTime;

use super::{
    id::JobId,
    job::{JobGuard, JobInner},
    postgres,
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

    /// Pulls a job from the queue
    #[expect(
        clippy::cast_sign_loss,
        reason = "predates this lint; fix when next touched"
    )]
    pub async fn pop<T>(&self) -> anyhow::Result<Option<JobGuard<'_, T>>>
    where
        T: for<'de> serde::Deserialize<'de> + Send + Unpin + 'static,
    {
        let mut tx = self.pool.begin().await?;

        let res = postgres::pop(tx.as_mut()).await?.map(|job| {
            let job = JobInner {
                id: job.id,
                created_at: job.created_at,
                updated_at: job.updated_at,
                desc: job.descriptor.0,
                max_attempts: job.max_attempts as u32,
                failed_attempts: job.attempt_count as u32,
            };
            JobGuard::new(tx, job)
        });
        Ok(res)
    }

    /// Subscribes to the `pgmq_jobs_available` channel
    pub async fn subscribe(&self) -> anyhow::Result<PgQueueListener> {
        PgQueueListener::new(&self.pool).await
    }

    /// Clears the queue
    pub async fn clear(&self) -> anyhow::Result<()> {
        postgres::clear(&self.pool).await
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
