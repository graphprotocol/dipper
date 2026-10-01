use time::OffsetDateTime;

use super::{id::JobId, postgres};

/// A guard for a job in the queue
///
/// This struct is used to ensure that the transaction is committed when the job is removed, marked
/// as failed, or rescheduled.
pub struct JobGuard<'c, T> {
    tx: Option<sqlx::Transaction<'c, sqlx::Postgres>>,
    job: JobInner<T>,
    consumed: bool,
}

impl<'c, T> JobGuard<'c, T> {
    /// Creates a new job guard
    pub(crate) fn new(tx: sqlx::Transaction<'c, sqlx::Postgres>, job: JobInner<T>) -> Self {
        Self {
            tx: Some(tx),
            job,
            consumed: false,
        }
    }
}

impl<T> Drop for JobGuard<'_, T> {
    fn drop(&mut self) {
        if !self.consumed {
            tracing::warn!(
                job_id=%self.job.id,
                "JobGuard dropped without remove/mark_as_failed — tx will rollback via sqlx Drop"
            );
        }
    }
}

/// Job data accessors
impl<T> JobGuard<'_, T> {
    /// The job ID
    pub fn id(&self) -> &JobId {
        &self.job.id
    }

    /// The job creation timestamp
    pub fn created_at(&self) -> &OffsetDateTime {
        &self.job.created_at
    }

    /// The job last update timestamp
    pub fn updated_at(&self) -> &OffsetDateTime {
        &self.job.updated_at
    }

    /// The job descriptor
    pub fn desc(&self) -> &T {
        &self.job.desc
    }

    /// The number of failed attempts
    pub fn failed_attempts(&self) -> u32 {
        self.job.failed_attempts
    }

    /// The maximum number of attempts before a job is considered failed
    pub fn max_attempts(&self) -> u32 {
        self.job.max_attempts
    }
}

/// Job actions
impl<T> JobGuard<'_, T> {
    /// Remove the job from the queue
    #[expect(
        clippy::expect_used,
        reason = "predates this lint; fix when next touched"
    )]
    pub async fn remove(mut self) -> anyhow::Result<()> {
        self.consumed = true;
        let mut tx = self
            .tx
            .take()
            .expect("JobGuard tx already taken; remove/mark_as_failed called twice");
        postgres::remove(tx.as_mut(), &self.job.id).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Mark the job as failed
    #[expect(
        clippy::expect_used,
        reason = "predates this lint; fix when next touched"
    )]
    pub async fn mark_as_failed(mut self) -> anyhow::Result<()> {
        self.consumed = true;
        let mut tx = self
            .tx
            .take()
            .expect("JobGuard tx already taken; remove/mark_as_failed called twice");
        postgres::mark_as_failed(tx.as_mut(), &self.job.id, None).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Mark the job as failed and reschedule it
    #[expect(
        clippy::expect_used,
        reason = "predates this lint; fix when next touched"
    )]
    pub async fn mark_as_failed_and_reschedule(
        mut self,
        schedule: time::OffsetDateTime,
    ) -> anyhow::Result<()> {
        self.consumed = true;
        let mut tx = self
            .tx
            .take()
            .expect("JobGuard tx already taken; remove/mark_as_failed called twice");
        postgres::mark_as_failed(tx.as_mut(), &self.job.id, Some(schedule)).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Re-queue the job for a later time without counting an attempt. Used for
    /// deferral (e.g. a contended lock) where retrying is normal and must not
    /// push the job toward its max-attempt ceiling.
    #[expect(
        clippy::expect_used,
        reason = "predates this lint; fix when next touched"
    )]
    pub async fn reschedule(mut self, schedule: time::OffsetDateTime) -> anyhow::Result<()> {
        self.consumed = true;
        let mut tx = self
            .tx
            .take()
            .expect("JobGuard tx already taken; remove/mark_as_failed called twice");
        postgres::reschedule(tx.as_mut(), &self.job.id, schedule).await?;
        tx.commit().await?;
        Ok(())
    }
}

/// A job in the queue
#[derive(Debug, Clone)]
pub(crate) struct JobInner<T> {
    /// The job ID
    pub id: JobId,

    /// The job creation timestamp
    pub created_at: OffsetDateTime,
    /// The job last update timestamp
    pub updated_at: OffsetDateTime,

    /// The job descriptor
    pub desc: T,

    /// The maximum number of attempts before a job is considered failed
    pub max_attempts: u32,
    /// The number of failed attempts
    pub failed_attempts: u32,
}
