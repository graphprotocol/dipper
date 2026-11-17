use async_trait::async_trait;
pub use dipper_pgmq::{
    JobBuilder, JobGuard, JobId, JobPriority, PgQueue, PgQueueListener, RetryOutcome,
};
use sqlx::PgPool;

/// A message queue.
///
/// This trait is used to interact with the message queue.
#[async_trait]
pub trait Queue<M>
where
    M: serde::Serialize + serde::de::DeserializeOwned,
{
    /// The notification source this queue hands out. An associated type rather
    /// than the concrete Postgres listener so the worker's subscription
    /// handling can be tested against a queue that has no database behind it.
    type Listener: JobNotifications;

    /// Pushes a message to the queue for immediate processing at `priority`.
    async fn push(&self, msg: M, priority: JobPriority) -> anyhow::Result<JobId>;

    /// Same as [`Queue::push`], but with a retry budget of this job's own
    /// rather than the queue-wide default, for work whose deadline decides how
    /// many attempts are worth making.
    async fn push_with_max_retries(
        &self,
        msg: M,
        priority: JobPriority,
        max_retries: u32,
    ) -> anyhow::Result<JobId>;

    /// Pulls a job from the queue
    async fn pop(&self) -> anyhow::Result<Option<JobGuard<'_, M>>>;

    /// Every message whose job isn't done yet: queued, waiting to retry, or running.
    async fn unfinished(&self) -> anyhow::Result<Vec<M>>;

    /// Subscribes to the `pgmq_jobs_available` channel
    async fn subscribe(&self) -> anyhow::Result<Self::Listener>;
}

#[derive(Clone)]
pub struct QueueImpl {
    inner: PgQueue,
}

impl QueueImpl {
    pub fn new(db_conn: PgPool) -> Self {
        Self {
            inner: PgQueue::with_max_retries(db_conn, 2),
        }
    }
}

#[async_trait]
impl<M> Queue<M> for QueueImpl
where
    M: serde::Serialize + serde::de::DeserializeOwned + Send + Unpin + 'static,
{
    type Listener = QueueImplListener;

    async fn push(&self, msg: M, priority: JobPriority) -> anyhow::Result<JobId> {
        self.inner
            .push(JobBuilder::new(msg).priority(priority))
            .await
    }

    async fn push_with_max_retries(
        &self,
        msg: M,
        priority: JobPriority,
        max_retries: u32,
    ) -> anyhow::Result<JobId> {
        self.inner
            .push(
                JobBuilder::new(msg)
                    .priority(priority)
                    .max_retries(max_retries),
            )
            .await
    }

    async fn pop(&self) -> anyhow::Result<Option<JobGuard<'_, M>>> {
        self.inner.pop().await
    }

    /// A stored message this release can't read, such as 1 from a newer release, is left out
    /// with a warning rather than failing the whole list.
    async fn unfinished(&self) -> anyhow::Result<Vec<M>> {
        let descriptors = self.inner.unfinished_descriptors().await?;
        Ok(descriptors
            .iter()
            .filter_map(|descriptor| match serde_json::from_str(descriptor) {
                Ok(msg) => Some(msg),
                Err(err) => {
                    tracing::warn!(error = %err, "skipping a queued job this release can't read");
                    None
                }
            })
            .collect())
    }

    async fn subscribe(&self) -> anyhow::Result<QueueImplListener> {
        self.inner.subscribe().await.map(QueueImplListener)
    }
}

/// A listener for the queue job available notification
pub struct QueueImplListener(PgQueueListener);

/// A source of "a job may be available" notifications. Abstracted behind a
/// trait so the worker loop's degrade-to-polling behaviour can be unit tested
/// without a live Postgres `LISTEN`/`NOTIFY` connection.
#[async_trait]
pub trait JobNotifications: Send {
    /// Waits for the next job-available notification.
    async fn wait_for_notification(&mut self) -> anyhow::Result<()>;
}

#[async_trait]
impl JobNotifications for QueueImplListener {
    async fn wait_for_notification(&mut self) -> anyhow::Result<()> {
        self.0.wait_for_notification().await
    }
}
