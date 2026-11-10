use async_trait::async_trait;
pub use dipper_pgmq::{
    JobBuilder, JobGuard, JobId, JobPriority, PgQueue, PgQueueListener, Popped, RetryOutcome,
    UnreadableJob,
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
        first_readable(|| self.inner.take_next()).await
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

/// Takes jobs until 1 can be read, so a stored job this release can't read, which the queue
/// has already marked failed for good, can't hold up the jobs behind it. A database error is
/// returned as it is.
async fn first_readable<'c, M, F, Fut>(mut take_next: F) -> anyhow::Result<Option<JobGuard<'c, M>>>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<Option<Popped<'c, M>>>>,
{
    loop {
        match take_next().await? {
            Some(Popped::Job(job)) => return Ok(Some(job)),
            Some(Popped::Unreadable(job)) => log_unreadable_job(&job),
            None => return Ok(None),
        }
    }
}

/// Alerts that a stored job couldn't be read and won't run, as a job failed for good. The full
/// stored job stays in the queue, marked failed, for an operator to look at.
fn log_unreadable_job(job: &UnreadableJob) {
    tracing::error!(
        event = super::service::JOB_FAILED_EVENT,
        job_id = %job.id,
        error = %job.error,
        stored = %job.stored_excerpt(),
        "Job can't be read by this release, so it was marked failed and will not run"
    );
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

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::{JobId, Popped, UnreadableJob, first_readable, log_unreadable_job};
    use crate::{test_support::logged, worker::messages::Message};

    fn unreadable(stored: &str) -> UnreadableJob {
        UnreadableJob {
            id: JobId::default(),
            error: serde_json::from_str::<Message>(stored).unwrap_err(),
            stored: stored.to_string(),
        }
    }

    /// Takes from `takes` in turn, as the queue would hand them out.
    async fn take_from(
        takes: Vec<anyhow::Result<Option<Popped<'static, Message>>>>,
    ) -> (anyhow::Result<bool>, String) {
        let mut takes = VecDeque::from(takes);
        let mut found = None;
        let logs = logged(async {
            found = Some(
                first_readable(|| {
                    let next = takes.pop_front().expect("taken past the end");
                    async move { next }
                })
                .await
                .map(|job| job.is_some()),
            );
        })
        .await;
        (found.expect("ran"), logs)
    }

    /// Unreadable jobs at the head of the queue are each alerted on, and the worker goes on to
    /// what lies behind them instead of stopping at the first.
    #[tokio::test]
    async fn unreadable_jobs_are_each_alerted_on_and_passed() {
        let (found, logs) = take_from(vec![
            Ok(Some(Popped::Unreadable(unreadable(
                r#"{"type":"RemovedJobType"}"#,
            )))),
            Ok(Some(Popped::Unreadable(unreadable(
                r#"{"type":"AnotherOne"}"#,
            )))),
            Ok(None),
        ])
        .await;

        assert!(
            !found.expect("no database error"),
            "the queue is then empty"
        );
        let alerts: Vec<&str> = logs
            .lines()
            .filter(|line| line.contains(r#"event="job_failed_permanently""#))
            .collect();
        assert_eq!(alerts.len(), 2, "{logs}");
        assert!(alerts[0].contains("RemovedJobType"), "{logs}");
        assert!(alerts[1].contains("AnotherOne"), "{logs}");
    }

    /// A database error still comes back as an error, for the worker to warn about and retry.
    #[tokio::test]
    async fn a_database_error_is_returned_as_before() {
        let (found, logs) = take_from(vec![
            Ok(Some(Popped::Unreadable(unreadable(
                r#"{"type":"RemovedJobType"}"#,
            )))),
            Err(anyhow::anyhow!("connection refused")),
        ])
        .await;

        let err = found.expect_err("the database error is returned");
        assert!(err.to_string().contains("connection refused"), "{err}");
        assert_eq!(logs.matches("job_failed_permanently").count(), 1, "{logs}");
    }

    /// The alert names the job and why it couldn't be read, and carries the stored job cut to
    /// a safe length.
    #[tokio::test]
    async fn an_unreadable_job_alert_names_the_job_the_error_and_what_was_stored() {
        let stored = format!(
            r#"{{"type":"RemovedJobType","padding":"{}"}}"#,
            "x".repeat(5_000)
        );
        let job = unreadable(&stored);

        let logs = logged(async { log_unreadable_job(&job) }).await;

        let line = logs
            .lines()
            .find(|line| line.contains(r#"event="job_failed_permanently""#))
            .unwrap_or_else(|| panic!("the alert is logged:\n{logs}"));
        assert!(line.contains("ERROR"), "{line}");
        assert!(line.contains(&JobId::default().to_string()), "{line}");
        assert!(line.contains("unknown variant `RemovedJobType`"), "{line}");
        assert!(line.contains("RemovedJobType"), "{line}");
        assert!(line.contains("characters in all"), "{line}");
        assert!(
            line.len() < 2_000,
            "the stored job is cut: {} bytes",
            line.len()
        );
    }
}
