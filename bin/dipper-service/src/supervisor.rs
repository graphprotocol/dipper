//! Process supervision for the task tree. Every long-running service runs in one
//! [`JoinSet`] and none finishes on its own, so a task finishing before shutdown
//! was requested has died and the process must restart rather than run on wedged.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio::{sync::Notify, task::JoinSet};

/// Shared shutdown coordination between [`supervise`] and the task running the
/// graceful stop sequence. Cloneable; all clones observe the same state.
#[derive(Clone, Default)]
pub struct Shutdown {
    requested: Arc<AtomicBool>,
    trigger: Arc<Notify>,
}

impl Shutdown {
    pub fn new() -> Self {
        Self::default()
    }

    /// Marks shutdown as requested and wakes whoever awaits
    /// [`Shutdown::requested_signal`]. Idempotent.
    pub fn request(&self) {
        self.requested.store(true, Ordering::SeqCst);
        // `notify_waiters`, not `notify_one`: several tasks may be parked here and
        // all must wake. It leaves no permit behind, but a waiter arriving later
        // sees the flag through the register-then-check below and never parks.
        self.trigger.notify_waiters();
    }

    /// Whether shutdown has been requested (by a signal or by an unexpected
    /// task exit).
    pub fn is_requested(&self) -> bool {
        self.requested.load(Ordering::SeqCst)
    }

    /// Resolves once shutdown has been requested. Registers interest before
    /// reading the flag so a [`Shutdown::request`] racing that read isn't lost.
    pub async fn requested_signal(&self) {
        let notified = self.trigger.notified();
        tokio::pin!(notified);
        // Register interest before re-reading the flag.
        notified.as_mut().enable();
        if self.is_requested() {
            return;
        }
        notified.await;
    }
}

/// Drains `task_tree`. A task finishing before shutdown was requested, and any failure or panic
/// at all (a crash mid-teardown must never read as clean), is fatal: shutdown is requested, the
/// drain continues, and the result is `Err`. `teardown_grace` bounds the whole teardown.
#[expect(
    clippy::cognitive_complexity,
    reason = "predates this lint; fix when next touched"
)]
pub async fn supervise(
    mut task_tree: JoinSet<anyhow::Result<()>>,
    shutdown: &Shutdown,
    teardown_grace: Duration,
) -> anyhow::Result<()> {
    // What actually went wrong first. This is the last line in the logs before a
    // restart, so it names the task rather than just saying something died.
    let mut first_failure: Option<String> = None;

    // Set on the first pass that sees shutdown requested, then reused. A per-wait
    // timeout would restart on every task that finishes, so a long tail of slow
    // stragglers could stretch the teardown past any bound we claimed to hold.
    let mut teardown_deadline: Option<tokio::time::Instant> = None;

    loop {
        // Before shutdown, tasks are meant to run forever, so wait unbounded. Once
        // it starts, the stop sequence itself runs in one of these tasks, so if
        // that one died nobody stops the rest: bound the wait as a watchdog.
        let next = if shutdown.is_requested() {
            let deadline = *teardown_deadline
                .get_or_insert_with(|| tokio::time::Instant::now() + teardown_grace);
            match tokio::time::timeout_at(deadline, task_tree.join_next_with_id()).await {
                Ok(next) => next,
                Err(_elapsed) => {
                    // Report only what the supervisor can see. Whether the stop sequence
                    // got as far as flushing events or closing the DB pool is its own
                    // business; all that is known here is which tasks are still running.
                    tracing::error!(
                        grace_secs = teardown_grace.as_secs(),
                        tasks_remaining = task_tree.len(),
                        first_failure = first_failure.as_deref().unwrap_or("none recorded"),
                        "teardown exceeded its grace period: some tasks were still running \
                         that long after shutdown was requested. Aborting them and exiting \
                         non-zero so the orchestrator restarts the process rather than hanging"
                    );
                    let remaining = task_tree.len();
                    task_tree.abort_all();
                    return Err(match first_failure {
                        Some(cause) => anyhow::anyhow!(
                            "teardown exceeded its {}s grace period with {remaining} task(s) \
                             still running, after: {cause}",
                            teardown_grace.as_secs()
                        ),
                        None => anyhow::anyhow!(
                            "teardown exceeded its {}s grace period with {remaining} task(s) \
                             still running",
                            teardown_grace.as_secs()
                        ),
                    });
                }
            }
        } else {
            tokio::select! {
                next = task_tree.join_next_with_id() => next,
                // Shutdown was requested with no task finishing to wake us. Loop
                // so the next wait is the bounded one and the watchdog starts;
                // `join_next_with_id` is cancel-safe, so dropping it loses none.
                _ = shutdown.requested_signal() => continue,
            }
        };

        let Some(res) = next else { break };

        let id = match &res {
            Ok((id, _)) => *id,
            Err(err) => err.id(),
        };

        match &res {
            Ok((_, Ok(()))) => {
                tracing::debug!(task_id = %id, "task completed");
            }
            Ok((_, Err(err))) => {
                // A task returning `Err` is a failure regardless of whether
                // shutdown is already underway; never let it pass as clean.
                tracing::error!(task_id = %id, error = ?err, "task failed");
                first_failure.get_or_insert_with(|| format!("task {id} failed: {err:#}"));
            }
            Err(err) => {
                // A panic (join error) is likewise fatal in either state, so a
                // crash while the tree is being torn down still exits non-zero.
                tracing::error!(task_id = %id, error = ?err, "task join error");
                first_failure.get_or_insert_with(|| format!("task {id} panicked: {err}"));
            }
        }

        if !shutdown.is_requested() {
            tracing::error!(
                task_id = %id,
                "a critical task exited unexpectedly; initiating shutdown so the process \
                 restarts rather than running on with a dead task"
            );
            first_failure
                .get_or_insert_with(|| format!("task {id} exited before shutdown was requested"));
            shutdown.request();
        }
    }

    if let Some(cause) = first_failure {
        anyhow::bail!("a critical task exited unexpectedly: {cause}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::task::JoinSet;

    use super::{Shutdown, supervise};

    /// Bound on how long `supervise` may run in a test. A broken impl that
    /// never requests shutdown would otherwise hang the coordinator stand-in
    /// forever; this turns that into a deterministic failure.
    const SUPERVISE_TIMEOUT: Duration = Duration::from_secs(5);

    /// Teardown grace passed to `supervise` in tests. Short so the stall test
    /// resolves quickly; the clean-path tests finish well within it.
    const TEST_TEARDOWN_GRACE: Duration = Duration::from_millis(200);

    /// A task finishing before shutdown was requested is fatal, and requests
    /// shutdown so the rest of the tree comes down. Regression test for the join
    /// loop that merely logged a dead task and let the process keep running.
    #[tokio::test]
    async fn unexpected_task_exit_is_fatal_and_triggers_shutdown() {
        let shutdown = Shutdown::new();
        let mut task_tree: JoinSet<anyhow::Result<()>> = JoinSet::new();

        // Stand-in for the stop-sequence task: drains once shutdown is asked
        // for. If `supervise` fails to request shutdown, this never completes
        // and the test hangs (caught by the harness timeout).
        let coordinator = shutdown.clone();
        task_tree.spawn(async move {
            coordinator.requested_signal().await;
            Ok(())
        });

        // A critical task that exits on its own, with nobody having asked for
        // shutdown.
        task_tree.spawn(async { Ok(()) });

        let result = tokio::time::timeout(
            SUPERVISE_TIMEOUT,
            supervise(task_tree, &shutdown, TEST_TEARDOWN_GRACE),
        )
        .await
        .expect("supervise did not request shutdown; coordinator never drained");

        assert!(result.is_err(), "an unexpected task exit must be fatal");
        assert!(
            shutdown.is_requested(),
            "an unexpected task exit must request shutdown"
        );
    }

    /// When shutdown was deliberately requested, tasks completing afterwards is
    /// the normal path and must not be reported as fatal.
    #[tokio::test]
    async fn deliberate_shutdown_is_not_fatal() {
        let shutdown = Shutdown::new();
        let mut task_tree: JoinSet<anyhow::Result<()>> = JoinSet::new();

        // The "signal handler": requests shutdown, then completes.
        let coordinator = shutdown.clone();
        task_tree.spawn(async move {
            coordinator.request();
            Ok(())
        });

        // Other services that exit only after shutdown is requested.
        for _ in 0..3 {
            let s = shutdown.clone();
            task_tree.spawn(async move {
                s.requested_signal().await;
                Ok(())
            });
        }

        let result = tokio::time::timeout(
            SUPERVISE_TIMEOUT,
            supervise(task_tree, &shutdown, TEST_TEARDOWN_GRACE),
        )
        .await
        .expect("supervise did not drain after a deliberate shutdown");

        assert!(
            result.is_ok(),
            "a deliberate shutdown must not be reported as fatal: {result:?}"
        );
    }

    /// When the stop sequence dies without stopping the rest, those services run
    /// forever and the drain would block for good. `supervise` must give up after
    /// the grace period, abort what's left, and exit non-zero instead of hanging.
    #[tokio::test]
    async fn teardown_stall_is_bounded_and_fatal() {
        let shutdown = Shutdown::new();
        let mut task_tree: JoinSet<anyhow::Result<()>> = JoinSet::new();

        // Stand-in for the stop-sequence task: it requests shutdown and then
        // exits without stopping anything (as if it had panicked mid-teardown).
        let coordinator = shutdown.clone();
        task_tree.spawn(async move {
            coordinator.request();
            Ok(())
        });

        // A service that never gets a stop signal, so it runs forever. Without
        // the watchdog the drain would wait on this task with no end.
        task_tree.spawn(async {
            std::future::pending::<()>().await;
            Ok(())
        });

        let result = tokio::time::timeout(
            SUPERVISE_TIMEOUT,
            supervise(task_tree, &shutdown, TEST_TEARDOWN_GRACE),
        )
        .await
        .expect("supervise hung on a stalled teardown instead of bounding it");

        assert!(
            result.is_err(),
            "a stalled teardown must be fatal, not a silent hang"
        );
    }

    /// The same stall with the stop sequence wedging on its first step, so *no*
    /// task finishes. The test above lets its coordinator return, which wakes the
    /// drain; here the watchdog has to start off the shutdown flag alone.
    #[tokio::test]
    async fn teardown_stall_before_any_task_finishes_is_bounded() {
        let shutdown = Shutdown::new();
        let mut task_tree: JoinSet<anyhow::Result<()>> = JoinSet::new();

        // Stand-in for the signal handler: asks for shutdown, then wedges on its
        // first stop step and never returns.
        let coordinator = shutdown.clone();
        task_tree.spawn(async move {
            coordinator.request();
            std::future::pending::<()>().await;
            Ok(())
        });

        // A service nobody is left to stop.
        task_tree.spawn(async {
            std::future::pending::<()>().await;
            Ok(())
        });

        let result = tokio::time::timeout(
            SUPERVISE_TIMEOUT,
            supervise(task_tree, &shutdown, TEST_TEARDOWN_GRACE),
        )
        .await
        .expect("supervise hung: the teardown watchdog never engaged");

        assert!(
            result.is_err(),
            "a teardown that stalls before any task finishes must still be fatal"
        );
    }

    /// A task reporting an error on its way out is how a failed signal handler
    /// registration reaches the exit code: without it the pod would run on unable
    /// to receive a stop signal, so the error itself has to reach the caller.
    #[tokio::test]
    async fn a_task_error_before_shutdown_is_reported_by_name() {
        let shutdown = Shutdown::new();
        let mut task_tree: JoinSet<anyhow::Result<()>> = JoinSet::new();

        // Stand-in for the stop-sequence task, so the drain can finish.
        let coordinator = shutdown.clone();
        task_tree.spawn(async move {
            coordinator.requested_signal().await;
            Ok(())
        });

        task_tree.spawn(async { Err(anyhow::anyhow!("signal registration failed")) });

        let result = tokio::time::timeout(
            SUPERVISE_TIMEOUT,
            supervise(task_tree, &shutdown, TEST_TEARDOWN_GRACE),
        )
        .await
        .expect("supervise did not request shutdown; coordinator never drained");

        let err = result.expect_err("a task reporting an error must be fatal");
        assert!(
            err.to_string().contains("signal registration failed"),
            "the exit error must carry what the task actually reported: {err}"
        );
        assert!(
            shutdown.is_requested(),
            "a task reporting an error must request shutdown"
        );
    }

    /// The same error once shutdown is already underway. Tasks finishing here is
    /// the normal path, which makes it tempting to treat everything that arrives
    /// as clean; an error is still an error and must not be absorbed.
    #[tokio::test]
    async fn a_task_error_during_shutdown_is_still_fatal() {
        let shutdown = Shutdown::new();
        let mut task_tree: JoinSet<anyhow::Result<()>> = JoinSet::new();

        let coordinator = shutdown.clone();
        task_tree.spawn(async move {
            coordinator.request();
            Ok(())
        });

        let s = shutdown.clone();
        task_tree.spawn(async move {
            s.requested_signal().await;
            Err(anyhow::anyhow!("the DB pool close failed"))
        });

        let result = tokio::time::timeout(
            SUPERVISE_TIMEOUT,
            supervise(task_tree, &shutdown, TEST_TEARDOWN_GRACE),
        )
        .await
        .expect("supervise did not drain after a task error during shutdown");

        let err = result.expect_err("an error during teardown must be fatal, not absorbed");
        assert!(
            err.to_string().contains("the DB pool close failed"),
            "the exit error must name what failed: {err}"
        );
    }

    /// The grace period is a budget for the whole teardown, not one that restarts
    /// every time a task finishes. Stragglers that each land inside the grace but
    /// together run well past it must not be able to stretch the drain.
    #[tokio::test(start_paused = true)]
    async fn teardown_grace_bounds_the_whole_drain_not_each_wait() {
        let shutdown = Shutdown::new();
        let mut task_tree: JoinSet<anyhow::Result<()>> = JoinSet::new();

        let coordinator = shutdown.clone();
        task_tree.spawn(async move {
            coordinator.request();
            Ok(())
        });

        // Each finishes within the grace of the one before it, so a per-wait timer
        // would keep resetting and the drain would run to at least 650ms.
        for step in 1..=3 {
            task_tree.spawn(async move {
                tokio::time::sleep(Duration::from_millis(150 * step)).await;
                Ok(())
            });
        }

        // And one that never stops, so the watchdog is what ends the drain.
        task_tree.spawn(async {
            std::future::pending::<()>().await;
            Ok(())
        });

        let start = tokio::time::Instant::now();
        let result = supervise(task_tree, &shutdown, TEST_TEARDOWN_GRACE).await;
        let elapsed = start.elapsed();

        assert!(result.is_err(), "an unbounded drain must be fatal");
        assert!(
            elapsed < TEST_TEARDOWN_GRACE * 2,
            "the grace period restarted on each finish: the drain took {elapsed:?}, which is \
             past the {TEST_TEARDOWN_GRACE:?} budget for the teardown as a whole"
        );
    }

    /// When the watchdog fires after a task had already failed, the exit error has
    /// to keep naming that failure. The timeout is the symptom; the earlier failure
    /// is the thing an operator needs to read in the last line before a restart.
    #[tokio::test(start_paused = true)]
    async fn a_failure_before_the_stall_survives_into_the_exit_error() {
        let shutdown = Shutdown::new();
        let mut task_tree: JoinSet<anyhow::Result<()>> = JoinSet::new();

        let coordinator = shutdown.clone();
        task_tree.spawn(async move {
            coordinator.request();
            Err(anyhow::anyhow!("the stop sequence blew up"))
        });

        // Nobody is left to stop this, so the watchdog ends the drain.
        task_tree.spawn(async {
            std::future::pending::<()>().await;
            Ok(())
        });

        let result = supervise(task_tree, &shutdown, TEST_TEARDOWN_GRACE).await;

        let err = result.expect_err("a stalled teardown must be fatal");
        assert!(
            err.to_string().contains("the stop sequence blew up"),
            "the stall error dropped the failure that caused it: {err}"
        );
    }

    /// A task that panics while the tree is being torn down must still exit the
    /// process non-zero, not be masked into a clean exit just because shutdown was
    /// already underway, and the error must name what died.
    #[tokio::test]
    async fn panic_during_shutdown_is_fatal() {
        let shutdown = Shutdown::new();
        let mut task_tree: JoinSet<anyhow::Result<()>> = JoinSet::new();

        // The "signal handler": requests shutdown, then completes cleanly.
        let coordinator = shutdown.clone();
        task_tree.spawn(async move {
            coordinator.request();
            Ok(())
        });

        // A service that panics once shutdown is underway.
        let s = shutdown.clone();
        task_tree.spawn(async move {
            s.requested_signal().await;
            panic!("boom during teardown");
        });

        let result = tokio::time::timeout(
            SUPERVISE_TIMEOUT,
            supervise(task_tree, &shutdown, TEST_TEARDOWN_GRACE),
        )
        .await
        .expect("supervise did not drain after a panic during shutdown");

        let err = result.expect_err("a panic during teardown must be fatal, not masked");
        assert!(
            err.to_string().contains("panicked"),
            "the error must name what actually died, not just say something did: {err}"
        );
    }

    /// A single `request()` must wake *every* parked waiter. Both are polled to
    /// their registered state while the flag is false, then `request()` is called
    /// once. Regression test for `notify_one`, which wakes only the first.
    #[test]
    fn request_wakes_all_concurrent_waiters() {
        use std::{
            future::Future,
            pin::pin,
            task::{Context, Poll},
        };

        let shutdown = Shutdown::new();
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);

        let mut w1 = pin!(shutdown.requested_signal());
        let mut w2 = pin!(shutdown.requested_signal());

        // With the flag still false, both register on the notify and suspend.
        assert!(matches!(w1.as_mut().poll(&mut cx), Poll::Pending));
        assert!(matches!(w2.as_mut().poll(&mut cx), Poll::Pending));

        shutdown.request();

        assert!(
            matches!(w1.as_mut().poll(&mut cx), Poll::Ready(())),
            "the first parked waiter must be woken by request()"
        );
        assert!(
            matches!(w2.as_mut().poll(&mut cx), Poll::Ready(())),
            "every parked waiter must be woken by request(), not just the first"
        );
    }
}
