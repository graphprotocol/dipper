//! Waiting at startup for the services dipper depends on. A dependency that is briefly away when
//! dipper starts, such as a database restarting or IISA between its old and new pod, is waited
//! for with growing pauses up to a bound, so dipper neither exits at the first refusal nor hangs
//! for ever on one that is not coming back.

use std::{fmt::Display, future::Future, time::Duration};

use tokio::time::Instant;
use url::Url;

/// The pause after the first failed attempt. Each later pause doubles, up to [`MAX_DELAY`].
pub const FIRST_DELAY: Duration = Duration::from_secs(1);

/// The longest pause between 2 attempts.
pub const MAX_DELAY: Duration = Duration::from_secs(30);

/// Why a wait at startup ended without success: the last attempt's error, how many attempts were
/// made and how long the wait took.
#[derive(Debug, PartialEq)]
pub struct GaveUp<E> {
    pub error: E,
    pub tries: u32,
    pub elapsed: Duration,
}

/// Runs `attempt` until it succeeds, pausing between tries for [`FIRST_DELAY`], then twice as
/// long each time up to [`MAX_DELAY`]. Each attempt is given the time left of `wait`, which it
/// must not exceed, so the whole wait stays within `wait`. Gives up on an error `worth_retrying`
/// refuses, or when the next try would start after `wait` has passed.
pub async fn retry_within<T, E, F, Fut>(
    what: &str,
    wait: Duration,
    worth_retrying: impl Fn(&E) -> bool,
    mut attempt: F,
) -> Result<T, GaveUp<E>>
where
    E: Display,
    F: FnMut(Duration) -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let started = Instant::now();
    let mut delay = FIRST_DELAY;
    let mut tries: u32 = 0;
    loop {
        tries += 1;
        let remaining = wait.saturating_sub(started.elapsed());
        let error = match attempt(remaining).await {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        let elapsed = started.elapsed();
        if !worth_retrying(&error) || elapsed + delay >= wait {
            return Err(GaveUp {
                error,
                tries,
                elapsed,
            });
        }
        tracing::warn!(
            what,
            attempt = tries,
            retry_in_secs = delay.as_secs(),
            waited_secs = elapsed.as_secs(),
            wait_secs = wait.as_secs(),
            error = %error,
            "a service dipper needs at startup is not ready yet, retrying"
        );
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(MAX_DELAY);
    }
}

/// Waits until IISA's health check passes, for up to `wait`. Dipper does not start without IISA:
/// every reassessment asks it which indexers to choose. Each check gets the time left of the wait
/// at most, so a check that hangs cannot stretch it.
pub async fn wait_for_iisa<F, Fut, E>(
    endpoint: &Url,
    wait: Duration,
    mut check: F,
) -> anyhow::Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<bool, E>>,
    E: Display,
{
    let checked = retry_within(
        "IISA health check",
        wait,
        |_| true,
        |remaining| {
            let check = check();
            async move {
                match tokio::time::timeout(remaining, check).await {
                    Ok(Ok(true)) => Ok(()),
                    Ok(Ok(false)) => Err("IISA reports itself unhealthy".to_string()),
                    Ok(Err(err)) => Err(err.to_string()),
                    Err(_) => Err("the health check did not answer in time".to_string()),
                }
            }
        },
    )
    .await;
    match checked {
        Ok(()) => {
            tracing::info!(%endpoint, "IISA service is healthy");
            Ok(())
        }
        Err(gave_up) => anyhow::bail!(
            "IISA service is not reachable at {endpoint} after {} tries over {}s: {}",
            gave_up.tries,
            gave_up.elapsed.as_secs(),
            gave_up.error
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, rc::Rc};

    use super::*;

    /// An attempt that fails `failures` times, then succeeds, counting its tries in `tries`.
    fn failing(
        failures: u32,
        tries: &Rc<Cell<u32>>,
    ) -> impl FnMut(Duration) -> std::future::Ready<Result<(), String>> {
        let tries = Rc::clone(tries);
        move |_remaining| {
            tries.set(tries.get() + 1);
            std::future::ready(if tries.get() > failures {
                Ok(())
            } else {
                Err(format!("try {} refused", tries.get()))
            })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn retries_until_the_dependency_answers() {
        let tries = Rc::new(Cell::new(0));
        let started = Instant::now();
        let result = retry_within(
            "test",
            Duration::from_secs(180),
            |_| true,
            failing(5, &tries),
        )
        .await;
        assert_eq!(result, Ok(()));
        assert_eq!(tries.get(), 6, "5 refusals, then the answer");
        assert_eq!(started.elapsed(), Duration::from_secs(1 + 2 + 4 + 8 + 16));
    }

    #[tokio::test(start_paused = true)]
    async fn gives_up_when_the_next_try_would_pass_the_wait() {
        let tries = Rc::new(Cell::new(0));
        let result = retry_within(
            "test",
            Duration::from_secs(10),
            |_| true,
            failing(u32::MAX, &tries),
        )
        .await;
        // Tries at 0s, 1s, 3s and 7s; the next would start at 15s, past the 10s wait.
        assert_eq!(
            result,
            Err(GaveUp {
                error: "try 4 refused".to_string(),
                tries: 4,
                elapsed: Duration::from_secs(7),
            }),
            "the last error, the tries made and the time actually waited come back"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn pauses_stop_growing_at_the_longest_delay() {
        let tries = Rc::new(Cell::new(0));
        let started = Instant::now();
        let _ = retry_within(
            "test",
            Duration::from_secs(200),
            |_| true,
            failing(u32::MAX, &tries),
        )
        .await;
        // Pauses of 1, 2, 4, 8 and 16s, then 30s each: tries end at 181s, as 211s is past 200s.
        assert_eq!(started.elapsed(), Duration::from_secs(181));
        assert_eq!(tries.get(), 11);
    }

    #[tokio::test(start_paused = true)]
    async fn an_error_not_worth_retrying_ends_the_wait_at_once() {
        let tries = Rc::new(Cell::new(0));
        let result = retry_within(
            "test",
            Duration::from_secs(180),
            |_| false,
            failing(u32::MAX, &tries),
        )
        .await;
        assert_eq!(result.map_err(|gave_up| gave_up.tries), Err(1));
    }

    #[tokio::test(start_paused = true)]
    async fn each_attempt_is_given_the_time_left() {
        let given = Rc::new(std::cell::RefCell::new(Vec::new()));
        let seen = Rc::clone(&given);
        let _ = retry_within(
            "test",
            Duration::from_secs(10),
            |_| true,
            move |remaining| {
                seen.borrow_mut().push(remaining);
                std::future::ready(Err::<(), _>("refused".to_string()))
            },
        )
        .await;
        assert_eq!(
            *given.borrow(),
            [10, 9, 7, 3].map(Duration::from_secs),
            "tries at 0s, 1s, 3s and 7s are each given what is left of 10s"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn waiting_for_iisa_rides_out_an_iisa_rollout() {
        // IISA's own rollout stops the old pod before the new one is ready, a gap of at least 90s
        // (its readiness probe's initial delay). A dipper starting in that gap must wait it out.
        let endpoint: Url = "http://iisa:8080".parse().unwrap();
        let back_at = Instant::now() + Duration::from_secs(90);
        let result = wait_for_iisa(&endpoint, Duration::from_secs(180), || async move {
            Ok::<_, std::io::Error>(Instant::now() >= back_at)
        })
        .await;
        assert!(result.is_ok(), "{result:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_hanging_iisa_health_check_cannot_stretch_the_wait() {
        // A check that never answers is cut off at the time left, so the wait ends on time.
        let endpoint: Url = "http://iisa:8080".parse().unwrap();
        let started = Instant::now();
        let err = wait_for_iisa(&endpoint, Duration::from_secs(60), || async {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            Ok::<_, std::io::Error>(true)
        })
        .await
        .unwrap_err();
        assert_eq!(started.elapsed(), Duration::from_secs(60), "{err}");
        assert!(err.to_string().contains("did not answer in time"), "{err}");
        assert!(
            err.to_string().contains("over 60s"),
            "the real time waited: {err}"
        );
    }
}
