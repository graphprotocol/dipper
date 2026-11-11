use std::time::Duration;

use dipper_pgmq::{JobBuilder, JobGuard, JobPriority, PgQueue, Popped, RetryOutcome};
use fake::{Dummy, Fake, Faker};
use pgtemp::PgTempDB;
use sqlx::{Pool, Postgres};

/// Initialize a temporary database for integration testing: spins up a temp
/// Postgres, runs the migrations, and returns the connection pool and the
/// temporary database guard.
#[expect(
    clippy::expect_used,
    reason = "predates this lint; fix when next touched"
)]
async fn temp_pgmq_db() -> (Pool<Postgres>, PgTempDB) {
    let temp_db = PgTempDB::new();
    let db = Pool::connect(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to temporary database");
    dipper_pgmq::run_db_migrations(&db)
        .await
        .expect("Failed to run DB migrations");
    (db, temp_db)
}

/// A test message for integration testing.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct TestMsg {
    data: String,
}

impl Dummy<Faker> for TestMsg {
    fn dummy_with_rng<R: fake::Rng + ?Sized>(config: &Faker, rng: &mut R) -> Self {
        Self {
            data: Dummy::dummy_with_rng(config, rng),
        }
    }
}

#[tokio::test]
async fn push_and_pop_job() {
    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db);

    let msg = Faker.fake::<TestMsg>();

    //* When
    let job_id = queue
        .push(msg.clone())
        .await
        .expect("Failed to push message to queue");

    let jobs = queue
        .pop()
        .await
        .expect("Failed to pull message from queue");

    //* Then
    // Assert the job is pulled
    assert!(jobs.is_some());

    // Assert the message is the same as the one we pushed
    let job: JobGuard<TestMsg> = jobs.expect("Failed to get job from queue");
    assert_eq!(job.id(), &job_id);
    assert_eq!(job.desc().data, msg.data);
}

#[tokio::test]
async fn job_pulled_only_once() {
    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db);

    let msg = Faker.fake::<TestMsg>();

    //* When
    let job_id = queue
        .push(msg.clone())
        .await
        .expect("Failed to push message to queue");

    let jobs1: Option<JobGuard<TestMsg>> = queue
        .pop()
        .await
        .expect("Failed to pull message from queue");

    let jobs2: Option<JobGuard<TestMsg>> = queue
        .pop()
        .await
        .expect("Failed to pull message from queue");

    //* Then
    // The job should be pulled only once, the second pull should return no jobs
    assert!(jobs1.is_some());
    assert!(jobs2.is_none());

    // Assert the message is the same as the one we pushed
    let job1: JobGuard<TestMsg> = jobs1.expect("Failed to get job from queue");
    assert_eq!(job1.id(), &job_id);
    assert_eq!(job1.desc().data, msg.data);
}

#[tokio::test]
async fn scheduled_job_becomes_available() {
    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db);

    let msg = Faker.fake::<TestMsg>();
    let msg_schedule =
        time::OffsetDateTime::now_utc().saturating_add(time::Duration::milliseconds(500));

    //* When
    // Push a message and schedule the job for the future (500 milliseconds from now)
    let job_id = queue
        .push(JobBuilder::new(msg.clone()).schedule_at(msg_schedule))
        .await
        .expect("Failed to push message to queue");

    // Wait for the job to be available
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Pull the jobs from the queue
    let jobs = queue
        .pop()
        .await
        .expect("Failed to pull message from queue");

    //* Then
    // Assert the job is pulled, as it is ready to be pulled
    assert!(jobs.is_some());

    // Assert the message is the same as the one we pushed
    let job: JobGuard<TestMsg> = jobs.expect("Failed to get job from queue");
    assert_eq!(job.id(), &job_id);
    assert_eq!(job.desc().data, msg.data);
}

#[tokio::test]
async fn scheduled_job_not_available_early() {
    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db);

    let msg = Faker.fake::<TestMsg>();
    let msg_schedule = time::OffsetDateTime::now_utc().saturating_add(time::Duration::minutes(1));

    //* When
    // Push a message and schedule the job for the future (1 minute from now)
    let _id = queue
        .push(JobBuilder::new(msg.clone()).schedule_at(msg_schedule))
        .await
        .expect("Failed to push message to queue");

    // Pull the jobs from the queue immediately
    let jobs: Option<JobGuard<TestMsg>> = queue
        .pop()
        .await
        .expect("Failed to pull message from queue");

    //* Then
    // Assert no jobs are pulled, as the job is scheduled for the future
    assert!(jobs.is_none());
}

#[tokio::test]
async fn past_scheduled_job_immediately_available() {
    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db);

    let msg = Faker.fake::<TestMsg>();
    let msg_schedule = time::OffsetDateTime::now_utc().saturating_sub(time::Duration::minutes(5));

    //* When
    // We push a message and schedule the job in the past (5 minutes ago)
    let job_id = queue
        .push(JobBuilder::new(msg.clone()).schedule_at(msg_schedule))
        .await
        .expect("Failed to push message to queue");

    let jobs = queue
        .pop()
        .await
        .expect("Failed to pull message from queue");

    //* Then
    // Assert the job is pulled, as the message was scheduled for the past
    assert!(jobs.is_some());

    // Assert the message is the same as the one we pushed
    let job: JobGuard<TestMsg> = jobs.expect("Failed to get job from queue");
    assert_eq!(job.id(), &job_id);
    assert_eq!(job.desc().data, msg.data);
}

#[tokio::test]
async fn clear_queue_removes_all_jobs() {
    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db);

    let msg = Faker.fake::<TestMsg>();

    //* When
    // We push a message for immediate processing
    queue
        .push(msg)
        .await
        .expect("Failed to push message to queue");

    // Clear all jobs from the queue
    queue.clear().await.expect("Failed to clear queue");

    let jobs: Option<JobGuard<TestMsg>> = queue
        .pop()
        .await
        .expect("Failed to pull message from queue");

    //* Then
    // Assert no jobs are pulled, as the queue was cleared
    assert!(jobs.is_none());
}

#[tokio::test]
async fn remove_job_after_processing() {
    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db);

    let msg = Faker.fake::<TestMsg>();

    //* When
    // Insert the message for immediate processing
    let _ = queue
        .push(msg.clone())
        .await
        .expect("Failed to push message to queue");

    // Get a job from the queue
    let job: Option<JobGuard<TestMsg>> = queue
        .pop()
        .await
        .expect("Failed to pull message from queue");

    // Remove the job from the queue, e.g., after successfully executing the job
    let job = job.expect("Failed to get job from queue");
    job.remove().await.expect("Failed to remove job from queue");

    // Pull the message from the queue
    let job: Option<JobGuard<TestMsg>> = queue
        .pop()
        .await
        .expect("Failed to pull message from queue");

    //* Then
    // Assert no jobs are pulled, as the job was removed
    assert!(job.is_none());
}

#[tokio::test]
async fn failed_job_gets_rescheduled() {
    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db);

    let msg = Faker.fake::<TestMsg>();

    //* When
    // Insert the message for immediate processing
    let job_id = queue
        .push(msg.clone())
        .await
        .expect("Failed to push message to queue");

    // Pull a job from the queue, mark them as "RUNNING"
    let job: Option<JobGuard<TestMsg>> = queue.pop().await.expect("Failed to pull jobs from queue");

    // Mark the job as failed, and reschedule it for immediate execution (`None`)
    let job = job.expect("Failed to get job from queue");
    job.mark_as_failed()
        .await
        .expect("Failed to mark job as failed");

    // Pull the jobs from the queue
    let jobs: Option<JobGuard<TestMsg>> = queue
        .pop()
        .await
        .expect("Failed to pull message from queue");

    //* Then
    // Assert the job is pulled again, as it was marked as failed
    assert!(jobs.is_some());

    // Assert the message is the same as the one we pushed
    let job: JobGuard<TestMsg> = jobs.expect("Failed to get job from queue");
    assert_eq!(job.id(), &job_id);
    assert_eq!(job.desc().data, msg.data);
}

#[tokio::test]
async fn failed_job_rescheduled_for_future() {
    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db);

    let msg = Faker.fake::<TestMsg>();

    //* When
    // Insert the message for immediate processing
    let job_id = queue
        .push(msg.clone())
        .await
        .expect("Failed to push message to queue");

    // Pull a job from the queue, marking it as "RUNNING"
    let job: Option<JobGuard<TestMsg>> = queue.pop().await.expect("Failed to pull jobs from queue");

    // Mark the job as failed, and reschedule it for the future (500 milliseconds from now)
    let job = job.expect("Failed to get job from queue");
    let msg_schedule =
        time::OffsetDateTime::now_utc().saturating_add(time::Duration::milliseconds(500));
    job.mark_as_failed_and_reschedule(msg_schedule)
        .await
        .expect("Failed to mark job as failed");

    // Pull the jobs from the queue
    let jobs1: Option<JobGuard<TestMsg>> = queue
        .pop()
        .await
        .expect("Failed to pull message from queue");

    // Wait for the failed (and re-scheduled) job to be available
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Pull the jobs from the queue
    let jobs2: Option<JobGuard<TestMsg>> = queue
        .pop()
        .await
        .expect("Failed to pull message from queue");

    //* Then
    // The job should not be pulled if the job is rescheduled for the future and pulled immediately
    assert!(jobs1.is_none());

    // Assert the job is pulled again, as it was marked as failed
    assert!(jobs2.is_some());

    // Assert the message is the same as the one we pushed
    let job: JobGuard<TestMsg> = jobs2.expect("Failed to get job from queue");
    assert_eq!(job.id(), &job_id);
    assert_eq!(job.desc().data, msg.data);
}

#[tokio::test]
#[expect(
    clippy::cast_possible_wrap,
    reason = "predates this lint; fix when next touched"
)]
async fn custom_max_retries_sets_max_attempts() {
    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db.clone());

    let msg = Faker.fake::<TestMsg>();
    let custom_max_retries = 4u32; // 4 retries = 5 total attempts

    //* When
    // Push a job with custom max_retries using JobBuilder
    let job_id = queue
        .push(JobBuilder::new(msg.clone()).max_retries(custom_max_retries))
        .await
        .expect("Failed to push message to queue");

    //* Then
    // Query the database directly to verify max_attempts was set correctly (retries + 1)
    let (max_attempts,): (i32,) =
        sqlx::query_as("SELECT max_attempts FROM pgmq_queue WHERE id = $1")
            .bind(job_id)
            .fetch_one(&db)
            .await
            .expect("Failed to query max_attempts from database");
    assert_eq!(
        max_attempts,
        (custom_max_retries + 1) as i32,
        "max_attempts should be retries + 1"
    );

    // Also verify the job can be popped and has the correct data
    let job: Option<JobGuard<TestMsg>> = queue
        .pop()
        .await
        .expect("Failed to pull message from queue");

    assert!(job.is_some());
    let job = job.expect("Failed to get job from queue");
    assert_eq!(job.id(), &job_id);
    assert_eq!(job.desc().data, msg.data);
}

#[tokio::test]
async fn default_max_attempts_value() {
    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db.clone());

    let msg = Faker.fake::<TestMsg>();

    //* When
    // Push a job without setting max_attempts (should use default)
    let job_id = queue
        .push(msg.clone())
        .await
        .expect("Failed to push message to queue");

    //* Then
    // Query the database directly to verify max_attempts uses the default value (3)
    let (max_attempts,): (i32,) =
        sqlx::query_as("SELECT max_attempts FROM pgmq_queue WHERE id = $1")
            .bind(job_id)
            .fetch_one(&db)
            .await
            .expect("Failed to query max_attempts from database");
    assert_eq!(max_attempts, 3, "Default max_attempts should be 3"); // DEFAULT_MAX_ATTEMPTS is 3
}

#[tokio::test]
#[expect(
    clippy::cast_possible_wrap,
    reason = "predates this lint; fix when next touched"
)]
async fn max_retries_with_scheduled_job() {
    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db.clone());

    let msg = Faker.fake::<TestMsg>();
    let custom_max_retries = 6u32; // 6 retries = 7 total attempts
    let msg_schedule = time::OffsetDateTime::now_utc().saturating_add(time::Duration::minutes(1));

    //* When
    // Push a scheduled job with custom max_retries
    let job_id = queue
        .push(
            JobBuilder::new(msg.clone())
                .max_retries(custom_max_retries)
                .schedule_at(msg_schedule),
        )
        .await
        .expect("Failed to push scheduled message to queue");

    //* Then
    // Query the database directly to verify max_attempts was set correctly for scheduled job (retries + 1)
    let row: (i32,) = sqlx::query_as("SELECT max_attempts FROM pgmq_queue WHERE id = $1")
        .bind(job_id)
        .fetch_one(&db)
        .await
        .expect("Failed to query max_attempts from database");

    let actual_max_attempts = row.0;
    assert_eq!(actual_max_attempts, (custom_max_retries + 1) as i32);
}

#[tokio::test]
#[expect(
    clippy::cast_possible_wrap,
    reason = "predates this lint; fix when next touched"
)]
async fn pop_marks_only_one_row_running_with_multiple_queued() {
    // Regression: the previous pop used `WHERE id IN (subquery)` with FOR UPDATE
    // SKIP LOCKED inside, which Postgres re-ran per outer row and marked every
    // queued row Running in one UPDATE. 15 rows defeats small-N planner masking.

    //* Given
    const QUEUED_ROWS: usize = 15;
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db.clone());

    // Push jobs sequentially so each gets an earlier created_at.
    let mut job_ids = Vec::new();
    for _ in 0..QUEUED_ROWS {
        let msg = Faker.fake::<TestMsg>();
        let id = queue
            .push(msg)
            .await
            .expect("Failed to push message to queue");
        job_ids.push(id);
    }

    //* When
    // Pop once and remove (commits the pop tx). With the bug pop marked every
    // row Running, orphaning all but the removed one; the fix marks only one.
    let popped = queue
        .pop::<TestMsg>()
        .await
        .expect("Failed to pull message from queue")
        .expect("expected pop to return a job");
    popped.remove().await.expect("Failed to remove job");

    //* Then
    let (running_count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM pgmq_queue WHERE status = -1")
            .fetch_one(&db)
            .await
            .expect("Failed to count Running rows");
    assert_eq!(
        running_count, 0,
        "after one pop+remove, no rows should be Running (got {running_count}); non-zero means pop marked extra rows that are now orphaned"
    );

    let (queued_count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM pgmq_queue WHERE status = 0")
            .fetch_one(&db)
            .await
            .expect("Failed to count Queued rows");
    let expected_queued = (QUEUED_ROWS - 1) as i64;
    assert_eq!(
        queued_count, expected_queued,
        "other queued rows must remain Queued after one pop; got {queued_count}, expected {expected_queued}"
    );
}

#[tokio::test]
async fn deferred_job_not_starved_by_fresh_jobs() {
    // Regression: pop() orders by created_at, not scheduled_for. A deferred job
    // is re-queued at now()+delay; ordering by scheduled_for would sort it behind
    // any freshly pushed job (scheduled_for = now()) and starve it under load.

    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db);

    let old_msg = Faker.fake::<TestMsg>();
    let new_msg = Faker.fake::<TestMsg>();

    //* When
    // Push the older job and defer it ~300ms into the future (the deferral path).
    let old_id = queue
        .push(old_msg.clone())
        .await
        .expect("Failed to push older message");
    let old_job = queue
        .pop::<TestMsg>()
        .await
        .expect("Failed to pop older job")
        .expect("expected the older job");
    let defer_until =
        time::OffsetDateTime::now_utc().saturating_add(time::Duration::milliseconds(300));
    old_job
        .reschedule(defer_until)
        .await
        .expect("Failed to defer older job");

    // Push a fresh job afterwards: its scheduled_for is earlier than the deferred
    // job's, but its created_at is later.
    queue
        .push(new_msg)
        .await
        .expect("Failed to push fresh message");

    // Wait until the deferred job is eligible again.
    tokio::time::sleep(Duration::from_millis(600)).await;

    //* Then
    // The deferred (older) job is popped first despite its later scheduled_for.
    let popped = queue
        .pop::<TestMsg>()
        .await
        .expect("Failed to pop")
        .expect("expected a job to be available");
    assert_eq!(
        popped.id(),
        &old_id,
        "the deferred job must keep its place ahead of fresher jobs"
    );
    assert_eq!(popped.desc().data, old_msg.data);
}

/// Queued, waiting-to-retry and running jobs are unfinished; a removed job and 1 that failed for
/// good are not.
#[tokio::test]
async fn lists_only_unfinished_jobs() {
    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db.clone());
    let msg = |data: &str| TestMsg {
        data: data.to_string(),
    };
    for (data, retries) in [("done", 2), ("gave up", 0), ("retrying", 2), ("running", 2)] {
        queue
            .push(JobBuilder::new(msg(data)).max_retries(retries))
            .await
            .expect("Failed to push message");
    }
    queue
        .push(msg("queued"))
        .await
        .expect("Failed to push message");
    let pop = || async {
        queue
            .pop::<TestMsg>()
            .await
            .expect("Failed to pop job")
            .expect("expected a job")
    };
    pop().await.remove().await.expect("Failed to remove job");
    pop()
        .await
        .mark_as_failed()
        .await
        .expect("Failed to fail job");
    let later = time::OffsetDateTime::now_utc() + time::Duration::hours(1);
    pop()
        .await
        .mark_as_failed_and_reschedule(later)
        .await
        .expect("Failed to fail job");
    let running = pop().await;

    //* When
    let mut unfinished: Vec<String> = queue
        .unfinished_descriptors()
        .await
        .expect("Failed to list unfinished jobs")
        .iter()
        .map(|d| {
            serde_json::from_str::<TestMsg>(d)
                .expect("a stored message")
                .data
        })
        .collect();
    unfinished.sort();
    drop(running);

    //* Then
    assert_eq!(unfinished, ["queued", "retrying", "running"]);
}

#[tokio::test]
async fn failed_job_gives_up_at_max_attempts() {
    // Regression: the give-up arm of requeue() (count_attempt=true,
    // attempt_count+1 >= max_attempts) must leave the job Failed with its
    // schedule untouched, so it is never popped again.

    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db.clone());

    let msg = Faker.fake::<TestMsg>();
    // max_retries(0) => max_attempts = 1, so the first failure exhausts it.
    let job_id = queue
        .push(JobBuilder::new(msg.clone()).max_retries(0))
        .await
        .expect("Failed to push message");

    // Capture the schedule the give-up arm must leave untouched.
    let (scheduled_before,): (time::OffsetDateTime,) =
        sqlx::query_as("SELECT scheduled_for FROM pgmq_queue WHERE id = $1")
            .bind(job_id)
            .fetch_one(&db)
            .await
            .expect("Failed to read scheduled_for");

    //* When
    let job = queue
        .pop::<TestMsg>()
        .await
        .expect("Failed to pop job")
        .expect("expected a job");
    let outcome = job
        .mark_as_failed()
        .await
        .expect("Failed to mark job as failed");

    //* Then
    assert_eq!(
        outcome,
        RetryOutcome::GaveUp,
        "the caller must be told the last attempt was the last"
    );
    let (status, attempt_count, scheduled_after): (i32, i32, time::OffsetDateTime) =
        sqlx::query_as("SELECT status, attempt_count, scheduled_for FROM pgmq_queue WHERE id = $1")
            .bind(job_id)
            .fetch_one(&db)
            .await
            .expect("Failed to read job row");
    assert_eq!(status, 1, "exhausted job should be Failed (status = 1)");
    assert_eq!(attempt_count, 1, "the single attempt should be recorded");
    assert_eq!(
        scheduled_after, scheduled_before,
        "give-up must not move scheduled_for"
    );

    // A Failed job is never handed out again.
    let next = queue.pop::<TestMsg>().await.expect("Failed to pop");
    assert!(next.is_none(), "a failed job must not be popped");
}

#[tokio::test]
async fn failed_job_below_ceiling_retries_with_incremented_attempt() {
    // Regression: the retry arm of requeue() (count_attempt=true,
    // attempt_count+1 < max_attempts) must keep the job Queued, record the
    // attempt, and move scheduled_for forward so the backoff delay applies.

    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db.clone());

    let msg = Faker.fake::<TestMsg>();
    // max_retries(2) => max_attempts = 3, so the first failure stays below it.
    let job_id = queue
        .push(JobBuilder::new(msg.clone()).max_retries(2))
        .await
        .expect("Failed to push message");

    let (scheduled_before,): (time::OffsetDateTime,) =
        sqlx::query_as("SELECT scheduled_for FROM pgmq_queue WHERE id = $1")
            .bind(job_id)
            .fetch_one(&db)
            .await
            .expect("Failed to read scheduled_for");

    //* When
    let job = queue
        .pop::<TestMsg>()
        .await
        .expect("Failed to pop job")
        .expect("expected a job");
    let reschedule_to = time::OffsetDateTime::now_utc() + time::Duration::hours(1);
    let outcome = job
        .mark_as_failed_and_reschedule(reschedule_to)
        .await
        .expect("Failed to reschedule failed job");

    //* Then
    assert_eq!(
        outcome,
        RetryOutcome::WillRetry,
        "the caller must be told the job runs again"
    );
    let (status, attempt_count, scheduled_after): (i32, i32, time::OffsetDateTime) =
        sqlx::query_as("SELECT status, attempt_count, scheduled_for FROM pgmq_queue WHERE id = $1")
            .bind(job_id)
            .fetch_one(&db)
            .await
            .expect("Failed to read job row");
    assert_eq!(
        status, 0,
        "a below-ceiling failure stays Queued (status = 0)"
    );
    assert_eq!(attempt_count, 1, "the failed attempt must be recorded");
    assert!(
        scheduled_after > scheduled_before,
        "retry must move scheduled_for forward for the backoff delay"
    );
}

#[tokio::test]
async fn deferred_job_does_not_count_an_attempt() {
    // Regression: reschedule() (the deferral path) leaves attempt_count
    // untouched, unlike mark_as_failed(), so a contended job never advances
    // toward its max-attempt ceiling.

    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db.clone());

    let msg = Faker.fake::<TestMsg>();

    //* When
    let job_id = queue
        .push(msg.clone())
        .await
        .expect("Failed to push message");
    let job = queue
        .pop::<TestMsg>()
        .await
        .expect("Failed to pop job")
        .expect("expected a job");
    // Defer it: re-queue immediately, no attempt recorded.
    job.reschedule(time::OffsetDateTime::now_utc())
        .await
        .expect("Failed to reschedule job");

    //* Then
    let (attempt_count,): (i32,) =
        sqlx::query_as("SELECT attempt_count FROM pgmq_queue WHERE id = $1")
            .bind(job_id)
            .fetch_one(&db)
            .await
            .expect("Failed to query attempt_count");
    assert_eq!(
        attempt_count, 0,
        "deferral must not record a failed attempt"
    );

    // The job is queued again and poppable with zero failed attempts.
    let job = queue
        .pop::<TestMsg>()
        .await
        .expect("Failed to pop deferred job")
        .expect("expected the deferred job");
    assert_eq!(job.id(), &job_id);
    assert_eq!(job.failed_attempts(), 0);
}

/// A Background job, such as an offer a sweep's reassessment created, pops before Sweep jobs
/// queued earlier, so a long sweep can't hold it up.
#[tokio::test]
async fn background_pops_before_earlier_sweep_jobs() {
    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db);
    for _ in 0..2 {
        queue
            .push(JobBuilder::new(Faker.fake::<TestMsg>()).priority(JobPriority::Sweep))
            .await
            .expect("Failed to push sweep job");
    }
    let background_id = queue
        .push(JobBuilder::new(Faker.fake::<TestMsg>()).priority(JobPriority::Background))
        .await
        .expect("Failed to push background job");

    //* When
    let first = queue
        .pop::<TestMsg>()
        .await
        .expect("Failed to pop")
        .expect("expected a job");

    //* Then
    assert_eq!(
        first.id(),
        &background_id,
        "background must pop before earlier sweep jobs"
    );
}

#[tokio::test]
async fn interactive_pops_before_earlier_background() {
    // pop() orders by priority DESC first: an Interactive job pushed after two
    // Background jobs still pops first. Within a priority class, order stays FIFO
    // by created_at (the monotonic v7 id breaks same-timestamp ties).

    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db);

    let bg_first = Faker.fake::<TestMsg>();
    let bg_second = Faker.fake::<TestMsg>();
    let interactive = Faker.fake::<TestMsg>();

    //* When
    // Two Background jobs first, then an Interactive job last.
    let bg_first_id = queue
        .push(JobBuilder::new(bg_first).priority(JobPriority::Background))
        .await
        .expect("Failed to push first background job");
    let bg_second_id = queue
        .push(JobBuilder::new(bg_second).priority(JobPriority::Background))
        .await
        .expect("Failed to push second background job");
    let interactive_id = queue
        .push(JobBuilder::new(interactive).priority(JobPriority::Interactive))
        .await
        .expect("Failed to push interactive job");

    //* Then
    // Interactive wins despite being inserted last.
    let first = queue
        .pop::<TestMsg>()
        .await
        .expect("Failed to pop")
        .expect("expected a job");
    assert_eq!(
        first.id(),
        &interactive_id,
        "the interactive job must pop before earlier background jobs"
    );
    first.remove().await.expect("Failed to remove interactive");

    // Then the two Background jobs come back in insertion order (FIFO).
    let second = queue
        .pop::<TestMsg>()
        .await
        .expect("Failed to pop")
        .expect("expected a job");
    assert_eq!(
        second.id(),
        &bg_first_id,
        "within a class, the earlier background job pops first"
    );
    second.remove().await.expect("Failed to remove bg_first");

    let third = queue
        .pop::<TestMsg>()
        .await
        .expect("Failed to pop")
        .expect("expected a job");
    assert_eq!(
        third.id(),
        &bg_second_id,
        "within a class, the later background job pops last"
    );
}

#[tokio::test]
async fn reschedule_preserves_priority() {
    // reschedule() re-queues a job without touching its priority column (it is
    // preserved by omission from the UPDATE), so a deferred Interactive job keeps
    // outranking Background work after being put back.

    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db.clone());

    let msg = Faker.fake::<TestMsg>();

    //* When
    let job_id = queue
        .push(JobBuilder::new(msg).priority(JobPriority::Interactive))
        .await
        .expect("Failed to push interactive job");
    let job = queue
        .pop::<TestMsg>()
        .await
        .expect("Failed to pop")
        .expect("expected a job");
    // Defer it back into the queue, eligible immediately.
    job.reschedule(time::OffsetDateTime::now_utc())
        .await
        .expect("Failed to reschedule");

    //* Then
    let (priority,): (i16,) = sqlx::query_as("SELECT priority FROM pgmq_queue WHERE id = $1")
        .bind(job_id)
        .fetch_one(&db)
        .await
        .expect("Failed to read priority");
    assert_eq!(
        priority, 1,
        "reschedule must preserve the Interactive priority (1)"
    );
}

#[tokio::test]
async fn future_interactive_does_not_preempt_eligible_background() {
    // Priority ordering applies only among eligible rows: the scheduled_for gate
    // runs first. An Interactive job scheduled in the future must not preempt a
    // Background job that is already eligible.

    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db);

    let background = Faker.fake::<TestMsg>();
    let interactive = Faker.fake::<TestMsg>();
    let future = time::OffsetDateTime::now_utc().saturating_add(time::Duration::minutes(1));

    //* When
    // An eligible Background job, plus a higher-priority Interactive job gated
    // one minute into the future.
    let background_id = queue
        .push(JobBuilder::new(background).priority(JobPriority::Background))
        .await
        .expect("Failed to push background job");
    queue
        .push(
            JobBuilder::new(interactive)
                .priority(JobPriority::Interactive)
                .schedule_at(future),
        )
        .await
        .expect("Failed to push future interactive job");

    //* Then
    // Only the eligible Background job is returned; the future Interactive one
    // is not yet visible to pop().
    let first = queue
        .pop::<TestMsg>()
        .await
        .expect("Failed to pop")
        .expect("expected a job");
    assert_eq!(
        first.id(),
        &background_id,
        "a future interactive job must not preempt an eligible background job"
    );
    first.remove().await.expect("Failed to remove background");

    let second: Option<JobGuard<TestMsg>> = queue.pop().await.expect("Failed to pop");
    assert!(
        second.is_none(),
        "the future interactive job must stay gated until its scheduled_for"
    );
}

/// A stored job this release can't read, such as 1 of a type a newer release added, allowed
/// `max_retries` retries, and a readable job queued after it.
async fn unreadable_job_ahead_of_a_good_one(
    queue: &PgQueue,
    max_retries: u32,
) -> anyhow::Result<(dipper_pgmq::JobId, serde_json::Value, dipper_pgmq::JobId)> {
    let unreadable = serde_json::json!({"type": "JobTypeOfANewerRelease", "request": 7});
    let unreadable_id = queue
        .push(JobBuilder::new(unreadable.clone()).max_retries(max_retries))
        .await?;
    let good_id = queue.push(Faker.fake::<TestMsg>()).await?;
    Ok((unreadable_id, unreadable, good_id))
}

/// A job's status, failed attempts, schedule and stored form.
async fn stored_job(
    db: &Pool<Postgres>,
    id: dipper_pgmq::JobId,
) -> anyhow::Result<(i32, i32, time::OffsetDateTime, serde_json::Value)> {
    let (status, attempt_count, scheduled_for, descriptor): (
        i32,
        i32,
        time::OffsetDateTime,
        sqlx::types::Json<serde_json::Value>,
    ) = sqlx::query_as(
        "SELECT status, attempt_count, scheduled_for, descriptor FROM pgmq_queue WHERE id = $1",
    )
    .bind(id)
    .fetch_one(db)
    .await?;
    Ok((status, attempt_count, scheduled_for, descriptor.0))
}

/// Make a postponed job due now, as if its delay had passed.
async fn make_due(db: &Pool<Postgres>, id: dipper_pgmq::JobId) -> anyhow::Result<()> {
    sqlx::query("UPDATE pgmq_queue SET scheduled_for = timezone('UTC', now()) WHERE id = $1")
        .bind(id)
        .execute(db)
        .await?;
    Ok(())
}

/// A job this release can't read may be 1 a newer release, running alongside it during a
/// rollout, can: it counts as a failed attempt and is put off for a while, which also takes it
/// off the head of the queue, so the job behind it is delivered at once.
#[tokio::test]
async fn an_unreadable_job_is_put_off_and_the_next_job_delivered() {
    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db.clone());
    let (unreadable_id, unreadable, good_id) = unreadable_job_ahead_of_a_good_one(&queue, 2)
        .await
        .expect("Failed to push the jobs");
    let before = time::OffsetDateTime::now_utc();

    //* When
    let first = queue
        .take_next::<TestMsg>()
        .await
        .expect("an unreadable job is not a database error")
        .expect("expected the unreadable job");
    let second = queue
        .take_next::<TestMsg>()
        .await
        .expect("Failed to take")
        .expect("expected the good job");

    //* Then
    let Popped::Unreadable(first) = first else {
        panic!("the unreadable job must be reported as unreadable");
    };
    assert_eq!(first.id, unreadable_id);
    assert_eq!(first.outcome, RetryOutcome::WillRetry);
    assert_eq!((first.failed_attempts, first.max_attempts), (0, 3));
    assert!(first.error.to_string().contains("data"), "{}", first.error);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&first.stored).expect("stored JSON"),
        unreadable
    );
    let Popped::Job(second) = second else {
        panic!("the good job must be delivered");
    };
    assert_eq!(second.id(), &good_id);
    second
        .remove()
        .await
        .expect("Failed to remove the good job");

    let (status, attempt_count, scheduled_for, stored) = stored_job(&db, unreadable_id)
        .await
        .expect("Failed to read the job row");
    assert_eq!(status, 0, "a postponed job stays queued (status = 0)");
    assert_eq!(attempt_count, 1, "the failed attempt is recorded");
    assert!(
        scheduled_for >= before + time::Duration::minutes(4),
        "put off long enough for a rollout to finish, until {scheduled_for}"
    );
    assert_eq!(stored, unreadable, "its stored JSON is untouched");
    let next = queue.take_next::<TestMsg>().await.expect("Failed to take");
    assert!(next.is_none(), "the postponed job isn't due yet");
}

/// Once its last attempt fails too, the unreadable job is failed for good, with its stored JSON
/// kept for an operator to look at, and not handed out again.
#[tokio::test]
async fn an_unreadable_job_is_failed_for_good_after_its_last_attempt() {
    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db.clone());
    let (unreadable_id, unreadable, good_id) = unreadable_job_ahead_of_a_good_one(&queue, 1)
        .await
        .expect("Failed to push the jobs");

    //* When
    let first = queue
        .take_next::<TestMsg>()
        .await
        .expect("Failed to take")
        .expect("expected the unreadable job");
    let Popped::Unreadable(first) = first else {
        panic!("the unreadable job must be reported as unreadable");
    };
    assert_eq!(first.outcome, RetryOutcome::WillRetry);
    let Popped::Job(good) = queue
        .take_next::<TestMsg>()
        .await
        .expect("Failed to take")
        .expect("expected the good job")
    else {
        panic!("the good job must be delivered");
    };
    assert_eq!(good.id(), &good_id);
    good.remove().await.expect("Failed to remove the good job");
    make_due(&db, unreadable_id)
        .await
        .expect("Failed to make the job due");
    let last = queue
        .take_next::<TestMsg>()
        .await
        .expect("Failed to take")
        .expect("expected the unreadable job again");

    //* Then
    let Popped::Unreadable(last) = last else {
        panic!("the unreadable job must be reported as unreadable");
    };
    assert_eq!(last.outcome, RetryOutcome::GaveUp);
    assert_eq!((last.failed_attempts, last.max_attempts), (1, 2));
    let (status, attempt_count, _, stored) = stored_job(&db, unreadable_id)
        .await
        .expect("Failed to read the job row");
    assert_eq!(
        (status, attempt_count),
        (1, 2),
        "failed (status = 1) after both attempts"
    );
    assert_eq!(stored, unreadable, "its stored JSON is untouched");
    let next = queue.take_next::<TestMsg>().await.expect("Failed to take");
    assert!(
        next.is_none(),
        "a job failed for good is not handed out again"
    );
}

/// An unreadable job whose attempt count can't be trusted, here already at its limit though it
/// is queued, can't be counted towards giving up, so it is failed for good at once rather than
/// left to stall the queue.
#[tokio::test]
async fn an_unreadable_job_with_an_untrustworthy_attempt_count_is_failed_at_once() {
    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db.clone());
    let (unreadable_id, unreadable, _) = unreadable_job_ahead_of_a_good_one(&queue, 2)
        .await
        .expect("Failed to push the jobs");
    sqlx::query("UPDATE pgmq_queue SET attempt_count = 2147483647 WHERE id = $1")
        .bind(unreadable_id)
        .execute(&db)
        .await
        .expect("Failed to corrupt the attempt count");

    //* When
    let first = queue
        .take_next::<TestMsg>()
        .await
        .expect("an unreadable job is not a database error")
        .expect("expected the unreadable job");

    //* Then
    let Popped::Unreadable(first) = first else {
        panic!("the unreadable job must be reported as unreadable");
    };
    assert_eq!(first.outcome, RetryOutcome::GaveUp);
    let (status, _, _, stored) = stored_job(&db, unreadable_id)
        .await
        .expect("Failed to read the job row");
    assert_eq!(status, 1, "failed for good (status = 1)");
    assert_eq!(stored, unreadable, "its stored JSON is untouched");
}

/// `pop` gets past an unreadable job the same way, delivering the job behind it.
#[tokio::test]
async fn pop_delivers_the_job_behind_an_unreadable_one() {
    //* Given
    let (db, _temp_db) = temp_pgmq_db().await;
    let queue = PgQueue::new(db.clone());
    let (unreadable_id, unreadable, good_id) = unreadable_job_ahead_of_a_good_one(&queue, 2)
        .await
        .expect("Failed to push the jobs");

    //* When
    let job: JobGuard<TestMsg> = queue
        .pop()
        .await
        .expect("an unreadable job is not a database error")
        .expect("expected the good job");

    //* Then
    assert_eq!(job.id(), &good_id);
    job.remove().await.expect("Failed to remove the good job");
    let (status, attempt_count, _, stored) = stored_job(&db, unreadable_id)
        .await
        .expect("Failed to read the job row");
    assert_eq!((status, attempt_count), (0, 1), "put off, not failed");
    assert_eq!(stored, unreadable);
}
