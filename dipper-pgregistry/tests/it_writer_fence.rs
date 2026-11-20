//! The writer fence against a real Postgres: who may write, how a process takes over, and what a
//! takeover does to the process it replaces.

use std::time::Duration;

use dipper_pgregistry::writer_fence;
use pgtemp::PgTempDB;
use sqlx::{
    ConnectOptions as _, Connection as _, PgConnection, PgPool, postgres::PgConnectOptions,
};
use uuid::Uuid;

/// A database with dipper's migrations, every table fenced, as dipper leaves it at start, and no
/// writer yet. Returns the options sessions are opened from.
#[expect(
    clippy::expect_used,
    reason = "test setup; a failure should stop the test"
)]
async fn fenced_db() -> (PgTempDB, PgConnectOptions) {
    let temp_db = PgTempDB::new();
    let base: PgConnectOptions = temp_db
        .connection_uri()
        .parse()
        .expect("pgtemp's URI is a Postgres URL");
    let mut operator = operator(&base).await;
    dipper_pgregistry::run_db_migrations(&mut operator)
        .await
        .expect("run the migrations");
    for table in writer_fence::unfenced(&mut operator)
        .await
        .expect("list the tables")
    {
        assert!(
            writer_fence::attach(&mut operator, &table, Duration::from_secs(5))
                .await
                .expect("fence the table"),
            "{table} is free to fence"
        );
    }
    (temp_db, base)
}

#[expect(
    clippy::expect_used,
    reason = "test setup; a failure should stop the test"
)]
async fn operator(base: &PgConnectOptions) -> PgConnection {
    writer_fence::operator_options(base.clone())
        .connect()
        .await
        .expect("open an operator session")
}

/// A session of the process with `token`.
#[expect(
    clippy::expect_used,
    reason = "test setup; a failure should stop the test"
)]
async fn session_of(base: &PgConnectOptions, token: Uuid) -> PgConnection {
    writer_fence::writer_options(base.clone(), token)
        .connect()
        .await
        .expect("open a session")
}

/// Makes the process with `token` the writer, as its start does, on an operator's session, so
/// it leaves no session of that process behind.
#[expect(
    clippy::expect_used,
    reason = "test setup; a failure should stop the test"
)]
async fn take_over(base: &PgConnectOptions, token: Uuid) {
    let mut conn = operator(base).await;
    assert!(
        writer_fence::take(&mut conn, token, "test", Duration::ZERO)
            .await
            .expect("take over"),
        "the takeover goes through"
    );
}

/// A write to 1 of dipper's tables, which the fence checks.
async fn write(conn: &mut PgConnection) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO dipper_kafka_consumer_offsets (topic, partition_id, next_offset) \
         VALUES (gen_random_uuid()::text, 0, 1)",
    )
    .execute(conn)
    .await
    .map(drop)
}

fn refused(result: sqlx::Result<()>) -> bool {
    result.is_err_and(|err| writer_fence::is_refusal(&err))
}

#[tokio::test]
async fn every_table_of_dipper_is_fenced_and_stays_fenced() {
    //* Given
    let (_temp_db, base) = fenced_db().await;
    let mut operator = operator(&base).await;

    //* Then - nothing is left unfenced, including the queue's table if it is there
    assert_eq!(
        writer_fence::unfenced(&mut operator).await.unwrap(),
        Vec::<String>::new()
    );

    //* When - someone disables 1 table's fence
    sqlx::query("ALTER TABLE dipper_kafka_consumer_offsets DISABLE TRIGGER dipper_fence")
        .execute(&mut operator)
        .await
        .unwrap();

    //* Then - the next start finds it and fences it again
    assert_eq!(
        writer_fence::unfenced(&mut operator).await.unwrap(),
        vec!["dipper_kafka_consumer_offsets".to_owned()]
    );
    assert!(
        writer_fence::attach(
            &mut operator,
            "dipper_kafka_consumer_offsets",
            Duration::from_secs(5)
        )
        .await
        .unwrap()
    );
    assert!(
        writer_fence::unfenced(&mut operator)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn only_the_writer_and_operators_may_write() {
    //* Given
    let (_temp_db, base) = fenced_db().await;
    let (writer, replaced) = (Uuid::now_v7(), Uuid::now_v7());
    take_over(&base, replaced).await;
    take_over(&base, writer).await;

    //* Then
    assert!(write(&mut session_of(&base, writer).await).await.is_ok());
    assert!(write(&mut operator(&base).await).await.is_ok());
    let mut stale = session_of(&base, replaced).await;
    assert!(refused(write(&mut stale).await), "a replaced writer");
    let mut unnamed = base.clone().connect().await.unwrap();
    assert!(
        refused(write(&mut unnamed).await),
        "a session with no token"
    );
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM dipper_kafka_consumer_offsets")
        .fetch_one(&mut stale)
        .await
        .expect("a replaced writer may still read");
    assert_eq!(rows, 2);
}

#[tokio::test]
async fn the_fence_switched_off_lets_every_session_write_until_it_is_on_again() {
    //* Given
    let (_temp_db, base) = fenced_db().await;
    let mut operator = operator(&base).await;
    let mut unnamed = base.clone().connect().await.unwrap();

    //* When
    sqlx::query("SELECT dipper_fence_set(false)")
        .execute(&mut operator)
        .await
        .unwrap();

    //* Then
    assert!(!writer_fence::enforced(&mut operator).await.unwrap());
    assert!(write(&mut unnamed).await.is_ok());

    //* When
    sqlx::query("SELECT dipper_fence_set(true)")
        .execute(&mut operator)
        .await
        .unwrap();

    //* Then
    assert!(refused(write(&mut unnamed).await));
}

#[tokio::test]
async fn a_process_takes_over_only_from_a_writer_that_released_or_went_silent() {
    //* Given - the 1st process takes over a new database at once
    let (_temp_db, base) = fenced_db().await;
    let (a, b) = (Uuid::now_v7(), Uuid::now_v7());
    let mut conn = operator(&base).await;
    let stale_after = Duration::from_secs(20);
    assert!(
        writer_fence::take(&mut conn, a, "a", stale_after)
            .await
            .unwrap()
    );

    //* Then - a 2nd waits while the 1st is healthy
    assert!(writer_fence::beat(&mut conn, a).await.unwrap());
    assert!(
        !writer_fence::take(&mut conn, b, "b", stale_after)
            .await
            .unwrap()
    );

    //* When - the 1st hands over
    assert!(writer_fence::release(&mut conn, a).await.unwrap());

    //* Then - the 2nd takes over, and asking again answers the same
    assert!(
        writer_fence::take(&mut conn, b, "b", stale_after)
            .await
            .unwrap()
    );
    assert!(
        writer_fence::take(&mut conn, b, "b", stale_after)
            .await
            .unwrap()
    );
    assert!(
        !writer_fence::beat(&mut conn, a).await.unwrap(),
        "the 1st is told"
    );
    assert!(
        !writer_fence::release(&mut conn, a).await.unwrap(),
        "the 1st's late release changes nothing"
    );

    //* When - the 2nd goes silent, for longer than a 3rd allows
    let c = Uuid::now_v7();
    assert!(
        writer_fence::take(&mut conn, c, "c", Duration::ZERO)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn a_takeover_refuses_the_old_writer_at_once_and_drain_waits_for_its_open_write() {
    //* Given - the old writer has a write in an open transaction
    let (_temp_db, base) = fenced_db().await;
    let (old, new) = (Uuid::now_v7(), Uuid::now_v7());
    take_over(&base, old).await;
    let mut old_session = session_of(&base, old).await;
    sqlx::query("BEGIN")
        .execute(&mut old_session)
        .await
        .unwrap();
    write(&mut old_session).await.expect("still the writer");

    //* When
    let mut new_session = session_of(&base, new).await;
    assert!(
        writer_fence::take(&mut new_session, new, "new", Duration::ZERO)
            .await
            .unwrap(),
        "the takeover doesn't wait for the old writer"
    );

    //* Then - the drain waits for the old writer's open transaction, which holds what passed
    // the check
    assert!(
        !writer_fence::drain(&mut new_session, Duration::from_millis(300))
            .await
            .unwrap()
    );
    assert_eq!(
        writer_fence::drain_blockers(&mut new_session)
            .await
            .unwrap(),
        vec![writer_fence::session_name(old)]
    );

    //* When - the old writer makes its next statement
    let next = write(&mut old_session).await;

    //* Then - it is refused, which aborts the old writer's transaction there and then: its
    // earlier write is rolled back, and the drain no longer waits
    assert!(refused(next));
    assert!(
        writer_fence::drain(&mut new_session, Duration::from_secs(5))
            .await
            .unwrap()
    );
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM dipper_kafka_consumer_offsets")
        .fetch_one(&mut new_session)
        .await
        .unwrap();
    assert_eq!(rows, 0, "nothing of the old writer's transaction landed");
}

#[tokio::test]
async fn a_cut_ends_only_sessions_of_processes_that_are_not_the_writer() {
    //* Given
    let (_temp_db, base) = fenced_db().await;
    let (old, new) = (Uuid::now_v7(), Uuid::now_v7());
    take_over(&base, old).await;
    let mut old_session = session_of(&base, old).await;
    let mut old_lock_session =
        writer_fence::named(writer_fence::writer_options(base.clone(), old), "lock")
            .connect()
            .await
            .unwrap();
    let mut operator_session = operator(&base).await;
    let mut new_session = session_of(&base, new).await;
    let mut another_new_session = session_of(&base, new).await;

    //* When
    take_over(&base, new).await;
    let ended = writer_fence::cut(&mut new_session, new).await.unwrap();

    //* Then - both of the old writer's sessions are ended, and nobody else's
    assert_eq!(ended, 2);
    assert!(old_session.ping().await.is_err());
    assert!(old_lock_session.ping().await.is_err());
    assert!(operator_session.ping().await.is_ok());
    assert!(another_new_session.ping().await.is_ok());

    //* Then - the replaced process may not cut either
    let mut old_again = session_of(&base, old).await;
    assert_eq!(writer_fence::cut(&mut old_again, old).await.unwrap(), 0);
    assert!(new_session.ping().await.is_ok());
}

#[tokio::test]
async fn a_broadcast_guard_holds_off_a_takeover_and_then_learns_of_it() {
    //* Given - the old writer is broadcasting
    let (_temp_db, base) = fenced_db().await;
    let (old, new) = (Uuid::now_v7(), Uuid::now_v7());
    take_over(&base, old).await;
    let pool = PgPool::connect_with(writer_fence::writer_options(base.clone(), old))
        .await
        .unwrap();
    let mut guard = writer_fence::BroadcastGuard::begin(&pool, old)
        .await
        .unwrap();
    assert!(guard.still_writer().await.unwrap());

    //* When
    let mut new_session = session_of(&base, new).await;
    assert!(
        writer_fence::take(&mut new_session, new, "new", Duration::ZERO)
            .await
            .unwrap()
    );

    //* Then - the takeover waits for the broadcast, whose next attempt is told to stop
    assert!(
        !writer_fence::drain(&mut new_session, Duration::from_millis(300))
            .await
            .unwrap()
    );
    assert!(!guard.still_writer().await.unwrap());

    //* When - the broadcast ends, and its connection goes back to the pool
    guard.end().await.unwrap();

    //* Then
    assert!(
        writer_fence::drain(&mut new_session, Duration::from_secs(5))
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn sessions_carry_their_token_and_name_for_the_whole_session() {
    //* Given
    let (_temp_db, base) = fenced_db().await;
    let token = Uuid::now_v7();
    let mut conn = writer_fence::named(writer_fence::writer_options(base, token), "heartbeat")
        .connect()
        .await
        .unwrap();

    //* When - something resets the session's settings
    sqlx::query("DISCARD ALL").execute(&mut conn).await.unwrap();
    sqlx::query("RESET ALL").execute(&mut conn).await.unwrap();

    //* Then - what was set at connect stays
    let (setting, name): (String, String) = sqlx::query_as(
        "SELECT current_setting('dipper.writer'), current_setting('application_name')",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(setting, token.to_string());
    assert_eq!(name, format!("dipper/{token}/heartbeat"));
}
