#![cfg(feature = "fake")]

use std::collections::HashSet;

use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
use dipper_pgregistry::{
    CancelKind, Error, IndexingAgreementStatus, IndexingAgreementTerms, IndexingRequestStatus,
    NewAgreementParams, PgRegistry, ReconciliationAudit, ReconciliationItem,
};
use fake::{Fake, Faker};
use pgtemp::PgTempDB;
use sqlx::{Executor as _, Pool, Postgres};
use thegraph_core::{
    DeploymentId, IndexerId,
    alloy::primitives::{ChainId, address},
    deployment_id,
    fake_impl::alloy::Alloy as FakeAlloy,
    indexer_id,
};
use url::Url;
use uuid::uuid;

/// Initialize a temporary database for integration testing.
///
/// This function creates a temporary database and runs the migrations.
/// It returns the database connection pool and the temporary database guard.
async fn temp_registry_db() -> (Pool<Postgres>, PgTempDB) {
    let temp_db = PgTempDB::new();
    let db = Pool::connect(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to temporary database");
    dipper_pgregistry::run_db_migrations(&db)
        .await
        .expect("Failed to run DB migrations");
    (db, temp_db)
}

/// Execute SQL fixture statements
async fn run_fixture(db: &Pool<Postgres>, sql: &str) -> Result<(), sqlx::Error> {
    let mut conn = db.acquire().await?;
    conn.execute(sql).await?;
    Ok(())
}

#[tokio::test]
async fn set_indexing_target_candidates_inserts_when_no_open_row_exists() {
    //* Given
    let requested_by = FakeAlloy.fake();
    let deployment_id = deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv");
    let deployment_chain_id = Faker.fake::<ChainId>();

    let (db, _temp_db) = temp_registry_db().await;
    let registry = PgRegistry::new(db);

    //* When
    let outcome = registry
        .set_indexing_target_candidates(requested_by, deployment_id, deployment_chain_id, 3)
        .await
        .expect("Failed to set indexing target candidates");

    //* Then
    assert!(matches!(
        outcome,
        dipper_pgregistry::IndexingRequestSetTargetOutcome::Inserted { .. }
    ));
}

#[tokio::test]
async fn set_indexing_target_candidates_updates_when_count_changes() {
    //* Given
    let requested_by = FakeAlloy.fake();
    let deployment_id = deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv");
    let deployment_chain_id = Faker.fake::<ChainId>();

    let (db, _temp_db) = temp_registry_db().await;
    let registry = PgRegistry::new(db);
    let initial = registry
        .set_indexing_target_candidates(requested_by, deployment_id, deployment_chain_id, 3)
        .await
        .expect("seed insert failed");
    let dipper_pgregistry::IndexingRequestSetTargetOutcome::Inserted { id: initial_id } = initial
    else {
        panic!("seed insert did not produce an Inserted outcome");
    };

    //* When
    let outcome = registry
        .set_indexing_target_candidates(requested_by, deployment_id, deployment_chain_id, 5)
        .await
        .expect("update call failed");

    //* Then
    match outcome {
        dipper_pgregistry::IndexingRequestSetTargetOutcome::Updated {
            id,
            new_num_candidates,
        } => {
            assert_eq!(id, initial_id);
            assert_eq!(new_num_candidates, 5);
        }
        other => panic!("expected Updated, got {other:?}"),
    }
}

#[tokio::test]
async fn set_indexing_target_candidates_noop_when_count_unchanged() {
    //* Given
    let requested_by = FakeAlloy.fake();
    let deployment_id = deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv");
    let deployment_chain_id = Faker.fake::<ChainId>();

    let (db, _temp_db) = temp_registry_db().await;
    let registry = PgRegistry::new(db);
    let initial = registry
        .set_indexing_target_candidates(requested_by, deployment_id, deployment_chain_id, 3)
        .await
        .expect("seed insert failed");
    let dipper_pgregistry::IndexingRequestSetTargetOutcome::Inserted { id: initial_id } = initial
    else {
        panic!("seed insert did not produce an Inserted outcome");
    };

    //* When
    let outcome = registry
        .set_indexing_target_candidates(requested_by, deployment_id, deployment_chain_id, 3)
        .await
        .expect("no-op call failed");

    //* Then
    match outcome {
        dipper_pgregistry::IndexingRequestSetTargetOutcome::NoOp { id } => {
            assert_eq!(id, initial_id)
        }
        other => panic!("expected NoOp, got {other:?}"),
    }
}

#[tokio::test]
async fn set_indexing_target_candidates_cancels_on_zero() {
    //* Given
    let requested_by = FakeAlloy.fake();
    let deployment_id = deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv");
    let deployment_chain_id = Faker.fake::<ChainId>();

    let (db, _temp_db) = temp_registry_db().await;
    let registry = PgRegistry::new(db);
    let initial = registry
        .set_indexing_target_candidates(requested_by, deployment_id, deployment_chain_id, 3)
        .await
        .expect("seed insert failed");
    let dipper_pgregistry::IndexingRequestSetTargetOutcome::Inserted { id: initial_id } = initial
    else {
        panic!("seed insert did not produce an Inserted outcome");
    };

    //* When
    let outcome = registry
        .set_indexing_target_candidates(requested_by, deployment_id, deployment_chain_id, 0)
        .await
        .expect("cancel call failed");

    //* Then
    match outcome {
        dipper_pgregistry::IndexingRequestSetTargetOutcome::Canceled { id } => {
            assert_eq!(id, initial_id);
            let request = registry
                .get_indexing_request_by_id(&initial_id)
                .await
                .expect("get by id failed")
                .expect("request should still exist");
            assert_eq!(
                request.status,
                dipper_pgregistry::IndexingRequestStatus::Canceled
            );
        }
        other => panic!("expected Canceled, got {other:?}"),
    }
}

#[tokio::test]
async fn set_indexing_target_candidates_noop_when_zero_against_empty_key() {
    //* Given
    let requested_by = FakeAlloy.fake();
    let deployment_id = deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv");
    let deployment_chain_id = Faker.fake::<ChainId>();

    let (db, _temp_db) = temp_registry_db().await;
    let registry = PgRegistry::new(db);

    //* When
    let outcome = registry
        .set_indexing_target_candidates(requested_by, deployment_id, deployment_chain_id, 0)
        .await
        .expect("empty-key cancel call failed");

    //* Then
    assert!(matches!(
        outcome,
        dipper_pgregistry::IndexingRequestSetTargetOutcome::NoOpAlreadyEmpty
    ));
}

#[tokio::test]
async fn set_indexing_target_candidates_reinsert_after_cancel() {
    //* Given: open then cancel
    let requested_by = FakeAlloy.fake();
    let deployment_id = deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv");
    let deployment_chain_id = Faker.fake::<ChainId>();

    let (db, _temp_db) = temp_registry_db().await;
    let registry = PgRegistry::new(db);
    let _ = registry
        .set_indexing_target_candidates(requested_by, deployment_id, deployment_chain_id, 3)
        .await
        .expect("seed insert failed");
    let _ = registry
        .set_indexing_target_candidates(requested_by, deployment_id, deployment_chain_id, 0)
        .await
        .expect("cancel call failed");

    //* When: re-register the same key
    let outcome = registry
        .set_indexing_target_candidates(requested_by, deployment_id, deployment_chain_id, 5)
        .await
        .expect("re-register call failed");

    //* Then: produces a fresh Open row
    assert!(matches!(
        outcome,
        dipper_pgregistry::IndexingRequestSetTargetOutcome::Inserted { .. }
    ));
}

#[tokio::test]
async fn get_all_indexing_requests() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(&db, include_str!("fixtures/0001_indexing_requests.sql"))
        .await
        .expect("Failed to run fixture");
    let registry = PgRegistry::new(db);

    //* When
    let indexing_requests = registry.get_all_indexing_requests().await;

    //* Then
    let indexing_requests = indexing_requests.expect("Failed to get all indexing requests");
    assert_eq!(indexing_requests.len(), 3);
}

#[tokio::test]
async fn indexing_request_get_by_id() {
    //* Given
    // Indexing request #1: OPEN
    let indexing_request_id = uuid!("019300ce-4751-780e-b58c-bf696b67eb23").into();

    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(&db, include_str!("fixtures/0001_indexing_requests.sql"))
        .await
        .expect("Failed to run fixture");
    let registry = PgRegistry::new(db);

    //* When
    let indexing_request = registry
        .get_indexing_request_by_id(&indexing_request_id)
        .await
        .expect("Failed to get indexing request by ID");

    //* Then
    let indexing_request = indexing_request.expect("No indexing request with the given ID");

    assert_eq!(indexing_request.id, indexing_request_id);
    assert_eq!(indexing_request.status, IndexingRequestStatus::Open);
    assert_eq!(
        indexing_request.requested_by,
        address!("442a24985444cdc6a4db9503d354918d27b5ea97")
    );
    assert_eq!(
        indexing_request.deployment_id,
        deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv")
    );
}

#[tokio::test]
async fn indexing_request_get_by_id_not_found() {
    //* Given
    // Non-existent indexing request
    let indexing_request_id = uuid!("01930119-9a0e-7ea2-8dad-691515451655").into();

    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(&db, include_str!("fixtures/0001_indexing_requests.sql"))
        .await
        .expect("Failed to run fixture");
    let registry = PgRegistry::new(db);

    //* When
    let indexing_request = registry
        .get_indexing_request_by_id(&indexing_request_id)
        .await
        .expect("Failed to get indexing request by ID");

    //* Then
    assert!(indexing_request.is_none());
}

#[tokio::test]
async fn indexing_request_get_by_id_unknown_status() {
    //* Given
    // Indexing request #3: Random state (should map to UNKNOWN)
    let indexing_request_id = uuid!("01930108-5942-7515-bd5e-2cba9c7027b7").into();

    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(&db, include_str!("fixtures/0001_indexing_requests.sql"))
        .await
        .expect("Failed to run fixture");
    let registry = PgRegistry::new(db);

    //* When
    let indexing_request = registry
        .get_indexing_request_by_id(&indexing_request_id)
        .await
        .expect("Failed to get indexing request by ID");

    //* Then
    let indexing_request = indexing_request.expect("No indexing request with the given ID");

    assert_eq!(indexing_request.id, indexing_request_id);
    assert_eq!(indexing_request.status, IndexingRequestStatus::Unknown);
    assert_eq!(
        indexing_request.requested_by,
        address!("be00782710cdf47168a386dfa79299729f076df6")
    );
    assert_eq!(
        indexing_request.deployment_id,
        deployment_id!("QmZ5EcVesbdDidvgdMtd4h5xugVkEQWBgJ84CEouZrHGEq")
    );
}

#[tokio::test]
async fn register_new_indexing_agreement_no_indexing_request() {
    //* Given
    // Indexing agreement
    let indexing_request_id = IndexingRequestId::new(); // Random ID
    let deployment_id = Faker.fake::<DeploymentId>();
    let indexer_id = Faker.fake::<IndexerId>();
    let indexer_url = Faker.fake::<Url>();

    let agreement_terms = Faker.fake::<IndexingAgreementTerms>();

    let (db, _temp_db) = temp_registry_db().await;
    let registry = PgRegistry::new(db);

    //* When
    let res = registry
        .register_new_indexing_agreement(NewAgreementParams {
            agreement_id: Faker.fake::<IndexingAgreementId>(),
            nonce_uuid: uuid::Uuid::now_v7(),
            request_id: indexing_request_id,
            deployment_id,
            indexer_id,
            indexer_url,
            terms: agreement_terms,
            terms_version_hash: None,
        })
        .await;

    //* Then
    let _error = res.expect_err("Expected error when registering agreement");
}

#[tokio::test]
async fn register_new_indexing_agreement() {
    //* Given
    // Indexing request
    let requested_by = address!("8f8c426f956876325b1e037c6eae9b189952994c");
    let deployment_id = deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv");
    let deployment_chain_id = 42161; // arbitrum-one (0xa4b1)

    // Indexing agreement
    let indexer_id = indexer_id!("3c584ee1d89f43c6ccee17e886a001de2bb4d8a9");
    let indexer_url = "http://localhost:8020".parse().expect("Invalid URL");

    // Indexing agreement terms
    let agreement_terms = Faker.fake::<IndexingAgreementTerms>();

    let (db, _temp_db) = temp_registry_db().await;
    let registry = PgRegistry::new(db);

    // Register a new indexing request
    let indexing_request_id = match registry
        .set_indexing_target_candidates(requested_by, deployment_id, deployment_chain_id, 3)
        .await
        .expect("Failed to set indexing target candidates")
    {
        dipper_pgregistry::IndexingRequestSetTargetOutcome::Inserted { id } => id,
        other => panic!("seed insert did not produce an Inserted outcome: {other:?}"),
    };

    //* When
    let res = registry
        .register_new_indexing_agreement(NewAgreementParams {
            agreement_id: Faker.fake::<IndexingAgreementId>(),
            nonce_uuid: uuid::Uuid::now_v7(),
            request_id: indexing_request_id,
            deployment_id,
            indexer_id,
            indexer_url,
            terms: agreement_terms,
            terms_version_hash: None,
        })
        .await;

    //* Then
    let _indexing_agreement_id = res.expect("Failed to register new indexing agreement");
}

#[sqlx::test]
async fn register_new_and_get_indexing_agreement_by_id() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    let registry = PgRegistry::new(db);

    // Indexing request
    let requested_by = address!("8f8c426f956876325b1e037c6eae9b189952994c");
    let deployment_id = deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv");
    let deployment_chain_id = 42161; // arbitrum-one (0xa4b1)

    // Indexing agreement
    let indexer_id = indexer_id!("3c584ee1d89f43c6ccee17e886a001de2bb4d8a9");
    let indexer_url = "http://localhost:8020".parse().expect("Invalid URL");
    let agreement_terms = {
        let mut terms = Faker.fake::<IndexingAgreementTerms>();
        terms.metadata.subgraph_deployment_id = deployment_id;
        terms
    };

    // Register a new indexing request
    let indexing_request_id = match registry
        .set_indexing_target_candidates(requested_by, deployment_id, deployment_chain_id, 3)
        .await
        .expect("Failed to set indexing target candidates")
    {
        dipper_pgregistry::IndexingRequestSetTargetOutcome::Inserted { id } => id,
        other => panic!("seed insert did not produce an Inserted outcome: {other:?}"),
    };

    // Register a new indexing agreement
    let indexing_agreement_id = registry
        .register_new_indexing_agreement(NewAgreementParams {
            agreement_id: Faker.fake::<IndexingAgreementId>(),
            nonce_uuid: uuid::Uuid::now_v7(),
            request_id: indexing_request_id,
            deployment_id,
            indexer_id,
            indexer_url,
            terms: agreement_terms,
            terms_version_hash: None,
        })
        .await
        .expect("Failed to register new indexing agreement");

    //* When
    let indexing_agreement = registry
        .get_indexing_agreement_by_id(&indexing_agreement_id)
        .await
        .expect("Failed to get indexing agreement by ID")
        .expect("Agreement not found");

    //* Then
    assert_eq!(indexing_agreement.id, indexing_agreement_id);
    assert_eq!(indexing_agreement.indexing_request_id, indexing_request_id);
    assert_eq!(indexing_agreement.status, IndexingAgreementStatus::Created);
    assert_eq!(indexing_agreement.indexer.id, indexer_id);
    assert_eq!(
        indexing_agreement.terms.metadata.subgraph_deployment_id,
        deployment_id
    );
}

#[tokio::test]
async fn get_indexing_agreements_by_indexing_request_id() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(&db, include_str!("fixtures/0002_indexing_agreements.sql"))
        .await
        .expect("Failed to run fixture");
    let registry = PgRegistry::new(db);

    let indexing_request_id = uuid!("019300ce-4751-780e-b58c-bf696b67eb23").into();

    //* When
    let res = registry
        .get_active_indexing_agreements_by_indexing_request_id(&indexing_request_id)
        .await;

    //* Then
    let agreements = res.expect("Failed to get indexing agreements by indexing request ID");
    assert_eq!(agreements.len(), 2);

    // Assert the agreements are in the expected state
    assert!(
        agreements
            .iter()
            .all(|agreement| { agreement.indexing_request_id == indexing_request_id }),
        "Expected all agreements to be associated with the given indexing request"
    );
    assert!(
        agreements.iter().all(|agreement| {
            matches!(
                agreement.status,
                IndexingAgreementStatus::Created | IndexingAgreementStatus::AcceptedOnChain
            )
        }),
        "Expected all agreements to be in CREATED or ACCEPTED_ON_CHAIN state"
    );
}

#[tokio::test]
async fn get_pending_agreement_indexers_by_deployment_aggregation() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");
    let registry = PgRegistry::new(db);

    // Indexer IDs from the fixture
    let indexer_a = indexer_id!("1111111111111111111111111111111111111111");
    let indexer_b = indexer_id!("2222222222222222222222222222222222222222");
    let indexer_c = indexer_id!("3333333333333333333333333333333333333333");

    //* When
    let result = registry
        .get_pending_agreement_indexers_by_deployment(&[indexer_a, indexer_b, indexer_c])
        .await
        .expect("Failed to get aggregated agreements");

    //* Then
    // Should return 3 deployments (only those with active agreements):
    // - QmAAAAAA...1a -> [Indexer A] (Created)
    // - QmBBBBBB...2b -> [Indexer A] (AcceptedOnChain)
    // - QmDDDDDD...4d -> [Indexer B] (Created)
    // CanceledByIndexer and Expired agreements should NOT be included
    assert_eq!(result.len(), 3);

    // Verify specific deployments
    let deployment_a: DeploymentId = "QmAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA1a"
        .parse()
        .unwrap();
    let deployment_b: DeploymentId = "QmBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB2b"
        .parse()
        .unwrap();
    let deployment_d: DeploymentId = "QmDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD4d"
        .parse()
        .unwrap();

    assert!(result.contains_key(&deployment_a));
    assert!(result.contains_key(&deployment_b));
    assert!(result.contains_key(&deployment_d));

    // Verify indexers per deployment (use HashSet for order-independent comparison)
    let to_set = |v: &Vec<IndexerId>| v.iter().copied().collect::<HashSet<_>>();
    assert_eq!(
        to_set(result.get(&deployment_a).unwrap()),
        HashSet::from([indexer_a])
    );
    assert_eq!(
        to_set(result.get(&deployment_b).unwrap()),
        HashSet::from([indexer_a])
    );
    assert_eq!(
        to_set(result.get(&deployment_d).unwrap()),
        HashSet::from([indexer_b])
    );
}

#[tokio::test]
async fn get_pending_agreement_indexers_by_deployment_empty_input() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    let registry = PgRegistry::new(db);

    //* When
    let result = registry
        .get_pending_agreement_indexers_by_deployment(&[])
        .await
        .expect("Failed to get agreements for empty input");

    //* Then
    assert!(result.is_empty(), "Empty input should return empty HashMap");
}

#[tokio::test]
async fn get_pending_agreement_indexers_by_deployment_no_active_agreements() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");
    let registry = PgRegistry::new(db);

    // Indexer C only has expired agreements
    let indexer_c = indexer_id!("3333333333333333333333333333333333333333");

    //* When
    let result = registry
        .get_pending_agreement_indexers_by_deployment(&[indexer_c])
        .await
        .expect("Failed to get agreements");

    //* Then
    // Indexer C has no active agreements, only expired
    assert!(
        result.is_empty(),
        "Indexer with only expired agreements should return empty HashMap"
    );
}

// =============================================================================
// get_declined_indexers_by_deployment tests
// =============================================================================

#[tokio::test]
async fn get_declined_indexers_by_deployment_returns_rejected() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");
    let registry = PgRegistry::new(db);

    //* When
    // Use 30 days lookback (agreements were created "now")
    let result = registry
        .get_declined_indexers_by_deployment(30, 1, 5, 1)
        .await
        .expect("Failed to get declined indexers");

    //* Then
    // Indexer A has CanceledByIndexer for deployment QmCCCC...3c
    // Indexer C has Expired for deployment QmEEEE...5e
    assert_eq!(
        result.len(),
        2,
        "Should have 2 deployments with declined indexers (CanceledByIndexer + Expired)"
    );

    // Check CanceledByIndexer
    let deployment_3c: DeploymentId = "QmCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC3c"
        .parse()
        .unwrap();
    let indexer_a = indexer_id!("1111111111111111111111111111111111111111");
    let declined_3c = result.get(&deployment_3c).expect("Deployment 3c not found");
    assert_eq!(declined_3c.len(), 1);
    assert!(declined_3c.contains(&indexer_a));

    // Check Expired
    let deployment_5e: DeploymentId = "QmEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE5e"
        .parse()
        .unwrap();
    let indexer_c = indexer_id!("3333333333333333333333333333333333333333");
    let declined_5e = result.get(&deployment_5e).expect("Deployment 5e not found");
    assert_eq!(declined_5e.len(), 1);
    assert!(declined_5e.contains(&indexer_c));
}

#[tokio::test]
async fn get_declined_indexers_by_deployment_empty_when_no_declines() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    // Use fixture without any rejected/canceled agreements
    run_fixture(&db, include_str!("fixtures/0001_indexing_requests.sql"))
        .await
        .expect("Failed to run fixture");
    let registry = PgRegistry::new(db);

    //* When
    let result = registry
        .get_declined_indexers_by_deployment(30, 1, 5, 1)
        .await
        .expect("Failed to get declined indexers");

    //* Then
    assert!(
        result.is_empty(),
        "No declined agreements should return empty HashMap"
    );
}

#[tokio::test]
async fn get_declined_indexers_by_deployment_respects_lookback() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");
    let registry = PgRegistry::new(db);

    //* When
    // Use 0 days lookback - should exclude everything
    let result = registry
        .get_declined_indexers_by_deployment(0, 0, 0, 0)
        .await
        .expect("Failed to get declined indexers");

    //* Then
    // With 0 days lookback, nothing should match (agreements were created "now", not in the future)
    assert!(
        result.is_empty(),
        "0 day lookback should return empty HashMap"
    );
}

#[tokio::test]
async fn get_declined_indexers_by_deployment_excludes_old_rejections() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");

    // Update the canceled-by-indexer agreement to be 31 days old
    sqlx::query(
        r#"
        UPDATE dipper_reg_indexing_agreements
        SET updated_at = timezone('UTC', now()) - interval '31 days'
        WHERE id = $1
        "#,
    )
    .bind(IndexingAgreementId::from_bytes([
        0xaa, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3,
    ]))
    .execute(&db)
    .await
    .expect("Failed to update CanceledByIndexer agreement timestamp");

    // Also update the expired agreement to be 31 days old
    // (declined_indexers query now includes both CanceledByIndexer and Expired)
    sqlx::query(
        r#"
        UPDATE dipper_reg_indexing_agreements
        SET updated_at = timezone('UTC', now()) - interval '31 days'
        WHERE id = $1
        "#,
    )
    .bind(IndexingAgreementId::from_bytes([
        0xcc, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
    ]))
    .execute(&db)
    .await
    .expect("Failed to update Expired agreement timestamp");

    let registry = PgRegistry::new(db);

    //* When
    // Use 30 days lookback - should NOT include the 31-day-old rejection
    let result = registry
        .get_declined_indexers_by_deployment(30, 1, 5, 1)
        .await
        .expect("Failed to get declined indexers");

    //* Then
    // Both CanceledByIndexer and Expired agreements are now 31 days old, outside the 30-day window
    assert!(
        result.is_empty(),
        "Rejections older than lookback period should not be returned"
    );
}

// =============================================================================
// mark_indexing_agreement_as_rejected tests
// =============================================================================

#[tokio::test]
async fn mark_indexing_agreement_as_rejected_transitions_created_to_rejected() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");
    let registry = PgRegistry::new(db);

    // Agreement in Created status
    let agreement_id =
        IndexingAgreementId::from_bytes([0xaa, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);

    //* When
    let result = registry
        .mark_indexing_agreement_as_rejected(&agreement_id, None)
        .await;

    //* Then
    result.expect("Should successfully mark as rejected");

    let agreement = registry
        .get_indexing_agreement_by_id(&agreement_id)
        .await
        .expect("Failed to get agreement")
        .expect("Agreement not found");
    assert_eq!(
        agreement.status,
        IndexingAgreementStatus::Rejected,
        "Status should be Rejected"
    );
}

#[tokio::test]
async fn mark_indexing_agreement_as_rejected_fails_if_not_created() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");
    let registry = PgRegistry::new(db);

    // Agreement in AcceptedOnChain status (not Created)
    let agreement_id =
        IndexingAgreementId::from_bytes([0xaa, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2]);

    //* When
    let result = registry
        .mark_indexing_agreement_as_rejected(&agreement_id, None)
        .await;

    //* Then
    let err = result.expect_err("Should fail for non-Created agreement");
    assert!(matches!(err, Error::NoRecordsUpdated));
}

#[tokio::test]
async fn mark_indexing_agreement_as_rejected_fails_if_not_found() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    let registry = PgRegistry::new(db);

    // Non-existent agreement
    let agreement_id =
        IndexingAgreementId::from_bytes([0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff]);

    //* When
    let result = registry
        .mark_indexing_agreement_as_rejected(&agreement_id, None)
        .await;

    //* Then
    let err = result.expect_err("Should fail for non-existent agreement");
    assert!(matches!(err, Error::NoRecordsUpdated));
}

#[tokio::test]
async fn get_declined_indexers_includes_rejected_status() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");

    // Mark one agreement as Rejected
    let agreement_id =
        IndexingAgreementId::from_bytes([0xbb, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    sqlx::query(
        r#"
        UPDATE dipper_reg_indexing_agreements
        SET status = 7
        WHERE id = $1
        "#,
    )
    .bind(agreement_id)
    .execute(&db)
    .await
    .expect("Failed to update agreement to Rejected");

    let registry = PgRegistry::new(db);

    //* When
    let result = registry
        .get_declined_indexers_by_deployment(30, 1, 5, 1)
        .await
        .expect("Failed to get declined indexers");

    //* Then
    // Should include CanceledByIndexer, Expired, AND Rejected
    assert_eq!(
        result.len(),
        3,
        "Should have 3 deployments with declined indexers"
    );

    // Check Rejected is included
    let deployment_4d: DeploymentId = "QmDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD4d"
        .parse()
        .unwrap();
    let indexer_b = indexer_id!("2222222222222222222222222222222222222222");
    let declined_4d = result.get(&deployment_4d).expect("Deployment 4d not found");
    assert!(
        declined_4d.contains(&indexer_b),
        "Rejected indexer should be in declined list"
    );
}

// =============================================================================
// get_unresponsive_indexers tests
// =============================================================================

#[tokio::test]
async fn get_unresponsive_indexers_returns_unresponsive() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");

    // 0003 has no Unresponsive (status 1) rows; mark indexer 2222's agreement
    // Unresponsive so exactly that indexer comes back for the request's chain.
    let agreement_id =
        IndexingAgreementId::from_bytes([0xbb, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    sqlx::query(
        r#"
        UPDATE dipper_reg_indexing_agreements
        SET status = 1, updated_at = timezone('UTC', now())
        WHERE id = $1
        "#,
    )
    .bind(agreement_id)
    .execute(&db)
    .await
    .expect("Failed to mark agreement Unresponsive");

    let registry = PgRegistry::new(db);

    //* When
    let result = registry
        .get_unresponsive_indexers(1, 42161)
        .await
        .expect("Failed to get unresponsive indexers");

    //* Then
    let indexer_b = indexer_id!("2222222222222222222222222222222222222222");
    assert_eq!(result.len(), 1, "only the unresponsive indexer is returned");
    assert!(
        result.contains(&indexer_b),
        "the unresponsive indexer should be returned"
    );
}

#[tokio::test]
async fn get_unresponsive_indexers_excludes_old() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");

    // Mark indexer 2222 Unresponsive but 31 days ago, outside a 30-day window.
    let agreement_id =
        IndexingAgreementId::from_bytes([0xbb, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    sqlx::query(
        r#"
        UPDATE dipper_reg_indexing_agreements
        SET status = 1, updated_at = timezone('UTC', now()) - interval '31 days'
        WHERE id = $1
        "#,
    )
    .bind(agreement_id)
    .execute(&db)
    .await
    .expect("Failed to age the Unresponsive agreement");

    let registry = PgRegistry::new(db);

    //* When
    let result = registry
        .get_unresponsive_indexers(30, 42161)
        .await
        .expect("Failed to get unresponsive indexers");

    //* Then
    assert!(
        result.is_empty(),
        "an Unresponsive row older than the lookback window is excluded"
    );
}

#[tokio::test]
async fn get_unresponsive_indexers_is_scoped_to_chain() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");

    // The fixture's request is on chain 42161; mark indexer 2222 Unresponsive there.
    let agreement_id =
        IndexingAgreementId::from_bytes([0xbb, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    sqlx::query(
        r#"
        UPDATE dipper_reg_indexing_agreements
        SET status = 1, updated_at = timezone('UTC', now())
        WHERE id = $1
        "#,
    )
    .bind(agreement_id)
    .execute(&db)
    .await
    .expect("Failed to mark agreement Unresponsive");

    let registry = PgRegistry::new(db);

    //* Then - querying the same chain returns the indexer
    let same_chain = registry
        .get_unresponsive_indexers(1, 42161)
        .await
        .expect("query same chain");
    assert_eq!(
        same_chain.len(),
        1,
        "an indexer unresponsive on the queried chain is returned"
    );

    //* Then - querying a different chain returns nothing
    let other_chain = registry
        .get_unresponsive_indexers(1, 1)
        .await
        .expect("query other chain");
    assert!(
        other_chain.is_empty(),
        "an indexer unresponsive on another chain is not returned"
    );
}

// =============================================================================
// Deadline expiration tests
// =============================================================================

#[tokio::test]
async fn get_expired_created_agreements_returns_past_deadline() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");

    // The fixture has deadline=1700000300 which is in the past
    // Agreement 01930100-0001-7000-8000-000000000001 is Created with past deadline
    // Agreement 01930100-0002-7000-8000-000000000001 is Created with past deadline

    let registry = PgRegistry::new(db);

    //* When
    let result = registry
        .get_expired_created_agreements(100, 1800000000)
        .await
        .expect("Failed to get expired agreements");

    //* Then
    // Should return the 2 Created agreements (both have past deadlines)
    assert_eq!(
        result.len(),
        2,
        "Should return 2 expired Created agreements"
    );

    let agreement_ids: Vec<_> = result.iter().map(|a| a.id).collect();
    let expected_id_1 =
        IndexingAgreementId::from_bytes([0xaa, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    let expected_id_2 =
        IndexingAgreementId::from_bytes([0xbb, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    assert!(
        agreement_ids.contains(&expected_id_1),
        "Should include first Created agreement"
    );
    assert!(
        agreement_ids.contains(&expected_id_2),
        "Should include second Created agreement"
    );
}

#[tokio::test]
async fn get_expired_created_agreements_excludes_future_deadline() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");

    // Update one agreement to have a future deadline (year 2100)
    let future_deadline: i64 = 4102444800; // 2100-01-01
    sqlx::query(
        r#"
        UPDATE dipper_reg_indexing_agreements
        SET terms = jsonb_set(terms::jsonb, '{deadline}', to_jsonb($1::bigint))
        WHERE id = $2
        "#,
    )
    .bind(future_deadline)
    .bind(IndexingAgreementId::from_bytes([
        0xaa, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
    ]))
    .execute(&db)
    .await
    .expect("Failed to update deadline");

    let registry = PgRegistry::new(db);

    //* When
    let result = registry
        .get_expired_created_agreements(100, 1800000000)
        .await
        .expect("Failed to get expired agreements");

    //* Then
    // Should only return 1 agreement (the one with past deadline)
    assert_eq!(
        result.len(),
        1,
        "Should return only 1 expired Created agreement"
    );
    let expected_id =
        IndexingAgreementId::from_bytes([0xbb, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    assert_eq!(
        result[0].id, expected_id,
        "Should return the past-deadline agreement"
    );
}

#[tokio::test]
async fn get_expired_created_agreements_respects_batch_size() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");

    let registry = PgRegistry::new(db);

    //* When
    let result = registry
        .get_expired_created_agreements(1, 1800000000) // Only request 1
        .await
        .expect("Failed to get expired agreements");

    //* Then
    assert_eq!(result.len(), 1, "Should respect batch_size limit");
}

#[tokio::test]
async fn get_expired_created_agreements_excludes_non_created_status() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");

    // The fixture also has AcceptedOnChain (status=6) and Expired (status=5) agreements
    // with past deadlines - these should NOT be returned

    let registry = PgRegistry::new(db);

    //* When
    let result = registry
        .get_expired_created_agreements(100, 1800000000)
        .await
        .expect("Failed to get expired agreements");

    //* Then
    // Verify none of the returned agreements have non-Created status
    for agreement in &result {
        assert_eq!(
            agreement.status,
            IndexingAgreementStatus::Created,
            "Should only return Created agreements"
        );
    }
}

#[tokio::test]
async fn mark_indexing_agreement_as_expired_transitions_created_to_expired() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");
    let registry = PgRegistry::new(db);

    // Agreement in Created status
    let agreement_id =
        IndexingAgreementId::from_bytes([0xaa, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);

    //* When
    let result = registry
        .mark_indexing_agreement_as_expired(&agreement_id)
        .await;

    //* Then
    result.expect("Should successfully mark as expired");

    let agreement = registry
        .get_indexing_agreement_by_id(&agreement_id)
        .await
        .expect("Failed to get agreement")
        .expect("Agreement not found");
    assert_eq!(
        agreement.status,
        IndexingAgreementStatus::Expired,
        "Status should be Expired"
    );
}

#[tokio::test]
async fn mark_indexing_agreement_as_expired_fails_if_not_created() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");
    let registry = PgRegistry::new(db);

    // Agreement in AcceptedOnChain status (not Created)
    let agreement_id =
        IndexingAgreementId::from_bytes([0xaa, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2]);

    //* When
    let result = registry
        .mark_indexing_agreement_as_expired(&agreement_id)
        .await;

    //* Then
    let err = result.expect_err("Should fail for non-Created agreement");
    assert!(matches!(err, Error::NoRecordsUpdated));
}

#[tokio::test]
async fn mark_indexing_agreement_as_expired_fails_if_not_found() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    let registry = PgRegistry::new(db);

    // Non-existent agreement
    let agreement_id =
        IndexingAgreementId::from_bytes([0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff]);

    //* When
    let result = registry
        .mark_indexing_agreement_as_expired(&agreement_id)
        .await;

    //* Then
    let err = result.expect_err("Should fail for non-existent agreement");
    assert!(matches!(err, Error::NoRecordsUpdated));
}

// =============================================================================
// Chain listener state tests
// =============================================================================

#[tokio::test]
async fn get_chain_listener_state_returns_none_when_not_exists() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    let registry = PgRegistry::new(db);

    //* When
    let result = registry.get_chain_listener_state(42161).await;

    //* Then
    let state = result.expect("Should not error");
    assert!(state.is_none(), "Should return None for non-existent chain");
}

#[tokio::test]
async fn update_chain_listener_state_creates_new_record() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    let registry = PgRegistry::new(db);
    let cursor_id = IndexingAgreementId::from_bytes([7u8; 16]);

    //* When
    registry
        .update_chain_listener_state(42161, 12345678, Some(cursor_id), Some(1700000000))
        .await
        .expect("Should create state");

    //* Then
    let state = registry
        .get_chain_listener_state(42161)
        .await
        .expect("Should not error")
        .expect("State should exist");
    assert_eq!(state.chain_id, 42161, "Chain ID should match");
    assert_eq!(
        state.last_processed_block, 12345678,
        "Block number should match"
    );
    assert_eq!(
        state.last_processed_id,
        Some(cursor_id),
        "Cursor id should match"
    );
    assert_eq!(
        state.last_processed_block_timestamp,
        Some(1700000000),
        "Timestamp should match"
    );
}

#[tokio::test]
async fn update_chain_listener_state_upserts_existing() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    let registry = PgRegistry::new(db);

    // Create initial state
    registry
        .update_chain_listener_state(42161, 1000, None, Some(1700000000))
        .await
        .expect("Should create state");

    //* When
    let new_id = IndexingAgreementId::from_bytes([9u8; 16]);
    registry
        .update_chain_listener_state(42161, 2000, Some(new_id), Some(1700001000))
        .await
        .expect("Should update state");

    //* Then
    let state = registry
        .get_chain_listener_state(42161)
        .await
        .expect("Should not error")
        .expect("State should exist");
    assert_eq!(
        state.last_processed_block, 2000,
        "Block number should be updated"
    );
    assert_eq!(
        state.last_processed_id,
        Some(new_id),
        "Cursor id should be updated"
    );
    assert_eq!(
        state.last_processed_block_timestamp,
        Some(1700001000),
        "Timestamp should be updated"
    );
}

// =============================================================================
// Indexer denylist tests
// =============================================================================

#[tokio::test]
async fn indexer_denylist_returns_denied_indexers() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    let registry = PgRegistry::new(db.clone());

    let indexer_a = indexer_id!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let indexer_b = indexer_id!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

    // Insert directly via SQL (simulating admin operations via kubectl)
    sqlx::query("INSERT INTO dipper_indexer_denylist (indexer_id, reason, created_by, expires_at) VALUES ($1, $2, $3, $4)")
        .bind(indexer_a.as_slice())
        .bind("Malicious behavior")
        .bind("test@example.com")
        .bind(time::OffsetDateTime::parse("3000-01-01T00:00:00Z", &time::format_description::well_known::Rfc3339).unwrap())
        .execute(&db)
        .await
        .expect("Failed to insert indexer A");
    sqlx::query("INSERT INTO dipper_indexer_denylist (indexer_id, reason, created_by, expires_at) VALUES ($1, $2, $3, $4)")
        .bind(indexer_b.as_slice())
        .bind("Poor performance")
        .bind("test@example.com")
        .bind(time::OffsetDateTime::parse("3000-01-01T00:00:00Z", &time::format_description::well_known::Rfc3339).unwrap())
        .execute(&db)
        .await
        .expect("Failed to insert indexer B");

    //* When
    let denylist = registry
        .get_indexer_denylist()
        .await
        .expect("Failed to get denylist");

    //* Then
    assert_eq!(denylist.len(), 2);
    assert!(denylist.contains(&indexer_a));
    assert!(denylist.contains(&indexer_b));
}

#[tokio::test]
async fn indexer_denylist_returns_empty_when_none_denied() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    let registry = PgRegistry::new(db);

    //* When
    let denylist = registry
        .get_indexer_denylist()
        .await
        .expect("Failed to get denylist");

    //* Then
    assert!(denylist.is_empty());
}

#[tokio::test]
async fn indexer_denylist_excludes_expired_entries() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    let registry = PgRegistry::new(db.clone());

    let active_indexer = indexer_id!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let expired_indexer = indexer_id!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

    // Insert an active denial (expires in year 3000)
    sqlx::query("INSERT INTO dipper_indexer_denylist (indexer_id, reason, created_by, expires_at) VALUES ($1, $2, $3, $4)")
        .bind(active_indexer.as_slice())
        .bind("Active denial")
        .bind("test@example.com")
        .bind(time::OffsetDateTime::parse("3000-01-01T00:00:00Z", &time::format_description::well_known::Rfc3339).unwrap())
        .execute(&db)
        .await
        .expect("Failed to insert active denial");

    // Insert an expired denial (expired yesterday)
    sqlx::query("INSERT INTO dipper_indexer_denylist (indexer_id, reason, created_by, expires_at) VALUES ($1, $2, $3, $4)")
        .bind(expired_indexer.as_slice())
        .bind("Expired denial")
        .bind("test@example.com")
        .bind(time::OffsetDateTime::now_utc() - time::Duration::days(1))
        .execute(&db)
        .await
        .expect("Failed to insert expired denial");

    //* When
    let denylist = registry
        .get_indexer_denylist()
        .await
        .expect("Failed to get denylist");

    //* Then
    assert_eq!(denylist.len(), 1, "Only active denial should be returned");
    assert!(denylist.contains(&active_indexer));
    assert!(
        !denylist.contains(&expired_indexer),
        "Expired denial should not be returned"
    );
}

// =============================================================================
// Reassessment query tests
// =============================================================================

#[tokio::test]
async fn get_open_indexing_requests_for_reassessment_filters_by_age_and_status() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(&db, include_str!("fixtures/0004_reassessment_requests.sql"))
        .await
        .expect("Failed to run fixture");
    let registry = PgRegistry::new(db);

    //* When
    // Use 1 hour (3600 seconds) min age
    let requests = registry
        .get_open_indexing_requests_for_reassessment(3600, 0)
        .await
        .expect("Failed to get requests for reassessment");

    //* Then
    // Should return 3 requests: #1, #2, #5 (all OPEN and older than 1 hour)
    // Should NOT include #3 (too new) or #4 (canceled status)
    assert_eq!(requests.len(), 3, "Expected 3 eligible requests");

    // All should be OPEN status
    assert!(
        requests
            .iter()
            .all(|r| r.status == IndexingRequestStatus::Open),
        "All requests should have OPEN status"
    );

    // Request #3 (created 5 min ago) should NOT be included
    let new_request_id: IndexingRequestId = uuid!("01940003-0003-7000-0003-000000000003").into();
    assert!(
        !requests.iter().any(|r| r.id == new_request_id),
        "New request should not be included"
    );

    // Request #4 (canceled) should NOT be included
    let canceled_request_id: IndexingRequestId =
        uuid!("01940004-0004-7000-0004-000000000004").into();
    assert!(
        !requests.iter().any(|r| r.id == canceled_request_id),
        "Canceled request should not be included"
    );
}

#[tokio::test]
async fn get_open_indexing_requests_for_reassessment_orders_by_updated_at() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(&db, include_str!("fixtures/0004_reassessment_requests.sql"))
        .await
        .expect("Failed to run fixture");
    let registry = PgRegistry::new(db);

    //* When
    let requests = registry
        .get_open_indexing_requests_for_reassessment(3600, 0)
        .await
        .expect("Failed to get requests for reassessment");

    //* Then
    // Results should be ordered by updated_at ASC (oldest first)
    // Request #5 (updated 3 hours ago) should come first
    // Request #2 (updated 2 hours ago) should come second
    // Request #1 (updated 30 min ago) should come last
    let expected_order: Vec<IndexingRequestId> = vec![
        uuid!("01940005-0005-7000-0005-000000000005").into(),
        uuid!("01940002-0002-7000-0002-000000000002").into(),
        uuid!("01940001-0001-7000-0001-000000000001").into(),
    ];

    let actual_order: Vec<IndexingRequestId> = requests.iter().map(|r| r.id).collect();
    assert_eq!(
        actual_order, expected_order,
        "Requests should be ordered by updated_at ASC"
    );
}

#[tokio::test]
async fn get_open_indexing_requests_for_reassessment_respects_batch_size() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(&db, include_str!("fixtures/0004_reassessment_requests.sql"))
        .await
        .expect("Failed to run fixture");
    let registry = PgRegistry::new(db);

    //* When
    // Request batch size of 2
    let requests = registry
        .get_open_indexing_requests_for_reassessment(3600, 2)
        .await
        .expect("Failed to get requests for reassessment");

    //* Then
    // Should return exactly 2 requests (limited by batch size)
    assert_eq!(requests.len(), 2, "Expected 2 requests (batch size limit)");

    // Should be the 2 oldest by updated_at
    let expected_ids: HashSet<IndexingRequestId> = [
        uuid!("01940005-0005-7000-0005-000000000005").into(),
        uuid!("01940002-0002-7000-0002-000000000002").into(),
    ]
    .into_iter()
    .collect();

    let actual_ids: HashSet<IndexingRequestId> = requests.iter().map(|r| r.id).collect();
    assert_eq!(
        actual_ids, expected_ids,
        "Should return the 2 oldest eligible requests"
    );
}

#[tokio::test]
async fn get_open_indexing_requests_for_reassessment_returns_empty_when_none_eligible() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(&db, include_str!("fixtures/0004_reassessment_requests.sql"))
        .await
        .expect("Failed to run fixture");
    let registry = PgRegistry::new(db);

    //* When
    // Use very long min age (1 week = 604800 seconds) so nothing qualifies
    let requests = registry
        .get_open_indexing_requests_for_reassessment(604800, 0)
        .await
        .expect("Failed to get requests for reassessment");

    //* Then
    assert!(
        requests.is_empty(),
        "No requests should be old enough for reassessment"
    );
}

#[tokio::test]
async fn get_open_indexing_requests_for_reassessment_zero_batch_returns_all() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(&db, include_str!("fixtures/0004_reassessment_requests.sql"))
        .await
        .expect("Failed to run fixture");
    let registry = PgRegistry::new(db);

    //* When
    // batch_size = 0 should return all eligible
    let requests = registry
        .get_open_indexing_requests_for_reassessment(3600, 0)
        .await
        .expect("Failed to get requests for reassessment");

    //* Then
    assert_eq!(requests.len(), 3, "Expected all 3 eligible requests");
}

// =============================================================================
// Liveness checker DB operation tests
// =============================================================================

#[tokio::test]
async fn test_get_accepted_on_chain_agreements() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(&db, include_str!("fixtures/0002_indexing_agreements.sql"))
        .await
        .expect("Failed to run fixture");
    let registry = PgRegistry::new(db);

    //* When
    let agreements = registry
        .get_accepted_on_chain_agreements(10)
        .await
        .expect("Failed to get AcceptedOnChain agreements");

    //* Then
    assert_eq!(
        agreements.len(),
        1,
        "Expected exactly 1 AcceptedOnChain agreement"
    );
    let agreement = &agreements[0];
    assert_eq!(
        agreement.status,
        IndexingAgreementStatus::AcceptedOnChain,
        "Status should be AcceptedOnChain"
    );
    assert!(
        agreement.last_block_height.is_none(),
        "last_block_height should be None before first liveness check"
    );
    assert!(
        agreement.last_progress_at.is_none(),
        "last_progress_at should be None before first liveness check"
    );
}

#[tokio::test]
async fn test_update_agreement_sync_progress() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(&db, include_str!("fixtures/0002_indexing_agreements.sql"))
        .await
        .expect("Failed to run fixture");
    let registry = PgRegistry::new(db);

    // AcceptedOnChain agreement from fixture 0002
    let agreement_id =
        IndexingAgreementId::from_bytes([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3]);
    let now = time::OffsetDateTime::now_utc();

    //* When
    registry
        .update_agreement_sync_progress(&agreement_id, 99999, now)
        .await
        .expect("Failed to update sync progress");

    //* Then
    let agreement = registry
        .get_indexing_agreement_by_id(&agreement_id)
        .await
        .expect("Failed to re-fetch agreement")
        .expect("Agreement not found");

    assert_eq!(
        agreement.last_block_height,
        Some(99999),
        "last_block_height should be updated"
    );
    assert!(
        agreement.last_progress_at.is_some(),
        "last_progress_at should be set"
    );
}

#[tokio::test]
async fn test_count_active_agreements_by_deployment() {
    //* Given
    // Fixture 0002 has 1 Created + 1 AcceptedOnChain for the same deployment
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(&db, include_str!("fixtures/0002_indexing_agreements.sql"))
        .await
        .expect("Failed to run fixture");
    let registry = PgRegistry::new(db);

    //* When
    let counts = registry
        .count_active_agreements_by_deployment()
        .await
        .expect("Failed to count active agreements by deployment");

    //* Then
    let deployment: DeploymentId = "QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv"
        .parse()
        .unwrap();
    let count = counts.get(&deployment).copied().unwrap_or(0);
    assert_eq!(
        count, 2,
        "Expected 2 active agreements (1 Created + 1 AcceptedOnChain)"
    );
}

#[tokio::test]
async fn test_mark_as_abandoned_transitions_status() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(&db, include_str!("fixtures/0002_indexing_agreements.sql"))
        .await
        .expect("Failed to run fixture");
    let registry = PgRegistry::new(db);

    // AcceptedOnChain agreement from fixture 0002
    let agreement_id =
        IndexingAgreementId::from_bytes([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3]);

    //* When
    let abandoned = registry
        .mark_indexing_agreement_as_abandoned(&agreement_id)
        .await
        .expect("Failed to mark agreement as abandoned");

    //* Then
    assert_eq!(
        abandoned.status,
        IndexingAgreementStatus::AbandonedByIndexer,
        "Status should be AbandonedByIndexer"
    );

    // Second call must fail — agreement is no longer AcceptedOnChain
    let err = registry
        .mark_indexing_agreement_as_abandoned(&agreement_id)
        .await
        .expect_err("Expected error on second mark_as_abandoned call");
    assert!(
        matches!(err, Error::NoRecordsUpdated),
        "Expected NoRecordsUpdated, got: {err:?}"
    );
}

// =============================================================================
// Rejection reason storage tests
// =============================================================================

#[tokio::test]
async fn mark_indexing_agreement_as_rejected_stores_price_too_low_reason() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");
    let registry = PgRegistry::new(db);

    // Agreement in Created status
    let agreement_id =
        IndexingAgreementId::from_bytes([0xaa, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);

    //* When
    let result = registry
        .mark_indexing_agreement_as_rejected(&agreement_id, Some("PRICE_TOO_LOW"))
        .await;

    //* Then
    result.expect("Should successfully mark as rejected");

    let agreement = registry
        .get_indexing_agreement_by_id(&agreement_id)
        .await
        .expect("Failed to get agreement")
        .expect("Agreement not found");
    assert_eq!(
        agreement.status,
        IndexingAgreementStatus::Rejected,
        "Status should be Rejected"
    );
    assert_eq!(
        agreement.rejection_reason.as_deref(),
        Some("PRICE_TOO_LOW"),
        "Rejection reason should be stored"
    );
}

#[tokio::test]
async fn mark_indexing_agreement_as_rejected_stores_other_reason() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");
    let registry = PgRegistry::new(db);

    // Agreement in Created status
    let agreement_id =
        IndexingAgreementId::from_bytes([0xaa, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);

    //* When
    let result = registry
        .mark_indexing_agreement_as_rejected(&agreement_id, Some("OTHER"))
        .await;

    //* Then
    result.expect("Should successfully mark as rejected");

    let agreement = registry
        .get_indexing_agreement_by_id(&agreement_id)
        .await
        .expect("Failed to get agreement")
        .expect("Agreement not found");
    assert_eq!(
        agreement.status,
        IndexingAgreementStatus::Rejected,
        "Status should be Rejected"
    );
    assert_eq!(
        agreement.rejection_reason.as_deref(),
        Some("OTHER"),
        "Rejection reason should be stored"
    );
}

// =============================================================================
// Differentiated lookback window tests
// =============================================================================

#[tokio::test]
async fn get_declined_indexers_price_too_low_excluded_after_1_day() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");

    // Mark an agreement as rejected with PRICE_TOO_LOW and set updated_at to 2 days ago
    let agreement_id =
        IndexingAgreementId::from_bytes([0xaa, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    sqlx::query(
        r#"
        UPDATE dipper_reg_indexing_agreements
        SET status = 7, rejection_reason = 'PRICE_TOO_LOW', updated_at = timezone('UTC', now()) - interval '2 days'
        WHERE id = $1
        "#,
    )
    .bind(agreement_id)
    .execute(&db)
    .await
    .expect("Failed to update agreement");

    let registry = PgRegistry::new(db);

    //* When
    // With 30-day default lookback and 1-day price lookback, the 2-day-old PRICE_TOO_LOW
    // rejection should NOT be included (it's outside the 1-day window)
    let result = registry
        .get_declined_indexers_by_deployment(30, 1, 5, 1)
        .await
        .expect("Failed to get declined indexers");

    //* Then
    // The PRICE_TOO_LOW rejection is 2 days old, which is outside the 1-day window
    // Only CanceledByIndexer (for deployment 3c) and Expired (for deployment 5e) remain
    let deployment_1a: DeploymentId = "QmAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA1a"
        .parse()
        .unwrap();
    assert!(
        !result.contains_key(&deployment_1a),
        "2-day-old PRICE_TOO_LOW rejection should not be in declined list (outside 1-day window)"
    );
}

#[tokio::test]
async fn get_declined_indexers_price_too_low_included_within_1_day() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");

    // Mark an agreement as rejected with PRICE_TOO_LOW (just now, so within 1-day window)
    let agreement_id =
        IndexingAgreementId::from_bytes([0xaa, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    sqlx::query(
        r#"
        UPDATE dipper_reg_indexing_agreements
        SET status = 7, rejection_reason = 'PRICE_TOO_LOW', updated_at = timezone('UTC', now())
        WHERE id = $1
        "#,
    )
    .bind(agreement_id)
    .execute(&db)
    .await
    .expect("Failed to update agreement");

    let registry = PgRegistry::new(db);

    //* When
    let result = registry
        .get_declined_indexers_by_deployment(30, 1, 5, 1)
        .await
        .expect("Failed to get declined indexers");

    //* Then
    // The PRICE_TOO_LOW rejection is fresh (within the 1-day window)
    let deployment_1a: DeploymentId = "QmAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA1a"
        .parse()
        .unwrap();
    let indexer_a = indexer_id!("1111111111111111111111111111111111111111");
    assert!(
        result.contains_key(&deployment_1a),
        "Fresh PRICE_TOO_LOW rejection should be in declined list"
    );
    let declined = result.get(&deployment_1a).expect("Deployment not found");
    assert!(
        declined.contains(&indexer_a),
        "Indexer A should be in declined list for deployment 1a"
    );
}

#[tokio::test]
async fn get_declined_indexers_other_reason_uses_30_day_window() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");

    // Mark an agreement as rejected with OTHER and set updated_at to 15 days ago
    let agreement_id =
        IndexingAgreementId::from_bytes([0xaa, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    sqlx::query(
        r#"
        UPDATE dipper_reg_indexing_agreements
        SET status = 7, rejection_reason = 'OTHER', updated_at = timezone('UTC', now()) - interval '15 days'
        WHERE id = $1
        "#,
    )
    .bind(agreement_id)
    .execute(&db)
    .await
    .expect("Failed to update agreement");

    let registry = PgRegistry::new(db);

    //* When
    // With 30-day default lookback, the 15-day-old OTHER rejection should be included
    let result = registry
        .get_declined_indexers_by_deployment(30, 1, 5, 1)
        .await
        .expect("Failed to get declined indexers");

    //* Then
    let deployment_1a: DeploymentId = "QmAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA1a"
        .parse()
        .unwrap();
    let indexer_a = indexer_id!("1111111111111111111111111111111111111111");
    assert!(
        result.contains_key(&deployment_1a),
        "15-day-old OTHER rejection should be in declined list (within 30-day window)"
    );
    let declined = result.get(&deployment_1a).expect("Deployment not found");
    assert!(
        declined.contains(&indexer_a),
        "Indexer A should be in declined list"
    );
}

#[tokio::test]
async fn get_declined_indexers_other_reason_excluded_after_30_days() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");

    // Mark an agreement as rejected with OTHER and set updated_at to 31 days ago
    let agreement_id =
        IndexingAgreementId::from_bytes([0xaa, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    sqlx::query(
        r#"
        UPDATE dipper_reg_indexing_agreements
        SET status = 7, rejection_reason = 'OTHER', updated_at = timezone('UTC', now()) - interval '31 days'
        WHERE id = $1
        "#,
    )
    .bind(agreement_id)
    .execute(&db)
    .await
    .expect("Failed to update agreement");

    // Also move the existing CanceledByIndexer and Expired agreements outside the window
    sqlx::query(
        r#"
        UPDATE dipper_reg_indexing_agreements
        SET updated_at = timezone('UTC', now()) - interval '31 days'
        WHERE status IN (4, 5)
        "#,
    )
    .execute(&db)
    .await
    .expect("Failed to update agreements");

    let registry = PgRegistry::new(db);

    //* When
    let result = registry
        .get_declined_indexers_by_deployment(30, 1, 5, 1)
        .await
        .expect("Failed to get declined indexers");

    //* Then
    // All rejections are now 31 days old, outside both windows
    assert!(
        result.is_empty(),
        "31-day-old OTHER rejection should not be in declined list (outside 30-day window)"
    );
}

/// A dipper-caused expiry moves to the 5 minute window rather than out of the query entirely,
/// so inside that window the indexer still counts as declined and a reassessment seconds later
/// does not hand them the same deployment while our submission path is still failing.
#[tokio::test]
async fn get_declined_indexers_expiry_without_offer_tx_included_within_5_minutes() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");

    // Age everything the fixture provides out of every window, so only the row under test
    // can appear in the result.
    sqlx::query(
        r#"
        UPDATE dipper_reg_indexing_agreements
        SET updated_at = timezone('UTC', now()) - interval '31 days'
        "#,
    )
    .execute(&db)
    .await
    .expect("Failed to age the fixture agreements");

    let agreement_id =
        IndexingAgreementId::from_bytes([0xaa, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    sqlx::query(
        r#"
        UPDATE dipper_reg_indexing_agreements
        SET status = 5, rejection_reason = NULL, offer_tx_hash = NULL,
            updated_at = timezone('UTC', now()) - interval '1 minute'
        WHERE id = $1
        "#,
    )
    .bind(agreement_id)
    .execute(&db)
    .await
    .expect("Failed to update agreement");

    let registry = PgRegistry::new(db);

    //* When
    let result = registry
        .get_declined_indexers_by_deployment(30, 1, 5, 1)
        .await
        .expect("Failed to get declined indexers");

    //* Then
    let deployment_1a: DeploymentId = "QmAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA1a"
        .parse()
        .unwrap();
    let indexer_a = indexer_id!("1111111111111111111111111111111111111111");
    let declined = result
        .get(&deployment_1a)
        .expect("Deployment 1a should be in the declined list");
    assert!(
        declined.contains(&indexer_a),
        "an expiry we caused should still count within the 5 minute window, got {declined:?}"
    );
}

/// An expiry with no offer transaction means we never landed the offer, so the indexer never
/// had one to accept and must not be benched for a month over it. On 2026-07-29 exactly this
/// benched a willing indexer for 30 days because our RPC provider answered HTTP 500.
#[tokio::test]
async fn get_declined_indexers_expiry_without_offer_tx_excluded_after_5_minutes() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");

    // Age everything the fixture provides out of every window, so only the row under test
    // can appear in the result.
    sqlx::query(
        r#"
        UPDATE dipper_reg_indexing_agreements
        SET updated_at = timezone('UTC', now()) - interval '31 days'
        "#,
    )
    .execute(&db)
    .await
    .expect("Failed to age the fixture agreements");

    let agreement_id =
        IndexingAgreementId::from_bytes([0xaa, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    sqlx::query(
        r#"
        UPDATE dipper_reg_indexing_agreements
        SET status = 5, rejection_reason = NULL, offer_tx_hash = NULL,
            updated_at = timezone('UTC', now()) - interval '1 hour'
        WHERE id = $1
        "#,
    )
    .bind(agreement_id)
    .execute(&db)
    .await
    .expect("Failed to update agreement");

    let registry = PgRegistry::new(db);

    //* When
    let result = registry
        .get_declined_indexers_by_deployment(30, 1, 5, 1)
        .await
        .expect("Failed to get declined indexers");

    //* Then
    assert!(
        result.is_empty(),
        "an expiry we caused must not bench the indexer, got {result:?}"
    );
}

/// The mirror case: the offer did land, so the indexer had one on chain and let the window
/// close, which is their decision and does earn the standard bench.
#[tokio::test]
async fn get_declined_indexers_expiry_with_offer_tx_included_within_30_days() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");

    sqlx::query(
        r#"
        UPDATE dipper_reg_indexing_agreements
        SET updated_at = timezone('UTC', now()) - interval '31 days'
        "#,
    )
    .execute(&db)
    .await
    .expect("Failed to age the fixture agreements");

    let agreement_id =
        IndexingAgreementId::from_bytes([0xaa, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    sqlx::query(
        r#"
        UPDATE dipper_reg_indexing_agreements
        SET status = 5, rejection_reason = NULL,
            offer_tx_hash = decode(repeat('11', 32), 'hex'),
            updated_at = timezone('UTC', now()) - interval '1 hour'
        WHERE id = $1
        "#,
    )
    .bind(agreement_id)
    .execute(&db)
    .await
    .expect("Failed to update agreement");

    let registry = PgRegistry::new(db);

    //* When
    let result = registry
        .get_declined_indexers_by_deployment(30, 1, 5, 1)
        .await
        .expect("Failed to get declined indexers");

    //* Then
    let deployment_1a: DeploymentId = "QmAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA1a"
        .parse()
        .unwrap();
    let indexer_a = indexer_id!("1111111111111111111111111111111111111111");
    let declined = result
        .get(&deployment_1a)
        .expect("Deployment 1a should be in the declined list");
    assert!(
        declined.contains(&indexer_a),
        "an expiry after the offer landed should still bench the indexer, got {declined:?}"
    );
}

#[tokio::test]
async fn get_declined_indexers_capacity_exceeded_included_within_5_minutes() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");

    // Mark an agreement as rejected with CAPACITY_EXCEEDED (just now, within 5-min window)
    let agreement_id =
        IndexingAgreementId::from_bytes([0xaa, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    sqlx::query(
        r#"
        UPDATE dipper_reg_indexing_agreements
        SET status = 7, rejection_reason = 'CAPACITY_EXCEEDED', updated_at = timezone('UTC', now())
        WHERE id = $1
        "#,
    )
    .bind(agreement_id)
    .execute(&db)
    .await
    .expect("Failed to update agreement");

    let registry = PgRegistry::new(db);

    //* When
    let result = registry
        .get_declined_indexers_by_deployment(30, 1, 5, 1)
        .await
        .expect("Failed to get declined indexers");

    //* Then
    let deployment_1a: DeploymentId = "QmAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA1a"
        .parse()
        .unwrap();
    let indexer_a = indexer_id!("1111111111111111111111111111111111111111");
    assert!(
        result.contains_key(&deployment_1a),
        "Fresh CAPACITY_EXCEEDED rejection should be in declined list"
    );
    let declined = result.get(&deployment_1a).expect("Deployment not found");
    assert!(
        declined.contains(&indexer_a),
        "Indexer A should be in declined list for deployment 1a"
    );
}

#[tokio::test]
async fn get_declined_indexers_capacity_exceeded_excluded_after_5_minutes() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");

    // Mark an agreement as rejected with CAPACITY_EXCEEDED, set updated_at to 10 minutes ago
    let agreement_id =
        IndexingAgreementId::from_bytes([0xaa, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    sqlx::query(
        r#"
        UPDATE dipper_reg_indexing_agreements
        SET status = 7, rejection_reason = 'CAPACITY_EXCEEDED', updated_at = timezone('UTC', now()) - interval '10 minutes'
        WHERE id = $1
        "#,
    )
    .bind(agreement_id)
    .execute(&db)
    .await
    .expect("Failed to update agreement");

    let registry = PgRegistry::new(db);

    //* When
    // With 5-minute transient lookback, the 10-minute-old CAPACITY_EXCEEDED
    // rejection should NOT be included (it's outside the 5-minute window)
    let result = registry
        .get_declined_indexers_by_deployment(30, 1, 5, 1)
        .await
        .expect("Failed to get declined indexers");

    //* Then
    let deployment_1a: DeploymentId = "QmAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA1a"
        .parse()
        .unwrap();
    assert!(
        !result.contains_key(&deployment_1a),
        "10-minute-old CAPACITY_EXCEEDED rejection should not be in declined list (outside 5-minute window)"
    );
}

#[tokio::test]
async fn get_declined_indexers_invalid_signature_included_within_5_minutes() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");

    // Mark an agreement as rejected with INVALID_SIGNATURE (just now, within 5-min window)
    let agreement_id =
        IndexingAgreementId::from_bytes([0xaa, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    sqlx::query(
        r#"
        UPDATE dipper_reg_indexing_agreements
        SET status = 7, rejection_reason = 'INVALID_SIGNATURE', updated_at = timezone('UTC', now())
        WHERE id = $1
        "#,
    )
    .bind(agreement_id)
    .execute(&db)
    .await
    .expect("Failed to update agreement");

    let registry = PgRegistry::new(db);

    //* When
    let result = registry
        .get_declined_indexers_by_deployment(30, 1, 5, 1)
        .await
        .expect("Failed to get declined indexers");

    //* Then
    let deployment_1a: DeploymentId = "QmAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA1a"
        .parse()
        .unwrap();
    let indexer_a = indexer_id!("1111111111111111111111111111111111111111");
    assert!(
        result.contains_key(&deployment_1a),
        "Fresh INVALID_SIGNATURE rejection should be in declined list"
    );
    let declined = result.get(&deployment_1a).expect("Deployment not found");
    assert!(
        declined.contains(&indexer_a),
        "Indexer A should be in declined list for deployment 1a"
    );
}

#[tokio::test]
async fn get_declined_indexers_invalid_signature_excluded_after_5_minutes() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");

    // Mark an agreement as rejected with INVALID_SIGNATURE, set updated_at to 10 minutes ago.
    // A dipper-side signing fault clears once dipper is fixed, so the freeze is short.
    let agreement_id =
        IndexingAgreementId::from_bytes([0xaa, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    sqlx::query(
        r#"
        UPDATE dipper_reg_indexing_agreements
        SET status = 7, rejection_reason = 'INVALID_SIGNATURE', updated_at = timezone('UTC', now()) - interval '10 minutes'
        WHERE id = $1
        "#,
    )
    .bind(agreement_id)
    .execute(&db)
    .await
    .expect("Failed to update agreement");

    let registry = PgRegistry::new(db);

    //* When
    let result = registry
        .get_declined_indexers_by_deployment(30, 1, 5, 1)
        .await
        .expect("Failed to get declined indexers");

    //* Then
    let deployment_1a: DeploymentId = "QmAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA1a"
        .parse()
        .unwrap();
    assert!(
        !result.contains_key(&deployment_1a),
        "10-minute-old INVALID_SIGNATURE rejection should not be in declined list (outside 5-minute window)"
    );
}

#[tokio::test]
async fn get_declined_indexers_replay_detected_excluded_after_5_minutes() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");

    // Mark an agreement as rejected with REPLAY_DETECTED, set updated_at to 10 minutes ago.
    // Like INVALID_SIGNATURE this is a dipper-side fault, so the freeze is short.
    let agreement_id =
        IndexingAgreementId::from_bytes([0xaa, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    sqlx::query(
        r#"
        UPDATE dipper_reg_indexing_agreements
        SET status = 7, rejection_reason = 'REPLAY_DETECTED', updated_at = timezone('UTC', now()) - interval '10 minutes'
        WHERE id = $1
        "#,
    )
    .bind(agreement_id)
    .execute(&db)
    .await
    .expect("Failed to update agreement");

    let registry = PgRegistry::new(db);

    //* When
    let result = registry
        .get_declined_indexers_by_deployment(30, 1, 5, 1)
        .await
        .expect("Failed to get declined indexers");

    //* Then
    let deployment_1a: DeploymentId = "QmAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA1a"
        .parse()
        .unwrap();
    assert!(
        !result.contains_key(&deployment_1a),
        "10-minute-old REPLAY_DETECTED rejection should not be in declined list (outside 5-minute window)"
    );
}

#[tokio::test]
async fn get_declined_indexers_sender_not_trusted_included_within_1_day() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");

    // SENDER_NOT_TRUSTED 12 hours ago: inside the 1-day uncertain window.
    let agreement_id =
        IndexingAgreementId::from_bytes([0xaa, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    sqlx::query(
        r#"
        UPDATE dipper_reg_indexing_agreements
        SET status = 7, rejection_reason = 'SENDER_NOT_TRUSTED', updated_at = timezone('UTC', now()) - interval '12 hours'
        WHERE id = $1
        "#,
    )
    .bind(agreement_id)
    .execute(&db)
    .await
    .expect("Failed to update agreement");

    let registry = PgRegistry::new(db);

    //* When
    let result = registry
        .get_declined_indexers_by_deployment(30, 1, 5, 1)
        .await
        .expect("Failed to get declined indexers");

    //* Then
    let deployment_1a: DeploymentId = "QmAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA1a"
        .parse()
        .unwrap();
    let indexer_a = indexer_id!("1111111111111111111111111111111111111111");
    assert!(
        result.contains_key(&deployment_1a),
        "12-hour-old SENDER_NOT_TRUSTED rejection should be in declined list (within 1-day window)"
    );
    let declined = result.get(&deployment_1a).expect("Deployment not found");
    assert!(
        declined.contains(&indexer_a),
        "Indexer A should be in declined list for deployment 1a"
    );
}

#[tokio::test]
async fn get_declined_indexers_sender_not_trusted_excluded_after_1_day() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");

    // SENDER_NOT_TRUSTED 2 days ago: outside the 1-day window. A 30-day window
    // would still include it, so this proves the shorter uncertain tier applies.
    let agreement_id =
        IndexingAgreementId::from_bytes([0xaa, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    sqlx::query(
        r#"
        UPDATE dipper_reg_indexing_agreements
        SET status = 7, rejection_reason = 'SENDER_NOT_TRUSTED', updated_at = timezone('UTC', now()) - interval '2 days'
        WHERE id = $1
        "#,
    )
    .bind(agreement_id)
    .execute(&db)
    .await
    .expect("Failed to update agreement");

    let registry = PgRegistry::new(db);

    //* When
    let result = registry
        .get_declined_indexers_by_deployment(30, 1, 5, 1)
        .await
        .expect("Failed to get declined indexers");

    //* Then
    let deployment_1a: DeploymentId = "QmAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA1a"
        .parse()
        .unwrap();
    assert!(
        !result.contains_key(&deployment_1a),
        "2-day-old SENDER_NOT_TRUSTED rejection should not be in declined list (outside 1-day window)"
    );
}

#[tokio::test]
async fn get_declined_indexers_unspecified_included_within_1_day() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");

    // UNSPECIFIED 12 hours ago: the catch-all (also the no-outcome path) shares
    // the 1-day uncertain window, so a fresh one is still excluded from selection.
    let agreement_id =
        IndexingAgreementId::from_bytes([0xaa, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    sqlx::query(
        r#"
        UPDATE dipper_reg_indexing_agreements
        SET status = 7, rejection_reason = 'UNSPECIFIED', updated_at = timezone('UTC', now()) - interval '12 hours'
        WHERE id = $1
        "#,
    )
    .bind(agreement_id)
    .execute(&db)
    .await
    .expect("Failed to update agreement");

    let registry = PgRegistry::new(db);

    //* When
    let result = registry
        .get_declined_indexers_by_deployment(30, 1, 5, 1)
        .await
        .expect("Failed to get declined indexers");

    //* Then
    let deployment_1a: DeploymentId = "QmAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA1a"
        .parse()
        .unwrap();
    let indexer_a = indexer_id!("1111111111111111111111111111111111111111");
    assert!(
        result.contains_key(&deployment_1a),
        "12-hour-old UNSPECIFIED rejection should be in declined list (within 1-day window)"
    );
    let declined = result.get(&deployment_1a).expect("Deployment not found");
    assert!(
        declined.contains(&indexer_a),
        "Indexer A should be in declined list for deployment 1a"
    );
}

#[tokio::test]
async fn get_declined_indexers_unspecified_excluded_after_1_day() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");

    // UNSPECIFIED 2 days ago: outside the 1-day window. A 30-day window would
    // still include it, so this proves the shorter uncertain tier applies.
    let agreement_id =
        IndexingAgreementId::from_bytes([0xaa, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    sqlx::query(
        r#"
        UPDATE dipper_reg_indexing_agreements
        SET status = 7, rejection_reason = 'UNSPECIFIED', updated_at = timezone('UTC', now()) - interval '2 days'
        WHERE id = $1
        "#,
    )
    .bind(agreement_id)
    .execute(&db)
    .await
    .expect("Failed to update agreement");

    let registry = PgRegistry::new(db);

    //* When
    let result = registry
        .get_declined_indexers_by_deployment(30, 1, 5, 1)
        .await
        .expect("Failed to get declined indexers");

    //* Then
    let deployment_1a: DeploymentId = "QmAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA1a"
        .parse()
        .unwrap();
    assert!(
        !result.contains_key(&deployment_1a),
        "2-day-old UNSPECIFIED rejection should not be in declined list (outside 1-day window)"
    );
}

#[tokio::test]
async fn get_declined_indexers_indexer_unavailable_excluded_after_5_minutes() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");

    // INDEXER_UNAVAILABLE 10 minutes ago: a transient reason, so outside the
    // 5-minute window it is dropped and the indexer becomes selectable again.
    let agreement_id =
        IndexingAgreementId::from_bytes([0xaa, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    sqlx::query(
        r#"
        UPDATE dipper_reg_indexing_agreements
        SET status = 7, rejection_reason = 'INDEXER_UNAVAILABLE', updated_at = timezone('UTC', now()) - interval '10 minutes'
        WHERE id = $1
        "#,
    )
    .bind(agreement_id)
    .execute(&db)
    .await
    .expect("Failed to update agreement");

    let registry = PgRegistry::new(db);

    //* When
    let result = registry
        .get_declined_indexers_by_deployment(30, 1, 5, 1)
        .await
        .expect("Failed to get declined indexers");

    //* Then
    let deployment_1a: DeploymentId = "QmAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA1a"
        .parse()
        .unwrap();
    assert!(
        !result.contains_key(&deployment_1a),
        "10-minute-old INDEXER_UNAVAILABLE rejection should not be in declined list (outside 5-minute window)"
    );
}

#[tokio::test]
async fn get_declined_indexers_manifest_too_large_uses_30_day_window() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");

    // MANIFEST_TOO_LARGE 15 days ago: a structural reason kept in the 30-day
    // window, so a 15-day-old rejection is still excluded from selection.
    let agreement_id =
        IndexingAgreementId::from_bytes([0xaa, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    sqlx::query(
        r#"
        UPDATE dipper_reg_indexing_agreements
        SET status = 7, rejection_reason = 'MANIFEST_TOO_LARGE', updated_at = timezone('UTC', now()) - interval '15 days'
        WHERE id = $1
        "#,
    )
    .bind(agreement_id)
    .execute(&db)
    .await
    .expect("Failed to update agreement");

    let registry = PgRegistry::new(db);

    //* When
    let result = registry
        .get_declined_indexers_by_deployment(30, 1, 5, 1)
        .await
        .expect("Failed to get declined indexers");

    //* Then
    let deployment_1a: DeploymentId = "QmAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA1a"
        .parse()
        .unwrap();
    let indexer_a = indexer_id!("1111111111111111111111111111111111111111");
    assert!(
        result.contains_key(&deployment_1a),
        "15-day-old MANIFEST_TOO_LARGE rejection should be in declined list (within 30-day window)"
    );
    let declined = result.get(&deployment_1a).expect("Deployment not found");
    assert!(
        declined.contains(&indexer_a),
        "Indexer A should be in declined list for deployment 1a"
    );
}

// =============================================================================
// apply_reconciliation_batch tests
//
// These exercise the production batch SQL path that the chain_listener loop
// drives every poll. The chain_listener-level tests use a MockRegistry whose
// batch falls back to the per-row trait default, so without these the real
// `apply_reconciliation_batch` SQL is only validated by the single-row
// `apply_reconciliation` tests above.
// =============================================================================

#[tokio::test]
async fn apply_reconciliation_batch_handles_all_four_item_shapes() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");
    let registry = PgRegistry::new(db);

    let accept_only_id =
        IndexingAgreementId::from_bytes([0xaa, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    let cancel_by_indexer_id =
        IndexingAgreementId::from_bytes([0xaa, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2]);
    let paired_id =
        IndexingAgreementId::from_bytes([0xbb, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    let recover_expired_id =
        IndexingAgreementId::from_bytes([0xcc, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);

    let items = vec![
        // accept-only: Created -> AcceptedOnChain
        ReconciliationItem {
            agreement_id: accept_only_id,
            apply_accept: true,
            cancel: None,
            audit: ReconciliationAudit::default(),
        },
        // cancel-only by indexer: AcceptedOnChain -> CanceledByIndexer
        ReconciliationItem {
            agreement_id: cancel_by_indexer_id,
            apply_accept: false,
            cancel: Some(CancelKind::ByIndexer),
            audit: ReconciliationAudit::default(),
        },
        // paired accept+cancel-by-requester: Created -> AcceptedOnChain -> CanceledByRequester
        ReconciliationItem {
            agreement_id: paired_id,
            apply_accept: true,
            cancel: Some(CancelKind::ByRequester),
            audit: ReconciliationAudit::default(),
        },
        // accept-only recovery from Expired
        ReconciliationItem {
            agreement_id: recover_expired_id,
            apply_accept: true,
            cancel: None,
            audit: ReconciliationAudit::default(),
        },
    ];

    //* When
    let outcomes = registry
        .apply_reconciliation_batch(&items)
        .await
        .expect("batch should succeed");

    //* Then
    // Every input id appears in the outcome map.
    assert_eq!(
        outcomes.len(),
        items.len(),
        "outcome map covers every input"
    );

    let accept_only = outcomes.get(&accept_only_id).copied().unwrap_or_default();
    assert!(accept_only.did_accept);
    assert!(!accept_only.did_cancel);

    let cancel_only = outcomes
        .get(&cancel_by_indexer_id)
        .copied()
        .unwrap_or_default();
    assert!(!cancel_only.did_accept);
    assert!(cancel_only.did_cancel);

    let paired = outcomes.get(&paired_id).copied().unwrap_or_default();
    assert!(paired.did_accept);
    assert!(paired.did_cancel);

    let recover = outcomes
        .get(&recover_expired_id)
        .copied()
        .unwrap_or_default();
    assert!(recover.did_accept);
    assert!(!recover.did_cancel);

    // Final on-disk statuses match the outcomes.
    let final_status = |id: &IndexingAgreementId| {
        let registry = registry.clone();
        let id = *id;
        async move {
            registry
                .get_indexing_agreement_by_id(&id)
                .await
                .expect("get failed")
                .expect("agreement missing")
                .status
        }
    };
    assert_eq!(
        final_status(&accept_only_id).await,
        IndexingAgreementStatus::AcceptedOnChain
    );
    assert_eq!(
        final_status(&cancel_by_indexer_id).await,
        IndexingAgreementStatus::CanceledByIndexer
    );
    assert_eq!(
        final_status(&paired_id).await,
        IndexingAgreementStatus::CanceledByRequester
    );
    assert_eq!(
        final_status(&recover_expired_id).await,
        IndexingAgreementStatus::AcceptedOnChain
    );
}

#[tokio::test]
async fn pending_emission_queries_cover_the_paired_accept_then_cancel() {
    // Regression guard: an agreement accepted and then cancelled in a single
    // snapshot (paired accept+cancel -- dipper was behind) ends up terminal, but
    // must still surface for BOTH the accepted and terminated emission sweeps. The
    // accepted event must not be lost just because the row is no longer
    // AcceptedOnChain.
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");
    let registry = PgRegistry::new(db);

    let paired_id =
        IndexingAgreementId::from_bytes([0xbb, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);

    // Paired accept+cancel with the on-chain audit populated, as the chain_listener
    // does from the observed snapshot.
    let items = vec![ReconciliationItem {
        agreement_id: paired_id,
        apply_accept: true,
        cancel: Some(CancelKind::ByRequester),
        audit: ReconciliationAudit {
            accepted_at: Some(1_700_000_001),
            accepted_tx: Some("0xacc".to_string()),
            canceled_at: Some(1_700_000_002),
            canceled_by: Some("0xcxl".to_string()),
            canceled_tx: Some("0xcxltx".to_string()),
        },
    }];
    let outcomes = registry
        .apply_reconciliation_batch(&items)
        .await
        .expect("batch should succeed");
    let paired = outcomes.get(&paired_id).copied().unwrap_or_default();
    assert!(
        paired.did_accept && paired.did_cancel,
        "paired accept+cancel applied"
    );

    // The row is now CanceledByRequester, yet it must appear in BOTH pending sets.
    let pending_accepted = registry
        .get_agreements_pending_accepted_emission(100)
        .await
        .expect("accepted query");
    assert!(
        pending_accepted.iter().any(|p| p.agreement_id == paired_id),
        "accepted event must NOT be lost for an accepted-then-cancelled agreement"
    );
    let pending_terminated = registry
        .get_agreements_pending_terminated_emission(100)
        .await
        .expect("terminated query");
    assert!(
        pending_terminated
            .iter()
            .any(|p| p.agreement_id == paired_id),
        "terminated event must surface for the cancelled agreement"
    );

    // After stamping each marker, neither query returns the row again (emit-once).
    registry
        .mark_accepted_event_emitted(&paired_id)
        .await
        .expect("mark accepted");
    registry
        .mark_terminated_event_emitted(&paired_id)
        .await
        .expect("mark terminated");
    assert!(
        !registry
            .get_agreements_pending_accepted_emission(100)
            .await
            .expect("accepted query")
            .iter()
            .any(|p| p.agreement_id == paired_id),
        "accepted marker prevents re-emit"
    );
    assert!(
        !registry
            .get_agreements_pending_terminated_emission(100)
            .await
            .expect("terminated query")
            .iter()
            .any(|p| p.agreement_id == paired_id),
        "terminated marker prevents re-emit"
    );
}

#[tokio::test]
async fn apply_reconciliation_batch_no_op_cas_miss_present_with_default_outcome() {
    // Documents the contract the chain_listener relies on to distinguish
    // "row CAS-guard didn't match" (default outcome present) from "whole
    // batch failed" (id missing from outcome map). Without the pre-fill,
    // the listener's batch-fail double-count fix would misclassify every
    // CAS no-op as a batch failure.

    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");
    let registry = PgRegistry::new(db);

    // Already in CanceledByIndexer; cancel-by-indexer's allowed_from is
    // [AcceptedOnChain] so this UPDATE will not match.
    let already_canceled_id =
        IndexingAgreementId::from_bytes([0xaa, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3]);
    let items = vec![ReconciliationItem {
        agreement_id: already_canceled_id,
        apply_accept: false,
        cancel: Some(CancelKind::ByIndexer),
        audit: ReconciliationAudit::default(),
    }];

    //* When
    let outcomes = registry
        .apply_reconciliation_batch(&items)
        .await
        .expect("batch should succeed even with CAS no-op rows");

    //* Then
    let outcome = outcomes
        .get(&already_canceled_id)
        .copied()
        .expect("CAS no-op id must be present in outcome map (default), not absent");
    assert!(!outcome.did_accept);
    assert!(!outcome.did_cancel);
}

#[tokio::test]
async fn apply_reconciliation_batch_empty_input_is_ok() {
    let (db, _temp_db) = temp_registry_db().await;
    let registry = PgRegistry::new(db);

    let outcomes = registry
        .apply_reconciliation_batch(&[])
        .await
        .expect("empty batch is a fast-path Ok");
    assert!(outcomes.is_empty());
}

#[tokio::test]
async fn apply_reconciliation_batch_paired_rolls_back_on_unmatched_cancel() {
    // The Accept-then-Cancel-in-one-snapshot invariant: if the accept
    // landed but the paired cancel matched no row, commit would leave
    // an AcceptedOnChain visible to concurrent readers without its
    // follow-up cancel. The batch must roll back the whole tx so the
    // accept doesn't land alone.

    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    run_fixture(
        &db,
        include_str!("fixtures/0003_multi_indexer_agreements.sql"),
    )
    .await
    .expect("Failed to run fixture");
    let registry = PgRegistry::new(db);

    let created_id =
        IndexingAgreementId::from_bytes([0xaa, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    // Both cancel kinds' allowed_from already includes AcceptedOnChain,
    // so a paired accept that lands always permits the cancel — the
    // rollback path is unreachable from valid input under the current
    // allowed_from sets. Asserts the corresponding success invariant.
    let items = vec![ReconciliationItem {
        agreement_id: created_id,
        apply_accept: true,
        cancel: Some(CancelKind::ByIndexer),
        audit: ReconciliationAudit::default(),
    }];

    //* When
    let outcomes = registry
        .apply_reconciliation_batch(&items)
        .await
        .expect("paired accept+cancel on Created row succeeds end-to-end");

    //* Then
    let outcome = outcomes.get(&created_id).copied().expect("id present");
    assert!(outcome.did_accept);
    assert!(outcome.did_cancel);
    let final_status = registry
        .get_indexing_agreement_by_id(&created_id)
        .await
        .expect("get failed")
        .expect("agreement missing")
        .status;
    assert_eq!(final_status, IndexingAgreementStatus::CanceledByIndexer);
}

#[tokio::test]
async fn count_created_agreements_by_indexer_counts_only_created() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    let registry = PgRegistry::new(db);

    let requested_by = address!("8f8c426f956876325b1e037c6eae9b189952994c");
    let deployments = [
        deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv"),
        deployment_id!("QmXbNL4EMkQ6DAPUcBjYSDXZJdpu1Kb1XkKvNvS8JdT7Hs"),
        deployment_id!("QmYbNL4EMkQ6DAPUcBjYSDXZJdpu1Kb1XkKvNvS8JdT7Hs"),
        deployment_id!("QmZbNL4EMkQ6DAPUcBjYSDXZJdpu1Kb1XkKvNvS8JdT7Hs"),
    ];
    let indexer_a = indexer_id!("3c584ee1d89f43c6ccee17e886a001de2bb4d8a9");
    let indexer_b = indexer_id!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let indexer_c = indexer_id!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    let indexer_url: Url = "http://localhost:8020".parse().expect("Invalid URL");

    // Register one agreement per (indexer, deployment) and return its id, so
    // each is a distinct Created row respecting the per-pair unique index.
    async fn seed(
        registry: &PgRegistry,
        requested_by: thegraph_core::alloy::primitives::Address,
        deployment_id: DeploymentId,
        indexer_id: IndexerId,
        indexer_url: Url,
    ) -> IndexingAgreementId {
        let request_id = match registry
            .set_indexing_target_candidates(requested_by, deployment_id, 42161, 3)
            .await
            .expect("set target candidates")
        {
            dipper_pgregistry::IndexingRequestSetTargetOutcome::Inserted { id }
            | dipper_pgregistry::IndexingRequestSetTargetOutcome::Updated { id, .. }
            | dipper_pgregistry::IndexingRequestSetTargetOutcome::NoOp { id } => id,
            other => panic!("unexpected set-target outcome: {other:?}"),
        };
        let mut terms = Faker.fake::<IndexingAgreementTerms>();
        terms.metadata.subgraph_deployment_id = deployment_id;
        registry
            .register_new_indexing_agreement(NewAgreementParams {
                agreement_id: Faker.fake::<IndexingAgreementId>(),
                nonce_uuid: uuid::Uuid::now_v7(),
                request_id,
                deployment_id,
                indexer_id,
                indexer_url,
                terms,
                terms_version_hash: None,
            })
            .await
            .expect("register agreement")
    }

    // Indexer A: 2 Created (deployments 0,1). Indexer B: 1 Created + 1 that
    // we flip to AcceptedOnChain (deployments 0,1). Indexer C: 1 that we flip
    // to Expired (deployment 0). Only Created rows should be counted.
    let _a0 = seed(
        &registry,
        requested_by,
        deployments[0],
        indexer_a,
        indexer_url.clone(),
    )
    .await;
    let _a1 = seed(
        &registry,
        requested_by,
        deployments[1],
        indexer_a,
        indexer_url.clone(),
    )
    .await;
    let _b0 = seed(
        &registry,
        requested_by,
        deployments[0],
        indexer_b,
        indexer_url.clone(),
    )
    .await;
    let b1 = seed(
        &registry,
        requested_by,
        deployments[1],
        indexer_b,
        indexer_url.clone(),
    )
    .await;
    let c0 = seed(
        &registry,
        requested_by,
        deployments[0],
        indexer_c,
        indexer_url.clone(),
    )
    .await;

    registry
        .apply_reconciliation(&b1, true, None)
        .await
        .expect("flip b1 to AcceptedOnChain");
    registry
        .mark_indexing_agreement_as_expired(&c0)
        .await
        .expect("flip c0 to Expired");

    //* When
    let (per_indexer, global) = registry
        .count_created_agreements_by_indexer()
        .await
        .expect("count created agreements");

    //* Then
    assert_eq!(
        per_indexer.get(&indexer_a).copied(),
        Some(2),
        "A has 2 Created"
    );
    assert_eq!(
        per_indexer.get(&indexer_b).copied(),
        Some(1),
        "B has 1 Created"
    );
    assert_eq!(
        per_indexer.get(&indexer_c).copied(),
        None,
        "C's only row is Expired, so it should be absent"
    );
    assert_eq!(global, 3, "global counts only the 3 Created rows");
}

#[tokio::test]
async fn kafka_consumer_offsets_roundtrip_and_upsert() {
    //* Given
    let (db, _temp_db) = temp_registry_db().await;
    let registry = PgRegistry::new(db);

    let topic = "studio.subgraph.indexing.requests";

    //* When / Then - no offset recorded yet
    let offset = registry
        .get_kafka_consumer_offset(topic, 0)
        .await
        .expect("get offset");
    assert_eq!(offset, None, "a fresh partition has no recorded offset");

    //* When / Then - first write inserts
    registry
        .set_kafka_consumer_offset(topic, 0, 5)
        .await
        .expect("set offset");
    let offset = registry
        .get_kafka_consumer_offset(topic, 0)
        .await
        .expect("get offset");
    assert_eq!(offset, Some(5));

    //* When / Then - second write updates in place
    registry
        .set_kafka_consumer_offset(topic, 0, 42)
        .await
        .expect("update offset");
    let offset = registry
        .get_kafka_consumer_offset(topic, 0)
        .await
        .expect("get offset");
    assert_eq!(offset, Some(42));

    //* When / Then - partitions and topics are independent keys
    registry
        .set_kafka_consumer_offset(topic, 7, 1)
        .await
        .expect("set offset on another partition");
    registry
        .set_kafka_consumer_offset("another.topic", 0, 9)
        .await
        .expect("set offset on another topic");
    assert_eq!(
        registry
            .get_kafka_consumer_offset(topic, 0)
            .await
            .expect("get offset"),
        Some(42)
    );
    assert_eq!(
        registry
            .get_kafka_consumer_offset(topic, 7)
            .await
            .expect("get offset"),
        Some(1)
    );
    assert_eq!(
        registry
            .get_kafka_consumer_offset("another.topic", 0)
            .await
            .expect("get offset"),
        Some(9)
    );
}
