//! Integration tests for the indexer URLs service against a wiremock subgraph:
//! `fetch_snapshot` (auth, pagination, invalid-entry skipping, error surfacing)
//! and the refresh loop (bad or empty refreshes preserve, good ones replace).

use std::time::Duration;

use thegraph_core::IndexerId;
use url::Url;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{body_string_contains, header, method},
};

use crate::network::service::indexer_urls::{Ctx, Handle, Snapshot, fetch_snapshot, new};

const ZERO_CURSOR: &str = "0x0000000000000000000000000000000000000000";

/// 20-byte hex address with `n` as its numeric value.
fn addr(n: u32) -> String {
    format!("0x{n:040x}")
}

fn page_body(indexers: &[(String, String)]) -> serde_json::Value {
    let entries: Vec<_> = indexers
        .iter()
        .map(|(id, url)| serde_json::json!({ "id": id, "url": url }))
        .collect();
    serde_json::json!({ "data": { "indexers": entries } })
}

#[tokio::test]
async fn fetch_paginates_past_a_full_page_and_skips_invalid_entries() {
    //* Given
    // A full first page of 1,000 indexers forces a second request; the
    // second page mixes a valid entry with an invalid URL and a bad id.
    let server = MockServer::start().await;

    let page1: Vec<_> = (1..=1000)
        .map(|n| (addr(n), format!("https://indexer-{n}.example.com/")))
        .collect();
    let page2 = vec![
        (addr(1001), "https://indexer-1001.example.com/".to_string()),
        (addr(1002), "not a url".to_string()),
        (
            "not-an-address".to_string(),
            "https://x.example.com/".to_string(),
        ),
    ];

    Mock::given(method("POST"))
        .and(header("authorization", "Bearer it-test-key"))
        .and(body_string_contains(ZERO_CURSOR))
        .respond_with(ResponseTemplate::new(200).set_body_json(page_body(&page1)))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(header("authorization", "Bearer it-test-key"))
        .and(body_string_contains(addr(1000)))
        .respond_with(ResponseTemplate::new(200).set_body_json(page_body(&page2)))
        .mount(&server)
        .await;

    //* When
    let client = reqwest::Client::new();
    let endpoint = server.uri().parse().unwrap();
    let snapshot = fetch_snapshot(&client, &endpoint, Some("it-test-key"))
        .await
        .expect("fetch should succeed");

    //* Then
    // 1,000 from page 1 plus the single valid entry from page 2.
    assert_eq!(snapshot.len(), 1_001);
    let valid: IndexerId = addr(1001).parse().unwrap();
    assert_eq!(
        snapshot.get(&valid).map(|url| url.as_str()),
        Some("https://indexer-1001.example.com/")
    );
    let invalid: IndexerId = addr(1002).parse().unwrap();
    assert!(!snapshot.contains_key(&invalid));
}

#[tokio::test]
async fn fetch_without_api_key_sends_no_authorization_header() {
    //* Given
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page_body(&[(
            addr(7),
            "https://indexer-7.example.com/".to_string(),
        )])))
        .mount(&server)
        .await;

    //* When
    let client = reqwest::Client::new();
    let endpoint = server.uri().parse().unwrap();
    let snapshot = fetch_snapshot(&client, &endpoint, None)
        .await
        .expect("fetch should succeed");

    //* Then
    assert_eq!(snapshot.len(), 1);
    let received = server.received_requests().await.unwrap();
    assert!(
        received
            .iter()
            .all(|req| !req.headers.contains_key("authorization"))
    );
}

#[tokio::test]
async fn fetch_surfaces_graphql_errors() {
    //* Given
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "errors": [{ "message": "Type `Bytes` is not a valid input type" }]
        })))
        .mount(&server)
        .await;

    //* When
    let client = reqwest::Client::new();
    let endpoint = server.uri().parse().unwrap();
    let result = fetch_snapshot(&client, &endpoint, None).await;

    //* Then
    let err = result.expect_err("fetch should fail").to_string();
    assert!(err.contains("subgraph errors"), "unexpected error: {err}");
}

#[tokio::test]
async fn fetch_names_the_http_status_of_a_refused_request() {
    //* Given
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(429).set_body_string("Too many requests"))
        .mount(&server)
        .await;

    //* When
    let client = reqwest::Client::new();
    let endpoint = server.uri().parse().unwrap();
    let result = fetch_snapshot(&client, &endpoint, None).await;

    //* Then
    let err = result.expect_err("fetch should fail").to_string();
    assert!(err.contains("429"), "unexpected error: {err}");
}

/// Spawn the refresh service against `server` with a 50 ms interval and a
/// single-entry init snapshot; returns the handle and the init entry.
fn spawn_service(server: &MockServer) -> (Handle, IndexerId, Url) {
    let id: IndexerId = addr(1).parse().unwrap();
    let url: Url = "https://indexer-1.example.com/".parse().unwrap();
    let init = Snapshot::from([(id, url.clone())]);

    let (handle, service) = new(
        Ctx {
            endpoint: server.uri().parse().unwrap(),
            api_key: None,
            update_interval: Duration::from_millis(50),
        },
        init,
    );
    tokio::spawn(service);
    (handle, id, url)
}

/// Wait until the mock server has seen at least `count` refresh requests.
async fn wait_for_requests(server: &MockServer, count: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if server.received_requests().await.unwrap().len() >= count {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {count} refresh requests"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn refresh_with_zero_indexers_preserves_the_previous_snapshot() {
    //* Given
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page_body(&[])))
        .mount(&server)
        .await;

    let (handle, id, url) = spawn_service(&server);

    //* When
    // Let at least 2 empty refreshes complete.
    wait_for_requests(&server, 2).await;

    //* Then
    assert_eq!(handle.get_indexer_url(&id), Some(url));
    handle.stop().await;
}

#[tokio::test]
async fn failed_refresh_preserves_the_previous_snapshot() {
    //* Given
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let (handle, id, url) = spawn_service(&server);

    //* When
    // Let at least 2 failing refreshes complete.
    wait_for_requests(&server, 2).await;

    //* Then
    assert_eq!(handle.get_indexer_url(&id), Some(url));
    handle.stop().await;
}

#[tokio::test]
async fn successful_refresh_replaces_the_snapshot() {
    //* Given
    // The subgraph now reports a different indexer than the init snapshot's.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page_body(&[(
            addr(2),
            "https://indexer-2.example.com/".to_string(),
        )])))
        .mount(&server)
        .await;

    let (handle, init_id, _) = spawn_service(&server);

    //* When
    let new_id: IndexerId = addr(2).parse().unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if handle.get_indexer_url(&new_id).is_some() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for the refreshed snapshot"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    //* Then
    // The snapshot is replaced wholesale, not merged into.
    assert_eq!(
        handle.get_indexer_url(&new_id).map(|url| url.to_string()),
        Some("https://indexer-2.example.com/".to_string())
    );
    assert_eq!(handle.get_indexer_url(&init_id), None);
    handle.stop().await;
}
