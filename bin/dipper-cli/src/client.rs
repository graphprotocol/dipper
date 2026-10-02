pub use dipper_rpc::admin::{
    indexing_agreements::IndexingAgreementsRpcClient, indexing_requests::IndexingRequestsRpcClient,
};
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use url::Url;

/// Create a new JSON-RPC HTTP client.
#[expect(
    clippy::unwrap_used,
    reason = "predates this lint; fix when next touched"
)]
pub fn new(url: &Url) -> HttpClient {
    HttpClientBuilder::new()
        .set_tcp_no_delay(true)
        .build(url)
        .unwrap()
}
