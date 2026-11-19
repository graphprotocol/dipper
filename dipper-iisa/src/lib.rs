mod api;
pub mod http_client;

pub use api::{
    CandidateSelection, DipsAcceptingSnapshot, PriceCeilings, SelectedIndexer, SelectionContext,
    SelectionError,
};
pub use http_client::{HttpClientConfig, HttpIisaClient};

/// The counter of requests to IISA, by `endpoint` and `outcome` (ok, rejected, unavailable, error).
pub const METRIC_REQUESTS: &str = "dipper_iisa_requests_total";
