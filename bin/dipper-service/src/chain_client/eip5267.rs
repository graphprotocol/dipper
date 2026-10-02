//! Fetch the RCA EIP-712 domain from the deployed RecurringCollector via
//! EIP-5267 (`eip712Domain()`), keeping dipper's proposal signing and offer
//! hashing aligned with the contract even across in-place proxy upgrades.

use thegraph_core::alloy::{
    primitives::{Address, U256},
    providers::Provider,
    rpc::types::TransactionRequest,
    sol_types::{Eip712Domain, SolCall},
};

use super::{abi::IRecurringCollector, rpc_provider::RpcProviderPool};
use crate::{chain_client::ChainClientError, config::ChainClientConfig};

/// Upper bound on the fetched domain name/version length. Generous for any
/// legitimate contract; rejects garbage from a wrong-address eth_call.
const MAX_DOMAIN_FIELD_LEN: usize = 256;

/// Fetch and validate the RecurringCollector's EIP-712 domain via EIP-5267.
/// Transient RPC failures retry and rotate through the configured provider
/// pool; decode and validation failures are deterministic and fail fast.
pub async fn fetch_rca_eip712_domain(
    config: &ChainClientConfig,
    chain_id: u64,
    recurring_collector: Address,
) -> Result<Eip712Domain, ChainClientError> {
    let pool = RpcProviderPool::new(
        config.providers.clone(),
        config.request_timeout,
        config.max_retries,
    )?;

    let calldata = IRecurringCollector::eip712DomainCall {}.abi_encode();
    let output = pool
        .execute("eip712_domain", |provider| {
            let calldata = calldata.clone();
            async move {
                let tx = TransactionRequest::default()
                    .to(recurring_collector)
                    .input(calldata.into());
                provider.call(tx).await
            }
        })
        .await?;

    let report =
        IRecurringCollector::eip712DomainCall::abi_decode_returns(&output).map_err(|err| {
            ChainClientError::ConfigError(format!(
                "contract at {recurring_collector} returned undecodable eip712Domain() \
                 data (not EIP-5267 compliant?): {err}"
            ))
        })?;

    let domain = domain_from_report(report, chain_id, recurring_collector)?;
    tracing::info!(
        name = %domain.name.as_deref().unwrap_or_default(),
        version = %domain.version.as_deref().unwrap_or_default(),
        chain_id,
        verifying_contract = %recurring_collector,
        "fetched RCA EIP-712 domain from RecurringCollector"
    );
    Ok(domain)
}

/// Re-fetch the domain and swap it into `shared` when it changed, so a running
/// dipper follows an in-place contract upgrade without a restart. Returns
/// whether the domain changed; on error the current domain stays in place.
#[expect(
    clippy::expect_used,
    reason = "predates this lint; fix when next touched"
)]
pub async fn refresh_rca_eip712_domain(
    config: &ChainClientConfig,
    chain_id: u64,
    recurring_collector: Address,
    shared: &std::sync::RwLock<Eip712Domain>,
) -> Result<bool, ChainClientError> {
    let fetched = fetch_rca_eip712_domain(config, chain_id, recurring_collector).await?;
    let mut current = shared.write().expect("RCA domain lock poisoned");
    if *current == fetched {
        return Ok(false);
    }
    tracing::warn!(
        old_name = %current.name.as_deref().unwrap_or_default(),
        old_version = %current.version.as_deref().unwrap_or_default(),
        new_name = %fetched.name.as_deref().unwrap_or_default(),
        new_version = %fetched.version.as_deref().unwrap_or_default(),
        "RecurringCollector EIP-712 domain changed; switching to the new domain"
    );
    *current = fetched;
    Ok(true)
}

/// Validate an EIP-5267 report against the configured chain id and contract
/// address, and build the domain dipper signs and hashes RCAs under.
fn domain_from_report(
    report: IRecurringCollector::eip712DomainReturn,
    expected_chain_id: u64,
    recurring_collector: Address,
) -> Result<Eip712Domain, ChainClientError> {
    // 0x0f = name | version | chainId | verifyingContract, the exact field
    // set rca_eip712_domain hashes. Any other bitmap (e.g. a salt bit) means
    // the contract's domain has a shape dipper's signing wouldn't reproduce.
    if report.fields.0 != [0x0f] {
        return Err(ChainClientError::ConfigError(format!(
            "RecurringCollector at {recurring_collector} reports EIP-712 domain field \
             bitmap {:#04x}, expected 0x0f (name, version, chainId, verifyingContract)",
            report.fields.0[0]
        )));
    }
    if !report.extensions.is_empty() {
        return Err(ChainClientError::ConfigError(format!(
            "RecurringCollector at {recurring_collector} reports {} EIP-5267 domain \
             extensions; dipper cannot reproduce extended domains",
            report.extensions.len()
        )));
    }
    for (label, value) in [("name", &report.name), ("version", &report.version)] {
        if value.is_empty() || value.len() > MAX_DOMAIN_FIELD_LEN {
            return Err(ChainClientError::ConfigError(format!(
                "RecurringCollector at {recurring_collector} reports an EIP-712 domain \
                 {label} of {} bytes; expected 1..={MAX_DOMAIN_FIELD_LEN}",
                value.len()
            )));
        }
    }
    if report.chainId != U256::from(expected_chain_id) {
        return Err(ChainClientError::ConfigError(format!(
            "RecurringCollector at {recurring_collector} reports chain id {}, but dipper \
             is configured for chain id {expected_chain_id}",
            report.chainId
        )));
    }
    if report.verifyingContract != recurring_collector {
        return Err(ChainClientError::ConfigError(format!(
            "RecurringCollector at {recurring_collector} reports verifying contract {}; \
             the configured address is likely a proxy admin or the wrong contract",
            report.verifyingContract
        )));
    }

    Ok(Eip712Domain::new(
        Some(report.name.into()),
        Some(report.version.into()),
        Some(U256::from(expected_chain_id)),
        Some(recurring_collector),
        None,
    ))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use dipper_rpc::indexer::rca_eip712_domain;
    use thegraph_core::alloy::{
        hex,
        primitives::{B256, FixedBytes},
    };
    use url::Url;
    use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate, matchers::method};

    use super::*;

    fn valid_report(
        chain_id: u64,
        recurring_collector: Address,
    ) -> IRecurringCollector::eip712DomainReturn {
        IRecurringCollector::eip712DomainReturn {
            fields: FixedBytes::from([0x0f]),
            name: "RecurringCollector".to_string(),
            version: "1".to_string(),
            chainId: U256::from(chain_id),
            verifyingContract: recurring_collector,
            salt: B256::ZERO,
            extensions: vec![],
        }
    }

    #[test]
    fn test_valid_report_matches_builtin_domain() {
        let collector = Address::repeat_byte(0xCC);
        let report = valid_report(1337, collector);

        let domain = domain_from_report(report, 1337, collector).expect("valid report");

        assert_eq!(domain, rca_eip712_domain(1337, collector));
        assert_eq!(
            domain.separator(),
            rca_eip712_domain(1337, collector).separator()
        );
    }

    #[test]
    fn test_chain_id_mismatch_rejected() {
        let collector = Address::repeat_byte(0xCC);
        let report = valid_report(42161, collector);

        let err = domain_from_report(report, 1337, collector).unwrap_err();

        assert!(err.to_string().contains("chain id 42161"), "{err}");
    }

    #[test]
    fn test_verifying_contract_mismatch_rejected() {
        let collector = Address::repeat_byte(0xCC);
        let mut report = valid_report(1337, collector);
        report.verifyingContract = Address::repeat_byte(0xDD);

        let err = domain_from_report(report, 1337, collector).unwrap_err();

        assert!(err.to_string().contains("verifying contract"), "{err}");
    }

    #[test]
    fn test_unexpected_field_bitmap_rejected() {
        let collector = Address::repeat_byte(0xCC);
        let mut report = valid_report(1337, collector);
        report.fields = FixedBytes::from([0x1f]); // salt bit set

        let err = domain_from_report(report, 1337, collector).unwrap_err();

        assert!(err.to_string().contains("bitmap"), "{err}");
    }

    #[test]
    fn test_extensions_rejected() {
        let collector = Address::repeat_byte(0xCC);
        let mut report = valid_report(1337, collector);
        report.extensions = vec![U256::from(1u64)];

        let err = domain_from_report(report, 1337, collector).unwrap_err();

        assert!(err.to_string().contains("extensions"), "{err}");
    }

    #[test]
    fn test_empty_name_rejected() {
        let collector = Address::repeat_byte(0xCC);
        let mut report = valid_report(1337, collector);
        report.name = String::new();

        let err = domain_from_report(report, 1337, collector).unwrap_err();

        assert!(err.to_string().contains("name of 0 bytes"), "{err}");
    }

    #[test]
    fn test_oversized_version_rejected() {
        let collector = Address::repeat_byte(0xCC);
        let mut report = valid_report(1337, collector);
        report.version = "v".repeat(MAX_DOMAIN_FIELD_LEN + 1);

        let err = domain_from_report(report, 1337, collector).unwrap_err();

        assert!(err.to_string().contains("version of 257 bytes"), "{err}");
    }

    /// Responds to a JSON-RPC eth_call with a fixed result, echoing the
    /// request id so alloy's transport accepts the response.
    struct EthCallResponder {
        result_hex: String,
    }

    impl Respond for EthCallResponder {
        fn respond(&self, request: &Request) -> ResponseTemplate {
            let body: serde_json::Value =
                serde_json::from_slice(&request.body).expect("JSON-RPC request body");
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0",
                "id": body["id"],
                "result": self.result_hex,
            }))
        }
    }

    fn test_chain_client_config(rpc_url: Url) -> ChainClientConfig {
        ChainClientConfig {
            enabled: true,
            providers: vec![rpc_url],
            request_timeout: Duration::from_secs(5),
            max_retries: 0,
            domain_refresh_interval: Duration::from_secs(3600),
            gas_price_multiplier: 1.2,
            max_gas_price_gwei: 100,
            gas_buffer_multiplier: 2.0,
            gas_floor: 100_000,
            gas_max_addition: 200_000,
        }
    }

    #[tokio::test]
    async fn test_fetch_round_trip_over_json_rpc() {
        let collector = Address::repeat_byte(0xCC);
        let report = valid_report(1337, collector);
        let encoded = IRecurringCollector::eip712DomainCall::abi_encode_returns(&report);
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(EthCallResponder {
                result_hex: format!("0x{}", hex::encode(encoded)),
            })
            .mount(&server)
            .await;
        let config = test_chain_client_config(server.uri().parse().unwrap());

        let domain = fetch_rca_eip712_domain(&config, 1337, collector)
            .await
            .expect("fetch should succeed");

        assert_eq!(domain, rca_eip712_domain(1337, collector));
    }

    #[tokio::test]
    async fn test_fetch_undecodable_data_fails_fast() {
        let collector = Address::repeat_byte(0xCC);
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(EthCallResponder {
                result_hex: "0xdeadbeef".to_string(),
            })
            .mount(&server)
            .await;
        let config = test_chain_client_config(server.uri().parse().unwrap());

        let err = fetch_rca_eip712_domain(&config, 1337, collector)
            .await
            .unwrap_err();

        assert!(err.to_string().contains("undecodable"), "{err}");
    }

    #[tokio::test]
    async fn test_refresh_swaps_in_a_changed_domain() {
        let collector = Address::repeat_byte(0xCC);
        // The contract now reports version "2"; the shared slot holds the
        // version "1" builtin, so the refresh should swap it.
        let mut report = valid_report(1337, collector);
        report.version = "2".to_string();
        let encoded = IRecurringCollector::eip712DomainCall::abi_encode_returns(&report);
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(EthCallResponder {
                result_hex: format!("0x{}", hex::encode(encoded)),
            })
            .mount(&server)
            .await;
        let config = test_chain_client_config(server.uri().parse().unwrap());
        let shared = std::sync::RwLock::new(rca_eip712_domain(1337, collector));

        let changed = refresh_rca_eip712_domain(&config, 1337, collector, &shared)
            .await
            .expect("refresh should succeed");

        assert!(changed);
        let domain = shared.read().unwrap().clone();
        assert_eq!(domain.version.as_deref(), Some("2"));
    }

    #[tokio::test]
    async fn test_refresh_is_a_no_op_when_unchanged() {
        let collector = Address::repeat_byte(0xCC);
        let report = valid_report(1337, collector);
        let encoded = IRecurringCollector::eip712DomainCall::abi_encode_returns(&report);
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(EthCallResponder {
                result_hex: format!("0x{}", hex::encode(encoded)),
            })
            .mount(&server)
            .await;
        let config = test_chain_client_config(server.uri().parse().unwrap());
        let shared = std::sync::RwLock::new(rca_eip712_domain(1337, collector));

        let changed = refresh_rca_eip712_domain(&config, 1337, collector, &shared)
            .await
            .expect("refresh should succeed");

        assert!(!changed);
        assert_eq!(
            shared.read().unwrap().clone(),
            rca_eip712_domain(1337, collector)
        );
    }

    #[tokio::test]
    async fn test_refresh_failure_keeps_the_current_domain() {
        let collector = Address::repeat_byte(0xCC);
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(EthCallResponder {
                result_hex: "0xdeadbeef".to_string(),
            })
            .mount(&server)
            .await;
        let config = test_chain_client_config(server.uri().parse().unwrap());
        let shared = std::sync::RwLock::new(rca_eip712_domain(1337, collector));

        let err = refresh_rca_eip712_domain(&config, 1337, collector, &shared)
            .await
            .unwrap_err();

        assert!(err.to_string().contains("undecodable"), "{err}");
        assert_eq!(
            shared.read().unwrap().clone(),
            rca_eip712_domain(1337, collector)
        );
    }

    #[tokio::test]
    async fn test_fetch_chain_id_mismatch_fails_validation() {
        // A well-formed report for the wrong chain: the decode succeeds and
        // the failure comes from validation, covering the full fetch path.
        let collector = Address::repeat_byte(0xCC);
        let report = valid_report(42161, collector);
        let encoded = IRecurringCollector::eip712DomainCall::abi_encode_returns(&report);
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(EthCallResponder {
                result_hex: format!("0x{}", hex::encode(encoded)),
            })
            .mount(&server)
            .await;
        let config = test_chain_client_config(server.uri().parse().unwrap());

        let err = fetch_rca_eip712_domain(&config, 1337, collector)
            .await
            .unwrap_err();

        assert!(err.to_string().contains("chain id 42161"), "{err}");
    }
}
