//! Startup preflight that verifies dipper's signer holds AGREEMENT_MANAGER_ROLE
//! on the RecurringAgreementManager. The manager gates offers and cancels behind
//! that role, so a missing grant fails fast here rather than reverting per offer.

use thegraph_core::alloy::{
    primitives::{Address, B256, keccak256},
    providers::Provider,
    rpc::types::TransactionRequest,
    sol,
    sol_types::SolCall,
};

use super::rpc_provider::RpcProviderPool;
use crate::{chain_client::ChainClientError, config::ChainClientConfig};

sol! {
    /// OpenZeppelin AccessControl membership check, the minimal slice needed to
    /// confirm dipper can drive the manager before it starts serving traffic.
    #[allow(missing_docs)]
    #[sol(abi)]
    interface IAccessControl {
        function hasRole(bytes32 role, address account) external view returns (bool);
    }
}

/// Verify `signer` holds AGREEMENT_MANAGER_ROLE on the manager at `manager`.
/// Transient RPC errors retry across the pool; a missing grant (or an address
/// that does not answer `hasRole`) fails fast.
pub async fn verify_signer_has_agreement_manager_role(
    config: &ChainClientConfig,
    manager: Address,
    signer: Address,
) -> Result<(), ChainClientError> {
    let pool = RpcProviderPool::new(
        config.providers.clone(),
        config.request_timeout,
        config.max_retries,
    )?;

    // The manager derives the role the same way: keccak256("AGREEMENT_MANAGER_ROLE").
    let role: B256 = keccak256("AGREEMENT_MANAGER_ROLE");
    let calldata = IAccessControl::hasRoleCall {
        role,
        account: signer,
    }
    .abi_encode();

    let output = pool
        .execute("has_agreement_manager_role", |provider| {
            let calldata = calldata.clone();
            async move {
                let tx = TransactionRequest::default()
                    .to(manager)
                    .input(calldata.into());
                provider.call(tx).await
            }
        })
        .await?;

    let has_role = IAccessControl::hasRoleCall::abi_decode_returns(&output).map_err(|err| {
        ChainClientError::ConfigError(format!(
            "contract at {manager} returned undecodable hasRole() data \
             (not a RecurringAgreementManager?): {err}"
        ))
    })?;

    if !has_role {
        return Err(ChainClientError::ConfigError(format!(
            "signer {signer} lacks AGREEMENT_MANAGER_ROLE on the RecurringAgreementManager \
             at {manager}; grant it (role admin is OPERATOR_ROLE) or every \
             offerAgreement/cancelAgreement will revert"
        )));
    }

    tracing::info!(
        signer = %signer,
        recurring_agreement_manager = %manager,
        "verified signer holds AGREEMENT_MANAGER_ROLE"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use thegraph_core::alloy::hex;
    use url::Url;
    use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate, matchers::method};

    use super::*;

    /// Answers any JSON-RPC eth_call with a fixed hex result, echoing the
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

    fn config_for(rpc_url: Url) -> ChainClientConfig {
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

    async fn server_returning(result_hex: String) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(EthCallResponder { result_hex })
            .mount(&server)
            .await;
        server
    }

    async fn verify_against(result_hex: String) -> Result<(), ChainClientError> {
        let server = server_returning(result_hex).await;
        let config = config_for(server.uri().parse().expect("mock server URL"));
        verify_signer_has_agreement_manager_role(
            &config,
            Address::repeat_byte(0x11),
            Address::repeat_byte(0x22),
        )
        .await
    }

    fn encoded_has_role(granted: bool) -> String {
        format!(
            "0x{}",
            hex::encode(IAccessControl::hasRoleCall::abi_encode_returns(&granted))
        )
    }

    #[tokio::test]
    async fn granted_role_passes() {
        verify_against(encoded_has_role(true))
            .await
            .expect("a granted role should pass the preflight");
    }

    #[tokio::test]
    async fn missing_role_fails_fast() {
        let err = verify_against(encoded_has_role(false))
            .await
            .expect_err("a missing role should fail the preflight");
        assert!(err.to_string().contains("AGREEMENT_MANAGER_ROLE"), "{err}");
    }

    #[tokio::test]
    async fn undecodable_response_fails_fast() {
        let err = verify_against("0x".to_string())
            .await
            .expect_err("an address that does not answer hasRole should fail");
        assert!(err.to_string().contains("undecodable"), "{err}");
    }
}
