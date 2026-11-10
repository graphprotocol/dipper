pub mod indexing_agreements;
pub mod indexing_requests;

mod message;

pub use message::SignedMessage;
use thegraph_core::alloy::{
    dyn_abi::Eip712Domain,
    primitives::{B256, ChainId, b256},
    sol_types::eip712_domain,
};

/// DIPs EIP-712 domain salt
pub(crate) const EIP712_DOMAIN_SALT: B256 =
    b256!("b4632c657c26dce5d4d7da1d65bda185b14ff8f905ddbb03ea0382ed06c5ef28");

/// The EIP-712 domain admin requests are signed under, for the dipper that serves the protocol
/// on `protocol_chain_id` (42161 on Arbitrum One, 421614 on Arbitrum Sepolia).
///
/// The chain is part of what gets signed, so a request made for the dipper on 1 network
/// recovers to a different signer on any other, where that signer is on no allowlist.
pub fn eip712_domain(protocol_chain_id: ChainId) -> Eip712Domain {
    eip712_domain! {
        name: "Graph Protocol",
        version: "0",
        chain_id: protocol_chain_id,
        salt: EIP712_DOMAIN_SALT,
    }
}
