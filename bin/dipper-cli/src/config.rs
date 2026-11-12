use dipper_core::config::{Hidden, HiddenSecretKeyAsHexStr};
use serde_with::{DisplayFromStr, serde_as};
use thegraph_core::alloy::{primitives::ChainId, signers::k256::SecretKey};
use url::Url;

/// The configuration for the DIPs CLI
#[serde_as]
#[derive(custom_debug::CustomDebug, serde::Serialize, serde::Deserialize)]
pub struct Config {
    /// The URL of the DIPs gateway server
    #[debug(with = std::fmt::Display::fmt)]
    #[serde_as(as = "DisplayFromStr")]
    pub server_url: Url,

    /// The signing key to use for authentication
    #[serde_as(as = "HiddenSecretKeyAsHexStr")]
    pub signing_key: Hidden<SecretKey>,
    /// The IPFS API subgraph manifests are read from
    #[debug(with = std::fmt::Display::fmt)]
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "default_ipfs_url")]
    pub ipfs_url: Url,
    /// The chain the DIPs contracts are on, which requests are signed for. Only a command that
    /// signs a request needs it, so it is not required to load the configuration.
    #[serde(default)]
    pub protocol_chain_id: Option<ChainId>,
}

#[expect(
    clippy::expect_used,
    reason = "predates this lint; fix when next touched"
)]
fn default_ipfs_url() -> Url {
    crate::chain::DEFAULT_IPFS_URL
        .parse()
        .expect("the default IPFS URL is valid")
}
