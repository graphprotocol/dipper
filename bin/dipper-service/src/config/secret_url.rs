//! A URL that may carry a credential, such as a hosted RPC endpoint with its API key in the path
//! or a Slack webhook.

use std::fmt;

use thegraph_core::alloy::primitives::keccak256;
use url::Url;

/// What masks a secret part of a URL.
const MASK: &str = "***";

/// A URL that may carry a credential. `Debug` and `Display` show it with its secret parts
/// masked, followed by a short fingerprint of the whole URL when anything was masked, so a log
/// says which endpoint and which key were in use without showing the key:
/// `https://eth-mainnet.g.alchemy.com/v2/*** (fingerprint 3fa1c2)`.
///
/// The parts shown are the scheme, the host, the port and the shape of the path; masked are the
/// user and password, every query value (its name stays), the fragment, and every path segment
/// but a version such as `v2`. Providers put their keys in any of these, and a key can look like
/// any word (`rpc-key`), so dipper does not guess which segment is the key: the fingerprint is
/// what tells 2 keys apart. A URL with nothing to mask shows in full.
#[derive(Clone, PartialEq, Eq)]
pub struct SecretUrl(Url);

impl SecretUrl {
    /// The URL itself, credential and all, for connecting with. Never log it.
    pub fn expose(&self) -> &Url {
        &self.0
    }

    /// The URL with its secret parts masked, and its fingerprint when anything was masked.
    pub fn redacted(&self) -> String {
        let url = &self.0;
        let mut masked = false;
        let mut out = format!("{}:", url.scheme());
        if url.cannot_be_a_base() {
            out.push_str(MASK);
            masked = true;
        } else {
            out.push_str("//");
            if !url.username().is_empty() || url.password().is_some() {
                out.push_str(MASK);
                out.push('@');
                masked = true;
            }
            out.push_str(url.host_str().unwrap_or_default());
            if let Some(port) = url.port() {
                out.push_str(&format!(":{port}"));
            }
            let segments: Vec<&str> = url
                .path()
                .split('/')
                .map(|segment| {
                    if !shown(segment) {
                        masked = true;
                        MASK
                    } else {
                        segment
                    }
                })
                .collect();
            out.push_str(&segments.join("/"));
        }
        if let Some(query) = url.query() {
            let pairs: Vec<String> = query
                .split('&')
                .map(|pair| match pair.split_once('=') {
                    Some((key, _)) => format!("{key}={MASK}"),
                    None => MASK.to_string(),
                })
                .collect();
            out.push('?');
            out.push_str(&pairs.join("&"));
            masked = true;
        }
        if url.fragment().is_some() {
            out.push('#');
            out.push_str(MASK);
            masked = true;
        }
        if masked {
            out.push_str(&format!(" (fingerprint {})", self.fingerprint()));
        }
        out
    }

    /// The first 3 bytes of the URL's keccak-256 hash, in hex: the same for the same URL, so 2
    /// logs can be told to use the same key or not, and too short to give a random key away. A
    /// guessable password in a URL could still be checked against it, guess by guess, so such a
    /// password belongs in a field of its own, such as `db.password`, not in a URL.
    pub fn fingerprint(&self) -> String {
        let hash = keccak256(self.0.as_str().as_bytes());
        hash[..3].iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

/// Whether a path segment is shown as it is: an empty one, or a version such as `v2` or `v3`.
fn shown(segment: &str) -> bool {
    match segment.strip_prefix('v') {
        Some(version) => {
            !version.is_empty() && version.len() <= 2 && version.chars().all(|c| c.is_ascii_digit())
        }
        None => segment.is_empty(),
    }
}

impl From<Url> for SecretUrl {
    fn from(url: Url) -> Self {
        Self(url)
    }
}

impl std::str::FromStr for SecretUrl {
    type Err = url::ParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Url::parse(value).map(Self)
    }
}

impl fmt::Display for SecretUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.redacted())
    }
}

impl fmt::Debug for SecretUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.redacted())
    }
}

impl<'de> serde::Deserialize<'de> for SecretUrl {
    /// Leaves the value out of any error, which serde and the `url` crate would otherwise quote.
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)
            .map_err(|_| serde::de::Error::custom("expected a URL as a string"))?;
        Url::parse(&value).map(Self).map_err(|err| {
            serde::de::Error::custom(format!("not a valid URL ({err}; the URL is left out)"))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::SecretUrl;

    fn redacted(url: &str) -> String {
        url.parse::<SecretUrl>().unwrap().redacted()
    }

    /// The redacted form, without its fingerprint.
    fn masked(url: &str) -> String {
        let out = redacted(url);
        match out.split_once(" (fingerprint ") {
            Some((masked, _)) => masked.to_string(),
            None => out,
        }
    }

    #[test]
    fn hosted_rpc_keys_in_the_path_are_masked() {
        assert_eq!(
            masked("https://eth-mainnet.g.alchemy.com/v2/AbCdEfGh0123456789AbCdEfGh012345"),
            "https://eth-mainnet.g.alchemy.com/v2/***"
        );
        assert_eq!(
            masked("https://mainnet.infura.io/v3/0123456789abcdef0123456789abcdef"),
            "https://mainnet.infura.io/v3/***"
        );
        assert_eq!(
            masked("https://rpc.ankr.com/arbitrum/0a1b2c3d4e5f60718293a4b5c6d7e8f9"),
            "https://rpc.ankr.com/***/***"
        );
        assert_eq!(
            masked("https://young-wild-sky.arbitrum-mainnet.quiknode.pro/0123abcd4567ef89/"),
            "https://young-wild-sky.arbitrum-mainnet.quiknode.pro/***/"
        );
    }

    /// A key that looks like a word is masked all the same.
    #[test]
    fn a_word_like_key_is_masked() {
        assert_eq!(
            masked("https://rpc.example.com/v2/rpc-key"),
            "https://rpc.example.com/v2/***"
        );
    }

    #[test]
    fn a_slack_webhook_shows_only_its_host() {
        assert_eq!(
            masked("https://hooks.slack.com/services/T0/B0/not-a-real-key"),
            "https://hooks.slack.com/***/***/***/***"
        );
    }

    #[test]
    fn user_info_query_values_and_fragments_are_masked() {
        assert_eq!(
            masked("https://dipper:hunter2@rpc.example.com:8443/v1?apikey=s3cret&chain=1#tok"),
            "https://***@rpc.example.com:8443/v1?apikey=***&chain=***#***"
        );
        assert_eq!(
            masked("https://s3cret@rpc.example.com/"),
            "https://***@rpc.example.com/"
        );
        assert_eq!(
            masked("https://rpc.example.com/?s3cret"),
            "https://rpc.example.com/?***"
        );
    }

    #[test]
    fn a_url_with_nothing_secret_shows_in_full_without_a_fingerprint() {
        assert_eq!(redacted("http://chain:8545"), "http://chain:8545/");
        assert_eq!(redacted("http://chain:8545/v1/"), "http://chain:8545/v1/");
    }

    #[test]
    fn the_fingerprint_tells_keys_apart() {
        let a = "https://mainnet.infura.io/v3/0123456789abcdef0123456789abcdef";
        let b = "https://mainnet.infura.io/v3/fedcba9876543210fedcba9876543210";
        assert_eq!(redacted(a), redacted(a));
        assert_ne!(redacted(a), redacted(b));
        assert!(redacted(a).ends_with(')'), "{}", redacted(a));
        assert_eq!(a.parse::<SecretUrl>().unwrap().fingerprint().len(), 6);
    }

    #[test]
    fn debug_and_display_never_show_the_key() {
        let url: SecretUrl = "https://mainnet.infura.io/v3/0123456789abcdef0123456789abcdef"
            .parse()
            .unwrap();
        for shown in [
            format!("{url}"),
            format!("{url:?}"),
            format!("{:?}", vec![url.clone()]),
        ] {
            assert!(!shown.contains("0123456789abcdef"), "{shown}");
        }
        assert!(url.expose().as_str().contains("0123456789abcdef"));
    }

    #[test]
    fn a_rejected_url_is_left_out_of_the_error() {
        for bad in [r#""not a url s3cret-key""#, "12345678"] {
            let err = serde_json::from_str::<SecretUrl>(bad).expect_err("not a URL");
            assert!(!err.to_string().contains("s3cret"), "{err}");
            assert!(!err.to_string().contains("12345678"), "{err}");
        }
    }
}
