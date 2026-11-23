//! Configuration types and utilities.

use std::str::FromStr as _;

use thegraph_core::alloy::{
    hex::FromHexError,
    primitives::B256,
    signers::k256::{SecretKey, elliptic_curve::Error as CryptoError},
};

/// A _new-type_ wrapper for a configuration value that should be kept secret.
///
/// Its `Debug` and `Display` never show the value, and a value that fails to deserialise is left
/// out of the error, which serde would otherwise quote and which ends up in logs.
#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct Hidden<T>(pub T);

/// What a secret that fails to deserialise is reported as, in place of serde's own message.
#[cfg(feature = "serde")]
const REJECTED_SECRET: &str = "not a valid value for this secret (left out of this message)";

#[cfg(feature = "serde")]
impl<'de, T> serde::Deserialize<'de> for Hidden<T>
where
    T: serde::Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        T::deserialize(deserializer)
            .map(Self)
            .map_err(|_| serde::de::Error::custom(REJECTED_SECRET))
    }
}

impl<T> Hidden<T> {
    /// Unwrap the hidden value
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> std::fmt::Display for Hidden<T>
where
    T: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<redacted>")
    }
}

impl<T> std::fmt::Debug for Hidden<T>
where
    T: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<redacted>")
    }
}

impl<T> std::str::FromStr for Hidden<T>
where
    T: std::str::FromStr,
{
    type Err = T::Err;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        T::from_str(value).map(Self)
    }
}

impl<T> AsRef<T> for Hidden<T> {
    fn as_ref(&self) -> &T {
        &self.0
    }
}

impl<T> std::borrow::Borrow<T> for Hidden<T> {
    fn borrow(&self) -> &T {
        &self.0
    }
}

impl<T> std::ops::Deref for Hidden<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// An error that can occur when parsing a secret key from a hex string.
#[derive(Debug, thiserror::Error)]
pub enum HiddenSecretKeyFromStrError {
    /// An error occurred while parsing the hex string.
    #[error(transparent)]
    FromHexError(#[from] FromHexError),
    /// An error occurred while parsing the secret key bytes.
    #[error(transparent)]
    CryptoError(#[from] CryptoError),
}

/// Parse a secret key from a hex string.
pub fn secret_key_from_str(s: &str) -> Result<Hidden<SecretKey>, HiddenSecretKeyFromStrError> {
    let bytes = B256::from_str(s)?;
    let key = SecretKey::from_slice(bytes.as_slice())?;
    Ok(Hidden(key))
}

/// Serialize (and deserialize) a `SecretKey` as a hex string.
#[cfg(feature = "serde")]
pub struct HiddenSecretKeyAsHexStr;

#[cfg(feature = "serde")]
impl serde_with::SerializeAs<Hidden<SecretKey>> for HiddenSecretKeyAsHexStr {
    fn serialize_as<S>(source: &Hidden<SecretKey>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let bytes = B256::from_slice(source.to_bytes().as_slice());
        serializer.serialize_str(&bytes.to_string())
    }
}

#[cfg(feature = "serde")]
impl<'de> serde_with::DeserializeAs<'de, Hidden<SecretKey>> for HiddenSecretKeyAsHexStr {
    fn deserialize_as<D>(deserializer: D) -> Result<Hidden<SecretKey>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes: B256 = serde::Deserialize::deserialize(deserializer)
            .map_err(|_| serde::de::Error::custom(REJECTED_SECRET))?;
        let key = SecretKey::from_slice(bytes.as_slice())
            .map_err(|_| serde::de::Error::custom(REJECTED_SECRET))?;
        Ok(Hidden(key))
    }
}

#[cfg(all(test, feature = "serde"))]
mod tests {
    use thegraph_core::alloy::signers::k256::SecretKey;

    use super::{Hidden, HiddenSecretKeyAsHexStr};

    #[serde_with::serde_as]
    #[derive(Debug, serde::Deserialize)]
    struct Signer {
        #[serde_as(as = "HiddenSecretKeyAsHexStr")]
        #[allow(dead_code, reason = "only its deserialisation is tested")]
        key: Hidden<SecretKey>,
    }

    #[derive(Debug, serde::Deserialize)]
    struct Db {
        #[allow(dead_code, reason = "only its deserialisation is tested")]
        password: Hidden<u32>,
    }

    /// A secret that fails to deserialise must not appear in the error, which ends up in logs.
    #[test]
    fn a_rejected_secret_is_left_out_of_the_error() {
        let err = serde_json::from_str::<Db>(r#"{"password": "hunter2-secret"}"#)
            .expect_err("a string is not a u32");
        assert!(!err.to_string().contains("hunter2"), "{err}");

        let err = serde_json::from_str::<Signer>(r#"{"key": "0xnot-hex-but-secret-bytes"}"#)
            .expect_err("not a key");
        assert!(!err.to_string().contains("secret-bytes"), "{err}");
    }

    /// The error still says where it stopped: just after the bad value, since it is raised once
    /// the value has been read.
    #[test]
    fn a_rejected_secret_keeps_a_position() {
        let err = serde_json::from_str::<Db>("{\n  \"password\": \"x\"\n}")
            .expect_err("a string is not a u32");
        assert!(err.line() >= 2, "{err}");
    }

    #[test]
    fn a_valid_secret_deserialises_as_before() {
        let db: Hidden<String> = serde_json::from_str(r#""hunter2""#).unwrap();
        assert_eq!(db.as_str(), "hunter2");
        assert_eq!(format!("{db:?} {db}"), "<redacted> <redacted>");
    }
}
