//! Common unique identifiers used in the DIPs Gateway (Dipper).
//!
//! [`IndexingAgreementId`] wraps the on-chain `bytes16` agreement ID derived from
//! `keccak256(abi.encode(payer, dataService, serviceProvider, deadline, nonce))[0..16]`.
//!
//! [`IndexingRequestId`] remains a UUID v7 *new-type* wrapper.

/// The on-chain agreement ID (`bytes16`).
///
/// Derived from `keccak256(abi.encode(payer, dataService, serviceProvider, deadline, nonce))[0..16]`.
/// Stored as `BYTEA` in Postgres, serialised as `0x`-prefixed hex over JSON/RPC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct IndexingAgreementId([u8; 16]);

impl IndexingAgreementId {
    /// Construct from a raw 16-byte array.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Consume and return the inner 16-byte array.
    pub fn into_bytes(self) -> [u8; 16] {
        self.0
    }

    /// Borrow the inner 16-byte array.
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl<'a> From<&'a [u8; 16]> for IndexingAgreementId {
    fn from(bytes: &'a [u8; 16]) -> Self {
        Self(*bytes)
    }
}

impl<'a> TryFrom<&'a [u8]> for IndexingAgreementId {
    type Error = std::array::TryFromSliceError;

    fn try_from(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        <[u8; 16]>::try_from(bytes).map(Self)
    }
}

impl std::fmt::Display for IndexingAgreementId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "0x")?;
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Error returned when parsing an [`IndexingAgreementId`] from a string fails.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ParseAgreementIdError(String);

impl std::str::FromStr for IndexingAgreementId {
    type Err = ParseAgreementIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let hex = s.strip_prefix("0x").unwrap_or(s);
        if hex.len() != 32 {
            return Err(ParseAgreementIdError(format!(
                "expected 32 hex chars (16 bytes), got {} chars",
                hex.len()
            )));
        }
        let mut bytes = [0u8; 16];
        for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
            let hi = hex_nibble(chunk[0]).ok_or_else(|| {
                ParseAgreementIdError(format!("invalid hex char: {}", chunk[0] as char))
            })?;
            let lo = hex_nibble(chunk[1]).ok_or_else(|| {
                ParseAgreementIdError(format!("invalid hex char: {}", chunk[1] as char))
            })?;
            bytes[i] = (hi << 4) | lo;
        }
        Ok(Self(bytes))
    }
}

/// Convert an ASCII hex character to its 4-bit value.
fn hex_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

#[cfg(feature = "serde")]
impl ::serde::Serialize for IndexingAgreementId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ::serde::Serializer,
    {
        // Serialize as "0x" + lowercase hex
        serializer.serialize_str(&self.to_string())
    }
}

#[cfg(feature = "serde")]
impl<'de> ::serde::Deserialize<'de> for IndexingAgreementId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: ::serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(::serde::de::Error::custom)
    }
}

#[cfg(feature = "sqlx")]
impl ::sqlx::Type<::sqlx::Postgres> for IndexingAgreementId {
    fn type_info() -> <::sqlx::Postgres as ::sqlx::Database>::TypeInfo {
        // BYTEA
        <Vec<u8> as ::sqlx::Type<::sqlx::Postgres>>::type_info()
    }
}

#[cfg(feature = "sqlx")]
impl<'q> ::sqlx::Encode<'q, ::sqlx::Postgres> for IndexingAgreementId {
    fn encode_by_ref(
        &self,
        buf: &mut <::sqlx::Postgres as ::sqlx::Database>::ArgumentBuffer,
    ) -> Result<::sqlx::encode::IsNull, ::sqlx::error::BoxDynError> {
        <&[u8] as ::sqlx::Encode<'q, ::sqlx::Postgres>>::encode_by_ref(&&self.0[..], buf)
    }
}

#[cfg(feature = "sqlx")]
impl<'r> ::sqlx::Decode<'r, ::sqlx::Postgres> for IndexingAgreementId {
    fn decode(
        value: <::sqlx::Postgres as ::sqlx::Database>::ValueRef<'r>,
    ) -> Result<Self, ::sqlx::error::BoxDynError> {
        let bytes: Vec<u8> = <Vec<u8> as ::sqlx::Decode<'r, ::sqlx::Postgres>>::decode(value)?;
        let arr: [u8; 16] = bytes.try_into().map_err(|v: Vec<u8>| {
            format!("expected 16 bytes for IndexingAgreementId, got {}", v.len())
        })?;
        Ok(Self(arr))
    }
}

#[cfg(feature = "sqlx")]
impl ::sqlx::postgres::PgHasArrayType for IndexingAgreementId {
    fn array_type_info() -> ::sqlx::postgres::PgTypeInfo {
        // BYTEA[] — needed so callers can bind `Vec<IndexingAgreementId>`
        // (or `&[IndexingAgreementId]`) directly to `... = ANY($1)` filters
        // for batched lookups and updates.
        ::sqlx::postgres::PgTypeInfo::with_name("_bytea")
    }
}

#[cfg(feature = "fake")]
impl ::fake::Dummy<fake::Faker> for IndexingAgreementId {
    fn dummy_with_rng<R: ::fake::Rng + ?Sized>(_: &fake::Faker, rng: &mut R) -> Self {
        let mut bytes = [0u8; 16];
        rng.fill_bytes(&mut bytes);
        Self(bytes)
    }
}

/// A unique identifier of an indexing request.
///
/// This is *new-type* wrapper around [`uuid::Uuid`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct IndexingRequestId(uuid::Uuid);

/// Implementations for the new-type wrappers around [`uuid::Uuid`].
macro_rules! uuid_new_type_impls {
    ($name:ident) => {
        impl $name {
            /// Create a new [`$name`].
            ///
            /// The [`$name`] is generated using the [`Uuid::now_v7`] method.
            ///
            /// [`Uuid::now_v7`]: uuid::Uuid::now_v7
            #[allow(clippy::new_without_default)]
            pub fn new() -> Self {
                Self(::uuid::Uuid::now_v7())
            }

            /// Parse a [`$name`] from bytes.
            pub fn from_bytes(bytes: [u8; 16]) -> Self {
                Self(::uuid::Uuid::from_bytes(bytes))
            }

            /// Unwrap the [`$name`] into the inner [`Uuid`].
            ///
            /// [`Uuid`]: uuid::Uuid
            pub fn into_inner(self) -> ::uuid::Uuid {
                self.0
            }

            /// Get a reference to the inner [`Uuid`].
            ///
            /// [`Uuid`]: uuid::Uuid
            pub fn as_uuid(&self) -> &::uuid::Uuid {
                &self.0
            }
        }

        impl From<::uuid::Uuid> for $name {
            fn from(id: ::uuid::Uuid) -> Self {
                Self(id)
            }
        }

        impl<'a> From<&'a [u8; 16]> for $name {
            fn from(bytes: &'a [u8; 16]) -> Self {
                Self(::uuid::Uuid::from_bytes(*bytes))
            }
        }

        impl<'a> TryFrom<&'a [u8]> for $name {
            type Error = ::uuid::Error;

            fn try_from(bytes: &'a [u8]) -> Result<Self, Self::Error> {
                ::uuid::Uuid::from_slice(bytes).map(Self)
            }
        }

        impl std::ops::Deref for $name {
            type Target = ::uuid::Uuid;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
                std::fmt::Display::fmt(&self.0, f)
            }
        }

        #[cfg(feature = "serde")]
        impl ::serde::Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: ::serde::Serializer,
            {
                self.0.serialize(serializer)
            }
        }

        #[cfg(feature = "serde")]
        impl<'de> ::serde::Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: ::serde::Deserializer<'de>,
            {
                ::serde::Deserialize::deserialize(deserializer).map(Self)
            }
        }

        #[cfg(feature = "sqlx")]
        impl ::sqlx::Type<::sqlx::Postgres> for $name {
            fn type_info() -> <::sqlx::Postgres as ::sqlx::Database>::TypeInfo {
                ::uuid::Uuid::type_info()
            }
        }

        #[cfg(feature = "sqlx")]
        impl<'q> ::sqlx::Encode<'q, ::sqlx::Postgres> for $name {
            fn encode_by_ref(
                &self,
                buf: &mut <::sqlx::Postgres as ::sqlx::Database>::ArgumentBuffer,
            ) -> Result<::sqlx::encode::IsNull, ::sqlx::error::BoxDynError> {
                self.0.encode_by_ref(buf)
            }
        }

        #[cfg(feature = "sqlx")]
        impl<'r> ::sqlx::Decode<'r, ::sqlx::Postgres> for $name {
            fn decode(
                value: <::sqlx::Postgres as ::sqlx::Database>::ValueRef<'r>,
            ) -> Result<Self, ::sqlx::error::BoxDynError> {
                ::uuid::Uuid::decode(value).map(Self)
            }
        }

        #[cfg(feature = "fake")]
        impl ::fake::Dummy<fake::Faker> for $name {
            fn dummy_with_rng<R: ::fake::Rng + ?Sized>(_: &fake::Faker, rng: &mut R) -> Self {
                use fake::uuid::UUIDv7;

                Self(::fake::Dummy::<UUIDv7>::dummy_with_rng(&UUIDv7, rng))
            }
        }
    };
}

uuid_new_type_impls!(IndexingRequestId);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_format_is_0x_lowercase_hex() {
        let id = IndexingAgreementId([
            0xaa, 0xbb, 0xcc, 0xdd, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00,
            0xef, 0xab,
        ]);
        assert_eq!(id.to_string(), "0xaabbccdd11223344556677889900efab");
    }

    #[test]
    fn roundtrip_display_then_from_str() {
        let original = IndexingAgreementId([
            0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54,
            0x32, 0x10,
        ]);
        let text = original.to_string();
        let parsed: IndexingAgreementId = text.parse().unwrap();
        assert_eq!(original, parsed);
    }

    #[test]
    fn from_str_with_uppercase_hex() {
        let id: IndexingAgreementId = "0xAABBCCDD11223344556677889900EFAB".parse().unwrap();
        assert_eq!(
            id.as_bytes(),
            &[
                0xaa, 0xbb, 0xcc, 0xdd, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00,
                0xef, 0xab
            ]
        );
    }

    #[test]
    fn from_str_without_0x_prefix() {
        let id: IndexingAgreementId = "aabbccdd11223344556677889900efab".parse().unwrap();
        assert_eq!(
            id.as_bytes(),
            &[
                0xaa, 0xbb, 0xcc, 0xdd, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00,
                0xef, 0xab
            ]
        );
    }

    #[test]
    fn from_str_rejects_odd_length() {
        // 31 hex chars (15.5 bytes) -- odd-length input
        let result = "0xaabbccdd1122334455667788990012".parse::<IndexingAgreementId>();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("expected 32 hex chars")
        );
    }

    #[test]
    fn from_str_rejects_too_short() {
        // 30 hex chars (15 bytes)
        let result = "0xaabbccdd112233445566778899001".parse::<IndexingAgreementId>();
        assert!(result.is_err());
    }

    #[test]
    fn from_str_rejects_too_long() {
        // 34 hex chars (17 bytes)
        let result = "0xaabbccdd1122334455667788990012ab99".parse::<IndexingAgreementId>();
        assert!(result.is_err());
    }

    #[test]
    fn from_str_rejects_invalid_hex_chars() {
        let result = "0xaabbccdd112233445566778899GGHHII".parse::<IndexingAgreementId>();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid hex char"));
    }

    #[cfg(feature = "serde")]
    #[test]
    fn serde_roundtrip() {
        let original = IndexingAgreementId([
            0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54,
            0x32, 0x10,
        ]);
        let json = serde_json::to_string(&original).unwrap();
        assert_eq!(json, "\"0x0123456789abcdeffedcba9876543210\"");
        let deserialized: IndexingAgreementId = serde_json::from_str(&json).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn error_type_is_parse_agreement_id_error() {
        let result = "too_short".parse::<IndexingAgreementId>();
        let err = result.unwrap_err();
        // Verify it's a ParseAgreementIdError (the type implements Error + Display)
        let _: &dyn std::error::Error = &err;
        assert!(!err.to_string().is_empty());
    }
}
