use sqlx::{
    Postgres,
    encode::IsNull,
    error::BoxDynError,
    postgres::{PgArgumentBuffer, PgHasArrayType, PgTypeInfo, PgValueRef},
};
use thegraph_core::{DeploymentId, IndexerId, alloy::primitives::Address};

/// Wrapper type for `u64` to implement `sqlx::Type` for `Postgres`.
///
/// This _new-type_ pattern maps the `u64` type to a `[u8; 8]` array
/// which corresponds to a Postgres `bytea` type.
#[repr(transparent)]
pub struct PgU64(pub u64);

impl sqlx::Type<Postgres> for PgU64 {
    fn type_info() -> PgTypeInfo {
        <[u8; 8] as sqlx::Type<Postgres>>::type_info()
    }
}

impl sqlx::Encode<'_, Postgres> for PgU64 {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <[u8; 8] as sqlx::Encode<'_, Postgres>>::encode(self.0.to_be_bytes(), buf)
    }
}

impl<'r> sqlx::Decode<'r, Postgres> for PgU64 {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        let bytes: [u8; 8] = sqlx::Decode::<Postgres>::decode(value)?;
        Ok(Self(u64::from_be_bytes(bytes)))
    }
}

/// Wrapper type for `Url` to implement `sqlx::Type` for `Postgres`.
///
/// This _new-type_ pattern maps the `Url` type to a `&str` which corresponds
/// to a Postgres `text` type.
#[repr(transparent)]
pub struct PgUrl(pub url::Url);

impl sqlx::Type<Postgres> for PgUrl {
    fn type_info() -> PgTypeInfo {
        <&str as sqlx::Type<Postgres>>::type_info()
    }
}

impl sqlx::Encode<'_, Postgres> for PgUrl {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <&str as sqlx::Encode<Postgres>>::encode(self.0.as_str(), buf)
    }
}

impl<'r> sqlx::Decode<'r, Postgres> for PgUrl {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        let url: &str = sqlx::Decode::<Postgres>::decode(value)?;
        let url = url
            .parse()
            .map_err(|err| sqlx::Error::Decode(Box::new(err)))?;
        Ok(Self(url))
    }
}

/// Wrapper type for `Address` to implement `sqlx::Type` for `Postgres`.
///
/// This _new-type_ pattern maps the `Address` type to a `[u8; 20]` array
/// which corresponds to a Postgres `bytea` type.
#[repr(transparent)]
pub struct PgAddress(pub Address);

impl sqlx::Type<Postgres> for PgAddress {
    fn type_info() -> PgTypeInfo {
        <[u8; 20] as sqlx::Type<Postgres>>::type_info()
    }
}

impl sqlx::Encode<'_, Postgres> for PgAddress {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <[u8; 20] as sqlx::Encode<'_, Postgres>>::encode(self.0.into(), buf)
    }
}

impl<'r> sqlx::Decode<'r, Postgres> for PgAddress {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        let bytes = <[u8; 20] as sqlx::Decode<'_, Postgres>>::decode(value)?;
        Ok(Self(Address::from(bytes)))
    }
}

/// Wrapper type for `IndexerId` to implement `sqlx::Type` for `Postgres`.
///
/// This _new-type_ pattern maps the `IndexerId` type to a `[u8; 20]` array
/// which corresponds to a Postgres `bytea` type.
#[repr(transparent)]
pub struct PgIndexerId(pub IndexerId);

impl sqlx::Type<Postgres> for PgIndexerId {
    fn type_info() -> PgTypeInfo {
        PgAddress::type_info()
    }
}

impl sqlx::Encode<'_, Postgres> for PgIndexerId {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        PgAddress(self.0.into_inner()).encode(buf)
    }
}

impl<'r> sqlx::Decode<'r, Postgres> for PgIndexerId {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        let PgAddress(address) = sqlx::Decode::<Postgres>::decode(value)?;
        Ok(Self(IndexerId::new(address)))
    }
}

impl PgHasArrayType for PgIndexerId {
    fn array_type_info() -> PgTypeInfo {
        // bytea[] type for arrays of 20-byte addresses
        PgTypeInfo::with_name("_bytea")
    }
}

/// Wrapper type for `DeploymentId` to implement `sqlx::Type` for `Postgres`.
///
/// This _new-type_ pattern maps the `DeploymentId` type to a `&str` which corresponds
/// to a Postgres `text` type.
#[repr(transparent)]
pub struct PgDeploymentId(pub DeploymentId);

impl sqlx::Type<Postgres> for PgDeploymentId {
    fn type_info() -> PgTypeInfo {
        <&str as sqlx::Type<Postgres>>::type_info()
    }
}

impl sqlx::Encode<'_, Postgres> for PgDeploymentId {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <&str as sqlx::Encode<Postgres>>::encode(self.0.to_string().as_str(), buf)
    }
}

impl<'r> sqlx::Decode<'r, Postgres> for PgDeploymentId {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        let value: &str = sqlx::Decode::<Postgres>::decode(value)?;
        let deployment_id = value
            .parse()
            .map_err(|err| sqlx::Error::Decode(Box::new(err)))?;
        Ok(Self(deployment_id))
    }
}
