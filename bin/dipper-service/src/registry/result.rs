/// Result type for registry operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors that can occur when interacting with the registry.
#[derive(Debug, thiserror::Error)]
#[expect(
    clippy::enum_variant_names,
    reason = "BackendError predates the third variant, and renaming it would touch every caller"
)]
pub enum Error {
    /// The update query failed as no records matching the criteria were found.
    #[error("No records were updated")]
    NoRecordsUpdated,

    /// A value is too large for the database column that stores it.
    #[error("{field} {value} is too large to store")]
    TooLarge { field: &'static str, value: usize },

    /// An error occurred while interacting with the registry backend.
    #[error(transparent)]
    BackendError(dipper_pgregistry::Error),
}

impl From<dipper_pgregistry::Error> for Error {
    fn from(value: dipper_pgregistry::Error) -> Self {
        match value {
            dipper_pgregistry::Error::NoRecordsUpdated => Error::NoRecordsUpdated,
            dipper_pgregistry::Error::DbError(_) => Error::BackendError(value),
        }
    }
}
