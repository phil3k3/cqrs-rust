#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Generic {0}")]
    Generic(String),

    #[error(transparent)]
    CqrsLibrary(#[from] cqrs_library::error::Error),
}
