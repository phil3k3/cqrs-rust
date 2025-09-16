#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    CqrsLibrary(#[from] cqrs_library::error::Error),
}
