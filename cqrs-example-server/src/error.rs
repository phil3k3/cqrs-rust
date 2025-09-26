#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    CqrsLibrary(#[from] cqrs_library::error::Error),

    #[error(transparent)]
    CqrsKafka(#[from] cqrs_kafka::error::Error),
}
