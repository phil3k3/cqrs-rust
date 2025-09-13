#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Generic {0}")]
    Generic(String),

    #[error(transparent)]
    Kafka(#[from] rdkafka::error::KafkaError),
}

impl From<Error> for cqrs_library::error::Error {
    fn from(value: Error) -> Self {
        cqrs_library::error::Error::Generic(value.to_string())
    }
}
