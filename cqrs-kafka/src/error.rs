/// Errors that can occur in the Kafka transport layer.
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Kafka client error.
    #[error(transparent)]
    Kafka(#[from] rdkafka::error::KafkaError),

    /// Failed to create Kafka consumer.
    #[error("Failed to create Kafka consumer: {0}")]
    ConsumerCreation(String),

    /// Failed to create Kafka producer.
    #[error("Failed to create Kafka producer: {0}")]
    ProducerCreation(String),

    /// Failed to send message to Kafka.
    #[error("Failed to send message to topic '{topic}': {message}")]
    MessageSendFailed { topic: String, message: String },

    /// Invalid or missing Kafka configuration.
    #[error("Invalid Kafka configuration: {0}")]
    Configuration(#[from] config::ConfigError),

    /// Consumer has no group metadata available, so a transaction can't be committed.
    #[error("Kafka consumer has no group metadata available (rebalance in progress or not part of a consumer group)")]
    MissingConsumerGroupMetadata,

    /// CQRS library error.
    #[error(transparent)]
    Cqrs(#[from] cqrs_library::error::Error),
}

impl From<Error> for cqrs_library::error::Error {
    fn from(value: Error) -> Self {
        match value {
            Error::Cqrs(e) => e,
            other => cqrs_library::error::Error::ChannelSendFailed(other.to_string()),
        }
    }
}
