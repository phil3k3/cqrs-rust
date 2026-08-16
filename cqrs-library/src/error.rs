use prost::DecodeError;
use prost::EncodeError;

/// Errors that can occur in the CQRS library.
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Command handler was not found for the specified command type.
    #[error("Command handler not found: {0}")]
    CommandHandlerNotFound(String),

    /// Failed to deserialize a command.
    #[error("Failed to deserialize command of type '{command_type}': {source}")]
    CommandDeserializationFailed {
        command_type: String,
        #[source]
        source: serde_json::Error,
    },

    /// Failed to serialize an event.
    #[error("Failed to serialize event of type '{event_type}': {source}")]
    EventSerializationFailed {
        event_type: String,
        #[source]
        source: serde_json::Error,
    },

    /// Command response was received for an unknown command.
    #[error("Received command response for unknown command ID: {0}")]
    UnknownCommandResponse(String),

    /// No command response was generated.
    #[error("No command response was generated")]
    NoCommandResponse,

    /// Failed to send a message through a channel.
    #[error("Failed to send message through channel: {0}")]
    ChannelSendFailed(String),

    /// Channel was not initialized.
    #[error("Channel not initialized")]
    ChannelNotInitialized,

    /// Error receiving from a oneshot channel.
    #[error(transparent)]
    OneshotRecv(#[from] tokio::sync::oneshot::error::RecvError),

    /// Error decoding a protobuf message.
    #[error(transparent)]
    ProtobufDecode(#[from] DecodeError),

    /// Error encoding a protobuf message.
    #[error(transparent)]
    ProtobufEncode(#[from] EncodeError),

    /// Error serializing or deserializing JSON.
    #[error(transparent)]
    Json(#[from] serde_json::error::Error),
}
