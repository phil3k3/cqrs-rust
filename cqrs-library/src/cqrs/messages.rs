use serde::{Deserialize, Serialize};

/// Wrapper for a command response with entity information.
///
/// Used for serialization when sending command responses over the wire.
#[derive(Debug, Deserialize, Serialize)]
pub struct CommandResponseResult {
    pub(crate) entity_id: String,
    pub(crate) result: CommandResponse,
}

/// Response to a command indicating success, failure, or not found.
///
/// This is a simplified response type. In production systems, you may want
/// to include additional information like error details or result data.
#[must_use = "Command responses should be checked to ensure the command was processed successfully"]
#[non_exhaustive]
#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub enum CommandResponse {
    /// Command was processed successfully.
    Ok,
    /// Command processing failed.
    Error,
    /// Command handler was not found for the command type.
    NotFound,
}
