use serde::{Deserialize, Serialize};

pub struct CommandMetadata {
    pub(crate) subject: String,
    pub(crate) command_type: String,
    pub(crate) version: i32,
}

pub struct CommandServerResult {
    pub(crate) command_response: CommandResponse,
    pub(crate) service_id: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CommandResponseResult {
    pub(crate) entity_id: String,
    pub(crate) result: CommandResponse,
}

#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub enum CommandResponse {
    Ok,
    Error,
    NotFound,
}
