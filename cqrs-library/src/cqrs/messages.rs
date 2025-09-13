use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

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
    pub(crate) result: String,
}

#[derive(PartialEq, Debug)]
pub enum CommandResponse {
    Ok,
    Error,
    NotFound,
}

impl Display for CommandResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CommandResponse::Ok => {
                write!(f, "Ok")
            }
            CommandResponse::Error => {
                write!(f, "Error")
            }
            CommandResponse::NotFound => {
                write!(f, "NotFound")
            }
        }
    }
}
