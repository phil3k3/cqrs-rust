use std::fmt::{Display, Formatter};
use uuid::Uuid;

pub struct CommandMetadata {
    pub(crate) subject: String,
    pub(crate) command_type: String,
    pub(crate) version: i32
}

pub struct CommandServerResult {
    pub(crate) command_response: CommandResponse,
    pub(crate) service_id: String,
}

pub struct CarrierEvent {
    pub payload: Vec<u8>,
    pub event_id: Uuid
}

#[derive(PartialEq, Debug)]
pub enum CommandResponse {
    Ok,
    Error,
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
        }
    }
}
