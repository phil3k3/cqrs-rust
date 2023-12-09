use std::fmt::{Display, Formatter};

pub struct CommandMetadata {
    pub(crate) subject: String,
    pub(crate) command_type: String,
    pub(crate) version: i32
}

pub struct CommandServerResult {
    pub(crate) command_response: CommandResponse,
    pub(crate) service_id: String,
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
