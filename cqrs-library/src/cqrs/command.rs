use std::collections::HashMap;
use serde::Deserialize;
use crate::cqrs::Command;
use crate::cqrs::messages::{CommandMetadata, CommandResponse, CommandServerResult};
use crate::cqrs::traits::EventProducer;

type CommandHandlerFn = fn(&mut CommandAccessor, &dyn EventProducer) -> CommandResponse;

pub struct CommandAccessor<'a> {
    pub serialized_command: &'a Vec<u8>,
    pub command_id: String,
    pub command_metadata: Option<CommandMetadata>
}
impl<'a> CommandAccessor<'a> {

    pub fn new(serialized_command: &'a Vec<u8>, command_id: String) -> CommandAccessor {
        CommandAccessor {
            serialized_command,
            command_id,
            command_metadata: None
        }
    }

    pub fn get_command<T: Deserialize<'a> + Command<'a>>(&mut self) -> Box<T> {
        let slice = self.serialized_command.as_slice();
        let command = serde_json::from_slice::<T>(slice).unwrap();
        self.command_metadata = Some(CommandMetadata {
            subject: command.get_subject(),
            command_type: command.get_type(),
            version: command.get_version()
        });
        Box::new(command)
    }
}


pub struct CommandStore {
    command_handlers: HashMap<String, CommandHandlerFn>,
    service_id: String
}

impl<'a> CommandStore {
    pub fn new(service_id: &str) -> CommandStore {
        CommandStore { command_handlers: HashMap::new(), service_id: String::from(service_id)}
    }
    pub fn register_handler(&mut self, command: &str, handler: CommandHandlerFn) {
        self.command_handlers.insert(String::from(command), handler);
    }
    pub fn handle_command(&self, command_type: &str, command_accessor: &mut CommandAccessor, event_producer: &'a dyn EventProducer) -> Option<CommandServerResult> {
        let command_response = self.command_handlers.get(command_type).unwrap()(command_accessor, event_producer);
        Some(CommandServerResult {
            command_response,
            service_id: self.service_id.to_owned()
        })
    }
}
