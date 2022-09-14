use std::collections::HashMap;
use std::future::{Future};
use std::io::Cursor;
use std::pin::Pin;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use chrono::Utc;
use prost::Message;
use log::{info, warn, error, debug};

struct CommandStore {
    command_handlers: HashMap<String, Box<dyn Fn(&mut CommandAccessor) -> CommandResponse>>,
    service_id: String
}

impl CommandStore {
    fn new(service_id: &str) -> CommandStore {
        CommandStore { command_handlers: HashMap::new(), service_id: String::from(service_id)}
    }
    fn register_handler(&mut self, command: &str, handler: Box<dyn Fn(&mut CommandAccessor) -> CommandResponse>) {
        self.command_handlers.insert(String::from(command), handler);
    }
    fn handle_command(&self, command_type: &str, command_accessor: &mut CommandAccessor) -> Option<CommandServerResult> {
        let command_response = self.command_handlers.get(command_type).unwrap()(command_accessor);
        Some(CommandServerResult {
            command_response,
            service_id: self.service_id.to_owned()
        })
    }
}

struct ProtobufSerializer {

}

impl ProtobufSerializer {
    fn serialize<M: Message+Sized>(envelope: &M) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.reserve(envelope.encoded_len());
        // Unwrap is safe, since we have reserved sufficient capacity in the vector.
        envelope.encode(&mut buf);
        buf
    }
}


pub mod envelope {
    include!(concat!(env!("OUT_DIR"), "/cqrs.rs"));
}

fn main() {
    println!("Hello, world!");
}

#[derive(Debug, Deserialize, Serialize)]
struct CreateUserCommand {
    user_id: String,
    name: String
}

struct CommandAccessor<'a> {
    serialized_command: &'a Vec<u8>,
    command_id: String,
    command_metadata: Option<CommandMetadata>
}

struct CommandMetadata {
    subject: String,
    command_type: String,
    version: i32
}

// how to make an object safe trait?
struct CommandServerResult {
    command_response: CommandResponse,
    service_id: String,
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

impl<'a> CommandAccessor<'a> {

    pub fn new(serialized_command: &Vec<u8>, command_id: String) -> CommandAccessor {
        CommandAccessor { serialized_command, command_id: command_id, command_metadata: None }
    }

    fn get_command<T: Deserialize<'a> + Command<'a>>(&mut self) -> Box<T> {
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

trait OutboundChannel {
    fn send(&mut self, message: Vec<u8>);
}

trait InboundChannel {
    fn consume(&mut self) -> Option<Vec<u8>>;
}

struct CommandServiceClient {
    service_id: String,
    service_instance_id: u32,
}


struct PendingResponse {
    response: Arc<Mutex<PendingResposeState>>
}

struct PendingResposeState {
    waker: Option<Waker>,
    command_response: Option<CommandResponse>
}


impl Future for PendingResponse {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut command_response_state = self.response.lock().unwrap();
        match &command_response_state.command_response {
            Some(commandResponse) => Poll::Ready(()),
            None => {
                command_response_state.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

impl<'a, 'b> CommandServiceClient {
    pub fn new(service_id: &str) -> CommandServiceClient {
        CommandServiceClient {
            service_id: String::from(service_id),
            service_instance_id: 0u32
        }
    }

    pub fn send_command<C: Command<'a>+?Sized>(&mut self, command: &C, outbound_channel: &mut dyn OutboundChannel) {
        let command_id = Uuid::new_v4().to_string();
        let serialized_command = serialize_to_protobuf(&command_id, command,  String::from(&self.service_id), self.service_instance_id);
        outbound_channel.send(serialized_command);
    }

    pub fn consume(&mut self, inbound_channel: &mut dyn InboundChannel) -> Option<CommandResponse> {
        let message = inbound_channel.consume();
        match message {
            None => None,
            Some(message) => {
                let command_response = envelope::UntypedCommandResponseEnvelopeProto::decode(&mut Cursor::new(&message)).unwrap();
                let command_response_result = deserialize_command_response(&command_response.response);
                if command_response_result.result.eq("Ok") {
                    return Some(CommandResponse::Ok)
                }
                Some(CommandResponse::Error)
            }
        }
    }
}

struct CommandServiceServer<'c> {
    command_store: &'c CommandStore
}

impl<'a> CommandServiceServer<'a> {
    pub fn new( command_store: &'a CommandStore) -> Box<CommandServiceServer<'a>> {
        Box::new(CommandServiceServer { command_store })
    }

    pub fn consume(&mut self, command_channel: &mut dyn InboundChannel, command_response_channel: &mut dyn OutboundChannel) {
        let message = command_channel.consume();
        match message {
            None => error!("No message"),
            Some(message) => {
                let command_response = handle_command(&message, &self.command_store);
                match command_response {
                    None => {
                        error!("No command response")
                    }
                    Some(command_response) => {
                        command_response_channel.send(command_response)
                    }
                }
            }
        }
    }
}

impl Command<'_> for CreateUserCommand {
    fn get_subject(&self) -> String {
        self.user_id.to_owned()
    }
    fn get_type(&self) -> String {
        String::from("CreateUserCommand")
    }
}

trait Command<'de> : Deserialize<'de> + Serialize {
    fn get_subject(&self) -> String;
    fn get_type(&self) -> String;
    fn get_version(&self) -> i32 {
        1
    }
}

fn deserialize<'a, T: Command<'a>>(command: &'a Vec<u8>) -> Box<T> {
    let v = command.as_slice();
    Box::new(serde_json::from_slice::<T>(v).unwrap())
}

fn serialize_to_json<'a, T: Command<'a>>(command: &T) -> Vec<u8> {
    serde_json::to_vec(command).unwrap()
}

fn serialize_to_protobuf<'a, C: Command<'a>>(command_id: &str, command: &C, service_id: String, service_instance_id: u32) -> Vec<u8> {
    let serialized_command = serialize_to_json(command);
    let service_instance_id_i32 = service_instance_id as i32;
    let command_envelope = envelope::UntypedCommandEnvelopeProto {
        id: String::from(command_id),
        timestamp: Utc::now().timestamp(),
        service_id,
        service_instance_id: service_instance_id_i32,
        transaction_id: Uuid::new_v4().to_string(),
        r#type: command.get_type().to_owned(),
        version: command.get_version().to_owned(),
        subject: command.get_subject().to_owned(),
        command: serialized_command.to_vec(),
        request_info: None,
        signature: String::from(""),
        privacy_key: String::from(""),
        correlation_id: String::from("")
    };
    ProtobufSerializer::serialize(&command_envelope)
}

fn handle_command(serialized_command: &Vec<u8>, command_store: &CommandStore) -> Option<Vec<u8>> {
    let result = envelope::UntypedCommandEnvelopeProto::decode(&mut Cursor::new(&serialized_command)).unwrap();

    let mut deserializer = CommandAccessor::new(&result.command, result.id);

    let command_response = command_store.handle_command(&result.r#type, &mut deserializer);

    match command_response {
        None => None,
        Some(command_server_result) => {
            let option = serialize_command_response(
                command_server_result.command_response,
                &deserializer,
                command_server_result.service_id
            );
            Some(option.unwrap())
        }
    }
}


#[derive(Debug, Deserialize, Serialize)]
struct CommandResponseResult {
    entity_id: String,
    result: String
}

fn deserialize_command_response(serialized_command_response: &Vec<u8>) -> CommandResponseResult {
    serde_json::from_slice(serialized_command_response).unwrap()
}

fn serialize_command_response(command_response: CommandResponse,
                              command_accessor: &CommandAccessor,
                              service_id: String) -> Option<Vec<u8>> {

    let command = &command_accessor.command_metadata;
    let command_id = &command_accessor.command_id;
    match command {
        None => None,
        Some(command) => {
            let command_response_result = CommandResponseResult {
                entity_id: command.subject.to_owned(),
                result: command_response.to_string()
            };
            let command_response_serialized = serde_json::to_string(&command_response_result).unwrap();
            let response_envelope = envelope::UntypedCommandResponseEnvelopeProto {
                transaction_id: Uuid::new_v4().to_string(),
                command_id: String::from(command_id),
                timestamp: Utc::now().timestamp(),
                service_id: service_id.to_owned(),
                r#type: String::from(&command.command_type),
                version: command.version,
                response: command_response_serialized.as_bytes().to_vec(),
                error: None,
                id: Uuid::new_v4().to_string()
            };
            Some(ProtobufSerializer::serialize(&response_envelope))
        }
    }

}

#[cfg(test)]
mod tests {

    use crate::{
        CreateUserCommand,
        deserialize,
        CommandAccessor,
        CommandStore,
        CommandResponse,
        CommandServiceClient,
        OutboundChannel,
        InboundChannel,
        CommandServiceServer
    };

    use log::{debug};

    struct CapturingChannel {
        messages: Vec<Vec<u8>>
    }

    impl OutboundChannel for CapturingChannel {
        fn send(&mut self, command: Vec<u8>) {
            debug!("Adding message");
            self.messages.push(command);
        }
    }

    impl InboundChannel for CapturingChannel {
        fn consume(&mut self) -> Option<Vec<u8>> {
            debug!("Removing message");
            self.messages.pop()
        }
    }

    #[test]
    fn test_serialize_json() {
        let command = CreateUserCommand {
            user_id: String::from("abc"),
            name: String::from("def")
        };
        let serialized_user = serde_json::to_vec(&command).unwrap();

        let deserialized_command = serde_json::from_slice::<CreateUserCommand>(serialized_user.as_slice()).unwrap();

        assert_eq!(command.user_id, deserialized_command.user_id);
        assert_eq!(command.name, deserialized_command.name);
    }

    #[test]
    fn test_serialize_anonymous() {
        let command = CreateUserCommand {
            user_id: String::from("abc"),
            name: String::from("def")
        };
        let serialized_user = serde_json::to_vec(&command).unwrap();

        let deserialized_command = deserialize::<CreateUserCommand>(&serialized_user);

        assert_eq!(command.user_id, deserialized_command.user_id);
        assert_eq!(command.name, deserialized_command.name);
    }

    fn verify_handle_create_user(command_accessor: &mut CommandAccessor) -> CommandResponse {
        let command: Box<CreateUserCommand> = command_accessor.get_command();

        assert_eq!(command.user_id, "user_id");
        assert_eq!(command.name, "user_name");

        CommandResponse::Ok
    }

    #[test]
    fn test_serialize_command_response() {

        let command = CreateUserCommand {
            user_id: String::from("user_id"),
            name: String::from("user_name")
        };

        let mut command_store = CommandStore::new("COMMAND-SERVER");
        command_store.register_handler("CreateUserCommand", Box::new(&verify_handle_create_user));

        let mut command_channel = CapturingChannel { messages: Vec::new() };
        let mut command_response_channel = CapturingChannel { messages: Vec::new() };

        let mut command_service_client = CommandServiceClient::new("COMMAND-SERVERIMPORT");
        let mut command_service_server = CommandServiceServer::new(&command_store);

        command_service_client.send_command(&command, &mut command_channel);

        command_service_server.consume(&mut command_channel, &mut command_response_channel);
        let command_response = command_service_client.consume(&mut command_response_channel);

        assert_eq!(command_response, Some(CommandResponse::Ok));
    }

    #[test]
    fn test_process() {
        let command = CreateUserCommand {
            user_id: String::from("user_id"),
            name: String::from("user_name")
        };

        let mut command_store = CommandStore::new("COMMAND-SERVER");
        command_store.register_handler("CreateUserCommand", Box::new(&verify_handle_create_user));

        let mut command_channel = CapturingChannel { messages: Vec::new() };
        let mut command_response_channel = CapturingChannel { messages: Vec::new() };

        let mut command_service_client = CommandServiceClient::new("COMMAND-CLIENT");
        let mut command_service_server = CommandServiceServer::new(&command_store);

        command_service_client.send_command(&command, &mut command_channel);

        command_service_server.consume(&mut command_channel, &mut command_response_channel);
        let command_response = command_service_client.consume(&mut command_response_channel);

        assert_eq!(command_response, Some(CommandResponse::Ok));
    }

}