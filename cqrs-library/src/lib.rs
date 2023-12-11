use std::collections::HashMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::Deref;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use chrono::Utc;
use prost::Message;
use log::{debug, error};
pub use crate::messages::{CommandMetadata, CommandResponse, CommandServerResult};

pub mod envelope {
    include!(concat!(env!("OUT_DIR"), "/cqrs.rs"));
}

mod messages;

type CommandHandlerFn = fn(&mut CommandAccessor, &mut dyn EventProducer) -> CommandResponse;

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
    fn handle_command(&self, command_type: &str, command_accessor: &mut CommandAccessor, event_producer: &'a mut dyn EventProducer) -> Option<CommandServerResult> {
        let command_response = self.command_handlers.get(command_type).unwrap()(command_accessor, event_producer);
        Some(CommandServerResult {
            command_response,
            service_id: self.service_id.to_owned()
        })
    }
}

pub struct EventProducerImpl {
    service_id: String,
    event_channel: Box<dyn OutboundChannel>
}

pub trait EventProducer {
    fn produce(&mut self, event: &dyn Event) ;
}

impl EventProducer for EventProducerImpl {

    fn produce(&mut self, event: &dyn Event) {
        let event_message = self.convert_event(event);
        self.event_channel.send(Vec::from(event.get_id()), event_message);
    }
}

impl<'e> EventProducerImpl {

    pub fn new(service_id: &str, event_channel: Box<dyn OutboundChannel>) -> EventProducerImpl {
        EventProducerImpl { service_id: String::from(service_id), event_channel }
    }

    fn convert_event(&mut self, event: &dyn Event) -> Vec<u8> {
        let event_id = Uuid::new_v4().to_string();
        let event_serialized = serialize_event_to_protobuf(event, self.service_id.as_str(), event_id.as_str());
        return event_serialized.0;
    }

}

fn serialize_event_to_protobuf(event: &dyn Event, service_id: &str, event_id: &str) -> (Vec<u8>, String) {
    let serialized_event = serde_json::to_vec(&event).unwrap();
    let event_id = String::from(event_id);
    let event_envelope = envelope::DomainEventEnvelopeProto {
        id: event_id.to_owned(),
        timestamp: Utc::now().timestamp(),
        transaction_id: Uuid::new_v4().to_string(),
        r#type: event.get_type().to_owned(),
        version: event.get_version().to_owned(),
        stream_info: None,
        event: serialized_event,
        partition_key: event.get_id(),
        producing_service_id: service_id.to_owned(),
        producing_service_version: "1".to_owned(),
    };
    (serialize_protobuf(&event_envelope), event_id)
}

pub struct CommandAccessor<'a> {
    serialized_command: &'a Vec<u8>,
    command_id: String,
    command_metadata: Option<CommandMetadata>
}


impl<'a> CommandAccessor<'a> {

    pub fn new(serialized_command: &Vec<u8>, command_id: String) -> CommandAccessor {
        CommandAccessor { serialized_command, command_id: command_id, command_metadata: None }
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

pub trait OutboundChannel {
    fn send(&mut self, key: Vec<u8>, message: Vec<u8>);
}

pub trait InboundChannel {
    fn consume(&mut self) -> Option<Vec<u8>>;
}

pub struct CommandServiceClient {
    service_id: String,
    service_instance_id: u32
}

impl CommandServiceClient {
    pub fn clone(&self) -> CommandServiceClient {
        return CommandServiceClient::new(self.service_id.clone().as_str());
    }
}

pub struct EventListener {
    handlers: HashMap<String, Vec<EventHandlerFn>>
}

type EventHandlerFn = fn(&dyn Event) -> ();

impl EventListener {

    pub fn new() -> EventListener {
        EventListener { handlers: HashMap::new() }
    }

    pub fn register_handler(&mut self, event_type: &str, handler: EventHandlerFn) {
        let handlers = self.handlers.get(event_type);
        match handlers {
            None => {
                self.handlers.insert(String::from(event_type), Vec::new());
            }
            _ => {}
        }
        let vec = self.handlers.get_mut(event_type).unwrap();
        vec.push(handler);
    }

    pub fn consume(&self, event_message: &[u8]) {
        let result = envelope::DomainEventEnvelopeProto::decode(&mut Cursor::new(&event_message)).unwrap();
        let event: Box<dyn Event> = serde_json::from_slice(result.event.as_slice()).unwrap();
        let handlers = self.handlers.get(result.r#type.as_str());
        match handlers {
            None => {
                error!("No event handlers found for event type {} and version {}",
                    event.get_type(), event.get_version());
            }
            Some(handlers) => {
                for handler in handlers {
                    handler(event.deref());
                }
            }
        }
    }
}

impl<'a> CommandServiceClient {

    pub fn new(service_id: &str) -> CommandServiceClient {
        CommandServiceClient {
            service_id: String::from(service_id),
            service_instance_id: 0u32,
        }
    }

    pub fn send_command<C: Command<'a>+?Sized>(&mut self, command: &C, command_channel: &mut (dyn OutboundChannel + Send + Sync)) {
        let command_id = Uuid::new_v4().to_string();
        let serialized_command = serialize_command_to_protobuf(&command_id, command, String::from(&self.service_id), self.service_instance_id);
        command_channel.send(command.get_subject().as_bytes().to_vec(),serialized_command.0);
    }

    pub fn send_command_async<C: Command<'a>+?Sized>(&mut self, command: &C, command_channel: &mut (dyn OutboundChannel + Send + Sync)) {
        let command_id = Uuid::new_v4().to_string();
        let serialized_command = serialize_command_to_protobuf(&command_id, command, String::from(&self.service_id), self.service_instance_id);
        command_channel.send(command.get_subject().as_bytes().to_vec(),serialized_command.0);
    }

    pub fn read_response(&mut self, command_response_channel: &mut (dyn InboundChannel)) -> Option<CommandResponse> {
        let serialized_message = command_response_channel.consume();
        return match serialized_message {
            None => {
                debug!("No response");
                None
            }
            Some(message) => {
                let command_response = envelope::CommandResponseEnvelopeProto::decode(&mut Cursor::new(&message)).unwrap();
                let command_response_result = serde_json::from_slice::<CommandResponseResult>(&command_response.response).unwrap();
                if command_response_result.result.eq("Ok") {
                    Some(CommandResponse::Ok)
                } else {
                    Some(CommandResponse::Error)
                }
            }
        }
    }
}

pub struct CommandServiceServer<'c> {
    command_store: &'c CommandStore,
    event_producer: &'c mut EventProducerImpl
}

impl<'a> CommandServiceServer<'a> {

    pub fn new(command_store: &'a CommandStore, event_producer: &'a mut EventProducerImpl) -> Box<CommandServiceServer<'a>> {
        Box::new(CommandServiceServer { command_store, event_producer })
    }

    pub fn consume(&mut self, command_channel: &mut dyn InboundChannel, command_response_channel: &mut dyn OutboundChannel) {
        let message = command_channel.consume();
        match message {
            None => debug!("No message"),
            Some(message) => {
                let command_response = handle_command(&message, &self.command_store, &mut self.event_producer);
                match command_response {
                    None => {
                        error!("No command response")
                    }
                    Some(command_response) => {
                        command_response_channel.send("".as_bytes().to_vec(), command_response)
                    }
                }
            }
        }
    }

    pub fn consume_async(&mut self, message: &mut Vec<u8>, command_response_channel: &mut dyn OutboundChannel) {
        let command_response = handle_command(&message, &self.command_store, &mut self.event_producer);
        match command_response {
            None => {
                error!("No command response")
            }
            Some(command_response) => {
                command_response_channel.send("".as_bytes().to_vec(), command_response);
            }
        }
    }

}



pub trait Command<'de> : Deserialize<'de> + Serialize {
    fn get_subject(&self) -> String;
    fn get_type(&self) -> String;
    fn get_version(&self) -> i32 {
        1
    }
}

#[typetag::serde(tag = "type")]
pub trait Event : Debug {
    fn get_id(&self) -> String;

    fn get_type(&self) -> String;

    fn get_version(&self) -> i32 {
        return 1;
    }
}


fn serialize_command_to_protobuf<'a, C: Command<'a>>(command_id: &str, command: &C, service_id: String, service_instance_id: u32) -> (Vec<u8>, String) {
    let serialized_command = serde_json::to_vec(command).unwrap();
    let service_instance_id_i32 = service_instance_id as i32;
    let command_id = String::from(command_id);
    let command_envelope = envelope::CommandEnvelopeProto {
        id: command_id.to_owned(),
        timestamp: Utc::now().timestamp(),
        service_id,
        service_instance_id: service_instance_id_i32,
        transaction_id: Uuid::new_v4().to_string(),
        r#type: command.get_type().to_owned(),
        version: command.get_version().to_owned(),
        subject: command.get_subject().to_owned(),
        command: serialized_command.to_vec()
    };
    (serialize_protobuf(&command_envelope), command_id)
}

fn serialize_command_response_to_protobuf(command_response: CommandResponse,
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
            let response_envelope = envelope::CommandResponseEnvelopeProto {
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
            Some(serialize_protobuf(&response_envelope))
        }
    }
}


fn serialize_protobuf<M: Message+Sized>(envelope: &M) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.reserve(envelope.encoded_len());
    envelope.encode(&mut buf).expect("Encoding failed");
    buf
}

fn handle_command(serialized_command: &Vec<u8>, command_store: &CommandStore, event_producer: &mut EventProducerImpl) -> Option<Vec<u8>> {
    let result = envelope::CommandEnvelopeProto::decode(&mut Cursor::new(&serialized_command)).unwrap();

    let mut deserializer = CommandAccessor::new(&result.command, result.id);

    let command_response = command_store.handle_command(&result.r#type, &mut deserializer, event_producer);

    match command_response {
        None => None,
        Some(command_server_result) => {
            let option = serialize_command_response_to_protobuf(
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

#[cfg(test)]
mod tests {

    use crate::{CommandAccessor, CommandStore, CommandResponse, CommandServiceClient, OutboundChannel, InboundChannel, CommandServiceServer, Command, EventProducer, Event};
    use serde::{Serialize, Deserialize, Deserializer, Serializer};

    use log::{debug};


    #[derive(Debug, Deserialize, Serialize)]
    struct TestCreateUserCommand {
        user_id: String,
        name: String
    }

    #[derive(Debug, Deserialize, Serialize)]
    struct UserCreatedEvent {
        user_id: String,
        name: String
    }

    impl Command<'_> for TestCreateUserCommand {
        fn get_subject(&self) -> String {
            self.user_id.to_owned()
        }
        fn get_type(&self) -> String {
            String::from("CreateUserCommand")
        }
    }

    #[typetag::serde]
    impl Event<'_> for UserCreatedEvent {
        fn get_id(&self) -> String {
            self.user_id.to_owned()
        }

        fn get_type(&self) -> String {
            String::from("UserCreatedEvent")
        }
    }

    struct CapturingChannel {
        messages: Vec<Vec<u8>>
    }

    impl OutboundChannel for CapturingChannel {
        fn send(&mut self, _key: Vec<u8>, command: Vec<u8>) {
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

    fn deserialize<'a, T: Command<'a>>(command: &'a Vec<u8>) -> Box<T> {
        let v = command.as_slice();
        Box::new(serde_json::from_slice::<T>(v).unwrap())
    }

    #[test]
    fn test_serialize_json() {
        let command = TestCreateUserCommand {
            user_id: String::from("abc"),
            name: String::from("def")
        };
        let serialized_user = serde_json::to_vec(&command).unwrap();

        let deserialized_command = serde_json::from_slice::<TestCreateUserCommand>(serialized_user.as_slice()).unwrap();

        assert_eq!(command.user_id, deserialized_command.user_id);
        assert_eq!(command.name, deserialized_command.name);
    }

    #[test]
    fn test_serialize_anonymous() {
        let command = TestCreateUserCommand {
            user_id: String::from("abc"),
            name: String::from("def")
        };
        let serialized_user = serde_json::to_vec(&command).unwrap();

        let deserialized_command = deserialize::<TestCreateUserCommand>(&serialized_user);

        assert_eq!(command.user_id, deserialized_command.user_id);
        assert_eq!(command.name, deserialized_command.name);
    }

    fn verify_handle_create_user(command_accessor: &mut CommandAccessor, event_producer: &mut dyn EventProducer) -> CommandResponse {
        let command: Box<TestCreateUserCommand> = command_accessor.get_command();

        assert_eq!(command.user_id, "user_id");
        assert_eq!(command.name, "user_name");

        let event = UserCreatedEvent { user_id: command.user_id, name: command.name };
        event_producer.produce(&event);

        CommandResponse::Ok
    }

    #[test]
    fn test_serialize_command_response() {

        let command = TestCreateUserCommand {
            user_id: String::from("user_id"),
            name: String::from("user_name")
        };

        let mut command_store = CommandStore::new("COMMAND-SERVER");
        command_store.register_handler("CreateUserCommand", verify_handle_create_user);

        let mut command_channel = CapturingChannel { messages: Vec::new() };
        let mut command_response_channel = CapturingChannel { messages: Vec::new() };
        let event_channel = CapturingChannel { messages: Vec::new() };

        let event_producer = EventProducer::new(&event_channel, "COMMAND-SERVER");
        let mut command_service_client = CommandServiceClient::new("COMMAND-SERVERIMPORT");
        let mut command_service_server = CommandServiceServer::new(&command_store, &event_producer);

        command_service_client.send_command(&command, &mut command_channel);

        command_service_server.consume(&mut command_channel, &mut command_response_channel);
        let command_response = command_service_client.read_response(&mut command_response_channel);

        assert_eq!(command_response, CommandResponse::Ok);
    }
}
