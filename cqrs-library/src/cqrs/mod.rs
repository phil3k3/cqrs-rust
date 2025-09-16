pub mod command;

pub mod traits;

pub mod messages;
mod operations;
use crate::cqrs::command::CommandStore;
use crate::cqrs::messages::CommandResponse;
use crate::cqrs::operations::{
    decode_message, handle_command, serialize_command_to_protobuf, serialize_event_to_protobuf,
};
use crate::cqrs::traits::{Command, Event, EventProducer, MessageConsumer, OutboundChannel};
use async_trait::async_trait;
use cqrs_messages::cqrs::messages::DomainEventEnvelopeProto;
use dashmap::DashMap;
use log::debug;
use prost::Message;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::oneshot::{channel, Sender};
use uuid::Uuid;

use crate::prelude::*;

pub struct CqrsEventProducer {
    service_id: String,
    event_channel: Box<dyn OutboundChannel + Send>,
}

impl EventProducer for CqrsEventProducer {
    fn produce(&self, event: &dyn Event) -> Result<()> {
        let event_message = self.convert_event(event)?;
        self.event_channel
            .send(Vec::from(event.get_id()), event_message);
        Ok(())
    }
}

impl<'e> CqrsEventProducer {
    pub fn new(
        service_id: &str,
        event_channel: Box<dyn OutboundChannel + Send>,
    ) -> CqrsEventProducer {
        CqrsEventProducer {
            service_id: String::from(service_id),
            event_channel,
        }
    }

    fn convert_event(&self, event: &dyn Event) -> Result<Vec<u8>> {
        let event_id = Uuid::new_v4().to_string();
        serialize_event_to_protobuf(event, self.service_id.as_str(), event_id.as_str())
    }
}

pub struct CommandServiceClient {
    service_id: String,
    service_instance_id: u32,
    command_channel: Box<dyn OutboundChannel + Sync + Send>,
    pending_responses_senders: Arc<DashMap<String, Sender<CommandResponse>>>,
}

#[async_trait]
impl MessageConsumer for CommandServiceClient {
    async fn consume(&self, message: &[u8]) -> Result<()> {
        self.consume_message(message.to_vec()).await
    }
}

pub struct EventListener {
    handlers: HashMap<String, Vec<EventHandlerFn>>,
}

type EventHandlerFn = fn(&dyn Event) -> ();

impl EventListener {
    pub fn new() -> EventListener {
        EventListener {
            handlers: HashMap::new(),
        }
    }

    pub fn register_handler(&mut self, event_type: &str, handler: EventHandlerFn) {
        self.handlers
            .entry(String::from(event_type))
            .or_insert(Vec::new())
            .push(handler);
    }
}

#[async_trait]
impl MessageConsumer for EventListener {
    async fn consume(&self, message: &[u8]) -> Result<()> {
        let proto_message = DomainEventEnvelopeProto::decode(message)?;
        let event = serde_json::from_slice::<Box<dyn Event>>(proto_message.event.as_slice())?;
        if let Some(handlers) = self.handlers.get(proto_message.r#type.as_str()) {
            for handler in handlers {
                handler(event.deref());
            }
        }
        Ok(())
    }
}

impl<'a> CommandServiceClient {
    pub fn new(
        service_id: &str,
        command_channel: Box<dyn OutboundChannel + Sync + Send>,
    ) -> CommandServiceClient {
        CommandServiceClient {
            service_id: String::from(service_id),
            service_instance_id: 0u32,
            command_channel,
            pending_responses_senders: Arc::new(DashMap::new()),
        }
    }

    pub async fn send_command<C: Command<'a> + ?Sized>(
        &self,
        command: &C,
    ) -> Result<CommandResponse> {
        let command_id = Uuid::new_v4().to_string();
        let serialized_command = serialize_command_to_protobuf(
            &command_id,
            command,
            String::from(&self.service_id),
            self.service_instance_id,
        )?;
        let (tx, rx) = channel();

        self.pending_responses_senders
            .insert(command_id.to_owned(), tx);

        self.command_channel.send(
            command.get_subject().as_bytes().to_vec(),
            serialized_command,
        );

        rx.await.map_err(|x| x.into())
    }

    pub fn send_command_async<C: Command<'a> + ?Sized>(
        &mut self,
        command: &C,
        command_channel: &mut (dyn OutboundChannel + Send + Sync),
    ) -> Result<()> {
        let command_id = Uuid::new_v4().to_string();
        let serialized_command = serialize_command_to_protobuf(
            &command_id,
            command,
            String::from(&self.service_id),
            self.service_instance_id,
        )?;
        command_channel.send(
            command.get_subject().as_bytes().to_vec(),
            serialized_command,
        );
        Ok(())
    }

    async fn consume_message(&self, message: Vec<u8>) -> Result<()> {
        let command_response = decode_message(message)?;
        if let Some(waiting_caller) = self
            .pending_responses_senders
            .remove(command_response.1.as_str())
        {
            debug!(
                "Received response for {}: {}",
                command_response.1.as_str(),
                command_response.0
            );
            waiting_caller
                .1
                .send(command_response.0)
                .expect("Command could not be delivered");
            Ok(())
        } else {
            Err(Error::Generic(String::from(
                "Command response for unknown command",
            )))
        }
    }
}

pub struct CommandServiceServer<'c> {
    command_store: &'c CommandStore,
    event_producer: &'c mut CqrsEventProducer,
    command_response_channel: &'c dyn OutboundChannel,
}

#[async_trait]
impl MessageConsumer for CommandServiceServer<'_> {
    async fn consume(&self, message: &[u8]) -> Result<()> {
        let command_response = handle_command(&message, &self.command_store, &self.event_producer)?;
        match command_response {
            None => Err(Error::Generic("No command response found".into())),
            Some(command_response) => {
                self.command_response_channel
                    .send("".as_bytes().to_vec(), command_response);
                Ok(())
            }
        }
    }
}

impl<'a> CommandServiceServer<'a> {
    pub fn new(
        command_store: &'a CommandStore,
        event_producer: &'a mut CqrsEventProducer,
        command_response_channel: &'a dyn OutboundChannel,
    ) -> CommandServiceServer<'a> {
        CommandServiceServer {
            command_store,
            event_producer,
            command_response_channel,
        }
    }
}

#[cfg(test)]
mod tests {
    use config::Config;
    use serde::{Deserialize, Serialize};
    use std::env;
    use std::sync::{Arc, Mutex};

    use crate::cqrs::command::{CommandAccessor, CommandStore};
    use crate::cqrs::messages::CommandResponse;
    use crate::cqrs::traits::{
        Command, EventProducer, InboundChannel, MessageConsumer, OutboundChannel,
    };
    use crate::cqrs::{CommandServiceClient, CommandServiceServer, CqrsEventProducer, Event};
    use tokio::sync::oneshot;
    use tokio::sync::oneshot::error::RecvError;
    use tokio::sync::oneshot::{Receiver, Sender};

    #[derive(Debug, Deserialize, Serialize)]
    struct TestCreateUserCommand {
        user_id: String,
        name: String,
    }

    #[derive(Debug, Deserialize, Serialize)]
    struct UserCreatedEvent {
        user_id: String,
        name: String,
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
    impl Event for UserCreatedEvent {
        fn get_id(&self) -> String {
            self.user_id.to_owned()
        }

        fn get_type(&self) -> String {
            String::from("UserCreatedEvent")
        }
    }

    struct TokioOutboundChannel {
        sender: Mutex<Option<Sender<Vec<u8>>>>,
    }

    impl TokioOutboundChannel {
        fn new(sender: Sender<Vec<u8>>) -> Self {
            TokioOutboundChannel {
                sender: Mutex::new(Some(sender)),
            }
        }
    }

    impl OutboundChannel for TokioOutboundChannel {
        fn send(&self, _key: Vec<u8>, message: Vec<u8>) {
            let mut guard = self.sender.lock().expect("Could not lock mutex");
            let tx = guard.take().expect("Could not take sender");
            tx.send(message).expect("Could not send message");
        }
    }

    struct TokioInboundChannel {
        receiver: Option<Receiver<Vec<u8>>>,
    }

    impl InboundChannel for TokioInboundChannel {
        fn consume(&mut self) -> Option<Vec<u8>> {
            if let Some(mut receiver) = self.receiver.take() {
                let result = match receiver.try_recv() {
                    Ok(k) => Some(k),
                    Err(_) => None,
                };
                result
            } else {
                None
            }
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
            name: String::from("def"),
        };
        let serialized_user = serde_json::to_vec(&command).unwrap();

        let deserialized_command =
            serde_json::from_slice::<TestCreateUserCommand>(serialized_user.as_slice()).unwrap();

        assert_eq!(command.user_id, deserialized_command.user_id);
        assert_eq!(command.name, deserialized_command.name);
    }

    #[test]
    fn test_serialize_anonymous() {
        let command = TestCreateUserCommand {
            user_id: String::from("abc"),
            name: String::from("def"),
        };
        let serialized_user = serde_json::to_vec(&command).unwrap();

        let deserialized_command = deserialize::<TestCreateUserCommand>(&serialized_user);

        assert_eq!(command.user_id, deserialized_command.user_id);
        assert_eq!(command.name, deserialized_command.name);
    }

    fn verify_handle_create_user(
        command_accessor: &mut CommandAccessor,
        event_producer: &dyn EventProducer,
    ) -> CommandResponse {
        let command: Box<TestCreateUserCommand> = command_accessor.get_command().unwrap();

        assert_eq!(command.user_id, "user_id");
        assert_eq!(command.name, "user_name");

        let event = UserCreatedEvent {
            user_id: command.user_id,
            name: command.name,
        };
        event_producer.produce(&event);

        CommandResponse::Ok
    }

    #[tokio::test]
    async fn test_serialize_command_response() {
        let command = TestCreateUserCommand {
            user_id: String::from("user_id"),
            name: String::from("user_name"),
        };

        if env::var("RUST_LOG").is_err() {
            env::set_var("RUST_LOG", "debug")
        }

        env_logger::init();

        let (command_sender, command_receiver): (Sender<Vec<u8>>, Receiver<Vec<u8>>) =
            oneshot::channel();
        let (response_sender, response_receiver): (Sender<Vec<u8>>, Receiver<Vec<u8>>) =
            oneshot::channel();
        let (event_sender, _event_receiver): (Sender<Vec<u8>>, Receiver<Vec<u8>>) =
            oneshot::channel();

        let server_handle = tokio::task::spawn(async move {
            let mut command_store = CommandStore::new("COMMAND-SERVER");
            command_store.register_handler("CreateUserCommand", verify_handle_create_user);

            let mut event_producer = CqrsEventProducer::new(
                "COMMAND-SERVER",
                Box::new(TokioOutboundChannel::new(event_sender)),
            );
            let outbound_channel = TokioOutboundChannel {
                sender: Mutex::new(Some(response_sender)),
            };
            let command_service_server =
                CommandServiceServer::new(&command_store, &mut event_producer, &outbound_channel);

            let result = command_receiver.await.unwrap();
            command_service_server
                .consume(result.as_slice())
                .await
                .unwrap();
        });

        let command_service_client = Arc::new(CommandServiceClient::new(
            "COMMAND-CLIENT",
            Box::new(TokioOutboundChannel::new(command_sender)),
        ));

        let client_for_task = Arc::clone(&command_service_client);
        let client_handle = tokio::task::spawn(async move {
            let result = response_receiver.await.unwrap();
            client_for_task.consume_message(result).await.unwrap();
        });

        let actual_command_response = command_service_client.send_command(&command).await.unwrap();
        assert_eq!(actual_command_response, CommandResponse::Ok);

        client_handle.abort();
        server_handle.abort();
    }
}
