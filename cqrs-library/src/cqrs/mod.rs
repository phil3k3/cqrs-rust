pub mod command;

pub mod traits;

pub mod messages;
mod operations;
use crate::cqrs::command::CommandStore;
use crate::cqrs::messages::{CommandResponse, CommandResponseResult};
use crate::cqrs::operations::{
    handle_command, serialize_command_to_protobuf, serialize_event_to_protobuf,
};
use crate::cqrs::traits::{
    Command, Event, EventProducer, InboundChannel, MessageConsumer, OutboundChannel,
};
use config::Config;
use cqrs_messages::cqrs::messages::{CommandResponseEnvelopeProto, DomainEventEnvelopeProto};
use log::{debug, error};
use prost::Message;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot::{channel, Sender};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::prelude::*;

pub struct CqrsEventProducer {
    service_id: String,
    event_channel: Box<dyn OutboundChannel + Send>,
}

impl EventProducer for CqrsEventProducer {
    fn produce(&self, event: &dyn Event) {
        let event_message = self.convert_event(event);
        self.event_channel
            .send(Vec::from(event.get_id()), event_message);
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

pub struct CommandServiceClient<T> {
    service_id: String,
    service_instance_id: u32,
    command_channel: Box<dyn OutboundChannel + Sync + Send>,
    pending_responses_senders: Arc<Mutex<HashMap<String, Sender<CommandResponse>>>>,
    channel_builder: InboundChannelBuilder<T>,
}

type InboundChannelBuilder<T> = Arc<Mutex<Option<Box<dyn FnOnce(Config) -> Box<T> + Sync + Send>>>>;

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

impl MessageConsumer for EventListener {
    fn consume(&self, message: &[u8]) -> Result<()> {
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

impl<'a, T: InboundChannel + Send + Sync + 'static> CommandServiceClient<T> {
    pub fn new(
        service_id: &str,
        channel_builder: InboundChannelBuilder<T>,
        command_channel: Box<dyn OutboundChannel + Sync + Send>,
    ) -> CommandServiceClient<T> {
        CommandServiceClient {
            service_id: String::from(service_id),
            service_instance_id: 0u32,
            command_channel,
            pending_responses_senders: Arc::new(Mutex::new(HashMap::new())),
            channel_builder,
        }
    }

    pub fn start(&mut self, settings: Config) -> JoinHandle<()> {
        // Clone Arc handles to move into the async block
        // let running = self.running.clone();
        let pending_responses_senders = self.pending_responses_senders.clone();
        let channel_reader = self.channel_builder.clone();

        tokio::task::spawn(async move {
            dbg!("RECEIVE");
            dbg!(Arc::as_ptr(&pending_responses_senders));
            let mut guard = channel_reader.lock().await;
            if let Some(func) = guard.take() {
                let mut channel = func(settings);
                loop {
                    let response = CommandServiceClient::read_response(&mut channel);
                    match response {
                        None => {
                            debug!("No result");
                        }
                        Some(command_response) => {
                            let mut waiting_callers = pending_responses_senders.lock().await;
                            {
                                dbg!("map addr (remove) =", (&*waiting_callers as *const _));
                                dbg!(&waiting_callers);
                                dbg!(&command_response);
                                if let Some(waiting_caller) =
                                    waiting_callers.remove(command_response.1.as_str())
                                {
                                    debug!(
                                        "Received response for {}: {}",
                                        command_response.1.as_str(),
                                        command_response.0
                                    );
                                    waiting_caller
                                        .send(command_response.0)
                                        .expect("Command could not be delivered");
                                } else {
                                    error!("Sender not found");
                                }
                            }
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(1000)).await;
                }
            }
        })
    }

    pub async fn send_command<C: Command<'a> + ?Sized>(
        &mut self,
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

        {
            let mut waiting_callers = self.pending_responses_senders.lock().await;
            waiting_callers.insert(command_id.to_owned(), tx);
            dbg!("map addr (insert) =", &*waiting_callers as *const _);
            dbg!(Arc::as_ptr(&self.pending_responses_senders));
            dbg!(&waiting_callers);
        }

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
    }

    fn read_response(command_response_channel: &mut Box<T>) -> Option<(CommandResponse, String)> {
        command_response_channel.consume().map(|message| {
            let result = CommandResponseEnvelopeProto::decode(message.as_slice());
            match result {
                Ok(command_response) => {
                    let command_response_result = serde_json::from_slice::<CommandResponseResult>(&command_response.response);
                    match command_response_result {
                        Ok(crr) => {
                            if crr.result.eq("Ok") {
                                Some((CommandResponse::Ok, command_response.command_id))
                            } else {
                                Some((CommandResponse::Error, command_response.command_id))
                            }
                        }
                        Err(err) =>  {
                            error!("{}", err);
                            None
                        }
                    }
                }
                Err(err) => {
                    error!("{}", err);
                    None
                }
            }
        }).flatten()
    }
}

pub struct CommandServiceServer<'c> {
    command_store: &'c CommandStore,
    event_producer: &'c mut CqrsEventProducer,
    command_response_channel: &'c dyn OutboundChannel,
}

impl MessageConsumer for CommandServiceServer<'_> {
    fn consume(&self, message: &[u8]) -> Result<()> {
        let command_response = handle_command(&message, &self.command_store, &self.event_producer);
        match command_response {
            None => {
                error!("No command response")
            }
            Some(command_response) => {
                self.command_response_channel
                    .send("".as_bytes().to_vec(), command_response);
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
        let command: Box<TestCreateUserCommand> = command_accessor.get_command();

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

            match command_receiver.await {
                Ok(k) => command_service_server.consume(k.as_slice()),
                Err(_) => {
                    assert!(false);
                }
            }
        });

        let mut command_service_client = CommandServiceClient::new(
            "COMMAND-CLIENT",
            Arc::new(tokio::sync::Mutex::new(Some(Box::new(|_config| {
                return Box::new(TokioInboundChannel {
                    receiver: Some(response_receiver),
                });
            })))),
            Box::new(TokioOutboundChannel::new(command_sender)),
        );

        let client_handle = command_service_client.start(Config::default());
        let actual_command_response = command_service_client.send_command(&command).await.unwrap();
        assert_eq!(actual_command_response, CommandResponse::Ok);

        client_handle.abort();
        server_handle.abort();
    }
}
