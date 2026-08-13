pub mod traits;

pub mod messages;
mod operations;
use crate::cqrs::messages::CommandResponse;
use crate::cqrs::operations::{
    decode_message, serialize_command_response_to_protobuf, serialize_command_to_protobuf,
    serialize_event_to_protobuf,
};
use crate::cqrs::traits::{
    Command, CommandEnvelope, EventEnvelope, EventProducer, MessageConsumer, OutboundChannel,
    Transport,
};
use async_trait::async_trait;
use cqrs_messages::cqrs::messages::{CommandEnvelopeProto, DomainEventEnvelopeProto};
use dashmap::DashMap;
use log::debug;
use prost::Message;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use tokio::sync::oneshot::{channel, Sender};
use uuid::Uuid;

use crate::prelude::*;

/// Client for sending commands and receiving responses.
///
/// The command service client sends commands to a command service server
/// and asynchronously receives responses. It manages pending command responses
/// using correlation IDs.
///
/// # Type Parameters
///
/// - `O`: The outbound channel type used for sending commands.
///
/// # Examples
///
/// ```ignore
/// use cqrs_library::cqrs::CommandServiceClient;
///
/// let client = CommandServiceClient::new("my-service", channel);
/// let response = client.send_command(&my_command).await?;
/// ```
pub struct CommandServiceClient<O: OutboundChannel + Sync + Send> {
    service_id: String,
    service_instance_id: u32,
    command_channel: O,
    pending_responses_senders: Arc<DashMap<String, Sender<CommandResponse>>>,
}

#[async_trait]
impl<O: OutboundChannel + Sync + Send> MessageConsumer for CommandServiceClient<O> {
    async fn consume(&self, message: &[u8]) -> Result<()> {
        self.consume_message(message.to_vec()).await
    }
}

/// Listens for events and dispatches them to a single handler.
///
/// The handler is expected to `match` over the service's own closed event
/// enum, so the compiler enforces that every event type is handled.
///
/// # Examples
///
/// ```ignore
/// use cqrs_library::cqrs::EventListener;
///
/// let listener = EventListener::new(|event: DomainEvent| match event {
///     DomainEvent::UserCreated(e) => handle_user_created(e),
/// });
/// ```
pub struct EventListener {
    dispatch: Box<dyn Fn(&[u8]) -> Result<()> + Send + Sync>,
}

impl EventListener {
    /// Creates an event listener that deserializes every consumed message
    /// into `E` (a service's closed event enum) and passes it to `handler`.
    pub fn new<E, F>(handler: F) -> Self
    where
        E: DeserializeOwned + 'static,
        F: Fn(E) + Send + Sync + 'static,
    {
        Self {
            dispatch: Box::new(move |raw: &[u8]| {
                let proto_message = DomainEventEnvelopeProto::decode(raw)?;
                let event: E = serde_json::from_slice(proto_message.event.as_slice())?;
                handler(event);
                Ok(())
            }),
        }
    }
}

#[async_trait]
impl MessageConsumer for EventListener {
    async fn consume(&self, message: &[u8]) -> Result<()> {
        (self.dispatch)(message)
    }
}

impl<O: OutboundChannel + Send + Sync> CommandServiceClient<O> {
    pub fn new(service_id: &str, command_channel: O) -> CommandServiceClient<O> {
        CommandServiceClient {
            service_id: String::from(service_id),
            service_instance_id: 0u32,
            command_channel,
            pending_responses_senders: Arc::new(DashMap::new()),
        }
    }

    pub async fn send_command<C: Command + ?Sized>(&self, command: &C) -> Result<CommandResponse> {
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
            command.get_subject().as_bytes(),
            serialized_command.as_slice(),
        )?;

        rx.await.map_err(|x| x.into())
    }

    pub fn send_command_async<C: Command + ?Sized>(
        &self,
        command: &C,
        command_channel: &O,
    ) -> Result<()> {
        let command_id = Uuid::new_v4().to_string();
        let serialized_command = serialize_command_to_protobuf(
            &command_id,
            command,
            String::from(&self.service_id),
            self.service_instance_id,
        )?;
        command_channel.send(
            command.get_subject().as_bytes(),
            serialized_command.as_slice(),
        )?;
        Ok(())
    }

    async fn consume_message(&self, message: Vec<u8>) -> Result<()> {
        let command_response = decode_message(message.as_slice())?;
        if let Some(waiting_caller) = self
            .pending_responses_senders
            .remove(command_response.1.as_str())
        {
            debug!(
                "Received response for {}: {:?}",
                command_response.1.as_str(),
                command_response.0
            );
            waiting_caller
                .1
                .send(command_response.0)
                .map_err(|_| Error::ChannelSendFailed("Failed to deliver command response".to_string()))?;
            Ok(())
        } else {
            Err(Error::UnknownCommandResponse(command_response.1))
        }
    }
}

/// Boxed, type-erased dispatch: decode the wire envelope, deserialize into
/// the service's closed command enum `C`, call the handler, and serialize
/// its response. `C` and the handler closure's type are erased here (inside
/// [`CommandServiceServer::new`]) so the struct itself only stays generic
/// over the event type `E`, which it needs to implement [`EventProducer`].
type BoxedDispatch<E> =
    Box<dyn Fn(&[u8], &dyn EventProducer<Event = E>) -> Result<Vec<u8>> + Send + Sync>;

/// Server for processing commands and producing events.
///
/// The command service server decodes each command, routes it to a single
/// application-supplied handler (which typically `match`es over the
/// service's own closed command enum), and lets that handler produce events
/// through the [`EventProducer`] it's given. It implements both
/// [`MessageConsumer`] for consuming commands and [`EventProducer`] for
/// publishing events.
///
/// # Type Parameters
///
/// - `T`: The transport type used for sending responses and events.
/// - `E`: The service's closed event enum.
///
/// # Examples
///
/// ```ignore
/// use cqrs_library::cqrs::CommandServiceServer;
///
/// let server = CommandServiceServer::new("my-service", transport, handle_command);
/// server.run().await?;
/// ```
pub struct CommandServiceServer<T: Transport + Send + Sync, E> {
    dispatch: BoxedDispatch<E>,
    transport: Arc<T>,
    service_id: String,
}

#[async_trait]
impl<T: Transport + Send + Sync, E: EventEnvelope + Send + Sync> MessageConsumer
    for CommandServiceServer<T, E>
{
    async fn consume(&self, message: &[u8]) -> Result<()> {
        let command_response = (self.dispatch)(message, self)?;
        self.transport
            .send_command_response("".as_bytes(), command_response.as_slice())?;
        Ok(())
    }
}

impl<T: Transport + Send + Sync, E: EventEnvelope> EventProducer for CommandServiceServer<T, E> {
    type Event = E;

    fn produce(&self, event: E) -> Result<()> {
        let event_id = Uuid::new_v4().to_string();
        let partition_key = event.id();
        let event_message =
            serialize_event_to_protobuf(event, self.service_id.as_str(), event_id.as_str())?;
        self.transport
            .send_event(partition_key.as_bytes(), event_message.as_slice())?;
        Ok(())
    }
}

impl<T: Transport + Send + Sync, E> CommandServiceServer<T, E> {
    /// Creates a command service server that deserializes every consumed
    /// command into `C` (a service's closed command enum) and routes it to
    /// `handler`. `transport` accepts either an owned `T` (wrapped in an
    /// `Arc` internally) or an `Arc<T>` the caller already holds.
    pub fn new<C, F>(
        service_id: &str,
        transport: impl Into<Arc<T>>,
        handler: F,
    ) -> CommandServiceServer<T, E>
    where
        C: CommandEnvelope + 'static,
        F: Fn(C, &dyn EventProducer<Event = E>) -> CommandResponse + Send + Sync + 'static,
    {
        let service_id_owned = service_id.to_owned();
        let dispatch_service_id = service_id_owned.clone();
        let dispatch: BoxedDispatch<E> = Box::new(move |raw, event_producer| {
            let envelope = CommandEnvelopeProto::decode(raw)?;
            let command: C = serde_json::from_slice(&envelope.command)?;
            let subject = command.subject();
            let command_type = command.command_type();
            let version = command.version();
            let response = handler(command, event_producer);
            serialize_command_response_to_protobuf(
                response,
                &subject,
                command_type,
                version,
                &envelope.id,
                &dispatch_service_id,
            )
        });
        CommandServiceServer {
            dispatch,
            transport: transport.into(),
            service_id: service_id_owned,
        }
    }

    pub async fn run(self) -> Result<()>
    where
        E: EventEnvelope + Send + Sync + 'static,
    {
        // Clone the Arc into a local first: `self.transport` can't be
        // borrowed to make this call while `self` (which owns that field)
        // is simultaneously moved into it as the argument below.
        let transport = Arc::clone(&self.transport);
        transport.consume_async_blocking(self).await
    }
}

#[cfg(test)]
mod tests {
    use crate::cqrs::messages::CommandResponse;
    use crate::cqrs::traits::{
        Command, CommandEnvelope, EventEnvelope, EventProducer, EventSender, MessageConsumer,
        OutboundChannel, Transport,
    };
    use crate::cqrs::{CommandServiceClient, CommandServiceServer, EventListener};
    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};
    use std::env;
    use std::sync::{Arc, Mutex};
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

    impl Command for TestCreateUserCommand {
        fn get_subject(&self) -> String {
            self.user_id.to_owned()
        }
        fn get_type(&self) -> String {
            String::from("CreateUserCommand")
        }
    }

    #[derive(Deserialize)]
    #[serde(tag = "type", content = "payload")]
    enum TestCommand {
        CreateUserCommand(TestCreateUserCommand),
    }

    impl CommandEnvelope for TestCommand {
        fn subject(&self) -> String {
            match self {
                Self::CreateUserCommand(c) => c.get_subject(),
            }
        }
        fn command_type(&self) -> &'static str {
            match self {
                Self::CreateUserCommand(_) => "CreateUserCommand",
            }
        }
    }

    #[derive(Serialize, Deserialize)]
    #[serde(tag = "type")]
    enum TestEvent {
        UserCreatedEvent(UserCreatedEvent),
    }

    impl EventEnvelope for TestEvent {
        fn id(&self) -> String {
            match self {
                Self::UserCreatedEvent(e) => e.user_id.to_owned(),
            }
        }
        fn event_type(&self) -> &'static str {
            match self {
                Self::UserCreatedEvent(_) => "UserCreatedEvent",
            }
        }
    }

    struct TokioOutboundChannel {
        sender: Mutex<Option<Sender<Vec<u8>>>>,
    }

    struct TokioTransport {
        sender_command_responses: Mutex<Option<Sender<Vec<u8>>>>,
        sender_events: Mutex<Option<Sender<Vec<u8>>>>,
        receiver_commands: tokio::sync::Mutex<Option<Receiver<Vec<u8>>>>,
    }

    impl EventSender for TokioTransport {
        fn send_event(&self, _key: &[u8], message: &[u8]) -> crate::prelude::Result<()> {
            let mut guard = self.sender_events.lock().expect("Could not lock mutex");
            let tx = guard.take().expect("Could not take sender");
            tx.send(Vec::from(message)).expect("Could not send message");

            Ok(())
        }
    }

    #[async_trait]
    impl Transport for TokioTransport {
        fn send_command_response(&self, _key: &[u8], message: &[u8]) -> crate::prelude::Result<()> {
            let mut guard = self
                .sender_command_responses
                .lock()
                .expect("Could not lock mutex");
            let tx = guard.take().expect("Could not take sender");
            tx.send(Vec::from(message)).expect("Could not send message");

            Ok(())
        }

        async fn consume_async_blocking<E: EventEnvelope + Send + Sync + 'static>(
            &self,
            command_service_server: CommandServiceServer<Self, E>,
        ) -> crate::prelude::Result<()> {
            let rx = {
                let mut guard = self.receiver_commands.lock().await;
                guard.take().unwrap()
            };
            let message = rx.await?; // awaiting consumes the oneshot receiver
            command_service_server.consume(message.as_slice()).await
        }
    }

    impl TokioOutboundChannel {
        fn new(sender: Sender<Vec<u8>>) -> Self {
            TokioOutboundChannel {
                sender: Mutex::new(Some(sender)),
            }
        }
    }

    impl OutboundChannel for TokioOutboundChannel {
        fn send(&self, _key: &[u8], message: &[u8]) -> crate::prelude::Result<()> {
            let mut guard = self.sender.lock().expect("Could not lock mutex");
            let tx = guard.take().expect("Could not take sender");
            tx.send(Vec::from(message)).expect("Could not send message");
            Ok(())
        }
    }

    fn deserialize<T: Command>(command: &Vec<u8>) -> Box<T> {
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
        command: TestCommand,
        event_producer: &dyn EventProducer<Event = TestEvent>,
    ) -> CommandResponse {
        let TestCommand::CreateUserCommand(command) = command;

        assert_eq!(command.user_id, "user_id");
        assert_eq!(command.name, "user_name");

        let event = UserCreatedEvent {
            user_id: command.user_id,
            name: command.name,
        };
        event_producer
            .produce(TestEvent::UserCreatedEvent(event))
            .expect("Could not produce event");

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
            let outbound_channel = TokioTransport {
                sender_command_responses: Mutex::new(Some(response_sender)),
                sender_events: Mutex::new(Some(event_sender)),
                receiver_commands: tokio::sync::Mutex::new(Some(command_receiver)),
            };
            let command_service_server: CommandServiceServer<TokioTransport, TestEvent> =
                CommandServiceServer::new(
                    "COMMAND-SERVER",
                    outbound_channel,
                    verify_handle_create_user,
                );

            command_service_server.run().await.unwrap();
        });

        let command_channel = TokioOutboundChannel::new(command_sender);
        let command_service_client =
            Arc::new(CommandServiceClient::new("COMMAND-CLIENT", command_channel));

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

    #[tokio::test]
    async fn test_event_handler_closure_captures_state() {
        use crate::cqrs::operations::serialize_event_to_protobuf;

        // Proves handlers can `move`-capture state (e.g. a read-model
        // repository) instead of being limited to bare `fn` pointers.
        let received: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let received_for_handler = Arc::clone(&received);

        let listener = EventListener::new(move |event: TestEvent| {
            let TestEvent::UserCreatedEvent(event) = event;
            received_for_handler.lock().unwrap().push(event.user_id);
        });

        let event = TestEvent::UserCreatedEvent(UserCreatedEvent {
            user_id: String::from("user-42"),
            name: String::from("Dana"),
        });
        let message = serialize_event_to_protobuf(event, "TEST-SERVICE", "event-1").unwrap();

        listener.consume(message.as_slice()).await.unwrap();

        assert_eq!(received.lock().unwrap().as_slice(), ["user-42"]);
    }
}
