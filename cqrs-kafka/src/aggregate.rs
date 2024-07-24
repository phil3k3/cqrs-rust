use std::error::Error;
use std::sync::{Arc, Mutex};
use async_trait::async_trait;
use config::Config;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use cqrs_library::{CommandResponse, CommandServiceServer, CommandStore, Event, EventHandlerFn, EventListener, EventProducerImpl, OutboundChannel, SerializableCommand, StreamCommandServiceClient, StreamInboundChannel};
use cqrs_library::locks::{TokioThreadSafeDataManager};
use crate::{ClientCarrier, ServerCarrier};


#[async_trait]
trait Aggregate<'aggregate> : Default + Serialize + Deserialize<'aggregate> + Sync + Send {
    type Command : SerializableCommand<'aggregate>;
    type Event: Event;
    async fn handle(command: Self::Command) -> Result<Vec<Box<dyn Event>>, Box<dyn Error>>;
    fn apply(event: Box<dyn Event>);
}

struct CqrsFramework<AGGREGATE, CARRIER> {
    aggregate: AGGREGATE,
    carrier: CARRIER
}

impl<AGGREGATE: Sync, CARRIER: ServerCarrier + Sync + Send + 'static> CqrsFramework<AGGREGATE, CARRIER> {
    pub fn start(self,
                       settings: Config,
                       service_id: String) -> JoinHandle<()> {
        return tokio::task::spawn_blocking( move || {
            let arc = self.carrier.get_event_channel().clone();
            let event_producer = EventProducerImpl::new(service_id.to_owned(), arc);
            let command_store = CommandStore::new(service_id.to_owned());
            let command_server: CommandServiceServer = CommandServiceServer::new(command_store, event_producer);
            let rt_handle = tokio::runtime::Handle::current();
            rt_handle.block_on(async {
                let mut input_channel = self.carrier.get_command_channel(settings.clone());

                input_channel.consume_async_blocking(Arc::new(Mutex::new(Box::new(command_server))),
                                                     self.carrier.get_response_channel(settings.clone())).await;
            })
        });
    }
}

impl<AGGREGATE: Default + Sync + Send, CARRIER: ServerCarrier + Sync + Send> CqrsFramework<AGGREGATE, CARRIER> {

    pub fn new_existing_carrier(carrier: CARRIER) -> CqrsFramework<AGGREGATE, CARRIER> {

        return CqrsFramework {
            aggregate: AGGREGATE::default(),
            carrier
        }
    }
}

struct CqrsClient<INBOUND: StreamInboundChannel,OUTBOUND: OutboundChannel> {
    command_sender: Arc<Mutex<CommandSender<INBOUND, OUTBOUND>>>,
    command_service_client: TokioThreadSafeDataManager<StreamCommandServiceClient<INBOUND, OUTBOUND>>,
    response_channel: TokioThreadSafeDataManager<INBOUND>
}

struct CommandSender<INBOUND: StreamInboundChannel,OUTBOUND: OutboundChannel> {
    command_service_client: TokioThreadSafeDataManager<StreamCommandServiceClient<INBOUND, OUTBOUND>>
}

impl<INBOUND: StreamInboundChannel + 'static,OUTBOUND: OutboundChannel + 'static> CommandSender<INBOUND,OUTBOUND> {
    pub async fn send<'aggregate, A : Aggregate<'aggregate>>(&mut self, command: A::Command) -> Option<CommandResponse> {
        return self.command_service_client.safe_call_multiple_async_return(|mut result2| {
            let fut = async move {
                result2.send_command(&command).await;
            };
            return fut;
        }).await
    }
}


impl<INBOUND: StreamInboundChannel+'static,OUTBOUND: OutboundChannel+'static> CqrsClient<INBOUND, OUTBOUND> {
    pub fn new<CARRIER: ClientCarrier<INBOUND,OUTBOUND>>(settings: Config, carrier: CARRIER) -> Self {
        let command_service_client = StreamCommandServiceClient::new(
            settings.clone(),
            carrier.get_response_channel(),
            carrier.get_command_channel()
        );
        let manager = TokioThreadSafeDataManager::wrapped(command_service_client);

        let command_sender = CommandSender {command_service_client: manager.clone()};

        return CqrsClient {
            command_sender: Arc::new(Mutex::new(command_sender)),
            command_service_client: manager.clone(),
            response_channel: carrier.get_response_channel()
        };
    }

    pub async fn send_command<'aggregate, A: Aggregate<'aggregate>>(&self, _command: A::Command) -> Option<CommandResponse> {
        let arc = self.command_sender.clone();
        let mut sender = arc.lock().unwrap();
        return sender.send::<A>(_command).await;
    }

    pub fn start(mut self) -> JoinHandle<()> {
        let listener1 = self.command_service_client.clone();
        return tokio::spawn(async move {
            let listener2 = listener1.clone();
            self.response_channel.safe_call_multiple_async(|mut result| {
                let listener3 =  listener2.clone();
                let fut = async move {
                    result.consume_async_blocking(listener3).await;
                };
                return fut;
            }).await;
        });
    }
}

struct CqrsQuery<INBOUND: StreamInboundChannel>  {
    event_listener: TokioThreadSafeDataManager<EventListener>,
    event_channel: TokioThreadSafeDataManager<INBOUND>
}

impl<INBOUND: StreamInboundChannel+ 'static> CqrsQuery<INBOUND> {
    pub fn new(event_type: &str, event_handler: EventHandlerFn, event_channel: TokioThreadSafeDataManager<INBOUND>) -> Self {
        let mut event_listener = EventListener::new();
        event_listener.register_handler(event_type, event_handler);

        return CqrsQuery {
            event_listener: TokioThreadSafeDataManager::wrapped(event_listener),
            event_channel
        };
    }

    pub fn start(mut self) -> JoinHandle<()> {
        let listener1 = self.event_listener.clone();
        return tokio::spawn({
            let rt_handle = tokio::runtime::Handle::current();
            let listener2 = listener1.clone();
            rt_handle.block_on(self.event_channel.safe_call(|mut result| {
                async {
                    result.consume_async_blocking(listener2).await;
                }
            }));
        });
    }
}


#[cfg(test)]
mod test {
    use std::error::Error;
    use async_trait::async_trait;
    use config::{Config, ConfigError};
    use serde::{Deserialize, Serialize};
    use cqrs_library::{CommandResponse, Event, SerializableCommand};
    use crate::aggregate::{Aggregate, CqrsClient, CqrsFramework, CqrsQuery};
    pub use crate::carrier::{TokioCarrier};
    use crate::carrier::TokioServerCarrier;
    use crate::QueryCarrier;

    #[derive(Debug, Deserialize, Serialize)]
    struct User {
        user_name: String
    }

    #[derive(Debug, Serialize, Deserialize)]
    enum UserCommand {
        Create{user_id: String},
        Delete{user_id: String}
    }

    #[derive(Debug, Serialize, Deserialize)]
    enum UserEvent {
        Created{user_id: String},
        Deleted{user_id: String}
    }

    #[typetag::serde]
    impl Event for UserEvent {
        fn get_id(&self) -> String {
            let event_id: &str = match self {
                UserEvent::Created { user_id } => user_id,
                UserEvent::Deleted { user_id } => user_id
            };
            return event_id.to_string();
        }

        fn get_type(&self) -> String {
            let event_type: &str = match self {
                UserEvent::Deleted { .. } => "UserDeleted",
                UserEvent::Created { .. } => "UserCreated"
            };
            return event_type.to_string();
        }

        fn get_version(&self) -> i32 {
            return 1;
        }
    }

    impl<'command> SerializableCommand<'command> for UserCommand {
        fn get_subject(&self) -> String {
           let command_subject: &str = match self {
               UserCommand::Create { user_id } => user_id,
               UserCommand::Delete { user_id } => user_id
           };
           return command_subject.to_string();
        }

        fn get_type(&self) -> String {
            let command_type: &str = match self {
                UserCommand::Create { .. } => { "UserCreated" }
                UserCommand::Delete { .. } => { "UserDeleted" }
            };
            return command_type.to_string();
        }
    }

    #[async_trait]
    impl<'aggregate> Aggregate<'aggregate> for User {
        type Command = UserCommand;
        type Event = UserEvent;

        async fn handle(_command: Self::Command) -> Result<Vec<Box<dyn Event>>, Box<dyn Error>> {
            return Err(Box::new(ConfigError::NotFound(String::new())));
        }

        fn apply(_event: Box<dyn Event>) {

        }
    }

    impl Default for User {
        fn default() -> Self {
            return Self {
                user_name: String::new()
            }
        }
    }

    fn handle_user_created(_event: &dyn Event) {
        print!("User created");
    }

    #[tokio::test]
    async fn test_use_framework() {
        let (server, client) = TokioCarrier::new();
        let framework = CqrsFramework::<User, TokioServerCarrier>::new_existing_carrier(server);

        let settings = Config::builder()
            .set_default("service_id", "TEST").unwrap()
            .set_default("service_instance_id", 1).unwrap()
            .build().unwrap();
        let handle = framework.start(settings.clone(), String::from("TEST_SERVICE"));

        // on the server side I need to start a runtime which listens to commands
        // each time a command arrives, it passes it to a command handler of the
        // aggregate, which returns a response and events, the response is written
        // back to the command response channel, the events are written to the events channel
        // then the explicit transaction finishes

        let cqrs_query = CqrsQuery::new("UserCreated", handle_user_created, client.get_event_channel());
        cqrs_query.start();

        let client = CqrsClient::new(settings.clone(), client);
        let join_handle = &client.start();
        let response = client.send_command::<User>(UserCommand::Create {
            user_id: "1".to_string()
        }).await;


        assert_eq!(CommandResponse::Ok, response.expect("TEST"));

        handle.await.unwrap();
        join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_receive_event() {

    }
}
