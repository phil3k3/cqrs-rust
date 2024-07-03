use std::error::Error;
use std::sync::{Arc, Mutex};
use async_trait::async_trait;
use config::Config;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use cqrs_library::{CommandResponse, CommandServiceClient, CommandServiceServer, CommandStore, Event, EventHandlerFn, EventListener, EventProducerImpl, SerializableCommand, StreamInboundChannel};
use cqrs_library::locks::TokioThreadSafeDataManager;
use crate::{QueryCarrier, ServerCarrier};
use crate::inbound::KafkaInboundChannel;
use crate::outbound::KafkaOutboundChannel;


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
    pub async fn start(self,
                       settings: Config,
                       service_id: String) -> JoinHandle<()> {
        return tokio::task::spawn_blocking( move || {
            let arc = self.carrier.get_event_channel().clone();
            let event_producer = EventProducerImpl::new(service_id.to_owned(), arc);
            let command_store = CommandStore::new(service_id.to_owned());
            let command_server: CommandServiceServer = CommandServiceServer::new(command_store, event_producer);

            let mut input_channel = self.carrier.get_command_channel(settings.clone());
            input_channel.consume_async_blocking(Arc::new(Mutex::new(Box::new(command_server))),
                                                 self.carrier.get_response_channel(settings.clone()));
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

struct CqrsClient {
    command_sender: CommandSender
}

struct CommandSender {
    command_service_client: CommandServiceClient<KafkaInboundChannel, KafkaOutboundChannel>
}

impl CommandSender{
    pub async fn send<'aggregate, A : Aggregate<'aggregate>>(&mut self, command: A::Command) -> CommandResponse {
        let result = self.command_service_client.send_command(&command).await;
        return result;
    }
}


impl CqrsClient {
    pub fn new(settings: Config) -> Self {
        let command_service_client = CommandServiceClient::new(settings.clone(),
                                                               Arc::new(tokio::sync::Mutex::new(Some(Box::new(|config| {
                                                                   let service_id = config.get_string("service_id").unwrap();
                                                                   let bootstrap_servers = config.get_string("bootstrap_server").unwrap();
                                                                   Box::new(KafkaInboundChannel::new(
                                                                       service_id,
                                                                       &[config.get_string("response_topic").unwrap().as_str()],
                                                                       bootstrap_servers
                                                                   ))
                                                               })))),
                                                               Arc::new(tokio::sync::Mutex::new(Some(Box::new(|config: Config| {
                                                                   let service_id = config.get_string("service_id").unwrap();
                                                                   let bootstrap_servers = config.get_string("bootstrap_server").unwrap();
                                                                   Box::new(KafkaOutboundChannel::new(service_id, bootstrap_servers))
                                                               })))));
        return CqrsClient { command_sender: CommandSender { command_service_client } };
    }

    pub async fn send_command<'aggregate, A: Aggregate<'aggregate>>(&self, _command: A::Command) -> CommandResponse {
        return CommandResponse::Ok;
    }
}

struct CqrsQuery<INBOUND: StreamInboundChannel>  {
    event_listener: Arc<Mutex<Box<EventListener>>>,
    event_channel: TokioThreadSafeDataManager<INBOUND>
}

impl<INBOUND: StreamInboundChannel+ 'static> CqrsQuery<INBOUND> {
    pub fn new<CARRIER: QueryCarrier<INBOUND>>(event_type: &str, event_handler: EventHandlerFn, carrier: CARRIER) -> Self {
        let mut event_listener = EventListener::new();
        event_listener.register_handler(event_type, event_handler);
        let event_channel = carrier.get_event_channel();

        return CqrsQuery {
            event_listener: Arc::new(Mutex::new(Box::new(event_listener))),
            event_channel,
        };
    }

    pub fn start(mut self) {
        let listener1 = self.event_listener.clone();
        tokio::spawn(async move {
            let listener2 = listener1.clone();
            self.event_channel.safe_call(|mut result| {
                result.consume_async_blocking(listener2);
            }).await;
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

        let settings = Config::default();
        let handle = framework.start(settings.clone(), String::from("TEST_SERVICE"));

        // on the server side I need to start a runtime which listens to commands
        // each time a command arrives, it passes it to a command handler of the
        // aggregate, which returns a response and events, the response is written
        // back to the command response channel, the events are written to the events channel
        // then the explicit transaction finishes

        let cqrs_query = CqrsQuery::new("UserCreated", handle_user_created, client);
        cqrs_query.start();

        let client = CqrsClient::new(settings.clone());
        let response = client.send_command::<User>(UserCommand::Create {
            user_id: "1".to_string()
        }).await;


        assert_eq!(CommandResponse::Ok, response);

        handle.await.abort_handle();
    }

}
