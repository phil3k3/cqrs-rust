use std::error::Error;
use async_trait::async_trait;
use config::Config;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use cqrs_library::{Command, CommandServiceServer, CommandStore, Event, EventProducerImpl, OutboundChannel};
use crate::inbound::StreamKafkaInboundChannel;


#[async_trait]
trait Aggregate<'de> : Default + Serialize + Deserialize<'de> + Sync + Send {
    type Command;
    type Event: Event;
    async fn handle(command: Self::Command) -> Result<Vec<Box<dyn Event>>, Box<dyn Error>>;
    fn apply(event: Box<dyn Event>);
}

struct CqrsFramework<AGGREGATE> {
    aggregate: AGGREGATE
}

impl<AGGREGATE> CqrsFramework<AGGREGATE> {
    pub fn start(&self, settings: Config,
                 service_id: &str,
                 event_channel: Box<dyn OutboundChannel + Send>,
                 mut response_channel: Box<dyn OutboundChannel + Send + Sync>) -> JoinHandle<()> {
        return tokio::task::spawn(|| {
            let mut event_producer = EventProducerImpl::new(service_id, Box::new(event_channel));
            let command_store = CommandStore::new(service_id);
            let command_server: Box<CommandServiceServer> = CommandServiceServer::new(&command_store, &mut event_producer);

            // I need to reach out to kafka and get the data from there
            let input_channel = StreamKafkaInboundChannel::new(
                service_id,
                &[&settings.get_string("topics").unwrap()],
                &settings.get_string("bootstrap_server").unwrap()
            );
            return input_channel.consume_async_blocking(command_server, &mut response_channel);
        })
    }
}

impl<AGGREGATE> CqrsFramework<AGGREGATE> {

    pub fn new() -> CqrsFramework<AGGREGATE> {
        return CqrsFramework {
            aggregate: AGGREGATE::default()
        }
    }
}

struct CqrsClient {

}

impl CqrsClient {
    pub fn new() -> Self {
        return CqrsClient {};
    }
}

struct CqrsQuery {

}

impl CqrsQuery {
    pub fn new(x: &str, x0: fn(Box<dyn Event>)) {

    }

    pub fn start(&self) {

    }
}

#[cfg(test)]
mod test {
    use std::error::Error;
    use config::{Config, ConfigError};
    use serde::{Deserialize, Serialize};
    use tokio::sync::oneshot;
    use tokio::sync::oneshot::{Receiver, Sender};
    use cqrs_library::{Command, CommandResponse, Event};
    use crate::aggregate::{Aggregate, CqrsClient, CqrsFramework, CqrsQuery};

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

    impl Command for UserCommand {
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

    impl<'aggregate> Aggregate<'aggregate> for User {
        type Command = UserCommand;
        type Event = UserEvent;

        async fn handle(command: Self::Command) -> Result<Vec<Box<dyn Event>>, Box<dyn Error>> {
            return Err(Box::new(ConfigError::NotFound(String::new())));
        }

        fn apply(event: Box<dyn Event>) {

        }
    }

    impl Default for User {
        fn default() -> Self {
            return Self {
                user_name: String::new()
            }
        }
    }

    fn handle_user_created(event: UserEvent) {
        print!("User created");
    }

    pub(crate) struct TokioOutboundChannel {
        sender: Option<Sender<Vec<u8>>>
    }

    impl TokioOutboundChannel {
        pub(crate) fn new(sender: Sender<Vec<u8>>) -> Self {
            return TokioOutboundChannel {
                sender: Some(sender)
            }
        }
    }

    #[tokio::test]
    async fn test_use_framework() {
        let framework = CqrsFramework::<User>::new();

        let (event_sender, _event_receiver) : (Sender<Vec<u8>>, Receiver<Vec<u8>>) = oneshot::channel();
        let (response_sender, _response_receiver) : (Sender<Vec<u8>>, Receiver<Vec<u8>>) = oneshot::channel();

        let settings = Config::default();
        let event_channel = TokioOutboundChannel::new(event_sender);
        let response_channel = TokioOutboundChannel::new(response_sender);
        let handle = framework.start(settings, "TEST_SERVICE", Box::new(event_channel), Box::new(response_channel));

        // on the server side I need to start a runtime which listens to commands
        // each time a command arrives, it passes it to a command handler of the
        // aggregate, which returns a response and events, the response is written
        // back to the command response channel, the events are written to the events channel
        // then the explicit transaction finishes

        let event_listener = CqrsQuery::new("UserCreated", handle_user_created);
        event_listener.start();

        let client = CqrsClient::new();
        let response = client.send_command(UserCommand::Create {
            user_id: "1".to_string()
        }).await;

        assert_eq!(CommandResponse::Ok, response);

        handle.abort();
    }

}
