use config::Config;
use cqrs_kafka::inbound::StreamKafkaInboundChannel;
use cqrs_kafka::outbound::KafkaOutboundChannel;
use cqrs_library::cqrs::command::{CommandAccessor, CommandStore};
use cqrs_library::cqrs::messages::CommandResponse;
use cqrs_library::cqrs::traits::{Command, Event, EventProducer};
use cqrs_library::cqrs::{CommandServiceServer, CqrsEventProducer};
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use std::env;

fn handle_create_user(
    command_accessor: &mut CommandAccessor,
    event_producer: &dyn EventProducer,
) -> CommandResponse {
    let command: Box<TestCreateUserCommand> = command_accessor.get_command();

    info!("Creating user {} with id {}", command.name, command.user_id);
    let event = UserCreatedEvent {
        user_id: command.user_id,
        name: command.name,
    };
    event_producer.produce(&event);
    CommandResponse::Ok
}

#[derive(Debug, Deserialize, Serialize)]
struct TestCreateUserCommand {
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

#[derive(Debug, Deserialize, Serialize)]
struct UserCreatedEvent {
    user_id: String,
    name: String,
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

#[tokio::main]
async fn main() {
    info!("=== STARTING EXAMPLE CQRS SERVER ===");

    let settings = Config::builder()
        .add_source(config::File::with_name("cqrs-example-server/src/Settings"))
        .build()
        .unwrap();

    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", &settings.get_string("log_level").unwrap())
    }

    env_logger::init();

    let command_response_channel = KafkaOutboundChannel::new(
        &settings.get_string("response_topic").unwrap(),
        &settings.get_string("bootstrap_server").unwrap(),
    );

    info!("Creating topics");
    command_response_channel
        .create_topic(&settings.get_string("command_topic").unwrap())
        .await;
    command_response_channel
        .create_topic(&settings.get_string("response_topic").unwrap())
        .await;
    command_response_channel
        .create_topic(&settings.get_string("events_topic").unwrap())
        .await;

    let mut command_store = CommandStore::new("COMMAND-SERVER");
    command_store.register_handler("CreateUserCommand", handle_create_user);

    let event_channel = KafkaOutboundChannel::new(
        &settings.get_string("events_topic").unwrap(),
        &settings.get_string("bootstrap_server").unwrap(),
    );

    let mut event_producer = CqrsEventProducer::new("COMMAND-SERVER", Box::new(event_channel));

    let command_service_server = CommandServiceServer::new(
        &command_store,
        &mut event_producer,
        &command_response_channel,
    );

    let command_channel = StreamKafkaInboundChannel::new(
        "COMMAND-SERVER",
        &[&settings.get_string("command_topic").unwrap()],
        &settings.get_string("bootstrap_server").unwrap(),
        command_service_server,
    )
    .expect("Failed to create command channel");

    command_channel.consume_async_blocking().await;
}
