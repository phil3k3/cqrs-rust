use std::env;
use std::sync::{Arc, Mutex};
use cqrs_library::{Event, Command, CommandAccessor, CommandResponse, CommandServiceServer, CommandStore, EventProducer, EventProducerImpl};
use cqrs_kafka::outbound::KafkaOutboundChannel;
use serde::{Serialize, Deserialize};
use log::{debug, info};
use config::Config;
use tokio::sync::mpsc::UnboundedSender;
use cqrs_kafka::inbound::StreamKafkaInboundChannel;

fn handle_create_user(command_accessor: &mut CommandAccessor, event_producer: &mut dyn EventProducer) -> CommandResponse {
    let command: Box<TestCreateUserCommand> = command_accessor.get_command();

    info!("Creating user {} with id {}", command.name, command.user_id);
    let event = UserCreatedEvent { user_id: command.user_id, name: command.name };
    event_producer.produce(&event);
    CommandResponse::Ok
}

#[derive(Debug, Deserialize, Serialize)]
struct TestCreateUserCommand {
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

#[derive(Debug, Deserialize, Serialize)]
struct UserCreatedEvent {
    user_id: String,
    name: String
}

#[typetag::serde]
impl Event for UserCreatedEvent {
    fn get_id(&self) -> String {
        return self.user_id.to_owned();
    }

    fn get_type(&self) -> String {
        return String::from("UserCreatedEvent");
    }
}

fn is_send<T: Send>() {}
fn is_sync<T: Sync>() {}

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

    let mut command_response_channel = KafkaOutboundChannel::new(
        settings.get_string("response_topic").unwrap(),
        settings.get_string("bootstrap_server").unwrap(),
    );

    info!("Creating topics");
    command_response_channel.create_topic(&settings.get_string("command_topic").unwrap()).await;
    command_response_channel.create_topic(&settings.get_string("response_topic").unwrap()).await;

}
