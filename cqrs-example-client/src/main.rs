use std::env;
use config::Config;
use cqrs_library::{CommandResponse, CommandServiceClient, OutboundChannel, InboundChannel, Command};
use cqrs_kafka::{KafkaOutboundChannel};
use serde::{Deserialize, Serialize};
use log::info;

#[derive(Debug, Deserialize, Serialize)]
struct CreateUserCommand {
    user_id: String,
    name: String
}

impl Command<'_> for CreateUserCommand {
    fn get_subject(&self) -> String {
        self.user_id.to_owned()
    }
    fn get_type(&self) -> String {
        String::from("CreateUserCommand")
    }
}

fn main() {

    let settings = Config::builder()
        .add_source(config::File::with_name("cqrs-example-client/src/Settings"))
        .build()
        .unwrap();

    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", &settings.get_string("log_level").unwrap())
    }

    env_logger::init();
    info!("=== STARTING EXAMPLE CQRS CLIENT ===");

    let settings = Config::builder()
        .add_source(config::File::with_name("cqrs-example-client/src/Settings"))
        .build()
        .unwrap();

    let mut command_service_client = CommandServiceClient::new(&settings.get_string("service_id").unwrap());

    let mut kafka_command_channel = KafkaOutboundChannel::new(
        &settings.get_string("service_id").unwrap(),
        "CQRS-SERVER",
        &settings.get_string("bootstrap_server").unwrap()
    );
    let command = CreateUserCommand {
        user_id: String::from("Test"),
        name: String::from("Name")
    };
    command_service_client.send_command(&command, &mut kafka_command_channel);
}