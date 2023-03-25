use std::env;
use std::thread::sleep;
use std::time::Duration;
use cqrs_library::{Command, CommandAccessor, CommandResponse, CommandServiceServer, CommandStore};
use cqrs_kafka::{KafkaOutboundChannel, KafkaInboundChannel, Runtime};
use serde::{Serialize, Deserialize};
use log::info;
use config::Config;

fn handle_create_user(command_accessor: &mut CommandAccessor) -> CommandResponse {
    let command: Box<TestCreateUserCommand> = command_accessor.get_command();

    info!("Creating user {} with id {}", command.name, command.user_id);
    CommandResponse::Ok
}

#[derive(Debug, Deserialize, Serialize)]
struct TestCreateUserCommand {
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

#[tokio::main]
async fn main() {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "debug")
    }

    env_logger::init();
    info!("=== STARTING EXAMPLE CQRS SERVER ===");

    let settings = Config::builder()
        .add_source(config::File::with_name("cqrs-example-server/src/Settings"))
        .build()
        .unwrap();

    let mut command_response_channel = KafkaOutboundChannel::new(
        "COMMAND-SERVER",
        &settings.get_string("response_topic").unwrap(),
        &settings.get_string("bootstrap_server").unwrap()
    );

    info!("Creating topics");
    command_response_channel.create_topic(&settings.get_string("command_topic").unwrap()).await;
    command_response_channel.create_topic(&settings.get_string("response_topic").unwrap()).await;

    let command_server = Runtime::new(move || {
        let mut command_store = CommandStore::new("COMMAND-SERVER");
        command_store.register_handler("CreateUserCommand", Box::new(&handle_create_user));

        let mut command_channel = KafkaInboundChannel::new(
            "COMMAND-SERVER",
            &[&settings.get_string("command_topic").unwrap()],
            &settings.get_string("bootstrap_server").unwrap()
        );

        let mut command_service_server = CommandServiceServer::new(&command_store);

        loop {
            command_service_server.consume(&mut command_channel, &mut command_response_channel);
            sleep(Duration::from_secs(1));
        }
    });

    command_server.join().expect("Command server panicked");
}