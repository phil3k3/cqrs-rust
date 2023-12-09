use std::env;
use cqrs_library::{Command, CommandAccessor, CommandResponse, CommandServiceServer, CommandStore, EventProducer};
use cqrs_kafka::{KafkaOutboundChannel, StreamKafkaInboundChannel};
use serde::{Serialize, Deserialize};
use log::{debug, info};
use config::Config;

fn handle_create_user(command_accessor: &mut CommandAccessor, _event_producer: &mut EventProducer) -> CommandResponse {
    let command: Box<TestCreateUserCommand> = command_accessor.get_command();

    info!("Creating user {} with id {}", command.name, command.user_id);
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
        "COMMAND-SERVER",
        &settings.get_string("response_topic").unwrap(),
        &settings.get_string("bootstrap_server").unwrap(),
    );

    info!("Creating topics");
    command_response_channel.create_topic(&settings.get_string("command_topic").unwrap()).await;
    command_response_channel.create_topic(&settings.get_string("response_topic").unwrap()).await;


    tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async move {
            let mut command_store = CommandStore::new("COMMAND-SERVER");
            command_store.register_handler("CreateUserCommand", Box::new(&handle_create_user));

            let mut command_channel = StreamKafkaInboundChannel::new(
                "COMMAND-SERVER",
                &[&settings.get_string("command_topic").unwrap()],
                &settings.get_string("bootstrap_server").unwrap(),
            );

            let mut event_producer = EventProducer::new("COMMAND-SERVER");

            let mut command_service_server = CommandServiceServer::new(&command_store, &mut event_producer);

            loop {
                debug!("Waiting for message");
                let message = command_channel.async_consume().await;
                match message {
                    None => {
                        debug!("No message!");
                    }
                    Some(mut message) => {
                        command_service_server.consume_async(&mut message, &mut command_response_channel);
                    }
                }
            }
        })
    }).await.unwrap();
}
