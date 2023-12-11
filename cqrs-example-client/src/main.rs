use std::env;
use std::process::exit;
use config::Config;
use cqrs_library::{CommandServiceClient, Command, EventListener, Event};
use cqrs_kafka::inbound::{KafkaInboundChannel, StreamKafkaInboundChannel};
use cqrs_kafka::outbound::KafkaOutboundChannel;
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

#[derive(Debug, Deserialize, Serialize)]
struct UserCreatedEvent {
    user_id: String,
    user_name: String
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

fn handle_event(event: &dyn Event) {
    dbg!(event);
    print!("user created received");
}

#[tokio::main]
async fn main() {

    info!("=== STARTING EXAMPLE CQRS CLIENT ===");

    let settings = Config::builder()
        .add_source(config::File::with_name("cqrs-example-client/src/Settings"))
        .build()
        .unwrap();

    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", &settings.get_string("log_level").unwrap())
    }

    env_logger::init();

    let settings = Config::builder()
        .add_source(config::File::with_name("cqrs-example-client/src/Settings"))
        .build()
        .unwrap();

    let mut command_service_client = CommandServiceClient::new(&settings.get_string("service_id").unwrap());

    let mut kafka_command_channel = KafkaOutboundChannel::new(
        &settings.get_string("command_topic").unwrap(),
        &settings.get_string("bootstrap_server").unwrap()
    );
    let command = CreateUserCommand {
        user_id: String::from("Test"),
        name: String::from("Name")
    };


    let mut kafka_command_response_channel = KafkaInboundChannel::new(
        &settings.get_string("service_id").unwrap(),
        &[&settings.get_string("response_topic").unwrap()],
        &settings.get_string("bootstrap_server").unwrap()
    );
    info!("Message sent!");

    tokio::spawn(async move {

        let mut event_listener = EventListener::new();
        event_listener.register_handler("UserCreatedEvent", handle_event);

        let x = &settings.get_string("service_subscriptions").unwrap();
        let topics = x.split(",").collect::<Vec<&str>>();
        let kafka_event_listener_channel =  StreamKafkaInboundChannel::new(
            &settings.get_string("service_id").unwrap(),
            topics.as_slice(),
            &settings.get_string("bootstrap_server").unwrap()
        );

        kafka_event_listener_channel.consume_async_blocking(&event_listener).await;
    });

    command_service_client.send_command(&command, &mut kafka_command_channel);
    loop {
        let response = command_service_client.read_response(&mut kafka_command_response_channel);
        match response {
            None => {}
            Some(result) => {
                info!("Command response {}", result);
                exit(0);
            }
        }
    }
}
