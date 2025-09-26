mod error;
mod prelude;

use crate::prelude::*;
use config::Config;
use cqrs_kafka::inbound::StreamKafkaInboundChannel;
use cqrs_kafka::outbound::{KafkaOutboundChannel, KafkaSettings, TransactionalKafkaOutboundChannel};
use cqrs_library::cqrs::command::{CommandAccessor, CommandStore};
use cqrs_library::cqrs::messages::CommandResponse;
use cqrs_library::cqrs::traits::{Command, Event, EventProducer};
use cqrs_library::cqrs::{CommandServiceServer, CqrsEventProducer};
use log::{error, info};
use serde::{Deserialize, Serialize};
use std::env;
use std::sync::Arc;

fn handle_create_user(
    command_accessor: &mut CommandAccessor,
    event_producer: &dyn EventProducer,
) -> CommandResponse {
    let command: Result<Box<TestCreateUserCommand>> =
        command_accessor.get_command().map_err(|x| x.into());

    match command {
        Ok(command) => {
            info!("===== Received command to create user {:?}", command);
            let event = UserCreatedEvent {
                user_id: command.user_id,
                name: command.name,
            };
            info!("===== Producing event as user was created {:?}", event);
            let result = event_producer.produce(&event);
            match result {
                Ok(_) => CommandResponse::Ok,
                Err(error) => {
                    error!("Error processing command: {}", error);
                    CommandResponse::Error
                }
            }
        }
        Err(error) => {
            error!("Error processing command: {}", error);
            CommandResponse::Error
        }
    }
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
async fn main() -> Result<()> {
    info!("=== STARTING EXAMPLE CQRS SERVER ===");

    let settings = Config::builder()
        .add_source(config::File::with_name("cqrs-example-server/src/Settings"))
        .build()
        .unwrap();

    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", &settings.get_string("log_level").unwrap())
    }

    env_logger::init();

    let kafka_settings = KafkaSettings::from(settings);
    let transaction_channel = TransactionalKafkaOutboundChannel::new(
        &kafka_settings
    )?;

    info!("Creating topics");
    transaction_channel
        .create_topic(&kafka_settings.commands_topic.as_str())
        .await?;
    transaction_channel
        .create_topic(&kafka_settings.command_response_topic.as_str())
        .await?;
    transaction_channel
        .create_topic(&kafka_settings.events_topic.as_str())
        .await?;

    let mut command_store = CommandStore::new("COMMAND-SERVER");
    command_store.register_handler("CreateUserCommand", handle_create_user);


    let command_service_server = CommandServiceServer::new(
        &command_store,
        &transaction_channel,
        &transaction_channel,
        kafka_settings.service_id.as_str(),
    );

    let command_channel = StreamKafkaInboundChannel::new(
        "COMMAND-SERVER",
        &[kafka_settings.commands_topic.as_str()],
        &kafka_settings.bootstrap_server,
        Arc::new(command_service_server),
        &transaction_channel,
        false
    )
    .expect("Failed to create command channel");

    command_channel.consume_async_blocking().await;

    Ok(())
}
