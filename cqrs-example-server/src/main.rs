mod error;
mod prelude;

use crate::prelude::*;
use config::Config;
use cqrs_kafka::{KafkaSettings, KafkaTransport};
use cqrs_library::cqrs::messages::CommandResponse;
use cqrs_library::cqrs::traits::{Command, CommandEnvelope, EventEnvelope, EventProducer};
use cqrs_library::cqrs::CommandServiceServer;
use log::{error, info};
use serde::{Deserialize, Serialize};
use std::env;
use std::sync::Arc;

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
    name: String,
}

/// Closes over every command type this service accepts. The compiler
/// enforces every variant is handled in `handle_command`'s `match`.
#[derive(Deserialize)]
#[serde(tag = "type", content = "payload")]
enum AppCommand {
    CreateUserCommand(TestCreateUserCommand),
}

impl CommandEnvelope for AppCommand {
    fn subject(&self) -> String {
        match self {
            Self::CreateUserCommand(c) => c.get_subject(),
        }
    }

    fn command_type(&self) -> &'static str {
        match self {
            Self::CreateUserCommand(_) => "CreateUserCommand",
        }
    }
}

/// Closes over every event type this service can produce.
#[derive(Serialize)]
#[serde(tag = "type")]
enum DomainEvent {
    UserCreatedEvent(UserCreatedEvent),
}

impl EventEnvelope for DomainEvent {
    fn id(&self) -> String {
        match self {
            Self::UserCreatedEvent(e) => e.user_id.to_owned(),
        }
    }

    fn event_type(&self) -> &'static str {
        match self {
            Self::UserCreatedEvent(_) => "UserCreatedEvent",
        }
    }
}

fn handle_command(
    command: AppCommand,
    event_producer: &dyn EventProducer<Event = DomainEvent>,
) -> CommandResponse {
    match command {
        AppCommand::CreateUserCommand(command) => {
            info!("===== Received command to create user {:?}", command);
            let event = UserCreatedEvent {
                user_id: command.user_id,
                name: command.name,
            };
            info!("===== Producing event as user was created {:?}", event);
            let result = event_producer.produce(DomainEvent::UserCreatedEvent(event));
            match result {
                Ok(_) => CommandResponse::Ok,
                Err(error) => {
                    error!("Error processing command: {}", error);
                    CommandResponse::Error
                }
            }
        }
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

    let kafka_settings = Arc::new(KafkaSettings::try_from(settings)?);
    let transport = KafkaTransport::new(Arc::clone(&kafka_settings))?;

    info!("Creating topics");
    transport
        .create_topic(&kafka_settings.commands_topic.as_str())
        .await?;
    transport
        .create_topic(&kafka_settings.command_response_topic.as_str())
        .await?;
    transport
        .create_topic(&kafka_settings.events_topic.as_str())
        .await?;

    let command_service_server: CommandServiceServer<KafkaTransport, DomainEvent> =
        CommandServiceServer::new(kafka_settings.service_id.as_str(), transport, handle_command);

    command_service_server.run().await.map_err(|x| x.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    #[derive(Default)]
    struct TestEventProducer {
        produced_events: RefCell<Vec<UserCreatedEvent>>,
    }

    impl EventProducer for TestEventProducer {
        type Event = DomainEvent;

        fn produce(&self, event: DomainEvent) -> cqrs_library::prelude::Result<()> {
            let DomainEvent::UserCreatedEvent(event) = event;
            self.produced_events.borrow_mut().push(event);
            Ok(())
        }
    }

    #[test]
    fn handle_command_produces_ok_response() {
        let command = TestCreateUserCommand {
            user_id: String::from("user-1"),
            name: String::from("Alice"),
        };
        let event_producer = TestEventProducer::default();

        let response = handle_command(AppCommand::CreateUserCommand(command), &event_producer);

        assert_eq!(response, CommandResponse::Ok);
        assert_eq!(event_producer.produced_events.borrow().len(), 1);
        assert_eq!(
            event_producer.produced_events.borrow()[0].user_id,
            "user-1"
        );
    }

    /// Documents the wire contract this redesign relies on: the client
    /// serializes a command wrapped as `{"type": ..., "payload": ...}`
    /// (see `serialize_command_to_protobuf`), and the server deserializes
    /// straight into `AppCommand` via `#[serde(tag = "type", content = "payload")]`.
    #[test]
    fn app_command_deserializes_from_tagged_wire_format() {
        let json = r#"{"type":"CreateUserCommand","payload":{"user_id":"user-2","name":"Bob"}}"#;
        let command: AppCommand = serde_json::from_str(json).unwrap();
        match command {
            AppCommand::CreateUserCommand(c) => {
                assert_eq!(c.user_id, "user-2");
                assert_eq!(c.name, "Bob");
            }
        }
    }

    /// Unlike the old string-keyed `CommandStore`, which silently orphaned
    /// unrecognized command types, deserializing an unknown type into the
    /// closed enum now fails immediately and explicitly.
    #[test]
    fn app_command_rejects_unknown_type() {
        let json = r#"{"type":"UnknownCommand","payload":{}}"#;
        let result: std::result::Result<AppCommand, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }
}
