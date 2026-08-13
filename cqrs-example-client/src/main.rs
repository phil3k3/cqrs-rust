use actix_web::{post, web, App, HttpResponse, HttpServer, Responder};
use config::Config;
use cqrs_kafka::inbound::StreamKafkaInboundChannel;
use cqrs_kafka::outbound::KafkaOutboundChannel;
use cqrs_library::cqrs::messages::CommandResponse;
use cqrs_library::cqrs::traits::Command;
use cqrs_library::cqrs::{CommandServiceClient, EventListener};
use log::info;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::{env, io};
use uuid::Uuid;

#[derive(Debug, Deserialize, Serialize)]
struct CreateUserCommand {
    user_id: String,
    name: String,
}

struct AppState {
    client: Arc<CommandServiceClient<KafkaOutboundChannel>>,
}

impl Command for CreateUserCommand {
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

/// Closes over every event type this service listens for.
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum DomainEvent {
    UserCreatedEvent(UserCreatedEvent),
}

fn handle_event(event: DomainEvent) {
    match event {
        DomainEvent::UserCreatedEvent(event) => {
            info!("===== Received event {:?}", event);
        }
    }
}

#[derive(Deserialize)]
struct UserPayload {
    name: String,
}

async fn create_user<O: cqrs_library::cqrs::traits::OutboundChannel + Sync + Send>(
    client: &CommandServiceClient<O>,
    name: String,
) -> cqrs_library::prelude::Result<(String, CommandResponse)> {
    let command = CreateUserCommand {
        user_id: Uuid::new_v4().to_string(),
        name,
    };
    info!("===== Sending command to create user '{:?}'", command);

    let response = client.send_command(&command).await?;
    Ok((command.user_id, response))
}

#[post("/users")]
async fn post_user(
    command_service_client: web::Data<AppState>,
    payload: web::Json<UserPayload>,
) -> impl Responder {
    match create_user(&command_service_client.client, payload.name.clone()).await {
        Ok((user_id, CommandResponse::Ok)) => HttpResponse::Ok().body(user_id),
        Ok(_) => HttpResponse::InternalServerError()
            .body("Failed to process command, check server logs"),
        Err(_) => {
            HttpResponse::InternalServerError().body("Failed to process command, check server logs")
        }
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    info!("=== STARTING EXAMPLE CQRS CLIENT ===");

    let settings = Arc::new(
        Config::builder()
            .add_source(config::File::with_name("cqrs-example-client/src/Settings"))
            .build()
            .expect("Cannot find settings"),
    );

    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", &settings.get_string("log_level").unwrap())
    }

    env_logger::init();

    let settings_event_listener = settings.clone();
    tokio::spawn(async move {
        let event_listener = EventListener::new(handle_event);

        let subscriptions_list = &settings_event_listener
            .get_string("service_subscriptions")
            .unwrap();
        let topics = subscriptions_list.split(",").collect::<Vec<&str>>();
        let transaction_handler = cqrs_kafka::traits::NoopTransactionHandler::default();
        let kafka_event_listener_channel = StreamKafkaInboundChannel::new(
            &settings_event_listener.get_string("service_id").unwrap(),
            topics.as_slice(),
            &settings_event_listener
                .get_string("bootstrap_server")
                .unwrap(),
            Arc::new(event_listener),
            &transaction_handler,
            false,
        )
        .expect("Could not create kafka event listener channel");

        kafka_event_listener_channel
            .consume_async_blocking()
            .await
            .expect("Failed to consume event channel");
    });

    let command_topic = settings
        .get_string("command_topic")
        .expect("Could not get command topic");
    let bootstrap_server = settings
        .get_string("bootstrap_server")
        .expect("Could not get bootstrap server");

    let kafka_command_channel = KafkaOutboundChannel::new(command_topic, bootstrap_server.as_str())
        .expect("Could not create kafka command channel");
    let client = CommandServiceClient::new(
        &settings.get_string("service_id").unwrap(),
        kafka_command_channel,
    );
    let command_service_client = Arc::new(client);

    let client_for_task = Arc::clone(&command_service_client);
    let settings_command_listener = settings.clone();
    tokio::spawn(async move {
        let command_response_channel = StreamKafkaInboundChannel::new(
            "COMMAND-CLIENT",
            &[&settings_command_listener
                .get_string("response_topic")
                .unwrap()],
            &settings_command_listener
                .get_string("bootstrap_server")
                .unwrap(),
            client_for_task,
            &cqrs_kafka::traits::NoopTransactionHandler {},
            false,
        )
        .expect("Failed to create command channel");

        command_response_channel
            .consume_async_blocking()
            .await
            .expect("Failed to consume command channel");
    });

    let command_service_client = command_service_client.clone();
    HttpServer::new(move || {
        let command_service_client = command_service_client.clone();
        let command_service_client_data = web::Data::new(AppState {
            client: command_service_client,
        });
        App::new()
            .app_data(command_service_client_data)
            .service(post_user)
    })
    .workers(1)
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqrs_library::cqrs::traits::{MessageConsumer, OutboundChannel};
    use cqrs_messages::cqrs::messages::{CommandEnvelopeProto, CommandResponseEnvelopeProto};
    use prost::Message;
    use std::sync::Mutex;
    use tokio::sync::oneshot;

    /// Test double that hands the raw bytes passed to `send` off to a
    /// receiver, so a test can decode and respond to them off-thread instead
    /// of actually talking to Kafka.
    struct RelayOutboundChannel {
        sender: Mutex<Option<oneshot::Sender<Vec<u8>>>>,
    }

    impl OutboundChannel for RelayOutboundChannel {
        fn send(&self, _key: &[u8], message: &[u8]) -> cqrs_library::prelude::Result<()> {
            let mut guard = self.sender.lock().expect("Could not lock mutex");
            let tx = guard.take().expect("Could not take sender");
            tx.send(message.to_vec()).expect("Could not relay message");
            Ok(())
        }
    }

    #[tokio::test]
    async fn create_user_returns_ok_when_command_service_responds_ok() {
        let (tx, rx) = oneshot::channel::<Vec<u8>>();
        let outbound_channel = RelayOutboundChannel {
            sender: Mutex::new(Some(tx)),
        };
        let client = Arc::new(CommandServiceClient::new("TEST-CLIENT", outbound_channel));

        let responder_client = Arc::clone(&client);
        let responder = tokio::spawn(async move {
            let sent_bytes = rx.await.expect("Command was never sent");
            let command_envelope = CommandEnvelopeProto::decode(sent_bytes.as_slice())
                .expect("Could not decode command envelope");

            let response_json = format!(
                r#"{{"entity_id":"{}","result":"Ok"}}"#,
                command_envelope.subject
            );
            let response_envelope = CommandResponseEnvelopeProto {
                transaction_id: String::from("test-transaction"),
                command_id: command_envelope.id,
                timestamp: 0,
                service_id: String::from("TEST-SERVER"),
                r#type: command_envelope.r#type,
                version: command_envelope.version,
                response: response_json.into_bytes(),
                error: None,
                id: String::from("test-response"),
            };
            let mut buf = Vec::new();
            response_envelope
                .encode(&mut buf)
                .expect("Could not encode response envelope");

            responder_client
                .consume(buf.as_slice())
                .await
                .expect("Could not consume response");
        });

        let (user_id, response) = create_user(&client, String::from("Alice"))
            .await
            .expect("create_user failed");

        responder.await.expect("Responder task panicked");

        assert!(!user_id.is_empty());
        assert_eq!(response, CommandResponse::Ok);
    }
}
