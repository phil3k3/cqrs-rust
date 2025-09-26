use actix_web::{post, web, App, HttpResponse, HttpServer, Responder};
use config::Config;
use cqrs_kafka::inbound::StreamKafkaInboundChannel;
use cqrs_kafka::outbound::KafkaOutboundChannel;
use cqrs_library::cqrs::messages::CommandResponse;
use cqrs_library::cqrs::traits::{Command, Event};
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

fn handle_event(event: &dyn Event) {
    info!("===== Received event {:?}", event);
}

#[derive(Deserialize)]
struct UserPayload {
    name: String,
}

#[post("/users")]
async fn post_user(
    command_service_client: web::Data<AppState>,
    payload: web::Json<UserPayload>,
) -> impl Responder {
    let command = CreateUserCommand {
        user_id: Uuid::new_v4().to_string(),
        name: payload.name.clone(),
    };
    info!("===== Sending command to create user '{:?}'", command);

    let result = command_service_client.client.send_command(&command).await;
    match result {
        Ok(result) => {
            if result == CommandResponse::Ok {
                HttpResponse::Ok().body(command.user_id)
            } else {
                HttpResponse::InternalServerError()
                    .body("Failed to process command, check server logs")
            }
        }
        Err(_) => {
            HttpResponse::InternalServerError().body("Failed to process command, check server logs")
        }
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    info!("=== STARTING EXAMPLE CQRS CLIENT ===");

    let settings = Config::builder()
        .add_source(config::File::with_name("cqrs-example-client/src/Settings"))
        .build()
        .unwrap();

    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", &settings.get_string("log_level").unwrap())
    }

    env_logger::init();

    tokio::spawn(async move {
        let mut event_listener = EventListener::new();
        event_listener.register_handler("UserCreatedEvent", handle_event);

        let subscriptions_list = &settings.get_string("service_subscriptions").unwrap();
        let topics = subscriptions_list.split(",").collect::<Vec<&str>>();
        let arc = Arc::new(event_listener);
        let transaction_handler = cqrs_kafka::traits::NoopTransactionHandler::default();
        let kafka_event_listener_channel = StreamKafkaInboundChannel::new(
            &settings.get_string("service_id").unwrap(),
            topics.as_slice(),
            &settings.get_string("bootstrap_server").unwrap(),
            arc.clone(),
            &transaction_handler,
            false,
        )
        .expect("Could not create kafka event listener channel");

        kafka_event_listener_channel.consume_async_blocking().await;
    });

    let settings_inner = Config::builder()
        .add_source(config::File::with_name("cqrs-example-client/src/Settings"))
        .build()
        .unwrap();
    let command_topic = settings_inner.get_string("command_topic").expect("Could not get command topic");
    let bootstrap_server = settings_inner.get_string("bootstrap_server").expect("Could not get bootstrap server");

    let kafka_command_channel = KafkaOutboundChannel::new(
        command_topic,
        bootstrap_server.as_str(),
    )
    .expect("Could not create kafka command channel");
    let client = CommandServiceClient::new(
        &settings_inner.get_string("service_id").unwrap(),
        kafka_command_channel
    );
    let command_service_client = Arc::new(client);

    let client_for_task = Arc::clone(&command_service_client);
    tokio::spawn(async move {
        let settings = Config::builder()
            .add_source(config::File::with_name("cqrs-example-client/src/Settings"))
            .build()
            .unwrap();
        let command_channel = StreamKafkaInboundChannel::new(
            "COMMAND-CLIENT",
            &[&settings.get_string("response_topic").unwrap()],
            &settings.get_string("bootstrap_server").unwrap(),
            client_for_task,
            &cqrs_kafka::traits::NoopTransactionHandler {},
            false,
        )
        .expect("Failed to create command channel");

        command_channel.consume_async_blocking().await;
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
