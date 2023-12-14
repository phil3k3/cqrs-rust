use std::{env, io};
use std::sync::{Arc, Mutex};
use config::Config;
use cqrs_library::{CommandServiceClient, Command, EventListener, Event, CommandResponse};
use cqrs_kafka::inbound::{KafkaInboundChannel, StreamKafkaInboundChannel};
use cqrs_kafka::outbound::KafkaOutboundChannel;
use serde::{Deserialize, Serialize};
use actix_web::{App, HttpResponse, HttpServer, post, Responder, web};
use log::info;
use uuid::Uuid;

#[derive(Debug, Deserialize, Serialize)]
struct CreateUserCommand {
    user_id: String,
    name: String,
}

struct AppState {
    client: Mutex<CommandServiceClient<KafkaInboundChannel>>,
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
        return self.user_id.to_owned();
    }

    fn get_type(&self) -> String {
        return String::from("UserCreatedEvent");
    }
}

fn handle_event(event: &dyn Event) {
    info!("{:?}", event);
}

#[post("/users/{user_id}")]
async fn post_user(
    command_service_client: web::Data<AppState>,
    user_id: web::Path<String>) -> impl Responder {
    info!("Creating user {}", user_id);
    let command = CreateUserCommand {
        user_id: Uuid::new_v4().to_string(),
        name: String::from(user_id.into_inner()),
    };
    match command_service_client.client.lock().unwrap().send_command(&command).await {
        Ok(result) => {
            if result == CommandResponse::Ok {
                HttpResponse::Ok().body(command.user_id)
            } else {
                HttpResponse::InternalServerError().body("Failed to process command, check server logs")
            }
        }
        Err(_) => {
            HttpResponse::InternalServerError().body("Failed to send command")
        }
    }
}

fn create_channel(settings: Config) -> Box<KafkaInboundChannel> {
    return Box::new(KafkaInboundChannel::new(
        &settings.get_string("service_id").unwrap(),
        &[&settings.get_string("response_topic").unwrap()],
        &settings.get_string("bootstrap_server").unwrap(),
    ))
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

        let x = &settings.get_string("service_subscriptions").unwrap();
        let topics = x.split(",").collect::<Vec<&str>>();
        let kafka_event_listener_channel = StreamKafkaInboundChannel::new(
            &settings.get_string("service_id").unwrap(),
            topics.as_slice(),
            &settings.get_string("bootstrap_server").unwrap(),
        );

        kafka_event_listener_channel.consume_async_blocking(&event_listener).await;
    });

    HttpServer::new(|| {
        let settings2 = Config::builder()
            .add_source(config::File::with_name("cqrs-example-client/src/Settings"))
            .build()
            .unwrap();

        let kafka_command_channel = KafkaOutboundChannel::new(
            &settings2.get_string("command_topic").unwrap(),
            &settings2.get_string("bootstrap_server").unwrap(),
        );

        let command_service_client_data = web::Data::new(
            AppState {
                client: Mutex::new(
                    CommandServiceClient::new(
                        &settings2.get_string("service_id").unwrap(),
                        Arc::new(create_channel),
                        Box::new(kafka_command_channel)
                    )
                )
            }
        );
        command_service_client_data.client.lock().unwrap().start(settings2);
        App::new()
            .app_data(command_service_client_data.clone())
            .service(post_user)
    }).bind(("127.0.0.1", 8080))?
        .run()
        .await

}
