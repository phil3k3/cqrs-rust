use cqrs::{CommandResponse, CommandServiceClient, OutboundChannel, InboundChannel, Command};
use cqrs_kafka::{KafkaCommandChannel};
use serde::{Deserialize, Serialize};

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
    let mut command_service_client = CommandServiceClient::new("COMMAND-CLIENT");
    let mut kafka_command_channel = KafkaCommandChannel::new("localhost:9092", "CQRS-SERVER");
    let command = CreateUserCommand {
        user_id: String::from("Test"),
        name: String::from("Name")
    };
    command_service_client.send_command(&command, &kafka_command_channel);

}