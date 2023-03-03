use cqrs_library::{CommandResponse, CommandServiceClient, OutboundChannel, InboundChannel, Command};
use cqrs_kafka::{KafkaOutboundChannel};
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
    let mut kafka_command_channel = KafkaOutboundChannel::new("CQRS-CLIENT", "CQRS-SERVER", "localhost:9092");
    let command = CreateUserCommand {
        user_id: String::from("Test"),
        name: String::from("Name")
    };
    command_service_client.send_command(&command, &mut kafka_command_channel);

}