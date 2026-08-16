use crate::cqrs::messages::{CommandResponse, CommandResponseResult};
use crate::cqrs::traits::{Command, EventEnvelope};
use crate::prelude::*;
use chrono::Utc;
use cqrs_messages::cqrs::messages::{
    CommandEnvelopeProto, CommandResponseEnvelopeProto, DomainEventEnvelopeProto,
};
use prost::Message;
use serde::Serialize;
use uuid::Uuid;

pub fn serialize_event_to_protobuf<E: EventEnvelope>(
    event: E,
    service_id: &str,
    event_id: &str,
) -> Result<Vec<u8>> {
    let event_type = event.event_type().to_owned();
    let version = event.version();
    let partition_key = event.id();
    let serialized_event = serde_json::to_vec(&event)?;
    let event_envelope = DomainEventEnvelopeProto {
        id: String::from(event_id),
        timestamp: Utc::now().timestamp(),
        transaction_id: Uuid::new_v4().to_string(),
        r#type: event_type,
        version,
        stream_info: None,
        event: serialized_event,
        partition_key,
        producing_service_id: service_id.to_owned(),
        producing_service_version: "1".to_owned(),
    };
    serialize_protobuf(&event_envelope)
}

/// Wire representation of a command sent by the client: the inner JSON is
/// tagged with the command's type so the server can deserialize it directly
/// into its closed `CommandEnvelope` enum (an `internally tagged, with
/// content` serde representation) without a runtime routing table.
#[derive(Serialize)]
struct TaggedCommand<'a, C> {
    r#type: &'a str,
    payload: &'a C,
}

pub fn serialize_command_to_protobuf<C: Command>(
    command_id: &str,
    command: &C,
    service_id: String,
    service_instance_id: u32,
) -> Result<Vec<u8>> {
    let command_type = command.get_type();
    let serialized_command = serde_json::to_vec(&TaggedCommand {
        r#type: &command_type,
        payload: command,
    })?;
    let service_instance_id_i32 = service_instance_id as i32;
    let command_id = String::from(command_id);
    let command_envelope = CommandEnvelopeProto {
        id: command_id.to_owned(),
        timestamp: Utc::now().timestamp(),
        service_id,
        service_instance_id: service_instance_id_i32,
        transaction_id: Uuid::new_v4().to_string(),
        r#type: command_type,
        version: command.get_version(),
        subject: command.get_subject().to_owned(),
        command: serialized_command,
    };
    serialize_protobuf(&command_envelope)
}

pub fn serialize_command_response_to_protobuf(
    command_response: CommandResponse,
    subject: &str,
    command_type: &str,
    version: i32,
    command_id: &str,
    service_id: &str,
) -> Result<Vec<u8>> {
    let command_response_result = CommandResponseResult {
        entity_id: subject.to_owned(),
        result: command_response,
    };
    let command_response_serialized = serde_json::to_string(&command_response_result)?;
    let response_envelope = CommandResponseEnvelopeProto {
        transaction_id: Uuid::new_v4().to_string(),
        command_id: command_id.to_owned(),
        timestamp: Utc::now().timestamp(),
        service_id: service_id.to_owned(),
        r#type: command_type.to_owned(),
        version,
        response: command_response_serialized.as_bytes().to_vec(),
        error: None,
        id: Uuid::new_v4().to_string(),
    };
    serialize_protobuf(&response_envelope)
}

fn serialize_protobuf<M: Message + Sized>(envelope: &M) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    buf.reserve(envelope.encoded_len());
    envelope.encode(&mut buf)?;
    Ok(buf)
}

pub fn decode_message(message: &[u8]) -> Result<(CommandResponse, String)> {
    let command_response = CommandResponseEnvelopeProto::decode(message)?;
    let command_response_result =
        serde_json::from_slice::<CommandResponseResult>(&command_response.response)?;
    Ok((command_response_result.result, command_response.command_id))
}
