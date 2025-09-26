use crate::cqrs::command::{CommandAccessor, CommandStore};
use crate::cqrs::messages::{CommandResponse, CommandResponseResult};
use crate::cqrs::traits::{Command, Event, EventProducer};
use crate::prelude::*;
use chrono::Utc;
use cqrs_messages::cqrs::messages::{
    CommandEnvelopeProto, CommandResponseEnvelopeProto, DomainEventEnvelopeProto,
};
use prost::Message;
use uuid::Uuid;

pub fn serialize_event_to_protobuf(
    event: &dyn Event,
    service_id: &str,
    event_id: &str,
) -> Result<Vec<u8>> {
    let serialized_event = serde_json::to_vec(&event)?;
    let event_id = String::from(event_id);
    let event_envelope = DomainEventEnvelopeProto {
        id: event_id.to_owned(),
        timestamp: Utc::now().timestamp(),
        transaction_id: Uuid::new_v4().to_string(),
        r#type: event.get_type().to_owned(),
        version: event.get_version().to_owned(),
        stream_info: None,
        event: serialized_event,
        partition_key: event.get_id(),
        producing_service_id: service_id.to_owned(),
        producing_service_version: "1".to_owned(),
    };
    serialize_protobuf(&event_envelope)
}

pub fn handle_command(
    serialized_command: &[u8],
    command_store: &CommandStore,
    event_producer: &impl EventProducer,
) -> Result<Option<Vec<u8>>> {
    let result = CommandEnvelopeProto::decode(serialized_command)?;

    let mut deserializer = CommandAccessor::new(&result.command, result.id);

    let command_response =
        command_store.handle_command(&result.r#type, &mut deserializer, event_producer);

    serialize_command_response_to_protobuf(
        command_response.command_response,
        &deserializer,
        command_response.service_id,
    )
}

pub fn serialize_command_to_protobuf<'a, C: Command<'a>>(
    command_id: &str,
    command: &C,
    service_id: String,
    service_instance_id: u32,
) -> Result<Vec<u8>> {
    let serialized_command = serde_json::to_vec(command)?;
    let service_instance_id_i32 = service_instance_id as i32;
    let command_id = String::from(command_id);
    let command_envelope = CommandEnvelopeProto {
        id: command_id.to_owned(),
        timestamp: Utc::now().timestamp(),
        service_id,
        service_instance_id: service_instance_id_i32,
        transaction_id: Uuid::new_v4().to_string(),
        r#type: command.get_type().to_owned(),
        version: command.get_version().to_owned(),
        subject: command.get_subject().to_owned(),
        command: serialized_command.to_vec(),
    };
    serialize_protobuf(&command_envelope)
}

pub fn serialize_command_response_to_protobuf(
    command_response: CommandResponse,
    command_accessor: &CommandAccessor,
    service_id: String,
) -> Result<Option<Vec<u8>>> {
    let command = &command_accessor.command_metadata;
    let command_id = &command_accessor.command_id;
    match command {
        None => Ok(None),
        Some(command) => {
            let command_response_result = CommandResponseResult {
                entity_id: command.subject.to_owned(),
                result: command_response,
            };
            let command_response_serialized = serde_json::to_string(&command_response_result)?;
            let response_envelope = CommandResponseEnvelopeProto {
                transaction_id: Uuid::new_v4().to_string(),
                command_id: String::from(command_id),
                timestamp: Utc::now().timestamp(),
                service_id: service_id.to_owned(),
                r#type: String::from(&command.command_type),
                version: command.version,
                response: command_response_serialized.as_bytes().to_vec(),
                error: None,
                id: Uuid::new_v4().to_string(),
            };
            let vec = serialize_protobuf(&response_envelope);
            vec.map(|x| Some(x))
        }
    }
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
