use chrono::Utc;
use uuid::Uuid;
use cqrs_messages::cqrs::messages::DomainEventEnvelopeProto;
use crate::cqrs::serialize_protobuf;
use crate::cqrs::traits::Event;

pub fn serialize_event_to_protobuf(event: &dyn Event, service_id: &str, event_id: &str) -> Vec<u8> {
    let serialized_event = serde_json::to_vec(&event).unwrap();
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
