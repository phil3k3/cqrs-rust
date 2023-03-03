use std::fmt::format;
use rdkafka::{ClientConfig, ClientContext, Message};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::error::KafkaError;
use rdkafka::message::DeliveryResult;
use rdkafka::producer::{BaseProducer, BaseRecord, ProducerContext, ThreadedProducer};
use cqrs_library::{InboundChannel, OutboundChannel};

struct ProducerCallbackLogger;
impl ClientContext for ProducerCallbackLogger {

}

pub struct KafkaOutboundChannel {
    topic: String,
    producer: ThreadedProducer<ProducerCallbackLogger>
}

pub struct KafkaInboundChannel {
    topic: String,
    bootstrap_server: String,
}

impl ProducerContext for ProducerCallbackLogger {
    type DeliveryOpaque = ();

    fn delivery(&self, delivery_result: &DeliveryResult<'_>, _delivery_opaque: Self::DeliveryOpaque) {
        let unwrapped_result = delivery_result.as_ref();
        match unwrapped_result {
            Ok(msg) => {
                let key : &str = msg.key_view().unwrap().unwrap();
                println!("Produced message with key {} in offset {} and partition {}", key, msg.offset(), msg.partition());
            }
            Err(producer_error) => {
                let key : &str = producer_error.1.key_view().unwrap().unwrap();
                println!("Failed to produce message with key {}: {}", key, producer_error.0);
            }
        }
    }
}

fn create_producer(bootstrap_server: String, service_id: &str) -> Result<ThreadedProducer<ProducerCallbackLogger>, KafkaError> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", bootstrap_server)
        .set("message.timeout.ms", "5000")
        .set("enable.idempotence", "true")
        .set("transactional.id", format!("{}-producer", &service_id))
        .set("debug", "eos")
        .set("client.id", format!("{}-producer", &service_id));
    config.set_log_level(RDKafkaLogLevel::Debug);
    config.create_with_context(ProducerCallbackLogger {})
}


impl KafkaOutboundChannel {
    pub fn new(service_id: &str, topic: &str, bootstrap_server: &str) -> KafkaOutboundChannel {
        KafkaOutboundChannel {
            topic: topic.to_owned(),
            producer: create_producer(bootstrap_server.to_string(), service_id).unwrap()
        }
    }
}

impl OutboundChannel for KafkaOutboundChannel {
    fn send(&mut self, key: Vec<u8>, message: Vec<u8>) {
        self.producer
            .send(BaseRecord::to(self.topic.as_str())
                .key(&key)
                .payload(&message))
            .expect("Failed to send message");
    }
}

impl InboundChannel for KafkaInboundChannel {
    fn consume(&mut self) -> Option<Vec<u8>> {
        todo!()
    }
}