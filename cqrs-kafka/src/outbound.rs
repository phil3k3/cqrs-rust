use crate::prelude::*;
use cqrs_library::cqrs::traits::OutboundChannel;
use log::{error, info};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer};
use rdkafka::{ClientConfig, ClientContext, Message};
use std::time::Duration;

pub struct KafkaOutboundChannel {
    topic: String,
    producer: ThreadedProducer<ProducerCallbackLogger>,
    admin_client: AdminClient<ProducerCallbackLogger>,
}

struct ProducerCallbackLogger;
impl ClientContext for ProducerCallbackLogger {}

impl ProducerContext for ProducerCallbackLogger {
    type DeliveryOpaque = ();

    fn delivery(
        &self,
        delivery_result: &DeliveryResult<'_>,
        _delivery_opaque: Self::DeliveryOpaque,
    ) {
        match delivery_result.as_ref() {
            Ok(msg) => {
                let key: &str = msg.key_view().unwrap().unwrap();
                info!(
                    "Produced message with key {} in offset {} and partition {}",
                    key,
                    msg.offset(),
                    msg.partition()
                );
            }
            Err(producer_error) => {
                let key: &str = producer_error.1.key_view().unwrap().unwrap();
                error!(
                    "Failed to produce message with key {}: {}",
                    key, producer_error.0
                );
            }
        }
    }
}

fn create_producer(bootstrap_server: String) -> Result<ThreadedProducer<ProducerCallbackLogger>> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", bootstrap_server)
        .set("message.timeout.ms", "5000")
        .set("debug", "broker,topic,msg");
    config.set_log_level(RDKafkaLogLevel::Debug);
    config
        .create_with_context(ProducerCallbackLogger {})
        .map_err(|x| x.into())
}

fn create_admin_client(bootstrap_server: String) -> Result<AdminClient<ProducerCallbackLogger>> {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", bootstrap_server);
    config.set_log_level(RDKafkaLogLevel::Debug);
    config
        .create_with_context(ProducerCallbackLogger {})
        .map_err(|x| x.into())
}

impl KafkaOutboundChannel {
    pub fn new(topic: &str, bootstrap_server: &str) -> Result<KafkaOutboundChannel> {
        let producer = create_producer(bootstrap_server.to_string())?;
        let admin_client = create_admin_client(bootstrap_server.to_string())?;
        Ok(KafkaOutboundChannel {
            topic: topic.to_owned(),
            producer,
            admin_client,
        })
    }

    pub async fn create_topic(&self, topic: &str) {
        let result = self
            .admin_client
            .create_topics(
                &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
                &AdminOptions::new(),
            )
            .await;
        match result {
            Ok(_result) => info!("Topic {} created", topic),
            Err(error) => error!("Error creating topic {}: {}", topic, error),
        }
    }
}

impl OutboundChannel for KafkaOutboundChannel {
    fn send(&self, key: &[u8], message: &[u8]) {
        self.producer
            .send(
                BaseRecord::to(self.topic.as_str())
                    .key(key)
                    .payload(message),
            )
            .expect("Failed to send message");
        for _ in 0..10 {
            self.producer.poll(Duration::from_millis(100));
        }
        self.producer
            .flush(Duration::from_secs(60))
            .expect("Flush failed");
    }
}
