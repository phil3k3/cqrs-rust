use crate::prelude::*;
use cqrs_library::cqrs::traits::{CommandResponseChannel, EventChannel, OutboundChannel};
use log::{error, info};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer};
use rdkafka::{ClientConfig, ClientContext, Message, TopicPartitionList};
use std::time::Duration;
use rdkafka::consumer::ConsumerGroupMetadata;
use rdkafka::util::Timeout;
use crate::KafkaSettings;
use crate::traits::TransactionHandler;

pub struct KafkaOutboundChannel {
    topic: String,
    producer: ThreadedProducer<ProducerCallbackLogger>
}

pub struct TransactionalKafkaOutboundChannel<'a> {
    pub kafka_settings: &'a KafkaSettings,
    pub producer: ThreadedProducer<ProducerCallbackLogger>,
    pub admin_client: AdminClient<ProducerCallbackLogger>
}

impl<'a> TransactionHandler for TransactionalKafkaOutboundChannel<'a> {
    fn begin_transaction(&self) -> Result<()> {
        self.producer.begin_transaction()
            .map_err(|x| Error::from(x))
    }

    fn commit_transaction(&self, list: &TopicPartitionList, consumer: &ConsumerGroupMetadata) -> Result<()> {
        self.producer.send_offsets_to_transaction(list, consumer, Timeout::After(Duration::from_secs(30)))
            .map_err(|x| Error::from(x))
    }
}

impl<'a> TransactionalKafkaOutboundChannel<'a> {

    pub fn new(kafka_settings: &'a KafkaSettings) -> Result<Self> {
        let producer = create_transactional_producer(
            kafka_settings.bootstrap_server.as_str(),
            kafka_settings.transaction_id.as_str(),
        )?;
        let admin_client= create_admin_client(kafka_settings.bootstrap_server.as_str())?;
        Ok(Self {
            kafka_settings,
            producer,
            admin_client
        })
    }

    pub async fn create_topic(&self, topic: &str) -> Result<()> {
        self
            .admin_client
            .create_topics(
                &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
                &AdminOptions::new(),
            )
            .await?;
        Ok(())
    }
}

impl<'a> EventChannel for TransactionalKafkaOutboundChannel<'a> {
    fn send_event(&self, key: &[u8], event: &[u8]) -> cqrs_library::prelude::Result<()> {
        self.producer
            .send(
                BaseRecord::to(self.kafka_settings.events_topic.as_str())
                    .key(key)
                    .payload(event),
            ).map_err(|x| Error::from(x.0))?;
        for _ in 0..10 {
            self.producer.poll(Duration::from_millis(100));
        }
        Ok(())
    }
}

impl<'a> CommandResponseChannel for TransactionalKafkaOutboundChannel<'a> {
    fn send_command_response(&self, key: &[u8], message: &[u8]) -> cqrs_library::prelude::Result<()> {
        self.producer
            .send(
                BaseRecord::to(self.kafka_settings.command_response_topic.as_str())
                    .key(key)
                    .payload(message),
            ).map_err(|x| Error::from(x.0))?;
        for _ in 0..10 {
            self.producer.poll(Duration::from_millis(100));
        }
        Ok(())
    }
}

pub struct ProducerCallbackLogger;
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

fn create_producer(bootstrap_server: &str) -> Result<ThreadedProducer<ProducerCallbackLogger>> {
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

fn create_transactional_producer(bootstrap_server: &str, transaction_id: &str) -> Result<ThreadedProducer<ProducerCallbackLogger>> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", bootstrap_server)
        .set("message.timeout.ms", "5000")
        .set("debug", "broker,topic,msg")
        .set("transactional.id", transaction_id)
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("max.in.flight.requests.per.connection", "1")
        .set("retries", "1000000")
        .set("linger.ms", "5");
    config.set_log_level(RDKafkaLogLevel::Debug);
    config
        .create_with_context(ProducerCallbackLogger {})
        .map_err(|x| x.into())
}


pub fn create_admin_client(bootstrap_server: &str) -> Result<AdminClient<ProducerCallbackLogger>> {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", bootstrap_server);
    config.set_log_level(RDKafkaLogLevel::Debug);
    config
        .create_with_context(ProducerCallbackLogger {})
        .map_err(|x| x.into())
}

impl KafkaOutboundChannel {
    pub fn new(topic: String, bootstrap_server: &str) -> Result<KafkaOutboundChannel> {
        let producer = create_producer(bootstrap_server)?;
        Ok(KafkaOutboundChannel {
            topic,
            producer,
        })
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
