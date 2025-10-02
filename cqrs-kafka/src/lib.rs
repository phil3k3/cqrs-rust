use crate::inbound::StreamKafkaInboundChannel;
use crate::outbound::{create_admin_client, create_transactional_producer, ProducerCallbackLogger};
use async_trait::async_trait;
use config::Config;
use cqrs_library::cqrs::traits::{EventSender, Transport};
use cqrs_library::cqrs::CommandServiceServer;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::consumer::ConsumerGroupMetadata;
use rdkafka::producer::{BaseRecord, Producer, ThreadedProducer};
use rdkafka::util::Timeout;
use rdkafka::TopicPartitionList;
use std::sync::Arc;
use std::time::Duration;

pub mod error;
pub mod inbound;
mod operations;
pub mod outbound;
mod prelude;
pub mod traits;

use crate::prelude::*;
use crate::traits::TransactionHandler;

pub struct KafkaSettings {
    pub bootstrap_server: String,
    pub transaction_id: String,
    pub events_topic: String,
    pub commands_topic: String,
    pub command_response_topic: String,
    pub service_id: String,
}

impl From<Config> for KafkaSettings {
    fn from(value: Config) -> Self {
        Self {
            bootstrap_server: value
                .get_string("bootstrap_server")
                .expect("Bootstrap server must be configured"),
            transaction_id: value
                .get_string("transaction_id")
                .expect("Transaction id must be configured"),
            events_topic: value
                .get_string("events_topic")
                .expect("Events topic must be configured"),
            command_response_topic: value
                .get_string("response_topic")
                .expect("Command response topic must be configured"),
            commands_topic: value
                .get_string("command_topic")
                .expect("Commands topic must be configured"),
            service_id: value
                .get_string("service_id")
                .expect("Service id must be configured"),
        }
    }
}

pub struct KafkaTransport<'a> {
    pub kafka_settings: &'a KafkaSettings,
    pub producer: ThreadedProducer<ProducerCallbackLogger>,
    pub admin_client: AdminClient<ProducerCallbackLogger>,
}

impl<'a> KafkaTransport<'a> {
    pub fn new(kafka_settings: &'a KafkaSettings) -> Result<Self> {
        let producer = create_transactional_producer(
            kafka_settings.bootstrap_server.as_str(),
            kafka_settings.transaction_id.as_str(),
        )?;
        let admin_client = create_admin_client(kafka_settings.bootstrap_server.as_str())?;

        Ok(Self {
            kafka_settings,
            producer,
            admin_client,
        })
    }

    pub async fn create_topic(&self, topic: &str) -> Result<()> {
        self.admin_client
            .create_topics(
                &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
                &AdminOptions::new(),
            )
            .await?;
        Ok(())
    }
}

impl<'a> EventSender for KafkaTransport<'a> {
    fn send_event(&self, key: &[u8], event: &[u8]) -> cqrs_library::prelude::Result<()> {
        self.producer
            .send(
                BaseRecord::to(self.kafka_settings.events_topic.as_str())
                    .key(key)
                    .payload(event),
            )
            .map_err(|x| Error::from(x.0))?;
        for _ in 0..10 {
            self.producer.poll(Duration::from_millis(100));
        }
        Ok(())
    }
}

#[async_trait]
impl<'a> Transport for KafkaTransport<'a> {
    type Transport = KafkaTransport<'a>;

    fn send_command_response(
        &self,
        key: &[u8],
        message: &[u8],
    ) -> cqrs_library::prelude::Result<()> {
        self.producer
            .send(
                BaseRecord::to(self.kafka_settings.command_response_topic.as_str())
                    .key(key)
                    .payload(message),
            )
            .map_err(|x| Error::from(x.0))?;
        for _ in 0..10 {
            self.producer.poll(Duration::from_millis(100));
        }
        Ok(())
    }

    async fn consume_async_blocking<'s>(
        &self,
        message_consumer: CommandServiceServer<'s, Self::Transport>,
    ) -> cqrs_library::prelude::Result<()> {
        let command_channel = StreamKafkaInboundChannel::new(
            self.kafka_settings.service_id.as_str(),
            &[self.kafka_settings.commands_topic.as_str()],
            &self.kafka_settings.bootstrap_server,
            Arc::new(message_consumer),
            self,
            false,
        )?;
        command_channel
            .consume_async_blocking()
            .await
            .map_err(|x| cqrs_library::error::Error::from(x))
    }
}

impl<'a> TransactionHandler for KafkaTransport<'a> {
    fn begin_transaction(&self) -> Result<()> {
        self.producer
            .begin_transaction()
            .map_err(|x| Error::from(x))
    }

    fn commit_transaction(
        &self,
        list: &TopicPartitionList,
        consumer: &ConsumerGroupMetadata,
    ) -> Result<()> {
        self.producer
            .send_offsets_to_transaction(list, consumer, Timeout::After(Duration::from_secs(30)))
            .map_err(|x| Error::from(x))
    }
}

#[cfg(test)]
mod tests {
    use crate::inbound::KafkaInboundChannel;
    use crate::outbound::{create_admin_client, KafkaOutboundChannel};
    use cqrs_library::cqrs::traits::{InboundChannel, OutboundChannel};
    use log::info;
    use rdkafka::admin::{AdminOptions, NewTopic, TopicReplication};
    use std::sync::mpsc::channel;
    use std::thread;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::kafka::{Kafka, KAFKA_PORT};

    #[tokio::test]
    async fn test_sync_send_receive() {
        env_logger::init();

        info!("Starting transmission test via Kafka");

        let kafka_node = Kafka::default().start().await.unwrap();
        let host_port = kafka_node.get_host_port_ipv4(KAFKA_PORT).await.unwrap();

        info!("Started Kafka container at port {}", host_port);

        let bootstrap_servers = format!("127.0.0.1:{}", host_port);

        info!("{}", bootstrap_servers);
        let outbound_channel =
            KafkaOutboundChannel::new(String::from("TEST"), &bootstrap_servers).unwrap();

        let admin_client = create_admin_client(&bootstrap_servers).unwrap();
        admin_client
            .create_topics(
                &[NewTopic::new("TEST", 1, TopicReplication::Fixed(1))],
                &AdminOptions::new(),
            )
            .await
            .unwrap();
        let inbound_channel =
            KafkaInboundChannel::new("TEST_IN", &["TEST"], &bootstrap_servers, true).unwrap();

        inbound_channel.consume();

        let sender = thread::spawn(move || {
            outbound_channel.send("KEY".as_bytes(), "MESSAGE".as_bytes());
        });

        info!("Waiting for sender");
        sender.join().expect("The sender thread has panicked");
        info!("Message sent");

        let receiver = thread::spawn(move || loop {
            info!("Waiting for message");
            let message = inbound_channel.consume();
            match message {
                Some(content) => {
                    let string = String::from_utf8(content).unwrap();
                    info!("Received message {}", &string);
                    assert_eq!("MESSAGE", string);
                    break;
                }
                _ => {}
            }
        });

        info!("Waiting for receiver");
        receiver.join().expect("The receiver thread has panicked");
    }

    #[test]
    fn test_channel() {
        info!("Starting test via Kafka");

        let (tx, rx) = channel();

        let sender = thread::spawn(move || {
            tx.send("Hello 2, thread".to_owned())
                .expect("Unable to send on channel");
        });

        let receiver = thread::spawn(move || {
            let value = rx.recv().expect("Unable to receive from channel");
            info!("{}", value);
        });

        sender.join().expect("The sender thread has panicked");
        receiver.join().expect("The receiver thread has panicked");
    }
}
