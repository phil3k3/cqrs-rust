use std::time::Duration;
use futures::TryStreamExt;
use log::info;
use rdkafka::{ClientConfig, ClientContext, Message, TopicPartitionList};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::{KafkaError, KafkaResult};
use cqrs_library::{EventListener, InboundChannel, MessageConsumer, MessageProcessor, OutboundChannel};

struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}
type LoggingConsumer = BaseConsumer<CustomContext>;
type LoggingStreamingConsumer = StreamConsumer<CustomContext>;

pub struct KafkaInboundChannel {
    consumer: BaseConsumer<CustomContext>,
}

pub struct StreamKafkaInboundChannel {
    consumer: StreamConsumer<CustomContext>,
}

impl KafkaInboundChannel {
    pub fn new(service_id: &str, topics: &[&str], bootstrap_server: &str) -> KafkaInboundChannel {
        let channel = KafkaInboundChannel {
            consumer: KafkaInboundChannel::create_consumer(bootstrap_server.to_string(), service_id.to_string()).unwrap()
        };
        channel.consumer.subscribe(&topics.to_vec()).expect("Could not subscribe");
        channel
    }
    fn create_consumer(bootstrap_server: String, service_id: String) -> Result<LoggingConsumer, KafkaError> {
        let mut config = ClientConfig::new();
        config
            .set("group.id", format!("{}-consumer", &service_id))
            .set("bootstrap.servers", &bootstrap_server)
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .set("isolation.level", "read_uncommitted")
            .set("auto.offset.reset", "earliest")
            .set("debug", "consumer,cgrp,topic,fetch");

        // all nodes of the same service are in a group and will get some partitions assigned
        config.set_log_level(RDKafkaLogLevel::Debug);
        config.create_with_context(CustomContext {})
    }
}

impl StreamKafkaInboundChannel {
    pub fn new(service_id: &str, topics: &[&str], bootstrap_server: &str) -> StreamKafkaInboundChannel {
        let channel = StreamKafkaInboundChannel {
            consumer: StreamKafkaInboundChannel::create_consumer(bootstrap_server.to_string(), service_id.to_string()).unwrap()
        };
        channel.consumer.subscribe(&topics.to_vec()).expect("Could not subscribe");
        channel
    }

    fn create_consumer(bootstrap_server: String, service_id: String) -> Result<LoggingStreamingConsumer, KafkaError> {
        let mut config = ClientConfig::new();
        config
            .set("group.id", format!("{}-consumer", &service_id))
            .set("bootstrap.servers", &bootstrap_server)
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .set("isolation.level", "read_uncommitted")
            .set("auto.offset.reset", "earliest")
            .set("debug", "consumer,cgrp,topic,fetch");

        // all nodes of the same service are in a group and will get some partitions assigned
        config.set_log_level(RDKafkaLogLevel::Debug);
        config.create_with_context(CustomContext {})
    }

    pub async fn async_consume(&mut self) -> Option<Vec<u8>> {
        let message = self.consumer.recv().await;
        match message {
            Ok(t) => t.payload().and_then(|y| Some(y.to_vec())),
            Err(_v) => None
        }
    }

    pub async fn consume_async_blocking_consumer(&self, message_consumer: Box<dyn MessageConsumer>) {
        return self.consumer.stream().try_for_each(|borrowed_message| {
            async move {
                message_consumer.consume(borrowed_message.payload().unwrap());
                let key = String::from_utf8_lossy(borrowed_message.key().unwrap());
                println!("Key: '{:?}', Topic: '{}', Partition: {}, Offset: {}",
                         key, borrowed_message.topic(), borrowed_message.partition(), borrowed_message.offset());
                Ok(())
            }
        }).await.expect("Stream processing failed");
    }

    pub async fn consume_async_blocking(&self, mut message_consumer: Box<dyn MessageProcessor>, outbound_channel: &mut Box<dyn OutboundChannel + Send + Sync>) {
        return self.consumer.stream().try_for_each(|borrowed_message| {
            async move {
                message_consumer.consume(borrowed_message.payload().unwrap(), outbound_channel);
                let key = String::from_utf8_lossy(borrowed_message.key().unwrap());
                println!("Key: '{:?}', Topic: '{}', Partition: {}, Offset: {}",
                         key, borrowed_message.topic(), borrowed_message.partition(), borrowed_message.offset());
                Ok(())
            }
        }).await.expect("Stream processing failed");
    }
}

impl InboundChannel for KafkaInboundChannel {
    fn consume(&mut self) -> Option<Vec<u8>> {
        return self.consumer.poll(Duration::from_secs(1))
                .and_then(|x| {
                    match x {
                        Ok(t) => {
                            let option = t.payload();
                            option.and_then(|y| {
                                Some(y.to_vec())
                            })
                        },
                        Err(_v) => None
                    }
                }
                );
    }
}
