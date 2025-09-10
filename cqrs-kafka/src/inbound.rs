use std::time::Duration;
use futures::TryStreamExt;
use log::info;
use rdkafka::{ClientConfig, ClientContext, Message, TopicPartitionList};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::{KafkaError, KafkaResult};
use cqrs_library::cqrs::EventListener;
use cqrs_library::cqrs::traits::InboundChannel;
use crate::prelude::*;

struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, _base_consumer: &BaseConsumer<Self>, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, _base_consumer: &BaseConsumer<Self>, rebalance: &Rebalance) {
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
    fn create_consumer(bootstrap_server: String, service_id: String) -> Result<LoggingConsumer> {
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
        config.create_with_context(CustomContext {}).map_err(|x| x.into())
    }
}

impl StreamKafkaInboundChannel {
    pub fn new(service_id: &str, topics: &[&str], bootstrap_server: &str) -> Result<StreamKafkaInboundChannel> {
        let result = StreamKafkaInboundChannel::create_consumer(bootstrap_server.to_string(), service_id.to_string())?;
        let channel = StreamKafkaInboundChannel {
            consumer: result
        };
        channel.consumer.subscribe(&topics.to_vec()).expect("Could not subscribe");
        Ok(channel)
    }

    fn create_consumer(bootstrap_server: String, service_id: String) -> Result<LoggingStreamingConsumer> {
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
        config.create_with_context(CustomContext {}).map_err(|x| x.into())
    }

    pub async fn async_consume(&mut self) -> Result<Vec<u8>> {
        loop {
            let msg = self.consumer.recv().await?;
            if let Some(payload) = msg.payload() {
                return Ok(payload.to_vec());
            }
        }
    }

    pub async fn consume_async_blocking(&self, event_listener: &EventListener) {
        self.consumer.stream().try_for_each(|borrowed_message| {
            async move {
                if let Some(message) = borrowed_message.payload() {
                    event_listener.consume(message);
                }
                Ok(())
            }
        }).await.expect("Stream processing failed")
    }
}

impl InboundChannel for KafkaInboundChannel {
    fn consume(&mut self) -> Option<Vec<u8>> {
        self.consumer.poll(Duration::from_secs(1))
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
                )
    }
}
