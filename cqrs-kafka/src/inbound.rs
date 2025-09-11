use crate::operations::{create_consumer, CustomContext};
use crate::prelude::*;
use cqrs_library::cqrs::traits::{InboundChannel, MessageConsumer};
use futures::TryStreamExt;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};
use std::sync::Arc;
use std::time::Duration;

type LoggingConsumer = BaseConsumer<CustomContext>;


pub struct KafkaInboundChannel {
    consumer: BaseConsumer<CustomContext>,
}

pub struct StreamKafkaInboundChannel<T: MessageConsumer> {
    consumer: StreamConsumer<CustomContext>,
    message_consumer: Arc<T>
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


impl<T: MessageConsumer> StreamKafkaInboundChannel<T> {
    pub fn new(service_id: &str, topics: &[&str], bootstrap_server: &str, message_consumer: T) -> Result<StreamKafkaInboundChannel<T>> {
        let result = create_consumer(bootstrap_server.to_string(), service_id.to_string())?;
        let channel = StreamKafkaInboundChannel {
            consumer: result,
            message_consumer: Arc::new(message_consumer),
        };
        channel.consumer.subscribe(&topics.to_vec())?;
        Ok(channel)
    }

    pub async fn consume_async_blocking(&self) {
        let consumer = self.message_consumer.clone();
        self.consumer.stream().try_for_each(|borrowed_message| {
            let consumer = consumer.clone();
            async move {
                if let Some(message) = borrowed_message.payload() {
                    consumer.consume(message);
                }
                Ok(())
            }
        }).await.expect("Stream processing failed")
    }
}

