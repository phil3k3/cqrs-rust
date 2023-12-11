use std::time::Duration;
use futures::TryStreamExt;
use log::info;
use rdkafka::{ClientConfig, ClientContext, Message, TopicPartitionList};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::{KafkaError, KafkaResult};
use cqrs_library::{EventListener, InboundChannel};

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
    consumer: BaseConsumer<CustomContext>
}

pub struct StreamKafkaInboundChannel {
    consumer: StreamConsumer<CustomContext>
}

impl KafkaInboundChannel {
    pub fn new(service_id: &str,topics: &[&str], bootstrap_server: &str) -> KafkaInboundChannel {
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

    pub async fn consume_async_blocking(&self, event_listener: &EventListener)  {

        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", "test_group")
            .set("bootstrap.servers", "localhost:9092")
            .set("enable.auto.commit", "true")
            .create()
            .expect("Consumer creation failed");
        return consumer.stream().try_for_each(|borrowed_message| {
            async move {
                let payload = match borrowed_message.payload_view::<str>() {
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        println!("Error while deserializing message payload: {:?}", e);
                        ""
                    },
                    None => "<no payload>",
                };
                event_listener.consume(borrowed_message.payload().unwrap());

                println!("Key: '{:?}', Payload: '{}', Topic: '{}', Partition: {}, Offset: {}",
                         borrowed_message.key(), payload, borrowed_message.topic(), borrowed_message.partition(), borrowed_message.offset());
                Ok(())  // Important to return Ok(()) for successful processing
            }
        }).await.expect("Stream processing failed");
    }
}

impl InboundChannel for KafkaInboundChannel {
    fn consume(&mut self) -> Option<Vec<u8>> {
        return self.consumer.poll(Duration::from_secs(1))
            .and_then(|x| match x {
                Ok(t) => t.payload().and_then(|y| Some(y.to_vec())),
                Err(_v) => None
            }
            );
    }
}