use log::info;
use rdkafka::{ClientConfig, ClientContext, TopicPartitionList};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::KafkaResult;
use crate::prelude::*;

pub struct CustomContext;

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

type LoggingStreamingConsumer = StreamConsumer<CustomContext>;

pub fn create_consumer(bootstrap_server: String, service_id: String) -> Result<LoggingStreamingConsumer> {
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