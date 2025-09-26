use crate::prelude::*;
use log::info;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::{ClientConfig, ClientContext, TopicPartitionList};

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

pub fn create_streaming_consumer(
    bootstrap_server: &str,
    service_id: &str,
    default_reset: bool,
) -> Result<LoggingStreamingConsumer> {
    let config = create_basic_config(&bootstrap_server, &service_id, default_reset);
    config
        .create_with_context(CustomContext {})
        .map_err(|x| x.into())
}

type LoggingConsumer = BaseConsumer<CustomContext>;

pub fn create_basic_consumer(
    bootstrap_server: String,
    service_id: String,
    default_reset: bool,
) -> Result<LoggingConsumer> {
    let config = create_basic_config(&bootstrap_server, &service_id, default_reset);
    config
        .create_with_context(CustomContext {})
        .map_err(|x| x.into())
}

fn create_basic_config(
    bootstrap_server: &str,
    service_id: &str,
    default_reset: bool,
) -> ClientConfig {
    let mut config = ClientConfig::new();
    config
        .set("group.id", format!("{}-consumer", &service_id))
        .set("bootstrap.servers", bootstrap_server)
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("isolation.level", "read_committed")
        .set("enable.auto.commit", "false")
        .set(
            "auto.offset.reset",
            if default_reset { "earliest" } else { "latest" },
        )
        .set("debug", "consumer,cgrp,topic,fetch");
    config.set_log_level(RDKafkaLogLevel::Debug);
    config
}
