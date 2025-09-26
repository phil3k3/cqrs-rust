use crate::operations::{create_basic_consumer, create_streaming_consumer, CustomContext};
use crate::prelude::*;
use cqrs_library::cqrs::traits::{InboundChannel, MessageConsumer};
use futures::TryStreamExt;
use rdkafka::consumer::{BaseConsumer, Consumer, StreamConsumer};
use rdkafka::{Message, Offset, TopicPartitionList};
use std::sync::Arc;
use std::time::Duration;
use crate::traits::TransactionHandler;

pub struct KafkaInboundChannel {
    consumer: BaseConsumer<CustomContext>,
}

pub struct StreamKafkaInboundChannel<'a, T: MessageConsumer, H: TransactionHandler> {
    consumer: StreamConsumer<CustomContext>,
    message_consumer: Arc<T>,
    transaction_handler: &'a H
}

impl KafkaInboundChannel {
    pub fn new(
        service_id: &str,
        topics: &[&str],
        bootstrap_server: &str,
        default_reset: bool,
    ) -> Result<KafkaInboundChannel> {
        let consumer = create_basic_consumer(
            bootstrap_server.to_string(),
            service_id.to_string(),
            default_reset,
        )?;
        let channel = KafkaInboundChannel { consumer };
        channel
            .consumer
            .subscribe(&topics.to_vec())
            .expect("Could not subscribe");
        Ok(channel)
    }
}

impl InboundChannel for KafkaInboundChannel {
    fn consume(&self) -> Option<Vec<u8>> {
        self.consumer
            .poll(Duration::from_secs(1))
            .and_then(|x| match x {
                Ok(t) => {
                    let option = t.payload();
                    option.and_then(|y| Some(y.to_vec()))
                }
                Err(_v) => None,
            })
    }
}

impl<'a, T: MessageConsumer, H: TransactionHandler> StreamKafkaInboundChannel<'a, T, H> {
    pub fn new(
        service_id: &str,
        topics: &[&str],
        bootstrap_server: &str,
        message_consumer: Arc<T>,
        transaction_handler: &'a H,
        default_reset: bool,
    ) -> Result<StreamKafkaInboundChannel<'a, T, H>> {
        let consumer = create_streaming_consumer(bootstrap_server, service_id, default_reset)?;
        let channel = StreamKafkaInboundChannel {
            consumer,
            message_consumer,
            transaction_handler,
        };
        channel.consumer.subscribe(topics)?;
        Ok(channel)
    }

    pub async fn consume_async_blocking(&self) {
        let consumer = self.message_consumer.clone();
        self.consumer
            .stream()
            .try_for_each(|borrowed_message| {
                let consumer = consumer.clone();
                async move {
                    let mut offsets = TopicPartitionList::new();
                    offsets.add_partition_offset(borrowed_message.topic(), borrowed_message.partition(), Offset::Offset(borrowed_message.offset()+1))?;
                    // this intentionally panics to make sure that the pod is crash looping on faulty message processing as we want to
                    // ensure consistency, hence we can't skip over a message
                    // ((( ----
                    self.transaction_handler.begin_transaction().expect("Could not begin transaction");
                    let consumer_metadata = self.consumer.group_metadata().expect("Could not get consumer metadata");
                    if let Some(message) = borrowed_message.payload() {
                        consumer.consume(message).await.expect("Could not consume message");
                    }
                    self.transaction_handler.commit(&offsets, &consumer_metadata).expect("Could not commit transaction");
                    // ))) ----
                    Ok(())
                }
            })
            .await
            .expect("Stream processing failed")
    }
}
