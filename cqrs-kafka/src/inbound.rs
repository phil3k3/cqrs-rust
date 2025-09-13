use crate::operations::{create_basic_consumer, create_streaming_consumer, CustomContext};
use crate::prelude::*;
use cqrs_library::cqrs::traits::{InboundChannel, MessageConsumer};
use futures::TryStreamExt;
use rdkafka::consumer::{BaseConsumer, Consumer, StreamConsumer};
use rdkafka::Message;
use std::sync::Arc;
use std::time::Duration;

pub struct KafkaInboundChannel {
    consumer: BaseConsumer<CustomContext>,
}

pub struct StreamKafkaInboundChannel<T: MessageConsumer> {
    consumer: StreamConsumer<CustomContext>,
    message_consumer: Arc<T>,
}

impl KafkaInboundChannel {
    pub fn new(
        service_id: &str,
        topics: &[&str],
        bootstrap_server: &str,
    ) -> Result<KafkaInboundChannel> {
        let consumer = create_basic_consumer(bootstrap_server.to_string(), service_id.to_string())?;
        let channel = KafkaInboundChannel { consumer };
        channel
            .consumer
            .subscribe(&topics.to_vec())
            .expect("Could not subscribe");
        Ok(channel)
    }
}

impl InboundChannel for KafkaInboundChannel {
    fn consume(&mut self) -> Option<Vec<u8>> {
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

impl<T: MessageConsumer> StreamKafkaInboundChannel<T> {
    pub fn new(
        service_id: &str,
        topics: &[&str],
        bootstrap_server: &str,
        message_consumer: T,
    ) -> Result<StreamKafkaInboundChannel<T>> {
        let consumer =
            create_streaming_consumer(bootstrap_server.to_string(), service_id.to_string())?;
        let channel = StreamKafkaInboundChannel {
            consumer,
            message_consumer: Arc::new(message_consumer),
        };
        channel.consumer.subscribe(&topics.to_vec())?;
        Ok(channel)
    }

    pub async fn consume_async_blocking(&self) {
        let consumer = self.message_consumer.clone();
        self.consumer
            .stream()
            .try_for_each(|borrowed_message| {
                let consumer = consumer.clone();
                async move {
                    if let Some(message) = borrowed_message.payload() {
                        let result = consumer.consume(message);
                        if let Err(e) = result {
                            // TODO this skips over erroneous messages, we might want to crash loop until 
                            // they are successfully consumed to preserve data integrity
                            eprintln!("{:?}", e);
                        }
                    }
                    Ok(())
                }
            })
            .await
            .expect("Stream processing failed")
    }
}
