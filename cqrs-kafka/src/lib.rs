use std::time::Duration;
use rdkafka::{ClientConfig, ClientContext, Message, TopicPartitionList};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::DeliveryResult;
use rdkafka::producer::{BaseRecord, ProducerContext, ThreadedProducer};
use cqrs_library::{InboundChannel, OutboundChannel};
use log::{info};


struct ProducerCallbackLogger;
impl ClientContext for ProducerCallbackLogger {

}

// A context can be used to change the behavior of producers and consumers by adding callbacks
// that will be executed by librdkafka.
// This particular context sets up custom callbacks to log rebalancing events.
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

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = BaseConsumer<CustomContext>;

pub struct KafkaOutboundChannel {
    topic: String,
    producer: ThreadedProducer<ProducerCallbackLogger>
}

pub struct KafkaInboundChannel {
    consumer: BaseConsumer<CustomContext>
}



impl ProducerContext for ProducerCallbackLogger {
    type DeliveryOpaque = ();

    fn delivery(&self, delivery_result: &DeliveryResult<'_>, _delivery_opaque: Self::DeliveryOpaque) {
        let unwrapped_result = delivery_result.as_ref();
        match unwrapped_result {
            Ok(msg) => {
                let key : &str = msg.key_view().unwrap().unwrap();
                println!("Produced message with key {} in offset {} and partition {}", key, msg.offset(), msg.partition());
            }
            Err(producer_error) => {
                let key : &str = producer_error.1.key_view().unwrap().unwrap();
                println!("Failed to produce message with key {}: {}", key, producer_error.0);
            }
        }
    }
}

fn create_producer(bootstrap_server: String, service_id: &str) -> Result<ThreadedProducer<ProducerCallbackLogger>, KafkaError> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", bootstrap_server)
        .set("message.timeout.ms", "5000");
    config.set_log_level(RDKafkaLogLevel::Debug);
    config.create_with_context(ProducerCallbackLogger {})
}


impl KafkaOutboundChannel {
    pub fn new(service_id: &str, topic: &str, bootstrap_server: &str) -> KafkaOutboundChannel {
        KafkaOutboundChannel {
            topic: topic.to_owned(),
            producer: create_producer(bootstrap_server.to_string(), service_id).unwrap()
        }
    }
}

impl OutboundChannel for KafkaOutboundChannel {
    fn send(&mut self, key: Vec<u8>, message: Vec<u8>) {
        self.producer
            .send(BaseRecord::to(self.topic.as_str())
                .key(&key)
                .payload(&message))
            .expect("Failed to send message");
    }
}

impl KafkaInboundChannel {
    pub fn new(service_id: &str,topics: &[&str], bootstrap_server: &str) -> KafkaInboundChannel {
       let channel = KafkaInboundChannel {
            consumer: create_consumer(bootstrap_server.to_string(), service_id.to_string()).unwrap()
        };
        channel.consumer.subscribe(&topics.to_vec()).expect("Could not subscribe");
        channel
    }
}

fn create_consumer(bootstrap_server: String, service_id: String) -> Result<LoggingConsumer, KafkaError> {
    let mut config = ClientConfig::new();
    config
        .set("group.id", format!("{}-consumer", &service_id))
        .set("bootstrap.servers", &bootstrap_server)
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest");

   // all nodes of the same service are in a group and will get some partitions assigned
    config.set_log_level(RDKafkaLogLevel::Debug);
    config.create_with_context(CustomContext {})
}

impl InboundChannel for KafkaInboundChannel {
    fn consume(&mut self) -> Option<Vec<u8>> {
        let message = self.consumer.poll(Duration::from_secs(30));
        return message.map(|x| x.expect("Could not read message").payload().unwrap().to_vec())
    }
}

struct Runtime {

}

#[cfg(test)]
mod tests {
    use testcontainers::{clients, images::kafka};
    use cqrs_library::{InboundChannel, OutboundChannel};
    use crate::{KafkaInboundChannel, KafkaOutboundChannel};

    #[test]
    fn test_sync_send_receive() {

        let docker = clients::Cli::default();
        let kafka_node = docker.run(kafka::Kafka::default());

        let bootstrap_servers = format!(
            "127.0.0.1:{}",
            kafka_node.get_host_port_ipv4(kafka::KAFKA_PORT)
        );

        let mut outbound_channel = KafkaOutboundChannel::new("TEST_OUT", "TEST", &bootstrap_servers);

        outbound_channel.send("KEY".as_bytes().to_vec(), "MESSAGE".as_bytes().to_vec());

        let mut inbound_channel = KafkaInboundChannel::new("TEST_IN", &["TEST"], &bootstrap_servers);

        let message = inbound_channel.consume().expect("Message not received");

        assert_eq!("MESSAGE", String::from_utf8(message).unwrap());
    }
}