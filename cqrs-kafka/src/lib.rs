use std::fmt::Display;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use blockingqueue::BlockingQueue;
use rdkafka::{ClientConfig, ClientContext, Message, TopicPartitionList};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::{BorrowedMessage, DeliveryResult};
use rdkafka::producer::{BaseRecord, Producer, ProducerContext, ThreadedProducer};
use cqrs_library::{InboundChannel, OutboundChannel};
use log::{info};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};


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
    producer: ThreadedProducer<ProducerCallbackLogger>,
    admin_client: AdminClient<ProducerCallbackLogger>
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


fn create_admin_client(bootstrap_server: String) -> Result<AdminClient<ProducerCallbackLogger>, KafkaError> {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", bootstrap_server);
    config.set_log_level(RDKafkaLogLevel::Debug);
    config.create_with_context(ProducerCallbackLogger {})
}


impl KafkaOutboundChannel {
    pub fn new(service_id: &str, topic: &str, bootstrap_server: &str) -> KafkaOutboundChannel {
        KafkaOutboundChannel {
            topic: topic.to_owned(),
            producer: create_producer(bootstrap_server.to_string(), service_id).unwrap(),
            admin_client: create_admin_client(bootstrap_server.to_string()).unwrap()
        }
    }

    pub async fn create_topic(&self, topic: &str) {
        self.admin_client
            .create_topics(
                &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
                &AdminOptions::new(),
            )
            .await
            .unwrap();
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
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest");

   // all nodes of the same service are in a group and will get some partitions assigned
    config.set_log_level(RDKafkaLogLevel::Debug);
    config.create_with_context(CustomContext {})
}

impl InboundChannel for KafkaInboundChannel {
    fn consume(&mut self) -> Option<Vec<u8>> {
        return self.consumer.poll(Duration::from_secs(1)).and_then(|x| match x {
                Ok(t) => t.payload().and_then(|y| Some(y.to_vec())),
                Err(_v) => None
            }
        );
    }
}

struct Runtime<TARGET> {
    handle: JoinHandle<TARGET>
}

impl<TARGET:Send + 'static> Runtime<TARGET> {
    fn start<FUNCTION: FnOnce() -> TARGET + Send + 'static>(f: FUNCTION) -> JoinHandle<TARGET>  {
        thread::spawn(f)
    }
}

struct Queue {
    queue: Vec<i32>
}

impl Queue {

    pub fn new() -> Self {
        return Queue {
            queue: Vec::new()
        }
    }

    pub fn enqueue(&mut self, item: i32) {
        self.queue.push(item);
    }

    pub fn dequeue(&mut self) -> i32 {
        // block until item becomes available

        self.queue.remove(0)
    }
}


#[cfg(test)]
mod tests {
    use std::sync::mpsc::channel;
    use std::{io, thread};
    use std::io::Write;
    use std::thread::sleep;
    use std::time::Duration;
    use blockingqueue::BlockingQueue;
    use log::info;
    use rdkafka::admin::AdminClient;
    use testcontainers::{clients, images::kafka};
    use cqrs_library::{InboundChannel, OutboundChannel};
    use crate::{KafkaInboundChannel, KafkaOutboundChannel, Runtime};

    #[tokio::test]
    async fn test_sync_send_receive() {

        println!("Starting transmission test via Kafka");

        let docker = clients::Cli::default();
        let kafka_node = docker.run(kafka::Kafka::default());

        println!("Started Kafka container");

        let bootstrap_servers = format!(
            "127.0.0.1:{}",
            kafka_node.get_host_port_ipv4(kafka::KAFKA_PORT)
        );

        println!("{}", bootstrap_servers);
        let mut outbound_channel = KafkaOutboundChannel::new("TEST_OUT", "TEST", &bootstrap_servers);

        outbound_channel.create_topic("TEST").await;

        let mut inbound_channel = KafkaInboundChannel::new("TEST_IN", &["TEST"], &bootstrap_servers);

        inbound_channel.consume();

        let sender = thread::spawn(move || {
            outbound_channel.send("KEY".as_bytes().to_vec(), "MESSAGE".as_bytes().to_vec());
        });

        println!("Waiting for sender");
        sender.join().expect("The sender thread has panicked");
        println!("Message sent");

        let receiver = Runtime::start(move || {
            loop {
                println!("Waiting for message");
                let message = inbound_channel.consume();
                match message {
                    Some(content) => {
                        println!("Received message");
                        assert_eq!("MESSAGE", String::from_utf8(content).unwrap());
                        break;
                    }
                    _ => {}
                }
            }
        });

        println!("Waiting for receiver");
        sleep(Duration::from_secs(60));
    }

    #[test]
    fn test_channel() {

        println!("Starting test via Kafka");

        let (tx, rx) = channel();

        let sender = thread::spawn(move || {
            tx.send("Hello 2, thread".to_owned())
                .expect("Unable to send on channel");
        });

        let receiver = thread::spawn(move || {
            let value = rx.recv().expect("Unable to receive from channel");
            println!("{}", value);
        });

        sender.join().expect("The sender 2 thread has panicked");
        receiver.join().expect("The receiver thread has panicked");
    }
}