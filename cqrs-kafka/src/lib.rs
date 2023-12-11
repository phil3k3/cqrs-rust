mod inbound;
mod outbound;

use rdkafka::{ClientContext, Message};
use rdkafka::consumer::Consumer;
use rdkafka::consumer::ConsumerContext;
use rdkafka::producer::{Producer, ProducerContext};
use cqrs_library::{InboundChannel, OutboundChannel};

#[cfg(test)]
mod tests {
    use std::sync::mpsc::channel;
    use std::{thread};
    use std::thread::JoinHandle;
    use futures::SinkExt;
    use log::info;
    use testcontainers::{clients, images::kafka};
    use cqrs_library::{InboundChannel, OutboundChannel};
    use crate::inbound::KafkaInboundChannel;
    use crate::outbound::KafkaOutboundChannel;

    impl<TARGET:Send + 'static> Runtime<TARGET> {
        pub fn new<FUNCTION: FnOnce() -> TARGET + Send + 'static>(f: FUNCTION) -> JoinHandle<TARGET>  {
            thread::spawn(f)
        }
    }


    pub struct Runtime<TARGET> {
        handle: JoinHandle<TARGET>
    }



    #[tokio::test]
    async fn test_sync_send_receive() {

        env_logger::init();

        info!("Starting transmission test via Kafka");

        let docker = clients::Cli::default();
        let kafka_node = docker.run(kafka::Kafka::default());

        info!("Started Kafka container");

        let bootstrap_servers = format!(
            "127.0.0.1:{}",
            kafka_node.get_host_port_ipv4(kafka::KAFKA_PORT)
        );

        info!("{}", bootstrap_servers);
        let mut outbound_channel = KafkaOutboundChannel::new("TEST_OUT", "TEST", &bootstrap_servers);

        outbound_channel.create_topic("TEST").await;

        let mut inbound_channel = KafkaInboundChannel::new("TEST_IN", &["TEST"], &bootstrap_servers);

        inbound_channel.consume();

        let sender = thread::spawn(move || {
            outbound_channel.send("KEY".as_bytes().to_vec(), "MESSAGE".as_bytes().to_vec());
        });

        info!("Waiting for sender");
        sender.join().expect("The sender thread has panicked");
        info!("Message sent");

        let receiver = Runtime::new(move || {
            loop {
                info!("Waiting for message");
                let message = inbound_channel.consume();
                match message {
                    Some(content) => {
                        let string = String::from_utf8(content).unwrap();
                        info!("Received message {}", &string);
                        assert_eq!("MESSAGE", string);
                        break;
                    }
                    _ => {}
                }
            }
        });

        info!("Waiting for receiver");
        receiver.join().expect("The receiver thread has panicked");
    }

    #[test]
    fn test_channel() {

        info!("Starting test via Kafka");

        let (tx, rx) = channel();

        let sender = thread::spawn(move || {
            tx.send("Hello 2, thread".to_owned())
                .expect("Unable to send on channel");
        });

        let receiver = thread::spawn(move || {
            let value = rx.recv().expect("Unable to receive from channel");
            info!("{}", value);
        });

        sender.join().expect("The sender thread has panicked");
        receiver.join().expect("The receiver thread has panicked");
    }
}
