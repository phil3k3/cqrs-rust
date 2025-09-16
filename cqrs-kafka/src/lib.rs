pub mod error;
pub mod inbound;
mod operations;
pub mod outbound;
mod prelude;

#[cfg(test)]
mod tests {
    use crate::inbound::KafkaInboundChannel;
    use crate::outbound::KafkaOutboundChannel;
    use cqrs_library::cqrs::traits::{InboundChannel, OutboundChannel};
    use log::info;
    use std::sync::mpsc::channel;
    use std::thread;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::kafka::{Kafka, KAFKA_PORT};

    #[tokio::test]
    async fn test_sync_send_receive() {
        env_logger::init();

        info!("Starting transmission test via Kafka");

        let kafka_node = Kafka::default().start().await.unwrap();
        let host_port = kafka_node.get_host_port_ipv4(KAFKA_PORT).await.unwrap();

        info!("Started Kafka container at port {}", host_port);

        let bootstrap_servers = format!("127.0.0.1:{}", host_port);

        info!("{}", bootstrap_servers);
        let outbound_channel = KafkaOutboundChannel::new("TEST", &bootstrap_servers).unwrap();

        outbound_channel.create_topic("TEST").await;

        let mut inbound_channel =
            KafkaInboundChannel::new("TEST_IN", &["TEST"], &bootstrap_servers, true).unwrap();

        inbound_channel.consume();

        let sender = thread::spawn(move || {
            outbound_channel.send("KEY".as_bytes().to_vec(), "MESSAGE".as_bytes().to_vec());
        });

        info!("Waiting for sender");
        sender.join().expect("The sender thread has panicked");
        info!("Message sent");

        let receiver = thread::spawn(move || loop {
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
