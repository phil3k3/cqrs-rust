use std::sync::{Arc, Mutex};
use async_trait::async_trait;
use config::Config;
use cqrs_library::{MessageConsumer, MessageProcessor, OutboundChannel};

pub mod inbound;
pub mod outbound;
mod aggregate;
mod command;
mod event;
mod carrier;

#[async_trait]
pub trait StreamInboundProcessingChannel {
    async fn consume_async_blocking(&mut self, message_consumer: Arc<Mutex<Box<dyn MessageProcessor + Send>>>, response_channel: Arc<Mutex<Box<dyn OutboundChannel + Send + Sync>>>);
}

#[async_trait]
pub trait StreamInboundChannel {
    async fn consume_async_blocking(&mut self, message_consumer: Arc<tokio::sync::Mutex<Box<dyn MessageConsumer + Send>>>);
}

pub trait ServerCarrier {
    fn get_event_channel(&self) -> Arc<Mutex<dyn OutboundChannel + Sync + Send>>;

     fn get_command_channel(&self, settings: Config) -> Box<dyn StreamInboundProcessingChannel + Sync + Send>;

     fn get_response_channel(&self, settings: Config) -> Box<dyn OutboundChannel + Sync + Send>;
}

pub trait ClientCarrier {
    fn get_event_channel(&self) -> Arc<tokio::sync::Mutex<Option<Box<dyn StreamInboundChannel + Sync + Send>>>>;

    fn get_response_channel(&self) -> Arc<tokio::sync::Mutex<Option<Box<dyn StreamInboundChannel + Sync + Send>>>>;

    fn get_command_channel(&self) -> Arc<tokio::sync::Mutex<Option<Box<dyn OutboundChannel>>>>;
}


#[cfg(test)]
mod tests {
    use std::sync::mpsc::channel;
    use std::{thread};
    use std::thread::JoinHandle;
    use log::info;
    use testcontainers::clients;
    use testcontainers_modules::kafka::{Kafka, KAFKA_PORT};
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
        let kafka_node = docker.run(Kafka::default());

        info!("Started Kafka container");

        let bootstrap_servers = format!(
            "127.0.0.1:{}",
            kafka_node.get_host_port_ipv4(KAFKA_PORT)
        );

        info!("{}", bootstrap_servers);
        let mut outbound_channel = KafkaOutboundChannel::new(String::from("TEST"), bootstrap_servers.clone());

        outbound_channel.create_topic("TEST").await;

        let mut inbound_channel = KafkaInboundChannel::new(String::from("TEST_IN"), &["TEST"], bootstrap_servers.clone());

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
