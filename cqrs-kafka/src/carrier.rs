use std::sync::{Arc, Mutex};
use config::Config;
use tokio::sync::oneshot;
use tokio::sync::oneshot::{Receiver, Sender};
use cqrs_library::OutboundChannel;
use crate::{ClientCarrier, ServerCarrier, StreamInboundChannel, StreamInboundProcessingChannel};
use cqrs_library::outbound::TokioOutboundChannel;


pub struct KafkaServerCarrier {

}

pub struct TokioCarrier {
}

impl TokioCarrier {
    pub(crate) fn new() -> (TokioServerCarrier, TokioClientCarrier) {
        let (event_sender, event_receiver) : (Sender<Vec<u8>>, Receiver<Vec<u8>>) = oneshot::channel();

        let server = TokioServerCarrier {
            event_sender: Arc::new(Mutex::new(TokioOutboundChannel::new(event_sender))),
        };
        let client= TokioClientCarrier {
            event_receiver
        };
        return (server, client);
    }
}

pub struct TokioServerCarrier {
    event_sender: Arc<Mutex<TokioOutboundChannel>>
}

pub struct TokioClientCarrier {
    event_receiver: Receiver<Vec<u8>>
}

impl ClientCarrier for TokioClientCarrier {
    fn get_event_channel(&self) -> Arc<tokio::sync::Mutex<Option<Box<dyn StreamInboundChannel + Sync + Send>>>> {
        todo!()
    }

    fn get_response_channel(&self) -> Arc<tokio::sync::Mutex<Option<Box<dyn StreamInboundChannel + Sync + Send>>>> {
        todo!()
    }

    fn get_command_channel(&self) -> Arc<tokio::sync::Mutex<Option<Box<dyn OutboundChannel>>>> {
        todo!()
    }
}

impl ServerCarrier for KafkaServerCarrier {
    fn get_event_channel(&self) -> Arc<std::sync::Mutex<dyn OutboundChannel + Sync + Send>> {
        todo!()
    }

    fn get_command_channel(&self, _settings: Config) -> Box<dyn StreamInboundProcessingChannel + Sync + Send> {
        todo!()
    }

    fn get_response_channel(&self, _settings: Config) -> Box<dyn OutboundChannel + Sync + Send> {
        todo!()
    }
}


impl ServerCarrier for TokioServerCarrier {

    fn get_event_channel(&self) -> Arc<Mutex<dyn OutboundChannel + Sync + Send>> {
        return self.event_sender.clone();
    }

    fn get_command_channel(&self, _settings: Config) -> Box<dyn StreamInboundProcessingChannel + Sync + Send> {
        todo!()
    }

    fn get_response_channel(&self, _settings: Config) -> Box<dyn OutboundChannel + Sync + Send> {
        todo!()
    }
}
