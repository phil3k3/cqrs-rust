use std::sync::{Arc, Mutex};
use config::Config;
use tokio::sync::oneshot;
use tokio::sync::oneshot::{Receiver, Sender};
use cqrs_library::OutboundChannel;
use crate::{ServerCarrier, StreamInboundChannel};
use cqrs_library::outbound::TokioOutboundChannel;


pub struct KafkaServerCarrier {

}

pub struct TokioCarrier {
   event_sender: Arc<Mutex<TokioOutboundChannel>>,
   event_receiver: Receiver<Vec<u8>>
}

impl Default for TokioCarrier {
    fn default() -> Self {
        let (event_sender, event_receiver) : (Sender<Vec<u8>>, Receiver<Vec<u8>>) = oneshot::channel();

        TokioCarrier {
            event_sender: Arc::new(Mutex::new(TokioOutboundChannel::new(event_sender))),
            event_receiver
        }
    }
}

impl TokioCarrier {

}

impl ServerCarrier for KafkaServerCarrier {
    fn get_event_channel(&self) -> Arc<std::sync::Mutex<dyn OutboundChannel + Sync + Send>> {
        todo!()
    }

    fn get_command_channel(&self, _settings: Config) -> Box<dyn StreamInboundChannel + Sync + Send> {
        todo!()
    }

    fn get_response_channel(&self, _settings: Config) -> Box<dyn OutboundChannel + Sync + Send> {
        todo!()
    }
}


impl ServerCarrier for TokioCarrier {

    fn get_event_channel(&self) -> Arc<Mutex<dyn OutboundChannel + Sync + Send>> {
        return self.event_sender.clone();
    }

    fn get_command_channel(&self, _settings: Config) -> Box<dyn StreamInboundChannel + Sync + Send> {
        todo!()
    }

    fn get_response_channel(&self, _settings: Config) -> Box<dyn OutboundChannel + Sync + Send> {
        todo!()
    }
}
