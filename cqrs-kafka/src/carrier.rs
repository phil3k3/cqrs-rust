use std::sync::{Arc, Mutex};
use config::Config;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use cqrs_library::locks::TokioThreadSafeDataManager;
use cqrs_library::{OutboundChannel, StreamInboundProcessingChannel};
use crate::{ClientCarrier, QueryCarrier, ServerCarrier};
use cqrs_library::outbound::TokioOutboundChannel;
use crate::inbound::StreamTokioChannel;

pub struct TokioCarrier {
}

impl TokioCarrier {
    pub(crate) fn new() -> (TokioServerCarrier, TokioClientCarrier) {
        let (event_sender, event_receiver) : (UnboundedSender<Vec<u8>>, UnboundedReceiver<Vec<u8>>) = mpsc::unbounded_channel();

        let server = TokioServerCarrier {
            event_sender: Arc::new(Mutex::new(TokioOutboundChannel::new(event_sender))),
        };
        let channel1 = StreamTokioChannel {
            receiver: TokioThreadSafeDataManager::new(event_receiver),
        };
        let client = TokioClientCarrier {
            event_receiver: Arc::new(tokio::sync::Mutex::new(Box::new(channel1)))
        };
        return (server, client);
    }
}

pub struct TokioServerCarrier {
    event_sender: Arc<Mutex<TokioOutboundChannel>>
}

pub struct TokioClientCarrier {
    event_receiver: Arc<tokio::sync::Mutex<Box<StreamTokioChannel>>>
}

impl ClientCarrier<StreamTokioChannel, TokioOutboundChannel> for TokioClientCarrier {


    fn get_response_channel(&self) -> TokioThreadSafeDataManager<Box<StreamTokioChannel>> {
        todo!()
    }

    fn get_command_channel(&self) -> Arc<tokio::sync::Mutex<Option<Box<TokioOutboundChannel>>>> {
        todo!()
    }
}

impl QueryCarrier<StreamTokioChannel> for TokioClientCarrier {
    fn get_event_channel(&self) -> Arc<tokio::sync::Mutex<Box<StreamTokioChannel>>> {
        return self.event_receiver.clone();
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
