use std::sync::{Arc, Mutex};
use config::Config;
use tokio::sync::oneshot;
use tokio::sync::oneshot::{Receiver, Sender};
use cqrs_library::locks::TokioThreadSafeDataManager;
use cqrs_library::{OutboundChannel, StreamInboundProcessingChannel};
use crate::{ClientCarrier, ServerCarrier};
use cqrs_library::outbound::TokioOutboundChannel;
use crate::inbound::StreamTokioChannel;

pub struct TokioCarrier {
}

impl TokioCarrier {
    pub(crate) fn new() -> (TokioServerCarrier, TokioClientCarrier) {
        let (event_sender, event_receiver) : (Sender<Vec<u8>>, Receiver<Vec<u8>>) = oneshot::channel();

        let server = TokioServerCarrier {
            event_sender: Arc::new(Mutex::new(TokioOutboundChannel::new(event_sender))),
        };
        let channel1 = StreamTokioChannel {
            receiver: TokioThreadSafeDataManager::new(event_receiver),
        };
        let client = TokioClientCarrier {
            event_receiver: TokioThreadSafeDataManager::new(Box::new(channel1))
        };
        return (server, client);
    }
}

pub struct TokioServerCarrier {
    event_sender: Arc<Mutex<TokioOutboundChannel>>
}

pub struct TokioClientCarrier {
    event_receiver: TokioThreadSafeDataManager<Box<StreamTokioChannel>>
}

impl ClientCarrier<StreamTokioChannel, TokioOutboundChannel> for TokioClientCarrier {
    fn get_event_channel(&self) -> TokioThreadSafeDataManager<Box<StreamTokioChannel>> {
       return self.event_receiver.clone();
    }

    fn get_response_channel(&self) -> TokioThreadSafeDataManager<Box<StreamTokioChannel>> {
        todo!()
    }

    fn get_command_channel(&self) -> Arc<tokio::sync::Mutex<Option<Box<TokioOutboundChannel>>>> {
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
