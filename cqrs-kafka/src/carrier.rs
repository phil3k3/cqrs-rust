use std::sync::{Arc, Mutex};
use config::Config;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::{Receiver, Sender};
use cqrs_library::locks::TokioThreadSafeDataManager;
use cqrs_library::{OutboundChannel, StreamInboundProcessingChannel};
use crate::{ClientCarrier, QueryCarrier, ServerCarrier};
use cqrs_library::outbound::TokioOutboundChannel;
use crate::inbound::{StreamProcessingTokioChannel, StreamTokioChannel};

pub struct TokioCarrier {
}

impl TokioCarrier {
    pub(crate) fn new() -> (TokioServerCarrier, TokioClientCarrier) {
        let (event_sender, event_receiver) : (UnboundedSender<Vec<u8>>, UnboundedReceiver<Vec<u8>>) = mpsc::unbounded_channel();
        let (command_sender, command_receiver) : (Sender<Vec<u8>>, Receiver<Vec<u8>>) = tokio::sync::oneshot::channel();
        let (command_response_sender, command_response_receiver) : (UnboundedSender<Vec<u8>>, UnboundedReceiver<Vec<u8>>) = mpsc::unbounded_channel();

        let stream_processing_tokio_channel = StreamProcessingTokioChannel {
            receiver: TokioThreadSafeDataManager::new(command_receiver),
        };
        let server = TokioServerCarrier {
            event_sender: Arc::new(Mutex::new(TokioOutboundChannel::new(event_sender))),
            command_receiver: Arc::new(Mutex::new(stream_processing_tokio_channel)),
            response_channel: Arc::new(Mutex::new(TokioOutboundChannel::new(command_response_sender)))
        };
        let channel1 = StreamTokioChannel {
            receiver: TokioThreadSafeDataManager::new(event_receiver),
        };
        let client = TokioClientCarrier {
            event_receiver: TokioThreadSafeDataManager::new(channel1)
        };
        return (server, client);
    }
}

pub struct TokioServerCarrier {
    event_sender: Arc<Mutex<TokioOutboundChannel>>,
    command_receiver: Arc<Mutex<StreamProcessingTokioChannel>>,
    response_channel: Arc<Mutex<TokioOutboundChannel>>
}

pub struct TokioClientCarrier {
    event_receiver: TokioThreadSafeDataManager<StreamTokioChannel>
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
    fn get_event_channel(&self) -> TokioThreadSafeDataManager<StreamTokioChannel> {
        return self.event_receiver.clone();
    }
}

impl ServerCarrier for TokioServerCarrier {

    fn get_event_channel(&self) -> Arc<Mutex<Box<dyn OutboundChannel + Sync + Send>>> {
        let event_sender = self.event_sender.clone();
        // Convert the Arc<Mutex<TokioOutboundChannel>> to Arc<Mutex<Box<dyn OutboundChannel + Sync + Send>>>
        // Lock the mutex and clone the inner value
        let result = event_sender.lock();
        let guard = result.unwrap();
        let cloned_event_sender = guard.clone();

        // Convert the cloned value into a Box<dyn OutboundChannel + Sync + Send>
        let boxed: Box<dyn OutboundChannel + Sync + Send> = Box::new(cloned_event_sender);

        // Wrap the boxed trait object in Arc<Mutex<_>> to match the return type
        let converted: Arc<Mutex<Box<dyn OutboundChannel + Sync + Send>>> = Arc::new(Mutex::new(boxed));

        converted
    }

    fn get_command_channel(&self, _settings: Config) -> Box<dyn StreamInboundProcessingChannel + Sync + Send> {
        let arc = self.command_receiver.clone();
        let result = arc.lock();
        let guard = result.unwrap();
        let cloned_channel = guard.clone();
        let boxed: Box<dyn StreamInboundProcessingChannel + Sync + Send> = Box::new(cloned_channel);

        boxed
    }

    fn get_response_channel(&self, _settings: Config) ->  Arc<Mutex<Box<dyn OutboundChannel + Sync + Send>>> {
        let event_sender = self.response_channel.clone();
        // Convert the Arc<Mutex<TokioOutboundChannel>> to Arc<Mutex<Box<dyn OutboundChannel + Sync + Send>>>
        // Lock the mutex and clone the inner value
        let result = event_sender.lock();
        let guard = result.unwrap();
        let cloned_event_sender = guard.clone();

        // Convert the cloned value into a Box<dyn OutboundChannel + Sync + Send>
        let boxed: Box<dyn OutboundChannel + Sync + Send> = Box::new(cloned_event_sender);

        // Wrap the boxed trait object in Arc<Mutex<_>> to match the return type
        let converted: Arc<Mutex<Box<dyn OutboundChannel + Sync + Send>>> = Arc::new(Mutex::new(boxed));

        converted
    }
}
