use std::sync::{Arc, Mutex};
use config::Config;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use cqrs_library::locks::{TokioArcMutexDataManager, TokioThreadSafeDataManager};
use cqrs_library::{OutboundChannel, StreamInboundProcessingChannel};
use crate::{ClientCarrier, QueryCarrier, ServerCarrier};
use cqrs_library::outbound::TokioOutboundChannel;
use crate::inbound::{StreamProcessingTokioChannel, StreamTokioChannel};

pub struct TokioCarrier {
}

impl TokioCarrier {
    pub(crate) fn new() -> (TokioServerCarrier, TokioClientCarrier) {
        let (event_sender, event_receiver) : (UnboundedSender<Vec<u8>>, UnboundedReceiver<Vec<u8>>) = mpsc::unbounded_channel();
        let (command_sender, command_receiver) : (UnboundedSender<Vec<u8>>, UnboundedReceiver<Vec<u8>>) = mpsc::unbounded_channel();
        let (command_response_sender, command_response_receiver) : (UnboundedSender<Vec<u8>>, UnboundedReceiver<Vec<u8>>) = mpsc::unbounded_channel();

        let command_receiver_channel = StreamProcessingTokioChannel {
            receiver: TokioThreadSafeDataManager::wrapped(command_receiver)
        };
        let command_response_channel = TokioOutboundChannel::new(command_response_sender);
        let server = TokioServerCarrier {
            event_sender: Arc::new(Mutex::new(TokioOutboundChannel::new(event_sender))),
            command_receiver: Arc::new(Mutex::new(command_receiver_channel)),
            response_channel: Arc::new(Mutex::new(command_response_channel))
        };
        let event_receiver_channel = StreamTokioChannel {
            receiver: TokioThreadSafeDataManager::wrapped(event_receiver)
        };
        let response_channel = StreamProcessingTokioChannel {
            receiver: TokioThreadSafeDataManager::wrapped(command_response_receiver)
        };
        let command_sender_channel = TokioOutboundChannel::new(command_sender);

        let client = TokioClientCarrier {
            event_receiver: TokioThreadSafeDataManager::wrapped(event_receiver_channel),
            response_channel: TokioArcMutexDataManager::wrapped(response_channel),
            command_channel: TokioThreadSafeDataManager::wrapped(command_sender_channel)
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
    event_receiver: TokioThreadSafeDataManager<StreamTokioChannel>,
    response_channel: TokioArcMutexDataManager<StreamProcessingTokioChannel>,
    command_channel: TokioThreadSafeDataManager<TokioOutboundChannel>
}


impl ClientCarrier<StreamProcessingTokioChannel, TokioOutboundChannel> for TokioClientCarrier {

    fn get_response_channel(&self) -> TokioArcMutexDataManager<StreamProcessingTokioChannel> {
        return self.response_channel.clone();
    }

    fn get_command_channel(&self) -> TokioThreadSafeDataManager<TokioOutboundChannel> {
        return self.command_channel.clone();
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
