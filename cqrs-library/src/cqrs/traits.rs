use crate::cqrs::messages::CommandResponseResult;
use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[typetag::serde(tag = "type")]
pub trait Event: Debug {
    fn get_id(&self) -> String;

    fn get_type(&self) -> String;

    fn get_version(&self) -> i32 {
        1
    }
}

pub trait EventProducer {
    fn produce(&self, event: &dyn Event) -> Result<()>;
}

pub trait OutboundChannel: Send + Sync {
    fn send(&self, key: Vec<u8>, message: Vec<u8>);
}

pub trait InboundChannel {
    fn consume(&mut self) -> Option<Vec<u8>>;
}

pub trait MessageConsumer {
    fn consume(&self, message: &[u8]) -> Result<()>;
}

pub trait Command<'de>: Deserialize<'de> + Serialize {
    fn get_subject(&self) -> String;
    fn get_type(&self) -> String;
    fn get_version(&self) -> i32 {
        1
    }
}
