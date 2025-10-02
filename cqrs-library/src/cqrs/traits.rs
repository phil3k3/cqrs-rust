use crate::cqrs::CommandServiceServer;
use crate::prelude::*;
use async_trait::async_trait;
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
    fn send(&self, key: &[u8], message: &[u8]);
}

pub trait EventSender {
    fn send_event(&self, key: &[u8], message: &[u8]) -> Result<()>;
}

#[async_trait]
pub trait Transport: Send + Sync + EventSender {
    type Transport: Transport;

    fn send_command_response(&self, key: &[u8], message: &[u8]) -> Result<()>;

    async fn consume_async_blocking<'a>(
        &self,
        command_service_server: CommandServiceServer<'a, Self::Transport>,
    ) -> Result<()>;
}

pub trait InboundChannel {
    fn consume(&self) -> Option<Vec<u8>>;
}

pub trait StreamInboundChannel {
    fn consume_async_blocking(&self) -> Result<()>;
}

#[async_trait]
pub trait MessageConsumer {
    async fn consume(&self, message: &[u8]) -> Result<()>;
}

pub trait Command<'de>: Deserialize<'de> + Serialize {
    fn get_subject(&self) -> String;
    fn get_type(&self) -> String;
    fn get_version(&self) -> i32 {
        1
    }
}
