use cqrs_library::{InboundChannel, OutboundChannel};

pub struct KafkaClientCommandChannel {

}

pub struct KafkaServerCommandChannel {

}

pub struct KafkaServerCommandResponseChannel {

}

pub struct KafkaClientCommandResponseChannel {

}

impl KafkaClientCommandChannel {
    fn new(service_id: &str) -> KafkaClientCommandChannel {
        KafkaClientCommandChannel { command_handlers: HashMap::new(), service_id: String::from(service_id) }
    }
}

impl OutboundChannel for KafkaClientCommandChannel {
    fn send(&mut self, message: Vec<u8>) {
        todo!()
    }
}

impl InboundChannel for KafkaServerCommandChannel {
    fn consume(&mut self) -> Option<Vec<u8>> {
        todo!()
    }
}

impl OutboundChannel for KafkaServerCommandResponseChannel {
    fn send(&mut self, message: Vec<u8>) {
        todo!()
    }
}

impl InboundChannel for KafkaClientCommandResponseChannel {
    fn consume(&mut self) -> Option<Vec<u8>> {
        todo!()
    }
}