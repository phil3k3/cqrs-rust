use tokio::sync::oneshot::Sender;
use crate::OutboundChannel;

pub struct TokioOutboundChannel {
    pub(crate) sender: Option<Sender<Vec<u8>>>
}

impl OutboundChannel for TokioOutboundChannel {
    fn send(&mut self, _key: Vec<u8>, message: Vec<u8>) {
        if let Some(sender) = self.sender.take() {
            sender.send(message).expect("Message sending failed");
        }
    }
}

impl TokioOutboundChannel {
    pub fn new(sender: Sender<Vec<u8>>) -> Self {
        return TokioOutboundChannel {
            sender: Some(sender)
        }
    }
}
