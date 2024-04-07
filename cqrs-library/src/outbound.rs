use tokio::sync::mpsc::UnboundedSender;
use crate::OutboundChannel;

pub struct TokioOutboundChannel {
    pub(crate) sender: Option<UnboundedSender<Vec<u8>>>
}

impl OutboundChannel for TokioOutboundChannel {
    fn send(&mut self, _key: Vec<u8>, message: Vec<u8>) {
        if let Some(sender) = self.sender.take() {
            sender.send(message).expect("Message sending failed");
        }
    }
}

impl TokioOutboundChannel {
    pub fn new(sender: UnboundedSender<Vec<u8>>) -> Self {
        return TokioOutboundChannel {
            sender: Some(sender)
        }
    }
}
