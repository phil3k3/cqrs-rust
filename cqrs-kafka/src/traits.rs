use crate::prelude::*;
use rdkafka::consumer::ConsumerGroupMetadata;
use rdkafka::TopicPartitionList;

pub trait TransactionHandler {
    fn begin_transaction(&self) -> Result<()> {
        Ok(())
    }

    fn commit_transaction(
        &self,
        _topic_partition_list: &TopicPartitionList,
        _consumer_group_metadata: &ConsumerGroupMetadata,
    ) -> Result<()> {
        Ok(())
    }
}

pub struct NoopTransactionHandler {}

impl TransactionHandler for NoopTransactionHandler {}

impl Default for NoopTransactionHandler {
    fn default() -> Self {
        NoopTransactionHandler {}
    }
}
