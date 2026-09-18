//! Kafka clients for dipper event streaming: a producer for the agreement
//! lifecycle events the dipper emits and a consumer for the indexing request
//! events Studio emits. Events are Protocol Buffers encoded.

mod connection;
mod consumer;
mod producer;

pub use connection::ConnectionError;
pub use consumer::{ConsumerError, KafkaConsumer, KafkaConsumerConfig};
pub use producer::{Error, KafkaConfig, KafkaProducer};
