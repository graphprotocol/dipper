//! Kafka client for subgraph indexing agreement event streaming.
//!
//! This module provides a Kafka producer for emitting subgraph indexing agreement lifecycle events
//! to a kafka topic
//!
//! Events are encoded using Protocol Buffers for compact, schema-enforced messages.
//!
//! # Example
//!
//! ```ignore
//! use dipper_producer::kafka::{KafkaConfig, KafkaProducer};
//!
//! let config = KafkaConfig {
//!     brokers: vec!["localhost:9092".to_string()],
//!     topic: "dipper.subgraph.indexing.agreement.events".to_string(),
//!     partitions: 16,
//! };
//!
//! let producer = KafkaProducer::new(&config).await?;
//!
//! // Send an event with partition key and protobuf payload
//! producer.send("QmT329Bej8AwSLahmgnmi6fdYkj3rorYAcCes45gDv9aJ4", &encoded_event).await?;
//! ```

mod producer;
mod reconnect;

pub use producer::{Error, KafkaConfig, KafkaProducer};
pub use reconnect::Reconnecting;
