pub mod events;
pub mod kafka;
pub mod proto;

/// The counter of agreement-event sends to Kafka, by `outcome` (ok, unreachable, error).
pub const METRIC_KAFKA_SENDS: &str = "dipper_kafka_sends_total";

/// The counter of diagnostic agreement events dropped before sending, by `reason` (queue_full,
/// queue_closed).
pub const METRIC_KAFKA_EVENTS_DROPPED: &str = "dipper_kafka_events_dropped_total";
