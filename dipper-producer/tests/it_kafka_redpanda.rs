//! Kafka producer/consumer roundtrip tests against a real Redpanda broker.
//! Gated on `REDPANDA_BROKERS` (e.g. `localhost:9092`): unset, every test
//! skips with a note; CI starts a Redpanda container and always runs them.

use dipper_producer::kafka::{
    ConsumerError, KafkaConfig, KafkaConsumer, KafkaConsumerConfig, KafkaProducer, OffsetAt,
};

fn brokers() -> Option<Vec<String>> {
    match std::env::var("REDPANDA_BROKERS") {
        Ok(value) if !value.trim().is_empty() => Some(
            value
                .split(',')
                .map(|broker| broker.trim().to_string())
                .collect(),
        ),
        // REQUIRE_REDPANDA turns the silent skip into a failure, so CI cannot
        // go green while accidentally testing nothing.
        _ if std::env::var("REQUIRE_REDPANDA").is_ok() => {
            panic!("REQUIRE_REDPANDA is set but REDPANDA_BROKERS is not")
        }
        _ => {
            eprintln!("skipping Redpanda-backed test: REDPANDA_BROKERS is not set");
            None
        }
    }
}

/// A topic name unique to this test process and call site, so parallel tests
/// on a shared broker never read each other's records.
fn unique_topic(label: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before unix epoch")
        .as_nanos();
    format!("dipper.test.{label}.{}.{nanos}", std::process::id())
}

async fn create_topic(brokers: &[String], topic: &str, partitions: i32) {
    let client = rskafka::client::ClientBuilder::new(brokers.to_vec())
        .build()
        .await
        .expect("connect to broker");
    client
        .controller_client()
        .expect("controller client")
        .create_topic(topic, partitions, 1, 5_000)
        .await
        .expect("create topic");
}

fn producer_config(brokers: Vec<String>, topic: &str, partitions: u32) -> KafkaConfig {
    serde_json::from_value(serde_json::json!({
        "brokers": brokers,
        "topic": topic,
        "partitions": partitions,
    }))
    .expect("valid producer config")
}

fn consumer_config(brokers: Vec<String>, topic: &str) -> KafkaConsumerConfig {
    serde_json::from_value(serde_json::json!({
        "brokers": brokers,
        "topic": topic,
    }))
    .expect("valid consumer config")
}

#[tokio::test]
async fn produce_then_consume_roundtrip_across_partitions() {
    let Some(brokers) = brokers() else { return };
    let topic = unique_topic("roundtrip");
    create_topic(&brokers, &topic, 2).await;

    // The producer compresses batches with GZIP, like Studio's producer does,
    // so a successful roundtrip also proves fetch-side decompression.
    let producer = KafkaProducer::new(&producer_config(brokers.clone(), &topic, 2))
        .await
        .expect("producer connects");
    let payloads: Vec<(String, Vec<u8>)> = (0..5)
        .map(|i| (format!("key-{i}"), format!("payload-{i}").into_bytes()))
        .collect();
    for (key, payload) in &payloads {
        producer.send(key, payload).await.expect("produce");
    }

    let consumer = KafkaConsumer::connect(&consumer_config(brokers, &topic))
        .await
        .expect("consumer connects");
    assert_eq!(consumer.partitions(), vec![0, 1]);

    let mut consumed: Vec<Vec<u8>> = Vec::new();
    for partition in consumer.partitions() {
        let mut offset = consumer
            .offset(partition, OffsetAt::Earliest)
            .await
            .expect("earliest offset");
        let end = consumer
            .offset(partition, OffsetAt::Latest)
            .await
            .expect("latest offset");
        while offset < end {
            let (records, _) = consumer
                .fetch(partition, offset, 1_048_576, 500)
                .await
                .expect("fetch");
            assert!(!records.is_empty(), "records expected below the watermark");
            for record in records {
                consumed.push(record.record.value.expect("record value"));
                offset = record.offset + 1;
            }
        }
    }

    let mut expected: Vec<Vec<u8>> = payloads.into_iter().map(|(_, payload)| payload).collect();
    expected.sort();
    consumed.sort();
    assert_eq!(consumed, expected, "every produced payload comes back");
}

#[tokio::test]
async fn consumer_refuses_a_missing_topic() {
    let Some(brokers) = brokers() else { return };
    let topic = unique_topic("never-created");

    let Err(err) = KafkaConsumer::connect(&consumer_config(brokers, &topic)).await else {
        panic!("a missing topic must fail the connect");
    };
    assert!(
        matches!(err, ConsumerError::TopicNotFound { .. }),
        "expected TopicNotFound, got {err:?}"
    );
}

#[tokio::test]
async fn fetching_beyond_the_retained_range_is_detectable() {
    let Some(brokers) = brokers() else { return };
    let topic = unique_topic("out-of-range");
    create_topic(&brokers, &topic, 1).await;

    let producer = KafkaProducer::new(&producer_config(brokers.clone(), &topic, 1))
        .await
        .expect("producer connects");
    producer.send("key", b"payload").await.expect("produce");

    let consumer = KafkaConsumer::connect(&consumer_config(brokers, &topic))
        .await
        .expect("consumer connects");
    let err = consumer
        .fetch(0, 5_000, 1_048_576, 500)
        .await
        .expect_err("an offset far past the watermark must error");
    assert!(
        err.is_offset_out_of_range(),
        "expected an offset-out-of-range error, got {err:?}"
    );
}
