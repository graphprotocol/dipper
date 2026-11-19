-- Kafka consumer offsets
--
-- The dipper consumes subgraph indexing request events that Studio produces on
-- a Redpanda topic. The Kafka client in use (rskafka) has no consumer groups,
-- so the broker cannot store consumer progress; the dipper records it here
-- instead, after each record is processed, and resumes from it on restart.

CREATE TABLE IF NOT EXISTS dipper_kafka_consumer_offsets (
    -- Topic name; the topic is deploy-time config, so it is part of the key.
    topic TEXT NOT NULL,
    -- Kafka partition id within the topic
    partition_id INT NOT NULL,
    -- The next offset to fetch: 1 past the last fully processed record
    next_offset BIGINT NOT NULL,
    -- Timestamps for auditing
    created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
    PRIMARY KEY (topic, partition_id)
);

COMMENT ON TABLE dipper_kafka_consumer_offsets IS 'Per-partition consumer progress for Kafka topics the dipper consumes; written after processing so delivery is at-least-once';
COMMENT ON COLUMN dipper_kafka_consumer_offsets.next_offset IS 'The next offset to fetch: 1 past the last fully processed record';
