-- Table: dipper_studio_set_aside_requests
-- Studio indexing requests whose subgraph manifest IPFS could not return when their Kafka record
-- was read. The consumer moves past such a record rather than hold up every later one, and keeps
-- the request here to apply once the manifest can be read. There is at most 1 row for each
-- requester, deployment and chain: a later record for the same 3 replaces or removes the row, so
-- an old request is never applied over a newer one.
CREATE TABLE dipper_studio_set_aside_requests
(
    requested_by             BYTEA       NOT NULL,
    deployment_id            TEXT        NOT NULL,
    deployment_chain_id      BYTEA       NOT NULL,
    -- A cancel (0) is never set aside: it is applied without reading the manifest.
    num_candidates           INT         NOT NULL CHECK (num_candidates > 0),
    -- Where the request's record sits in Kafka, and the id Studio gave its event.
    topic                    TEXT        NOT NULL,
    partition_id             INT         NOT NULL,
    record_offset            BIGINT      NOT NULL,
    event_id                 TEXT        NOT NULL,
    -- Why the manifest could not be read, as of the last attempt.
    reason                   TEXT        NOT NULL,
    set_aside_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- How many times the manifest was read again since, and when the next read is due.
    attempts                 INT         NOT NULL DEFAULT 0,
    next_attempt_at          TIMESTAMPTZ NOT NULL,
    -- Set once the manifest could be read; the reader of the row's partition then applies it.
    ready                    BOOLEAN     NOT NULL DEFAULT FALSE,
    -- When the alert for a request still waiting after 24 hours was raised, so it is raised once.
    still_waiting_alerted_at TIMESTAMPTZ,
    PRIMARY KEY (requested_by, deployment_id, deployment_chain_id)
);
