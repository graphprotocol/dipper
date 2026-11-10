-- Table: dipper_indexer_dips_info_missing
-- How long each indexer holding an accepted agreement has gone without a readable /dips/info
-- answer, counted only over the checks dipper could judge. A readable answer removes the row,
-- so a row exists only while the indexer's DIPs info is missing.
CREATE TABLE dipper_indexer_dips_info_missing
(
    indexer_id   BYTEA PRIMARY KEY,
    missing_secs BIGINT      NOT NULL CHECK (missing_secs >= 0),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
);
