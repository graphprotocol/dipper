-- replacement_pending: the agreement's indexer stopped serving it and dipper has yet to queue a
-- reassessment to replace it, which waits until the agreement can no longer be paid.
ALTER TABLE dipper_reg_indexing_agreements
    ADD COLUMN replacement_pending BOOLEAN NOT NULL DEFAULT false;

CREATE INDEX idx_indexing_agreements_replacement_pending
    ON dipper_reg_indexing_agreements (updated_at)
    WHERE replacement_pending;
