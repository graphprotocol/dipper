-- Cancelling (status = 9): dipper has decided to end the agreement and keeps sending the
-- on-chain cancel until the chain confirms it ended; only then does the row become
-- CanceledByRequester, which announces the end.
--
-- cancel_attempts counts cancels that reached the chain without ending the agreement,
-- so one that can never work stops being retried and is left for an operator.
ALTER TABLE dipper_reg_indexing_agreements
    ADD COLUMN cancel_attempts INTEGER NOT NULL DEFAULT 0;

-- Partial, so it only covers agreements still being cancelled (normally none).
CREATE INDEX idx_indexing_agreements_cancelling
    ON dipper_reg_indexing_agreements (updated_at)
    WHERE status = 9;
