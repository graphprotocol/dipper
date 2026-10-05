-- ended_seen_at: when dipper's cancel retry first found a Cancelling agreement no longer live
-- on-chain without a cancel of its own to show for it. The chain listener gets an hour from
-- then to record how it ended before the retry closes it out without those details.
ALTER TABLE dipper_reg_indexing_agreements
    ADD COLUMN ended_seen_at TIMESTAMPTZ;
