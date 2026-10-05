-- ended_seen_at: when dipper's cancel retry first found a Cancelling agreement no longer live
-- on-chain. Where the retry has no cancel of its own to show for the end, the chain listener
-- gets an hour from then to record how it ended before the retry closes it out without that.
ALTER TABLE dipper_reg_indexing_agreements
    ADD COLUMN ended_seen_at TIMESTAMPTZ;
