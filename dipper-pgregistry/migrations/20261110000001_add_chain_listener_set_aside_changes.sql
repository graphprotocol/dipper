-- Table: dipper_chain_listener_set_aside_changes
-- Agreements whose on-chain change the chain listener kept failing to apply, so it moved its
-- cursor past them rather than hold up every later change. The listener reads each one's current
-- state again on every poll, and removes its row once the change applied or there was nothing
-- left to apply. There is no foreign key to the agreements table: the subgraph can name an
-- agreement this database has no row for, and that must not stop the change being recorded.
CREATE TABLE dipper_chain_listener_set_aside_changes
(
    agreement_id BYTEA PRIMARY KEY,
    -- The block of the latest change seen to the agreement. A state the subgraph reports from
    -- before it predates the change, so the row stays until the subgraph catches up.
    change_block BIGINT      NOT NULL CHECK (change_block >= 0),
    set_aside_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
);
