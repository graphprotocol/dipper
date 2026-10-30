-- While an indexer's DIPs info has been missing long enough, the watcher blocks it until this
-- time, renewing it each cycle, so the block lapses on its own if the watcher stops. The
-- denylist read includes blocked indexers, so IISA drops them and dipper replaces them.
ALTER TABLE dipper_indexer_dips_info_missing
    ADD COLUMN blocked_until TIMESTAMPTZ;
