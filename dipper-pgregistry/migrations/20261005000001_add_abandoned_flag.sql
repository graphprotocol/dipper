-- abandoned: dipper is cancelling the agreement because its indexer stopped serving it, so once
-- the chain confirms dipper's cancel it ends AbandonedByIndexer (8) rather than
-- CanceledByRequester (3).
ALTER TABLE dipper_reg_indexing_agreements
    ADD COLUMN abandoned BOOLEAN NOT NULL DEFAULT false;
