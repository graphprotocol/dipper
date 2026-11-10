-- Requests could store more indexers than the admin API now allows (100), or a negative count where
-- the API wrapped 2^31 or more into this 32-bit column. Bring them to the limit, then refuse
-- negative counts so every stored count reads back as a size.
UPDATE dipper_reg_indexing_requests
SET num_candidates = 100
WHERE num_candidates < 0 OR num_candidates > 100;

ALTER TABLE dipper_reg_indexing_requests
    ADD CONSTRAINT dipper_reg_indexing_requests_num_candidates_not_negative
    CHECK (num_candidates >= 0);
