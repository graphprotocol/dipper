-- Fixture for testing batch queries across multiple indexers
-- Creates agreements for 3 different indexers with various statuses

-- Insert an indexing request
INSERT INTO dipper_reg_indexing_requests (id,
                                          created_at,
                                          updated_at,
                                          status,
                                          requested_by,
                                          deployment_id,
                                          deployment_chain_id,
                                          num_candidates)
VALUES ('01930100-0000-7000-8000-000000000001'::uuid,
        timezone('UTC', now()),
        timezone('UTC', now()),
        0, -- IndexingRequestStatus::Open
        '\x442a24985444cdc6a4db9503d354918d27b5ea97'::bytea,
        'QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv',
        '\x000000000000a4b1'::bytea,
        3);

-- Indexer A: 0x1111111111111111111111111111111111111111
-- Has 2 active agreements (1 Created, 1 AcceptedOnChain)
INSERT INTO dipper_reg_indexing_agreements (id, nonce_uuid, created_at, updated_at, status, indexing_request_id, deployment_id, indexer_id, indexer_url, terms)
VALUES ('\xaa000000000000000000000000000001'::bytea, '01930100-0001-7000-8000-000000000001'::uuid, timezone('UTC', now()), timezone('UTC', now()),
        -1, -- Created
        '01930100-0000-7000-8000-000000000001'::uuid,
        'QmAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA1a',
        '\x1111111111111111111111111111111111111111'::bytea,
        'https://indexer-a.com',
        '{"payer": "0x442a24985444cdc6a4db9503d354918d27b5ea97", "service_provider": "0x1111111111111111111111111111111111111111", "data_service": "0x442a24985444cdc6a4db9503d354918d27b5ea97", "deadline": 1700000300, "ends_at": 1700086400, "max_initial_tokens": "1000", "max_ongoing_tokens_per_second": "100", "min_seconds_per_collection": 86400, "max_seconds_per_collection": 864000, "metadata": {"tokens_per_second": "10", "tokens_per_entity_per_second": "1", "subgraph_deployment_id": "QmAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA1a", "protocol_network": 1, "chain_id": 1}}'::json);

INSERT INTO dipper_reg_indexing_agreements (id, nonce_uuid, created_at, updated_at, status, indexing_request_id, deployment_id, indexer_id, indexer_url, terms)
VALUES ('\xaa000000000000000000000000000002'::bytea, '01930100-0001-7000-8000-000000000002'::uuid, timezone('UTC', now()), timezone('UTC', now()),
        6, -- AcceptedOnChain
        '01930100-0000-7000-8000-000000000001'::uuid,
        'QmBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB2b',
        '\x1111111111111111111111111111111111111111'::bytea,
        'https://indexer-a.com',
        '{"payer": "0x442a24985444cdc6a4db9503d354918d27b5ea97", "service_provider": "0x1111111111111111111111111111111111111111", "data_service": "0x442a24985444cdc6a4db9503d354918d27b5ea97", "deadline": 1700000300, "ends_at": 1700086400, "max_initial_tokens": "1000", "max_ongoing_tokens_per_second": "100", "min_seconds_per_collection": 86400, "max_seconds_per_collection": 864000, "metadata": {"tokens_per_second": "10", "tokens_per_entity_per_second": "1", "subgraph_deployment_id": "QmBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB2b", "protocol_network": 1, "chain_id": 1}}'::json);

-- Indexer A also has 1 canceled-by-indexer agreement (should NOT be returned by active query)
INSERT INTO dipper_reg_indexing_agreements (id, nonce_uuid, created_at, updated_at, status, indexing_request_id, deployment_id, indexer_id, indexer_url, terms)
VALUES ('\xaa000000000000000000000000000003'::bytea, '01930100-0001-7000-8000-000000000003'::uuid, timezone('UTC', now()), timezone('UTC', now()),
        4, -- CanceledByIndexer
        '01930100-0000-7000-8000-000000000001'::uuid,
        'QmCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC3c',
        '\x1111111111111111111111111111111111111111'::bytea,
        'https://indexer-a.com',
        '{"payer": "0x442a24985444cdc6a4db9503d354918d27b5ea97", "service_provider": "0x1111111111111111111111111111111111111111", "data_service": "0x442a24985444cdc6a4db9503d354918d27b5ea97", "deadline": 1700000300, "ends_at": 1700086400, "max_initial_tokens": "1000", "max_ongoing_tokens_per_second": "100", "min_seconds_per_collection": 86400, "max_seconds_per_collection": 864000, "metadata": {"tokens_per_second": "10", "tokens_per_entity_per_second": "1", "subgraph_deployment_id": "QmCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC3c", "protocol_network": 1, "chain_id": 1}}'::json);

-- Indexer B: 0x2222222222222222222222222222222222222222
-- Has 1 active agreement (Created)
INSERT INTO dipper_reg_indexing_agreements (id, nonce_uuid, created_at, updated_at, status, indexing_request_id, deployment_id, indexer_id, indexer_url, terms)
VALUES ('\xbb000000000000000000000000000001'::bytea, '01930100-0002-7000-8000-000000000001'::uuid, timezone('UTC', now()), timezone('UTC', now()),
        -1, -- Created
        '01930100-0000-7000-8000-000000000001'::uuid,
        'QmDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD4d',
        '\x2222222222222222222222222222222222222222'::bytea,
        'https://indexer-b.com',
        '{"payer": "0x442a24985444cdc6a4db9503d354918d27b5ea97", "service_provider": "0x2222222222222222222222222222222222222222", "data_service": "0x442a24985444cdc6a4db9503d354918d27b5ea97", "deadline": 1700000300, "ends_at": 1700086400, "max_initial_tokens": "1000", "max_ongoing_tokens_per_second": "100", "min_seconds_per_collection": 86400, "max_seconds_per_collection": 864000, "metadata": {"tokens_per_second": "10", "tokens_per_entity_per_second": "1", "subgraph_deployment_id": "QmDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD4d", "protocol_network": 1, "chain_id": 1}}'::json);

-- Indexer C: 0x3333333333333333333333333333333333333333
-- Has NO active agreements (only expired). The expiry carries an offer transaction, so it is an
-- indexer who had an offer on chain and let the window close, which earns a missed-offer bench.
-- Leave the transaction set: without it the row reads as dipper failing to submit, and every test
-- that expects this row benched would silently move to the 5-minute dipper-side window instead.
INSERT INTO dipper_reg_indexing_agreements (id, nonce_uuid, created_at, updated_at, status, indexing_request_id, deployment_id, indexer_id, indexer_url, terms, offer_tx_hash)
VALUES ('\xcc000000000000000000000000000001'::bytea, '01930100-0003-7000-8000-000000000001'::uuid, timezone('UTC', now()), timezone('UTC', now()),
        5, -- Expired
        '01930100-0000-7000-8000-000000000001'::uuid,
        'QmEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE5e',
        '\x3333333333333333333333333333333333333333'::bytea,
        'https://indexer-c.com',
        '{"payer": "0x442a24985444cdc6a4db9503d354918d27b5ea97", "service_provider": "0x3333333333333333333333333333333333333333", "data_service": "0x442a24985444cdc6a4db9503d354918d27b5ea97", "deadline": 1700000300, "ends_at": 1700086400, "max_initial_tokens": "1000", "max_ongoing_tokens_per_second": "100", "min_seconds_per_collection": 86400, "max_seconds_per_collection": 864000, "metadata": {"tokens_per_second": "10", "tokens_per_entity_per_second": "1", "subgraph_deployment_id": "QmEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE5e", "protocol_network": 1, "chain_id": 1}}'::json,
        decode(repeat('cc', 32), 'hex'));
