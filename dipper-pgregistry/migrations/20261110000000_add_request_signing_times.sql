-- The newest signing time dipper has accepted an admin request with, for each requester,
-- deployment and chain. A request signed at or before the time stored here is refused, so a
-- request that someone recorded can't be sent again to undo a later one.
CREATE TABLE dipper_reg_request_signing_times
(
    requested_by        BYTEA  NOT NULL,
    deployment_id       TEXT   NOT NULL,
    deployment_chain_id BYTEA  NOT NULL,
    -- Seconds since the Unix epoch.
    newest_signed_at    BIGINT NOT NULL,
    PRIMARY KEY (requested_by, deployment_id, deployment_chain_id)
);
