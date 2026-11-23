-- The writer fence. Dipper's flows assume a single process writes to this database; the fence is
-- what makes that true. 1 row names the process allowed to write, by a random token every
-- process picks at start and sets on each of its sessions as `dipper.writer`. A trigger on every
-- table refuses a write from any session whose token isn't that one, so a process that has been
-- replaced is refused however late it learns of it. Statements that passed the check hold a
-- shared advisory lock until their transaction ends, which lets a new writer wait for them.
--
-- Sessions marked `dipper.writer = operator` (manual work and migrations) pass the check, and
-- `SELECT dipper_fence_set(false)` turns the fence off until `dipper_fence_set(true)`.

CREATE TABLE dipper_writer
(
    id       BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),        -- 1 row
    token    UUID        NOT NULL,                               -- the current writer's token
    holder   TEXT        NOT NULL,                               -- its host name, for operators
    taken_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    enforced BOOLEAN     NOT NULL DEFAULT TRUE                   -- false lets every write through
);

-- Handover only: lets a starting process wait while the writer is healthy. Never used for safety.
CREATE TABLE dipper_writer_heartbeat
(
    id       BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),        -- 1 row
    token    UUID        NOT NULL,
    seen_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    released BOOLEAN     NOT NULL DEFAULT TRUE
);

-- No writer yet, and released, so the first process to start takes over at once.
INSERT INTO dipper_writer (token, holder) VALUES (gen_random_uuid(), 'nobody');
INSERT INTO dipper_writer_heartbeat (token) SELECT token FROM dipper_writer;

-- Taken by every write and every chain broadcast that passed the check, until its transaction
-- ends. The 2-integer form shares no key space with the 1-bigint advisory locks used elsewhere.
CREATE FUNCTION dipper_fence_lock_shared() RETURNS void
    LANGUAGE sql AS
$$ SELECT pg_advisory_xact_lock_shared(1684631664, 1) $$;

-- Refuses a write unless this session carries the current writer's token. The lock comes first
-- and the token is read after it, with a fresh statement, so every statement that passed the
-- check holds the lock, and a new writer's drain waits for exactly those.
CREATE FUNCTION dipper_fence() RETURNS trigger
    LANGUAGE plpgsql AS
$$
DECLARE
    who TEXT := current_setting('dipper.writer', TRUE);
BEGIN
    IF who = 'operator' THEN
        RETURN NULL;
    END IF;
    IF (SELECT NOT enforced FROM dipper_writer) THEN
        RETURN NULL;
    END IF;
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'dipper: its tables are written under READ COMMITTED only'
            USING ERRCODE = 'DP002';
    END IF;
    PERFORM dipper_fence_lock_shared();
    IF NOT EXISTS (SELECT 1 FROM dipper_writer WHERE token::TEXT = who) THEN
        RAISE EXCEPTION 'dipper: this session is not the current writer'
            USING ERRCODE = 'DP001',
                HINT = 'A newer dipper took over, or this session has no dipper.writer setting.';
    END IF;
    RETURN NULL;
END
$$;

-- The same check without the lock, for the job queue: a job's transaction stays open for the
-- whole job, and must not hold a new writer back. The job's writes to the other tables are fenced
-- with the lock, and a takeover ends the old writer's sessions, which puts its jobs back.
CREATE FUNCTION dipper_fence_nolock() RETURNS trigger
    LANGUAGE plpgsql AS
$$
DECLARE
    who TEXT := current_setting('dipper.writer', TRUE);
BEGIN
    IF who = 'operator' THEN
        RETURN NULL;
    END IF;
    IF (SELECT NOT enforced FROM dipper_writer) THEN
        RETURN NULL;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM dipper_writer WHERE token::TEXT = who) THEN
        RAISE EXCEPTION 'dipper: this session is not the current writer'
            USING ERRCODE = 'DP001',
                HINT = 'A newer dipper took over, or this session has no dipper.writer setting.';
    END IF;
    RETURN NULL;
END
$$;

-- The off switch: 1 row update, so it takes no table lock, and it stays as set across restarts.
CREATE FUNCTION dipper_fence_set(enforce BOOLEAN) RETURNS void
    LANGUAGE sql AS
$$ UPDATE dipper_writer SET enforced = enforce $$;

-- The tables in this schema without an enabled fence. Checked at every start, so a table a later
-- migration adds is fenced without anyone having to remember to.
CREATE FUNCTION dipper_fence_unfenced() RETURNS SETOF regclass
    LANGUAGE sql
    STABLE AS
$$
SELECT c.oid::regclass
FROM pg_class c
         JOIN pg_namespace s ON s.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p')
  AND s.nspname = current_schema()
  AND c.relname NOT IN ('dipper_writer', 'dipper_writer_heartbeat', '_sqlx_migrations')
  AND NOT EXISTS (SELECT 1
                  FROM pg_trigger g
                  WHERE g.tgrelid = c.oid
                    AND g.tgname = 'dipper_fence'
                    AND g.tgenabled IN ('O', 'A'))
$$;

-- Fences 1 table. Called for 1 table at a time, each in a transaction of its own with a lock
-- timeout, so a table busy with a long transaction can't make it hold locks on the others.
CREATE FUNCTION dipper_fence_attach(tbl regclass) RETURNS void
    LANGUAGE plpgsql AS
$$
BEGIN
    BEGIN
        EXECUTE format(
                'CREATE TRIGGER dipper_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON %s
                 FOR EACH STATEMENT EXECUTE FUNCTION %s()',
                tbl,
                CASE WHEN tbl = to_regclass('pgmq_queue') THEN 'dipper_fence_nolock' ELSE 'dipper_fence' END);
    EXCEPTION
        WHEN duplicate_object THEN NULL; -- another starting process got there first
    END;
    EXECUTE format('ALTER TABLE %s ENABLE TRIGGER dipper_fence', tbl);
END
$$;

-- Takes over as the writer if the current one has released, or has been silent for
-- `stale_after`. False means a healthy writer exists: wait, and ask again. From the moment this
-- commits every new statement of the old writer is refused; `dipper_writer_drain` waits for the
-- statements it had already started.
CREATE FUNCTION dipper_writer_take(new UUID, me TEXT, stale_after INTERVAL) RETURNS BOOLEAN
    LANGUAGE plpgsql AS
$$
DECLARE
    h dipper_writer_heartbeat;
BEGIN
    -- 1 starting process at a time, and the current writer's heartbeat waits here too.
    SELECT * INTO h FROM dipper_writer_heartbeat FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'dipper: the writer fence has no row' USING ERRCODE = 'DP003';
    END IF;
    IF h.token = new THEN
        RETURN TRUE; -- an earlier call of ours already took over
    END IF;
    IF NOT (h.released OR h.seen_at < clock_timestamp() - stale_after) THEN
        RETURN FALSE;
    END IF;
    UPDATE dipper_writer SET token = new, holder = me, taken_at = clock_timestamp();
    IF NOT FOUND THEN
        RAISE EXCEPTION 'dipper: the writer fence has no row' USING ERRCODE = 'DP003';
    END IF;
    UPDATE dipper_writer_heartbeat SET token = new, seen_at = clock_timestamp(), released = FALSE;
    RETURN TRUE;
END
$$;

-- Waits until every write and broadcast that passed the check before the takeover has ended.
-- Statements arriving later queue behind this request, then read the new token and are refused.
CREATE FUNCTION dipper_writer_drain() RETURNS void
    LANGUAGE sql AS
$$ SELECT pg_advisory_xact_lock(1684631664, 1) $$;

-- Ends every session named for another dipper process (`dipper/<its token>...`), which rolls back
-- what it had open and frees the jobs and session locks it held. It waits up to 1 second for each
-- to be gone, so what they held is free when it returns. It acts only for the current writer,
-- and holds the row a takeover takes first, so no process can take over while it runs: a session
-- it ends is never the current writer's.
CREATE FUNCTION dipper_writer_cut(mine UUID) RETURNS INT
    LANGUAGE plpgsql AS
$$
DECLARE
    s RECORD;
    n INT := 0;
BEGIN
    PERFORM 1 FROM dipper_writer_heartbeat WHERE token = mine FOR UPDATE;
    IF NOT FOUND THEN
        RETURN 0;
    END IF;
    FOR s IN SELECT pid
             FROM pg_stat_activity
             WHERE datname = current_database()
               AND pid <> pg_backend_pid()
               AND application_name LIKE 'dipper/%'
               AND application_name NOT LIKE 'dipper/' || mine || '%'
        LOOP
            BEGIN
                IF pg_terminate_backend(s.pid, 1000) THEN
                    n := n + 1;
                END IF;
            EXCEPTION
                WHEN insufficient_privilege THEN NULL; -- another role's session: not ours to end
            END;
        END LOOP;
    RETURN n;
END
$$;

-- The writer's heartbeat. False means another process has taken over: stop at once.
CREATE FUNCTION dipper_writer_beat(mine UUID) RETURNS BOOLEAN
    LANGUAGE plpgsql AS
$$
BEGIN
    UPDATE dipper_writer_heartbeat SET seen_at = clock_timestamp() WHERE token = mine;
    RETURN FOUND;
END
$$;

-- A clean shutdown's last step: lets the next process take over without waiting.
CREATE FUNCTION dipper_writer_release(mine UUID) RETURNS BOOLEAN
    LANGUAGE plpgsql AS
$$
BEGIN
    UPDATE dipper_writer_heartbeat SET released = TRUE WHERE token = mine;
    RETURN FOUND;
END
$$;

-- A chain broadcast's guard. Called at the start of the broadcast's transaction and again before
-- each attempt: it holds off a new writer's drain, and says whether this process is still the
-- writer. Under any isolation but READ COMMITTED each call would read the token as the first did,
-- and miss a takeover, so it refuses as the write fence does.
CREATE FUNCTION dipper_writer_guard(mine UUID) RETURNS BOOLEAN
    LANGUAGE plpgsql AS
$$
BEGIN
    IF (SELECT NOT enforced FROM dipper_writer) THEN
        RETURN TRUE;
    END IF;
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'dipper: a broadcast is checked under READ COMMITTED only'
            USING ERRCODE = 'DP002';
    END IF;
    PERFORM dipper_fence_lock_shared();
    RETURN EXISTS (SELECT 1 FROM dipper_writer WHERE token = mine);
END
$$;
