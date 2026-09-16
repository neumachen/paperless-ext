-- 0001: durable job identity and append-only job history.
--
-- Scope note: this increment persists job identity, dispatch progress and
-- delivery outcomes. Name reservations and delivery receipts belong to the
-- normalization milestone and are deliberately absent rather than created as
-- unused tables.

CREATE TABLE IF NOT EXISTS jobs (
    job_id                uuid PRIMARY KEY,
    contract_version      integer     NOT NULL,
    state                 text        NOT NULL,
    -- Restricted columns. Original submission identity is retained for
    -- recovery and explanation, and is never emitted in ordinary logs or in
    -- metric labels.
    source_root           text        NOT NULL,
    source_name           text        NOT NULL,
    size_bytes            bigint,
    fingerprint_algorithm text,
    content_fingerprint   bytea,
    -- Naming policy outputs. Null until normalization exists.
    policy_version        text        NOT NULL,
    normalized_name       text,
    reserved_name         text,
    -- Progress accounting.
    dispatch_attempts     integer     NOT NULL DEFAULT 0,
    delivery_attempts     integer     NOT NULL DEFAULT 0,
    failure_category      text,
    claimed_by            text,
    claimed_at            timestamptz,
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now(),
    dispatched_at         timestamptz,
    last_delivery_at      timestamptz,
    terminal_at           timestamptz,
    CONSTRAINT jobs_state_check CHECK (state IN (
        'pending_dispatch', 'dispatching', 'dispatched',
        'processing', 'held', 'delivered', 'uncertain'
    )),
    CONSTRAINT jobs_source_name_not_empty CHECK (length(source_name) > 0),
    CONSTRAINT jobs_source_root_absolute CHECK (source_root LIKE '/%')
);

-- No unique constraint on (source_root, source_name): a reused filename may
-- represent a distinct submission, and each submission keeps its own identity.
CREATE INDEX IF NOT EXISTS jobs_source_idx ON jobs (source_root, source_name);

-- Partial indexes for the two worker scans in the watcher.
CREATE INDEX IF NOT EXISTS jobs_pending_dispatch_idx
    ON jobs (created_at) WHERE state = 'pending_dispatch';
CREATE INDEX IF NOT EXISTS jobs_dispatching_idx
    ON jobs (claimed_at) WHERE state = 'dispatching';

-- Append-only history. ON DELETE RESTRICT keeps a job's explanation intact:
-- nothing in this increment deletes ledger rows, and the constraint makes an
-- accidental future purge fail loudly instead of silently discarding evidence.
CREATE TABLE IF NOT EXISTS job_events (
    event_id    bigserial PRIMARY KEY,
    job_id      uuid        NOT NULL REFERENCES jobs (job_id) ON DELETE RESTRICT,
    occurred_at timestamptz NOT NULL DEFAULT now(),
    event_type  text        NOT NULL,
    from_state  text,
    to_state    text,
    category    text,
    actor       text        NOT NULL,
    attempt     integer,
    detail      jsonb       NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS job_events_job_idx ON job_events (job_id, event_id);
CREATE INDEX IF NOT EXISTS job_events_occurred_idx ON job_events (occurred_at);
