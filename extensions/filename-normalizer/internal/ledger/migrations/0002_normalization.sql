-- 0002: destination reservations, delivery receipts, and discovery identity.
--
-- The foundation deliberately shipped without these tables. Normalization
-- exists now, so the three things it needs durably are added here:
--
--   1. A reservation that makes a destination name exclusive across concurrent
--      jobs, retries and restarts.
--   2. A receipt that records what was actually delivered.
--   3. Enough source identity on the job row to detect a submission that was
--      replaced or modified after discovery.
--
-- Nothing here is ever deleted. ON DELETE RESTRICT and the absence of any
-- cleanup path are deliberate: a reservation must outlive the file it names,
-- because Paperless removes a document from the consume directory once it has
-- ingested it, and a name that has already been handed over must never be
-- handed over again to a different submission.

-- ---------------------------------------------------------------------------
-- Source identity, for change detection between discovery and publication.
-- ---------------------------------------------------------------------------
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS source_inode      bigint;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS source_device     bigint;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS source_modified_at timestamptz;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS discovered_at     timestamptz;

-- The publication intent. Written immediately before the destination link is
-- attempted and cleared by the receipt, so a crash in between leaves evidence
-- that a publication may have happened. Without it, "never published" and
-- "published but the receipt was lost" would be indistinguishable, and the
-- contract requires that distinction to stay visible rather than be guessed.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS publish_attempted_at timestamptz;

-- ---------------------------------------------------------------------------
-- Destination reservations.
-- ---------------------------------------------------------------------------
-- reservation_key is the case-folded NFC form of the reserved name, so two
-- names that a case-insensitive or Unicode-folding filesystem would consider
-- the same cannot both be reserved. The primary key is what makes allocation
-- atomic: concurrent workers race on one INSERT and exactly one wins.
CREATE TABLE IF NOT EXISTS name_reservations (
    destination_root text        NOT NULL,
    reservation_key  text        NOT NULL,
    reserved_name    text        NOT NULL,
    job_id           uuid        NOT NULL REFERENCES jobs (job_id) ON DELETE RESTRICT,
    sequence         integer     NOT NULL,
    created_at       timestamptz NOT NULL DEFAULT now(),
    -- Set when the destination was found already occupied by a file this job
    -- did not publish. The reservation is kept rather than deleted so the
    -- conflict stays visible and the sequence does not silently rewind.
    blocked_at       timestamptz,
    PRIMARY KEY (destination_root, reservation_key),
    CONSTRAINT name_reservations_sequence_nonnegative CHECK (sequence >= 0),
    CONSTRAINT name_reservations_name_not_empty CHECK (length(reserved_name) > 0)
);

-- A job reserves at most one usable name per destination root. The partial
-- index excludes blocked reservations, which a job may accumulate while
-- walking the collision sequence.
CREATE UNIQUE INDEX IF NOT EXISTS name_reservations_job_active_idx
    ON name_reservations (job_id, destination_root)
    WHERE blocked_at IS NULL;

CREATE INDEX IF NOT EXISTS name_reservations_job_idx ON name_reservations (job_id);

-- ---------------------------------------------------------------------------
-- Delivery receipts.
-- ---------------------------------------------------------------------------
-- One receipt per job, holding what was delivered and the fingerprint of the
-- bytes that were delivered. A receipt is the only evidence that permits the
-- delivered state, and its absence is never read as "not delivered" -- only as
-- "not recorded as delivered".
CREATE TABLE IF NOT EXISTS delivery_receipts (
    job_id              uuid        PRIMARY KEY REFERENCES jobs (job_id) ON DELETE RESTRICT,
    destination_root    text        NOT NULL,
    delivered_name      text        NOT NULL,
    size_bytes          bigint      NOT NULL,
    content_fingerprint bytea       NOT NULL,
    attempt             integer     NOT NULL,
    delivered_at        timestamptz NOT NULL DEFAULT now(),
    -- Records that the delivered file was no longer present at a later
    -- observation. That is the expected consequence of Paperless consuming it,
    -- and it must never be read as a failed delivery.
    absent_observed_at  timestamptz,
    CONSTRAINT delivery_receipts_size_nonnegative CHECK (size_bytes >= 0)
);

CREATE INDEX IF NOT EXISTS delivery_receipts_delivered_idx ON delivery_receipts (delivered_at);

-- ---------------------------------------------------------------------------
-- New closed-set states.
-- ---------------------------------------------------------------------------
-- 'publishing' is the window the intent column describes. It is a distinct
-- state so a recovery scan can find jobs that were mid-publication without
-- inferring it from timestamps.
ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_state_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_state_check CHECK (state IN (
    'pending_dispatch', 'dispatching', 'dispatched',
    'processing', 'publishing', 'held', 'delivered', 'uncertain'
));

-- Discovery must not register the same submission twice. Identity is the
-- source root plus the name plus the inode and device: a producer that writes
-- a new document under a previously used name is a distinct submission and
-- gets a distinct job, which is why the name alone is not unique.
CREATE UNIQUE INDEX IF NOT EXISTS jobs_source_identity_idx
    ON jobs (source_root, source_name, source_inode, source_device)
    WHERE source_inode IS NOT NULL;

CREATE INDEX IF NOT EXISTS jobs_publishing_idx
    ON jobs (publish_attempted_at) WHERE state = 'publishing';
