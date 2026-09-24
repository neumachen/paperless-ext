-- 0008: a delivered original can be moved out of the drop folder.
--
-- Additive: one new table. When the deployment asks for it, a delivered
-- original is moved from the incoming root into an archive directory inside it,
-- so the drop folder holds only what has not been handled yet. Nothing is
-- deleted: the archive is where the original goes, and a source that changed
-- after it was delivered is left where it is.

-- ---------------------------------------------------------------------------
-- Source archival.
-- ---------------------------------------------------------------------------
-- One row per job whose original is to be moved once it is delivered. The row
-- is written with the job, in the registration transaction, so whether a job's
-- original may be moved is decided when the job is accepted and not
-- reinterpreted later by whatever configuration is running.
--
--   pending   the job's original has not been moved yet
--   archived  moved into the archive directory; archived_name says where
--   absent    the name no longer holds this job's original, so nothing was moved
--   refused   the original is still there but changed after it was delivered;
--             it is left in place, because the version in the drop folder is
--             not the one that was delivered
CREATE TABLE IF NOT EXISTS source_archivals (
    job_id          uuid        PRIMARY KEY REFERENCES jobs (job_id) ON DELETE RESTRICT,
    state           text        NOT NULL DEFAULT 'pending',
    -- The directory, relative to the job's source root, the original goes to.
    archive_dir     text        NOT NULL,
    archived_name   text,
    category        text,
    attempts        integer     NOT NULL DEFAULT 0,
    requested_at    timestamptz NOT NULL DEFAULT now(),
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    settled_at      timestamptz,
    updated_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT source_archivals_state_check
        CHECK (state IN ('pending', 'archived', 'absent', 'refused')),
    CONSTRAINT source_archivals_dir_not_empty CHECK (length(archive_dir) > 0)
);

CREATE INDEX IF NOT EXISTS source_archivals_pending_idx
    ON source_archivals (next_attempt_at) WHERE state = 'pending';
