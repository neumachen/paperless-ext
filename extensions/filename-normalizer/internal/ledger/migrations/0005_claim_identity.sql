-- 0005: claims identify an ATTEMPT, and ownership survives more than one of them.
--
-- 0004 made publication exclusive per job, which closed the case it was written
-- for. Two things it did not close:
--
--   * `publish_claimed_by` held the INSTANCE name, and the claim guard accepted
--     a request whose claimed_by equalled the current holder. Every handler in
--     one renamer shares that name, so two concurrent deliveries of the same
--     job inside a single process both satisfied the guard: the second simply
--     overwrote the first's recorded inode and both believed they held the
--     right to publish. "Exclusive per job" was really "exclusive per job per
--     instance".
--
--   * The claim recorded ONE inode -- whichever attempt wrote it last. An
--     earlier attempt that resumes (after a pause, a takeover, or a restart)
--     and finds the destination occupied then compares it against an inode
--     belonging to a different attempt OF THE SAME JOB, concludes the file is
--     foreign, and advances to a suffixed name. One submission, two documents.
--     Separate staging and consume filesystems make this the normal case
--     rather than the exotic one, because competing attempts then stage
--     different inodes by construction.
--
-- A claim token identifies the attempt rather than the process, and every
-- inode a job has ever staged for publication is remembered, so "is this file
-- mine?" is a question about the job and not about the attempt that happens to
-- be asking.

-- Identifies the ATTEMPT that holds the claim. publish_claimed_by stays as it
-- is, naming the instance for anyone reading the row; the guard uses this.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS publish_claim_token text;

-- Every inode this job has staged for publication, across all attempts.
--
-- After a successful link the destination IS one of these inodes. Ownership is
-- therefore decidable by the filesystem for any attempt, including one that
-- resumes long after the claim moved on. Content equality is still never
-- consulted: two distinct submissions may hold identical bytes and the
-- contract requires them to stay distinct.
CREATE TABLE IF NOT EXISTS job_publication_inodes (
    job_id      uuid        NOT NULL REFERENCES jobs (job_id) ON DELETE RESTRICT,
    device      bigint      NOT NULL,
    inode       bigint      NOT NULL,
    attempt     integer     NOT NULL,
    claimed_by  text        NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, device, inode)
);

CREATE INDEX IF NOT EXISTS job_publication_inodes_job_idx
    ON job_publication_inodes (job_id);

-- Carry forward what 0004 recorded, so a job that was mid-publication across
-- this migration keeps its provable ownership instead of silently losing it.
INSERT INTO job_publication_inodes (job_id, device, inode, attempt, claimed_by)
SELECT job_id, publish_device, publish_inode, 0, COALESCE(publish_claimed_by, 'pre-0005')
  FROM jobs
 WHERE publish_inode IS NOT NULL
   AND publish_device IS NOT NULL
ON CONFLICT DO NOTHING;
