-- 0004: exclusive publication claims and provable destination ownership.
--
-- Three defects shared one cause: the pipeline decided from a job snapshot
-- loaded at delivery time, while the durable transitions that acted on those
-- decisions had no guards. Two attempts could therefore both believe they were
-- the publisher, and a stale attempt could overwrite a newer outcome.
--
--   * Overlapping attempts. A records intent and links the base name, then
--     pauses. B records intent too -- the old check allowed it -- links, gets
--     EEXIST, and consults ITS OWN snapshot, which predates A's intent. Seeing
--     no intent, B concludes A's document is foreign, blocks the name and
--     publishes a suffix. One submission, two documents.
--
--   * False ownership. A foreign file whose bytes happen to equal this job's
--     was adopted as this job's delivery, because intent plus matching content
--     was treated as proof. Equal bytes are not proof: the contract says
--     distinct submissions stay distinct even when their content matches.
--
--   * Contradiction. RecordUncertain had no state guard at all, so an attempt
--     that resumed after another had delivered overwrote `delivered` with
--     `uncertain`.
--
-- The claim below makes publication exclusive per job, and the recorded inode
-- makes ownership provable rather than inferred.

-- Who holds the right to publish this job right now. Taken in the same
-- statement that moves the job to `publishing`, so two attempts cannot both
-- hold it.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS publish_claimed_by text;

-- The identity of the file this job actually linked into place.
--
-- After a successful link the destination and the staged temporary are the
-- same inode, so recording the staged inode at claim time gives recovery
-- something it can check. A foreign file with identical bytes has a different
-- inode, which is exactly the distinction content comparison cannot make.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS publish_inode  bigint;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS publish_device bigint;

CREATE INDEX IF NOT EXISTS jobs_publish_claim_idx
    ON jobs (publish_claimed_by) WHERE state = 'publishing';

-- Historical rows have no recorded destination root, and the runtime check
-- skipped NULL as "compatible with anything" -- which let work accepted before
-- the column existed follow a newly configured destination. Ambiguity is now
-- explicit: rows that predate the column are marked as such, so the renamer
-- can hold them instead of silently adopting the current configuration.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS destination_root_unknown boolean NOT NULL DEFAULT false;

UPDATE jobs
   SET destination_root_unknown = true
 WHERE destination_root IS NULL
   AND state NOT IN ('delivered', 'held', 'uncertain');
