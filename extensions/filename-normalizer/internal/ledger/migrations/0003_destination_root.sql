-- 0003: the destination a job was accepted for.
--
-- Publication used to read the consume root from the renamer's current
-- configuration. A root-only restart could therefore redirect work that had
-- already been accepted -- and, for a job that already held a reservation,
-- publish it somewhere that reservation did not cover, so the exclusivity the
-- reservation provides would not apply to the directory actually written.
--
-- The accepted root is now recorded on the job. A renamer configured with a
-- different one holds the job as destination_mismatch instead of redirecting
-- it. Existing rows keep NULL, which reads as "no accepted root recorded" and
-- is treated as compatible with any configuration, so this migration does not
-- strand work registered before it ran.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS destination_root text;

CREATE INDEX IF NOT EXISTS jobs_destination_root_idx
    ON jobs (destination_root) WHERE destination_root IS NOT NULL;
