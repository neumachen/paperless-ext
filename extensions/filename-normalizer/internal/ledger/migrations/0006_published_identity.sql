-- 0006: a staged inode is not a published one, and a receipt names the file.
--
-- 0005 recorded every inode a job staged for publication so that ownership
-- became a question about the JOB rather than about whichever attempt happened
-- to be asking. It left two things open, and both of them can adopt somebody
-- else's file:
--
--   * A staged inode is recorded at CLAIM time, before the link, and stays
--     recorded after the temporary it describes has been removed unlinked.
--     Inodes are reused. A file that a different writer later creates at this
--     job's reserved name can therefore carry a device/inode pair this job
--     still claims, and the recovery path adopts it as its own. Only an inode
--     this job actually LINKED into the destination can authorize that, which
--     is what linked_at records.
--
--   * A receipt described a name, a size and a content hash, but not the file.
--     After the link, the destination IS a specific inode; the receipt is the
--     durable record of that publication and it could not say which file it
--     meant. An attempt resuming later therefore could not tell "the receipt
--     describes the file I just linked" from "the receipt describes a
--     different file, so mine is a duplicate" -- and removed the only copy of
--     a document whose delivery had just been recorded.

-- Only a linked inode authorizes adoption. NULL means "staged, never linked".
ALTER TABLE job_publication_inodes ADD COLUMN IF NOT EXISTS linked_at timestamptz;

CREATE INDEX IF NOT EXISTS job_publication_inodes_linked_idx
    ON job_publication_inodes (job_id) WHERE linked_at IS NOT NULL;

-- The identity of the file a receipt describes. NULL where it is genuinely
-- unknown: a delivery reconciled from a destination that had already been
-- taken by the consumer has a name and recorded content, but no file left to
-- identify, and inventing one would be worse than admitting it.
ALTER TABLE delivery_receipts ADD COLUMN IF NOT EXISTS published_device bigint;
ALTER TABLE delivery_receipts ADD COLUMN IF NOT EXISTS published_inode  bigint;

-- Backfill, conservatively and only where the evidence is unambiguous.
--
-- A job that already holds a receipt was published, so the inode its claim
-- recorded is the inode that was linked. Jobs with no receipt get nothing:
-- their rows stay NULL and their destinations will be treated as foreign until
-- an attempt links and records one, which is the safe direction to be wrong in.
UPDATE job_publication_inodes i
   SET linked_at = i.recorded_at
  FROM delivery_receipts r
 WHERE r.job_id = i.job_id
   AND i.linked_at IS NULL;

UPDATE delivery_receipts r
   SET published_device = j.publish_device,
       published_inode  = j.publish_inode
  FROM jobs j
 WHERE j.job_id = r.job_id
   AND r.published_inode IS NULL
   AND j.publish_inode IS NOT NULL
   AND j.publish_device IS NOT NULL
   AND EXISTS (SELECT 1 FROM job_publication_inodes i
                WHERE i.job_id = r.job_id
                  AND i.device = j.publish_device
                  AND i.inode  = j.publish_inode);
