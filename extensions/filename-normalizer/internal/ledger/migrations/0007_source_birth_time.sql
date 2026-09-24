-- 0007: a source's birth time is part of its identity.
--
-- No row is rewritten and none is removed. One column is added, and one unique
-- index is narrowed to the rows it can still speak for.
--
-- Discovery recognised an already-registered submission by root, name, device
-- and inode, and two things break that:
--
--   * A device number is not a property of a file. It names the mount the file
--     was reached through, and an SMB mount gets a fresh anonymous device every
--     time it is mounted. A drop folder on a NAS therefore presents every file
--     still sitting in it as a NEW submission after a remount, and each one
--     would be delivered a second time.
--
--   * An inode number is reused. ext4 hands a freed number to the next file
--     created in the same directory, so a new document dropped under a name
--     that was used before can arrive with the very identity the old
--     registration recorded -- and it was silently never registered.
--
-- The birth time answers both. It is recorded when the file is created,
-- survives a remount, and differs between a file and the one that later reuses
-- its number. It is not beyond a writer's reach -- over SMB a client may set it,
-- and a Mac copying a file sets it to the source's creation date, before the
-- file appears under its final name -- but writing to a file does not move it,
-- and paired with the inode it tells a reused number apart. Where it is
-- recorded it replaces the device in the identity; rows registered before this
-- migration, and filesystems that do not report one, keep the device.

-- ---------------------------------------------------------------------------
-- Source birth time.
-- ---------------------------------------------------------------------------
-- NULL means "not recorded": every row before this migration, and any
-- filesystem that does not report a birth time.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS source_birth_time timestamptz;

-- A submission whose birth time is known is identified by it instead of by
-- the device.
CREATE UNIQUE INDEX IF NOT EXISTS jobs_source_birth_identity_idx
    ON jobs (source_root, source_name, source_inode, source_birth_time)
    WHERE source_inode IS NOT NULL AND source_birth_time IS NOT NULL;

-- The device-based index keeps covering exactly the rows it can still speak
-- for. Left as it was, it would refuse the reused-name-and-inode submission
-- this migration exists to register: same root, same name, same device, same
-- inode -- a different file, told apart only by its birth time. Every existing
-- row has no birth time, so the narrowed index holds the same rows as before.
DROP INDEX IF EXISTS jobs_source_identity_idx;
CREATE UNIQUE INDEX IF NOT EXISTS jobs_source_identity_idx
    ON jobs (source_root, source_name, source_inode, source_device)
    WHERE source_inode IS NOT NULL AND source_birth_time IS NULL;
