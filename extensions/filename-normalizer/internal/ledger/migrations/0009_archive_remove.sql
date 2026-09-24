-- 0009: a delivered original can be removed instead of moved.
--
-- Some deployments want exactly two folders: the one documents are dropped
-- into, and the one the consumer takes them from. Moving each delivered
-- original into a third folder is then clutter, not a feature. For those, the
-- original is removed from the drop folder once the delivered copy -- byte for
-- byte the same content, verified before it was published -- is in place.
--
-- Additive. One column records which of the two a job was accepted with, and
-- two checks are widened to admit it; no row is rewritten or removed, and every
-- existing row is a move, which is what the column defaults to.

ALTER TABLE source_archivals ADD COLUMN IF NOT EXISTS action text NOT NULL DEFAULT 'move';

ALTER TABLE source_archivals DROP CONSTRAINT IF EXISTS source_archivals_action_check;
ALTER TABLE source_archivals ADD CONSTRAINT source_archivals_action_check
    CHECK (action IN ('move', 'remove'));

-- A move needs a directory to move into; a removal has none.
ALTER TABLE source_archivals DROP CONSTRAINT IF EXISTS source_archivals_dir_not_empty;
ALTER TABLE source_archivals ADD CONSTRAINT source_archivals_dir_not_empty
    CHECK (action <> 'move' OR length(archive_dir) > 0);

--   removed  the original was verified to be what was delivered, and removed
ALTER TABLE source_archivals DROP CONSTRAINT IF EXISTS source_archivals_state_check;
ALTER TABLE source_archivals ADD CONSTRAINT source_archivals_state_check
    CHECK (state IN ('pending', 'archived', 'removed', 'absent', 'refused'));
