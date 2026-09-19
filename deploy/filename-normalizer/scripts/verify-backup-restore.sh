#!/bin/sh
# Demonstrate backup and restore on DISPOSABLE, project-owned state.
#
# # What this does and does not touch
#
# It dumps the live application database and the project's own document
# volumes, then restores BOTH into a throwaway database and throwaway volumes
# that this run creates and removes. The live database and the live volumes are
# only ever READ. Nothing here can resolve an uncertain job, delete a source, or
# overwrite a receipt, because nothing here writes to the live system at all.
#
# # Why both halves, together
#
# The ledger and the filesystem are one system. A database restored without its
# documents describes files that are not there; documents restored without the
# database are files nothing can explain. The restore is therefore verified by
# reconciling the two against each other, not by checking that each is
# non-empty.
. "$(dirname "$0")/lib.sh"

OUT="$EVIDENCE_DIR/backup-restore.txt"
FAILURES=0
CREATED_VOLUMES=""
RESTORE_DB="fn_restore_check_$$"
report_begin backup-restore "$OUT" "$0"

emit() { printf '%s\n' "$*" >> "$OUT"; }
bad() { printf '    MISMATCH: %s\n' "$*" >&2; emit "    MISMATCH: $*"; FAILURES=$((FAILURES + 1)); }
psqlq() {
    compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" \
        postgres-primary psql -U "${FN_DB_USER:-fn_app}" -d "${2:-${FN_DB_NAME:-filename_normalizer}}" \
        -tA -c "$1" < /dev/null 2>/dev/null | tr -d '\r'
}

restore_cleanup() {
    log "removing the throwaway restore target"
    compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" \
        postgres-primary psql -U "${FN_DB_USER:-fn_app}" -d postgres \
        -c "DROP DATABASE IF EXISTS $RESTORE_DB;" >/dev/null 2>&1 </dev/null || true
    for _v in $CREATED_VOLUMES; do
        docker volume rm "$_v" >/dev/null 2>&1 || true
    done
    _left=0
    for _v in $CREATED_VOLUMES; do
        [ -z "$(docker volume ls -q --filter "name=^${_v}$" 2>/dev/null)" ] || _left=$((_left + 1))
    done
    _db_left="$(psqlq "SELECT count(*) FROM pg_database WHERE datname = '$RESTORE_DB';" postgres)"
    emit ""
    emit "restoration of this exercise's own resources:"
    emit "  throwaway volumes left:       ${_left}   (expected 0)"
    emit "  throwaway database left:      ${_db_left:-unknown}   (expected 0)"
    [ "$_left" = "0" ] || bad "this exercise left $_left throwaway volume(s) behind"
    [ "${_db_left:-1}" = "0" ] || bad "this exercise left its throwaway database behind"
    if [ "$FAILURES" = "0" ]; then report_restored; fi
    report_keep
}
trap restore_cleanup EXIT

emit "Backup and restore, on disposable copies of project-owned state"
emit ""

# ---------------------------------------------------------------------------
# 1. What the live system holds right now. Read only.
# ---------------------------------------------------------------------------
log "1/4: recording what the live system holds"
LIVE_JOBS="$(psqlq "SELECT count(*) FROM jobs;")"
LIVE_RECEIPTS="$(psqlq "SELECT count(*) FROM delivery_receipts;")"
LIVE_UNCERTAIN="$(psqlq "SELECT count(*) FROM jobs WHERE state = 'uncertain';")"
LIVE_CONSUME="$(compose run --rm --no-deps -T --entrypoint sh storage-init \
    -c 'ls -1 /srv/fn/consume 2>/dev/null | wc -l' </dev/null 2>/dev/null | tr -d ' \r\n')"
# One delivered document, by name, to reconcile across the restore.
SAMPLE="$(psqlq "SELECT delivered_name FROM delivery_receipts ORDER BY delivered_at DESC LIMIT 1;")"
SAMPLE_SUM="$(compose run --rm --no-deps -T -e FN_S="/srv/fn/consume/$SAMPLE" --entrypoint sh storage-init \
    -c 'if [ -e "$FN_S" ]; then sha256sum "$FN_S" | cut -d" " -f1; else echo absent; fi' </dev/null 2>/dev/null | tr -d ' \r\n')"

emit "1. the live system, read without being touched"
emit "   jobs:                         $LIVE_JOBS"
emit "   delivery receipts:            $LIVE_RECEIPTS"
emit "   uncertain jobs:               $LIVE_UNCERTAIN"
emit "   entries in consume:           $LIVE_CONSUME"
emit "   sample document:              ${SAMPLE:-none}"
emit "   its sha256:                   ${SAMPLE_SUM:-none}"
case "$LIVE_JOBS" in ''|*[!0-9]*) bad "could not read the live job count; nothing below is established" ;; esac

# ---------------------------------------------------------------------------
# 2. Back both halves up, at the same moment.
# ---------------------------------------------------------------------------
log "2/4: dumping the database and the documents"
BACKUP_VOL="fn-backup-$$"
docker volume create "$BACKUP_VOL" >/dev/null 2>&1 && CREATED_VOLUMES="$CREATED_VOLUMES $BACKUP_VOL"
DUMP_OK=no
compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" postgres-primary \
    pg_dump -U "${FN_DB_USER:-fn_app}" -d "${FN_DB_NAME:-filename_normalizer}" -Fc \
    </dev/null > "$EVIDENCE_DIR/.fn-backup-$$.dump" 2>/dev/null && DUMP_OK=yes
DUMP_BYTES="$(wc -c < "$EVIDENCE_DIR/.fn-backup-$$.dump" 2>/dev/null | tr -d ' ')"

TAR_OK=no
docker run --rm -v "${PROJECT}_fn-consume:/consume:ro" -v "$BACKUP_VOL:/backup" \
    "$UTIL_IMAGE" sh -c 'tar -cf /backup/consume.tar -C /consume . && echo ok' >/dev/null 2>&1 && TAR_OK=yes
TAR_BYTES="$(docker run --rm -v "$BACKUP_VOL:/backup" "$UTIL_IMAGE" \
    sh -c 'wc -c < /backup/consume.tar' 2>/dev/null | tr -d ' \r\n')"

emit ""
emit "2. the backup"
emit "   database dump written:        $DUMP_OK   (${DUMP_BYTES:-0} bytes)"
emit "   document archive written:     $TAR_OK   (${TAR_BYTES:-0} bytes)"
[ "$DUMP_OK" = "yes" ] || bad "the database dump failed"
[ "$TAR_OK" = "yes" ] || bad "the document archive failed"

# ---------------------------------------------------------------------------
# 3. Restore into a throwaway database and throwaway volume.
# ---------------------------------------------------------------------------
log "3/4: restoring into a throwaway target"
compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" postgres-primary \
    psql -U "${FN_DB_USER:-fn_app}" -d postgres -c "CREATE DATABASE $RESTORE_DB;" >/dev/null 2>&1 </dev/null
RESTORE_DB_OK=no
compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" postgres-primary \
    pg_restore -U "${FN_DB_USER:-fn_app}" -d "$RESTORE_DB" --no-owner \
    < "$EVIDENCE_DIR/.fn-backup-$$.dump" >/dev/null 2>&1 && RESTORE_DB_OK=yes

RESTORE_VOL="fn-restore-$$"
docker volume create "$RESTORE_VOL" >/dev/null 2>&1 && CREATED_VOLUMES="$CREATED_VOLUMES $RESTORE_VOL"
RESTORE_FS_OK=no
docker run --rm -v "$BACKUP_VOL:/backup:ro" -v "$RESTORE_VOL:/restored" \
    "$UTIL_IMAGE" sh -c 'tar -xf /backup/consume.tar -C /restored && echo ok' >/dev/null 2>&1 && RESTORE_FS_OK=yes

emit ""
emit "3. the restore, into a target this run created"
emit "   database restored:            $RESTORE_DB_OK   (into $RESTORE_DB)"
emit "   documents restored:           $RESTORE_FS_OK   (into $RESTORE_VOL)"
[ "$RESTORE_DB_OK" = "yes" ] || bad "the database restore failed"
[ "$RESTORE_FS_OK" = "yes" ] || bad "the document restore failed"

# ---------------------------------------------------------------------------
# 4. Reconcile the two halves against each other.
# ---------------------------------------------------------------------------
log "4/4: reconciling the restored halves"
R_JOBS="$(psqlq "SELECT count(*) FROM jobs;" "$RESTORE_DB")"
R_RECEIPTS="$(psqlq "SELECT count(*) FROM delivery_receipts;" "$RESTORE_DB")"
R_UNCERTAIN="$(psqlq "SELECT count(*) FROM jobs WHERE state = 'uncertain';" "$RESTORE_DB")"
R_CONSUME="$(docker run --rm -v "$RESTORE_VOL:/restored:ro" "$UTIL_IMAGE" \
    sh -c 'ls -1 /restored 2>/dev/null | wc -l' 2>/dev/null | tr -d ' \r\n')"
R_SAMPLE_SUM="$(docker run --rm -v "$RESTORE_VOL:/restored:ro" -e FN_S="/restored/$SAMPLE" "$UTIL_IMAGE" \
    sh -c 'if [ -e "$FN_S" ]; then sha256sum "$FN_S" | cut -d" " -f1; else echo absent; fi' 2>/dev/null | tr -d ' \r\n')"
# The reconciliation that matters: every receipt in the restored ledger whose
# document should be present has it, in the restored filesystem.
docker run --rm -v "$RESTORE_VOL:/restored:ro" "$UTIL_IMAGE" sh -c 'ls -1 /restored' 2>/dev/null \
    | tr -d '\r' | sort > "$EVIDENCE_DIR/.restored-names-$$"
psqlq "SELECT delivered_name FROM delivery_receipts;" "$RESTORE_DB" | sed '/^$/d' | sort \
    > "$EVIDENCE_DIR/.restored-receipts-$$"
MISSING="$(comm -23 "$EVIDENCE_DIR/.restored-receipts-$$" "$EVIDENCE_DIR/.restored-names-$$" | wc -l | tr -d ' ')"
rm -f "$EVIDENCE_DIR/.restored-names-$$" "$EVIDENCE_DIR/.restored-receipts-$$"

emit ""
emit "4. the restored halves, reconciled against each other"
emit "   jobs:                         $R_JOBS   (live: $LIVE_JOBS)"
emit "   delivery receipts:            $R_RECEIPTS   (live: $LIVE_RECEIPTS)"
emit "   uncertain jobs:               $R_UNCERTAIN   (live: $LIVE_UNCERTAIN)"
emit "   entries in consume:           $R_CONSUME   (live: $LIVE_CONSUME)"
emit "   sample document sha256:       ${R_SAMPLE_SUM:-none}"
emit "   receipts whose document is missing from the restored filesystem: $MISSING"
emit "     (some are expected: a consumer legitimately takes delivered files,"
emit "      and this compares a ledger and a directory captured moments apart)"
[ "$R_JOBS" = "$LIVE_JOBS" ] || bad "the restored ledger holds $R_JOBS jobs, not $LIVE_JOBS"
[ "$R_RECEIPTS" = "$LIVE_RECEIPTS" ] || bad "the restored ledger holds $R_RECEIPTS receipts, not $LIVE_RECEIPTS"
[ "$R_UNCERTAIN" = "$LIVE_UNCERTAIN" ] || bad "the restored ledger holds $R_UNCERTAIN uncertain jobs, not $LIVE_UNCERTAIN"
[ "$R_CONSUME" = "$LIVE_CONSUME" ] || bad "the restored filesystem holds $R_CONSUME entries, not $LIVE_CONSUME"
if [ "$SAMPLE_SUM" != "absent" ] && [ -n "$SAMPLE_SUM" ]; then
    [ "$R_SAMPLE_SUM" = "$SAMPLE_SUM" ] || bad "the restored sample document's bytes differ from the live one's"
fi

# The live system is untouched.
NOW_JOBS="$(psqlq "SELECT count(*) FROM jobs;")"
NOW_UNCERTAIN="$(psqlq "SELECT count(*) FROM jobs WHERE state = 'uncertain';")"
emit ""
emit "   the LIVE system, re-read afterwards:"
emit "     jobs:                       $NOW_JOBS   (expected $LIVE_JOBS: unchanged)"
emit "     uncertain jobs:             $NOW_UNCERTAIN   (expected $LIVE_UNCERTAIN: unchanged)"
[ "$NOW_JOBS" = "$LIVE_JOBS" ] || bad "the live job count changed during a read-only exercise"
[ "$NOW_UNCERTAIN" = "$LIVE_UNCERTAIN" ] || bad "the live uncertain count changed during a read-only exercise"

rm -f "$EVIDENCE_DIR/.fn-backup-$$.dump"
emit ""
emit "mismatches: $FAILURES"
if [ "$FAILURES" = "0" ]; then
    report_success
    log "PASSED: a backup restores into a reconcilable system, and the live one was only read"
    note "evidence: $OUT"
    exit 0
fi
echo "FAILED: $FAILURES expectation(s) not met; see $OUT" >&2
exit 1
