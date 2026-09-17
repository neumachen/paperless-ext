#!/bin/sh
# A9 — dry run preserves sources and the operational ledger.
#
# A dry run must compute and record a name and do nothing else. "Nothing else"
# is the part worth proving, and it can only be proven if the dry-run renamer
# is the ONLY consumer: with its siblings running, a document would be
# published by one of them and the absence of a published file would prove
# nothing.
#
# So this exercise stops the ordinary renamers, runs a dry-run renamer alone,
# submits a document through the real watcher and the real broker, and then
# checks what did and did not change. The ordinary renamers are restarted
# afterwards, and the submission is left in place: nothing here deletes a
# document.
. "$(dirname "$0")/lib.sh"

OUT="$EVIDENCE_DIR/a9-dry-run.txt"
mkdir -p "$EVIDENCE_DIR"
FAILURES=0

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
DOC="dry-run-probe-${STAMP}.pdf"
NORMALIZED="dry-run-probe-$(printf '%s' "$STAMP" | tr 'A-Z' 'a-z').pdf"

emit() { printf '%s\n' "$*" >> "$OUT"; printf '%s\n' "$*"; }
bad() { printf '    MISMATCH: %s\n' "$*" >&2; emit "    MISMATCH: $*"; FAILURES=$((FAILURES + 1)); }

# psql on the primary, using the same credential file the applications use.
psql_primary() {
    compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" \
        postgres-primary psql -U "${FN_DB_USER:-fn_app}" -d "${FN_DB_NAME:-filename_normalizer}" -tA -c "$1"
}

count_consume() {
    compose run --rm --no-deps -T --entrypoint sh storage-init \
        -c 'ls -1 /srv/fn/consume 2>/dev/null | wc -l' 2>/dev/null | tr -d ' \r\n'
}

restore() {
    log "restarting the ordinary renamers"
    compose stop renamer-dry-run >/dev/null 2>&1 || true
    compose rm -f renamer-dry-run >/dev/null 2>&1 || true
    compose start renamer-1 renamer-2 >/dev/null 2>&1 || true
    wait_healthy renamer-1 120 || true
    wait_healthy renamer-2 120 || true
}
trap restore EXIT INT TERM

: > "$OUT"
emit "A9 — dry run preserves sources and the operational ledger"
emit ""

# ---------------------------------------------------------------------------
log "1/5: stopping the ordinary renamers so the dry-run instance is the only consumer"
compose stop renamer-1 renamer-2 >/dev/null 2>&1
emit "ordinary renamers stopped"

log "2/5: starting a renamer with dry_run enabled"
compose --profile fault up -d --wait --wait-timeout 180 renamer-dry-run >/dev/null 2>&1 || {
    echo "the dry-run renamer did not become healthy" >&2
    exit 1
}
emit "dry-run renamer started (FN_RENAMER_DRY_RUN=true)"
emit ""

# ---------------------------------------------------------------------------
log "3/5: recording the before state"
BEFORE_JOBS="$(psql_primary 'SELECT count(*) FROM jobs;')"
BEFORE_RES="$(psql_primary 'SELECT count(*) FROM name_reservations;')"
BEFORE_RECEIPTS="$(psql_primary 'SELECT count(*) FROM delivery_receipts;')"
BEFORE_CONSUME="$(count_consume)"
emit "before:  jobs=$BEFORE_JOBS reservations=$BEFORE_RES receipts=$BEFORE_RECEIPTS consume_entries=$BEFORE_CONSUME"

# ---------------------------------------------------------------------------
log "4/5: submitting a document through the real watcher and broker"
compose run --rm --no-deps -T --entrypoint sh storage-init -c \
    "printf '%%PDF-1.4 dry run probe\\n' > /srv/fn/incoming/.wip-dryrun && \
     mv /srv/fn/incoming/.wip-dryrun '/srv/fn/incoming/$DOC'" >/dev/null 2>&1
emit "submitted: $DOC"

# Wait for the dry-run renamer to have handled it.
_seen=0
_i=0
while [ "$_i" -lt 60 ]; do
    if [ "$(psql_primary "SELECT count(*) FROM jobs WHERE source_name = '$DOC' AND normalized_name IS NOT NULL;")" = "1" ]; then
        _seen=1
        break
    fi
    sleep 1
    _i=$((_i + 1))
done
if [ "$_seen" != "1" ]; then
    bad "the dry-run renamer never recorded a normalized name for the submission"
fi

# ---------------------------------------------------------------------------
log "5/5: checking what did and did not change"
AFTER_RES="$(psql_primary 'SELECT count(*) FROM name_reservations;')"
AFTER_RECEIPTS="$(psql_primary 'SELECT count(*) FROM delivery_receipts;')"
AFTER_CONSUME="$(count_consume)"
STATE="$(psql_primary "SELECT state FROM jobs WHERE source_name = '$DOC';")"
NORMALIZED_RECORDED="$(psql_primary "SELECT coalesce(normalized_name,'-') FROM jobs WHERE source_name = '$DOC';")"
RESERVED="$(psql_primary "SELECT coalesce(reserved_name,'-') FROM jobs WHERE source_name = '$DOC';")"
SOURCE_PRESENT="$(compose run --rm --no-deps -T --entrypoint sh storage-init \
    -c "test -f '/srv/fn/incoming/$DOC' && echo yes || echo no" 2>/dev/null | tr -d ' \r\n')"
STAGED="$(compose run --rm --no-deps -T --entrypoint sh storage-init \
    -c 'ls -1 /srv/fn/staging 2>/dev/null | wc -l' 2>/dev/null | tr -d ' \r\n')"

emit "after:   jobs=$(psql_primary 'SELECT count(*) FROM jobs;') reservations=$AFTER_RES receipts=$AFTER_RECEIPTS consume_entries=$AFTER_CONSUME"
emit ""
emit "the submitted job:"
emit "  state:                 $STATE"
emit "  normalized_name:       $NORMALIZED_RECORDED   (computed and recorded)"
emit "  reserved_name:         $RESERVED   (must be '-': a dry run reserves nothing)"
emit "  source still present:  $SOURCE_PRESENT"
emit "  staging entries:       $STAGED"
emit ""

[ "$NORMALIZED_RECORDED" = "$NORMALIZED" ] || bad "normalized name is '$NORMALIZED_RECORDED', expected '$NORMALIZED'"
[ "$RESERVED" = "-" ] || bad "a dry run reserved the destination name '$RESERVED'"
[ "$AFTER_RES" = "$BEFORE_RES" ] || bad "reservations changed: $BEFORE_RES -> $AFTER_RES"
[ "$AFTER_RECEIPTS" = "$BEFORE_RECEIPTS" ] || bad "delivery receipts changed: $BEFORE_RECEIPTS -> $AFTER_RECEIPTS"
[ "$AFTER_CONSUME" = "$BEFORE_CONSUME" ] || bad "the consume directory changed: $BEFORE_CONSUME -> $AFTER_CONSUME entries"
[ "$SOURCE_PRESENT" = "yes" ] || bad "the source was removed"
[ "$STATE" != "delivered" ] || bad "a dry run marked the job delivered"

emit "A dry run computed the name and recorded it, and changed nothing else: no"
emit "destination reservation, no delivery receipt, no file in the consume"
emit "directory, and the source untouched. The name it computed is the name the"
emit "ordinary pipeline would have published."
emit ""
emit "mismatches: $FAILURES"

if [ "$FAILURES" -ne 0 ]; then
    echo >&2
    echo "FAILED: $FAILURES dry-run expectation(s) not met; see $OUT" >&2
    exit 1
fi

log "PASSED: a dry run preserves sources and the operational ledger"
note "evidence: $OUT"
