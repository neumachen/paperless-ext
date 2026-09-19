#!/bin/sh
# A reconciled receipt must not turn the sole publication into a duplicate.
#
# # The sequence
#
# Attempt A links its document into the consume directory and pauses before it
# can record anything. Its claim expires. A sibling delivery arrives, recovery
# finds the destination, recognises the inode as this job's own, hashes it and
# records the delivery -- a reconciliation, which is exactly what recovery is
# for. Then A wakes up.
#
# A used to ask one question: "does a receipt exist for this job?" It did, so A
# concluded its own file was a duplicate and removed it. The result was one
# document deleted, one receipt saying it had been delivered, and nothing at the
# destination for the consumer to take. The document was gone and the ledger
# said it had arrived.
#
# This exercise reproduces that sequence against the real stack and asserts the
# corrected outcome: A recognises that the receipt describes the file it linked
# and leaves it alone.
. "$(dirname "$0")/lib.sh"

OUT="$EVIDENCE_DIR/reconcile-resume.txt"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
LOWER="$(printf '%s' "$STAMP" | tr 'A-Z' 'a-z')"
FAILURES=0
CREATED=""
MUTATED=0
RESTORED=0

emit() { printf '%s\n' "$*" >> "$OUT"; printf '%s\n' "$*"; }
bad() { printf '    MISMATCH: %s\n' "$*" >&2; emit "    MISMATCH: $*"; FAILURES=$((FAILURES + 1)); }

psqlq() {
    compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" \
        postgres-primary psql -U "${FN_DB_USER:-fn_app}" -d "${FN_DB_NAME:-filename_normalizer}" \
        -tA -c "$1" < /dev/null | tr -d ' \r\n'
}
in_storage() {
    compose run --rm --no-deps -T --entrypoint sh storage-init -c "$1" 2>/dev/null < /dev/null | tr -d ' \r\n'
}

start_fault() {
    _sf="$1"; shift
    if [ -n "$(compose --profile fault ps -aq "$_sf" 2>/dev/null)" ]; then
        echo "error: '$_sf' already exists; this invocation did not create it." >&2
        return 1
    fi
    MUTATED=1
    CREATED="$CREATED $_sf"
    (
        for _kv in "$@"; do export "$_kv"; done
        compose --profile fault up -d "$_sf" >/dev/null 2>&1
    ) || { echo "error: could not start '$_sf'" >&2; return 1; }
    return 0
}

restore() {
    if [ "$RESTORED" = "1" ]; then return 0; fi
    RESTORED=1
    if [ "$MUTATED" = "0" ]; then
        log "nothing was changed; leaving the stack alone"
        exercise_unlock
        report_keep
        return 0
    fi
    log "restoring the stack"
    _ok=1
    for _c in $CREATED; do
        compose --profile fault stop "$_c" >/dev/null 2>&1 || true
        compose --profile fault rm -f "$_c" >/dev/null 2>&1 || true
        if [ -n "$(compose --profile fault ps -aq "$_c" 2>/dev/null)" ]; then
            echo "error: '$_c' could not be removed." >&2
            _ok=0
        fi
    done
    compose start renamer-1 renamer-2 >/dev/null 2>&1 || true
    wait_healthy renamer-1 120 || true
    wait_healthy renamer-2 120 || true
    _r1="$(compose exec -T renamer-1 /usr/local/bin/fn-renamer healthcheck --require-ready >/dev/null 2>&1 && echo yes || echo no)"
    _r2="$(compose exec -T renamer-2 /usr/local/bin/fn-renamer healthcheck --require-ready >/dev/null 2>&1 && echo yes || echo no)"
    [ "$_r1" = "yes" ] && [ "$_r2" = "yes" ] || _ok=0
    {
        printf '\nrestoration (read back from the running stack):\n'
        printf '  renamer-1 ready:   %s\n' "$_r1"
        printf '  renamer-2 ready:   %s\n' "$_r2"
        printf '  fault services left:%s\n' "$(compose --profile fault ps -aq $CREATED 2>/dev/null | wc -l | tr -d ' ')"
    } >> "$OUT"
    exercise_unlock
    if [ "$_ok" != "1" ]; then
        echo "FAILED: the stack was NOT restored. See $OUT." >&2
        printf '\nRESTORATION FAILED\n' >> "$OUT"
        report_keep
        exit 1
    fi
    # Order matters: report_keep promotes only when restoration has already
    # been recorded, so keeping first meant this exercise's passing runs never
    # reached the success slot and the one there stayed from an older run.
    report_restored
    report_keep
    note "restored: ordinary renamers ready, fault services removed"
}

exercise_lock reconcile-resume || exit 1
trap 'restore' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for _pf in renamer-hold renamer-taker; do
    if [ -n "$(compose --profile fault ps -aq "$_pf" 2>/dev/null)" ]; then
        echo "error: service '$_pf' already exists; this invocation did not create it" >&2
        echo "       and will not remove it. Refusing before anything is changed." >&2
        exit 1
    fi
done

report_begin "reconcile-resume" "$OUT" "$0"
emit "A reconciled receipt must not turn the sole publication into a duplicate"
emit ""

DOC="reconcile-resume-$STAMP.pdf"
NAME="reconcile-resume-$LOWER.pdf"

log "1/1: A links and pauses; a sibling reconciles; A resumes"
MUTATED=1
compose stop renamer-1 renamer-2 >/dev/null 2>&1

# A pauses AFTER the link, with a claim that expires long before it wakes.
# Prefetch 1, or the paused holder starves the sibling.
#
# A renamer prefetches two deliveries by default. The holder pauses after its
# link while still holding BOTH -- including the duplicate this exercise
# publishes for the sibling to pick up -- so the sibling never sees the job, no
# reconciliation happens, and the holder eventually wakes up and records its own
# receipt. That is correct behaviour and a useless observation: the sequence
# under test never occurs. One message at a time is what makes the sibling the
# one that finds it.
start_fault renamer-hold FN_FAULT_POINTS=hold_after_link FN_FAULT_HOLD=75s \
    FN_PUBLISH_TAKEOVER_AFTER=10s FN_RENAMER_PREFETCH=1 || exit 1
sleep 6
compose run --rm --no-deps -T --entrypoint sh storage-init -c \
    "printf '%%PDF-1.4 reconcile resume probe\n' > /srv/fn/incoming/.wip-rr && \
     mv /srv/fn/incoming/.wip-rr '/srv/fn/incoming/$DOC'" >/dev/null 2>&1
emit "submitted: $DOC"

# Wait for the link itself.
_i=0
LINKED=no
while [ "$_i" -lt 120 ]; do
    if [ "$(in_storage "test -f '/srv/fn/consume/$NAME' && echo yes || echo no")" = "yes" ]; then
        LINKED=yes
        break
    fi
    sleep 2; _i=$((_i + 2))
done
JOB="$(psqlq "SELECT job_id FROM jobs WHERE source_name = '$DOC';")"
INODE_A="$(in_storage "ls -i '/srv/fn/consume/$NAME' 2>/dev/null | awk '{print \$1}'")"
RECEIPTS_BEFORE="$(psqlq "SELECT count(*) FROM delivery_receipts WHERE job_id = '$JOB';")"

emit "1. attempt A linked its document and paused"
emit "   linked:                       $LINKED   (expected yes)"
emit "   inode at the destination:     ${INODE_A:-none}"
emit "   receipts so far:              $RECEIPTS_BEFORE   (expected 0: A paused before recording)"
[ "$LINKED" = "yes" ] || bad "A never linked; the sequence cannot be exercised"
[ "${RECEIPTS_BEFORE:-1}" = "0" ] || bad "a receipt already exists; A did not pause where this exercise needs it"

# The claim expires (10s), then a sibling delivery arrives for the same job.
sleep 12
start_fault renamer-taker FN_PUBLISH_TAKEOVER_AFTER=10s || exit 1
sleep 4
compose exec -T rabbitmq rabbitmqadmin \
    --vhost "${FN_AMQP_VHOST:-filename-normalizer}" \
    --username "${FN_AMQP_USER:-fn_app}" \
    --password "$(cat "$DEPLOY_DIR/secrets/fn_amqp_password")" \
    --non-interactive \
    publish message \
    --exchange "${FN_AMQP_EXCHANGE:-filename_normalizer.jobs}" \
    --routing-key "${FN_AMQP_ROUTING_KEY:-normalize}" \
    --properties '{"delivery_mode":2,"content_type":"application/json"}' \
    --payload "{\"contract_version\":1,\"job_id\":\"$JOB\",\"attempt\":2,\"enqueued_at\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" >/dev/null 2>&1 \
    && SIBLING=yes || SIBLING=no

# The sibling reconciles.
_i=0
RECONCILED=0
while [ "$_i" -lt 120 ]; do
    RECONCILED="$(psqlq "SELECT count(*) FROM delivery_receipts WHERE job_id = '$JOB';")"
    [ "${RECONCILED:-0}" -ge 1 ] && break
    sleep 2; _i=$((_i + 2))
done
STATE_MID="$(psqlq "SELECT state FROM jobs WHERE job_id = '$JOB';")"
RECEIPT_INODE="$(psqlq "SELECT coalesce(published_inode::text,'-') FROM delivery_receipts WHERE job_id = '$JOB';")"
RECON_EVENTS="$(psqlq "SELECT count(*) FROM job_events WHERE job_id = '$JOB' AND event_type = 'reconciled';")"

emit "2. a sibling reconciled A's file while A was paused"
emit "   sibling delivery published:   $SIBLING"
emit "   receipts now:                 $RECONCILED   (expected 1)"
emit "   state now:                    $STATE_MID   (expected delivered)"
emit "   the receipt names inode:      $RECEIPT_INODE   (expected $INODE_A: A's own file)"
emit "   reconciliation events:        $RECON_EVENTS   (expected 1)"
[ "${RECONCILED:-0}" = "1" ] || bad "the sibling did not reconcile A's file; the sequence was not exercised"
[ "$RECEIPT_INODE" = "$INODE_A" ] || bad "the receipt names inode $RECEIPT_INODE, not A's $INODE_A"

# Now A wakes up and decides what to do about a receipt it did not write.
_i=0
while [ "$_i" -lt 150 ]; do
    if compose --profile fault logs renamer-hold 2>/dev/null \
        | grep -q 'publication_reconciled_by_sibling\|duplicate_publication_withdrawn\|delivery_settled'; then
        break
    fi
    sleep 3; _i=$((_i + 3))
done
sleep 6
KEPT="$(compose --profile fault logs renamer-hold 2>/dev/null | grep "$JOB" | grep -c 'publication_reconciled_by_sibling\|publication_superseded' || true)"
{
    printf '\n--- what A logged for %s ---\n' "$JOB"
    compose --profile fault logs renamer-hold 2>/dev/null | grep "$JOB" | tail -12
    printf '\n--- what the sibling logged ---\n'
    compose --profile fault logs renamer-taker 2>/dev/null | grep "$JOB" | tail -12
} >> "$EVIDENCE_DIR/reconcile-resume-events.log" 2>/dev/null || true
WITHDREW="$(compose --profile fault logs renamer-hold 2>/dev/null | grep -c 'duplicate_publication_withdrawn' || true)"
PRESENT="$(in_storage "test -f '/srv/fn/consume/$NAME' && echo yes || echo no")"
INODE_FINAL="$(in_storage "ls -i '/srv/fn/consume/$NAME' 2>/dev/null | awk '{print \$1}'")"
COPIES="$(in_storage "ls -1 /srv/fn/consume 2>/dev/null | grep -c '^reconcile-resume-$LOWER' || true")"
RECEIPTS_FINAL="$(psqlq "SELECT count(*) FROM delivery_receipts WHERE job_id = '$JOB';")"
STATE_FINAL="$(psqlq "SELECT state FROM jobs WHERE job_id = '$JOB';")"
SRC="$(in_storage "test -f '/srv/fn/incoming/$DOC' && echo yes || echo no")"

emit "3. A resumed and found a receipt it did not write"
emit "   A stood down instead of deleting: $KEPT time(s)   (expected >= 1)"
emit "   A withdrew a duplicate:       $WITHDREW time(s)   (expected 0: there is no duplicate)"
emit "   the document is still there:  $PRESENT   (expected yes)"
emit "   still the same file:          $INODE_FINAL   (expected $INODE_A)"
emit "   documents with that name:     $COPIES   (expected 1)"
emit "   receipts:                     $RECEIPTS_FINAL   (expected 1)"
emit "   final state:                  $STATE_FINAL   (expected delivered)"
emit "   source preserved:             $SRC   (expected yes)"
[ "$PRESENT" = "yes" ] || bad "the document a receipt describes was deleted by the attempt that published it"
[ "$INODE_FINAL" = "$INODE_A" ] || bad "the file at the destination is inode $INODE_FINAL, not the published $INODE_A"
[ "${WITHDREW:-0}" = "0" ] || bad "A withdrew its own publication as a duplicate"
[ "${KEPT:-0}" -ge 1 ] || bad "A did not report standing down; it may have treated the receipt as somebody else's"
[ "${COPIES:-0}" = "1" ] || bad "$COPIES documents exist for one publication"
[ "${RECEIPTS_FINAL:-0}" = "1" ] || bad "$RECEIPTS_FINAL receipts exist"
[ "$STATE_FINAL" = "delivered" ] || bad "the job ended as '$STATE_FINAL'"
[ "$SRC" = "yes" ] || bad "the source was removed"
emit ""
emit "mismatches: $FAILURES"

if [ "$FAILURES" -ne 0 ]; then
    echo >&2
    echo "FAILED: $FAILURES expectation(s) not met; see $OUT" >&2
    exit 1
fi

report_success
log "PASSED: a reconciled receipt does not make the published document a duplicate"
note "evidence: $OUT"
