#!/bin/sh
# A job closed AFTER an attempt's last authority check cannot be reopened.
#
# # Why this is a different case from scenario 11
#
# Recovery scenario 11 pauses an attempt BEFORE its pre-link check: it wakes,
# asks whether it still holds the publication, is told no, and stands down
# without linking. That is a check doing its job.
#
# This is the case a check cannot do anything about. The attempt asks, is told
# yes, and only then is delayed -- so its authority is a snapshot that stops
# being true while it holds it. It links, and by the time it writes the receipt
# a sibling has recorded the job `uncertain`. The write used to set
# state = 'delivered' unconditionally, so a terminal outcome a person was meant
# to resolve was reopened by an attempt whose permission had expired.
#
# The sequence here: the attempt links and pauses; its document is taken from
# the directory, as a consumer would take it; its claim expires; a sibling finds
# no destination and cannot confirm the publication, so it records `uncertain`;
# the attempt wakes and tries to record its delivery.
. "$(dirname "$0")/lib.sh"

OUT="$EVIDENCE_DIR/post-check-closure.txt"
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
        printf '  renamer-1 ready:     %s\n' "$_r1"
        printf '  renamer-2 ready:     %s\n' "$_r2"
        printf '  fault services left: %s\n' "$(compose --profile fault ps -aq $CREATED 2>/dev/null | wc -l | tr -d ' ')"
    } >> "$OUT"
    exercise_unlock
    if [ "$_ok" != "1" ]; then
        echo "FAILED: the stack was NOT restored. See $OUT." >&2
        printf '\nRESTORATION FAILED\n' >> "$OUT"
        report_keep
        exit 1
    fi
    report_restored
    report_keep
    note "restored: ordinary renamers ready, fault services removed"
}

exercise_lock post-check-closure || exit 1
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

report_begin "post-check-closure" "$OUT" "$0"
emit "A job closed after an attempt's last authority check is not reopened"
emit ""

DOC="post-check-$STAMP.pdf"
NAME="post-check-$LOWER.pdf"

log "1/1: the attempt links, is delayed, and the job is closed underneath it"
MUTATED=1
compose stop renamer-1 renamer-2 >/dev/null 2>&1

# Paused AFTER the link and BEFORE the receipt: past every authority check it
# makes. Prefetch 1 so it cannot also hold the sibling's delivery.
start_fault renamer-hold FN_FAULT_POINTS=hold_after_link FN_FAULT_HOLD=75s \
    FN_PUBLISH_TAKEOVER_AFTER=10s FN_RENAMER_PREFETCH=1 || exit 1
sleep 6
compose run --rm --no-deps -T --entrypoint sh storage-init -c \
    "printf '%%PDF-1.4 post check probe\n' > /srv/fn/incoming/.wip-pc && \
     mv /srv/fn/incoming/.wip-pc '/srv/fn/incoming/$DOC'" >/dev/null 2>&1
emit "submitted: $DOC"

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
RECEIPTS_MID="$(psqlq "SELECT count(*) FROM delivery_receipts WHERE job_id = '$JOB';")"

emit "1. the attempt linked its document and was delayed before recording it"
emit "   linked:                       $LINKED   (expected yes)"
emit "   receipts so far:              $RECEIPTS_MID   (expected 0: the delay is before the receipt)"
[ "$LINKED" = "yes" ] || bad "nothing was linked; this is not the case under test"
[ "${RECEIPTS_MID:-1}" = "0" ] || bad "a receipt already exists; the pause is not where this exercise needs it"

# The document is taken out of the directory, as a consumer takes it. The
# sibling that follows therefore finds no destination and cannot confirm
# anything about the publication.
TAKEN="$(in_storage "rm -f '/srv/fn/consume/$NAME' 2>/dev/null; test -e '/srv/fn/consume/$NAME' && echo present || echo taken")"
emit "   the document was taken:       $TAKEN   (as a consumer would take it)"
[ "$TAKEN" = "taken" ] || bad "the document could not be taken, so the sibling would not face an absent destination"

# The claim expires, then a sibling is given a delivery of its own.
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

_i=0
CLOSED=""
while [ "$_i" -lt 120 ]; do
    CLOSED="$(psqlq "SELECT state FROM jobs WHERE job_id = '$JOB';")"
    case "$CLOSED" in uncertain|held|delivered) break ;; esac
    sleep 2; _i=$((_i + 2))
done
SIBLING_ACTED="$(compose --profile fault logs renamer-taker 2>/dev/null | grep -c "$JOB" || true)"

emit "2. a sibling closed the job while the attempt was delayed"
emit "   a delivery was given to it:   $SIBLING"
emit "   the sibling handled the job:  $SIBLING_ACTED log line(s)   (expected >= 1)"
emit "   the outcome it recorded:      $CLOSED   (expected uncertain: it found no destination)"
[ "${SIBLING_ACTED:-0}" -ge 1 ] || bad "the sibling never saw the job, so nothing closed it and this run tests nothing"
[ "$CLOSED" = "uncertain" ] || bad "the sibling recorded '$CLOSED'; this exercise needs the job closed as uncertain"

# The attempt wakes up and tries to record a delivery for a job that is closed.
_i=0
while [ "$_i" -lt 210 ]; do
    if compose --profile fault logs renamer-hold 2>/dev/null | grep "$JOB" \
        | grep -qE 'publication_superseded|delivery_settled|receipt_write_failed'; then
        break
    fi
    sleep 5; _i=$((_i + 5))
done
sleep 8

SUPERSEDED="$(compose --profile fault logs renamer-hold 2>/dev/null | grep "$JOB" \
    | grep -c 'publication_superseded' || true)"
WHATLOGGED="$(compose --profile fault logs renamer-hold 2>/dev/null | grep "$JOB" \
    | grep -oE 'publication_superseded|document_published|delivery_settled|receipt_write_failed' \
    | sort -u | tr '\n' ' ')"
FINAL="$(psqlq "SELECT state FROM jobs WHERE job_id = '$JOB';")"
RECEIPTS="$(psqlq "SELECT count(*) FROM delivery_receipts WHERE job_id = '$JOB';")"
COPIES="$(in_storage "ls -1 /srv/fn/consume 2>/dev/null | grep -c '^post-check-$LOWER' || true")"
SRC="$(in_storage "test -f '/srv/fn/incoming/$DOC' && echo yes || echo no")"
{
    printf '\n--- what each side logged for %s ---\n' "$JOB"
    compose --profile fault logs renamer-hold 2>/dev/null | grep "$JOB" | tail -10
    printf '\n--- the sibling ---\n'
    compose --profile fault logs renamer-taker 2>/dev/null | grep "$JOB" | tail -10
} >> "$EVIDENCE_DIR/post-check-closure-events.log" 2>/dev/null || true

emit "3. the delayed attempt tried to record its delivery"
emit "   waited for it to resume:      ${_i}s (its pause is 75s)"
emit "   it reported being superseded: $SUPERSEDED time(s)   (expected >= 1)"
emit "   what it logged:               ${WHATLOGGED:-none}"
emit "   final state:                  $FINAL   (expected uncertain: NOT reopened)"
emit "   delivery receipts:            $RECEIPTS   (expected 0)"
emit "   documents in the directory:   $COPIES   (expected 0)"
emit "   source preserved:             $SRC   (expected yes)"
[ "${SUPERSEDED:-0}" -ge 1 ] || bad "the delayed attempt never reported that the job had been closed under it"
[ "$FINAL" = "uncertain" ] || bad "the job is '$FINAL': a terminal outcome was reopened by an attempt whose authority had expired"
[ "${RECEIPTS:-0}" = "0" ] || bad "$RECEIPTS receipt(s) exist for a job nobody could confirm"
[ "${COPIES:-0}" = "0" ] || bad "$COPIES document(s) are in the directory for a job recorded uncertain"
[ "$SRC" = "yes" ] || bad "the source was removed"
emit ""
emit "   => this job is UNRESOLVED BY DESIGN: job_id=$JOB"
emit ""
emit "mismatches: $FAILURES"

if [ "$FAILURES" -ne 0 ]; then
    echo >&2
    echo "FAILED: $FAILURES expectation(s) not met; see $OUT" >&2
    exit 1
fi

report_success
log "PASSED: a job closed after the last authority check stays closed"
note "evidence: $OUT"
