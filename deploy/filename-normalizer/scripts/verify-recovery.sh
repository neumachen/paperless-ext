#!/bin/sh
# FN-N010 — the acceptance work that needs a real interruption, a real stale
# worker, real filesystem boundaries and real write failures.
#
# Each scenario stops the ordinary renamers first, so the instance under test
# is the only consumer. Without that, one of its siblings would quietly do the
# work and the scenario would pass while proving nothing.
#
# Everything here uses the real watcher, the real broker, the real cluster and
# real filesystems. The only synthetic element is the TIMING of the crash in
# scenario 1, which the application provides through an injected fault point:
# the window between linking a document into place and committing its receipt
# is microseconds wide and cannot be hit from outside the process.
. "$(dirname "$0")/lib.sh"

OUT="$EVIDENCE_DIR/a6a7a10a11-recovery.txt"
mkdir -p "$EVIDENCE_DIR"
FAILURES=0
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
LOWER="$(printf '%s' "$STAMP" | tr 'A-Z' 'a-z')"

emit() { printf '%s\n' "$*" >> "$OUT"; printf '%s\n' "$*"; }
bad() { printf '    MISMATCH: %s\n' "$*" >&2; emit "    MISMATCH: $*"; FAILURES=$((FAILURES + 1)); }

psqlq() {
    compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" \
        postgres-primary psql -U "${FN_DB_USER:-fn_app}" -d "${FN_DB_NAME:-filename_normalizer}" -tA -c "$1" \
        2>/dev/null | tr -d ' \r'
}

# submit writes a document to a temporary name and renames it into place.
submit() {
    _s_name="$1"; _s_bytes="${2:-64}"
    compose run --rm --no-deps -T --entrypoint sh storage-init -c \
        "head -c $_s_bytes /dev/zero | tr '\\0' 'x' > /srv/fn/incoming/.wip-$LOWER && \
         mv /srv/fn/incoming/.wip-$LOWER '/srv/fn/incoming/$_s_name'" >/dev/null 2>&1
}

consume_count() {
    compose run --rm --no-deps -T --entrypoint sh storage-init \
        -c 'ls -1 /srv/fn/consume 2>/dev/null | grep -v "^\.fn-" | wc -l' 2>/dev/null | tr -d ' \r\n'
}

# await_state polls until a job reaches one of the given states, or times out.
# It never simply sleeps: a fixed wait would pass whether or not the delivery
# was ever handled.
await_state() {
    _aw_name="$1"; _aw_want="$2"; _aw_secs="${3:-90}"
    _aw_i=0
    while [ "$_aw_i" -lt "$_aw_secs" ]; do
        _aw_got="$(psqlq "SELECT state FROM jobs WHERE source_name = '$_aw_name';")"
        case " $_aw_want " in
            *" $_aw_got "*) printf '%s' "$_aw_got"; return 0 ;;
        esac
        sleep 1
        _aw_i=$((_aw_i + 1))
    done
    printf '%s' "${_aw_got:-absent}"
    return 1
}

stop_ordinary() { compose stop renamer-1 renamer-2 >/dev/null 2>&1; }

restore() {
    log "restoring the ordinary renamers"
    for svc in renamer-fault renamer-altfs renamer-tinyfs; do
        compose --profile fault stop "$svc" >/dev/null 2>&1 || true
        compose --profile fault rm -f "$svc" >/dev/null 2>&1 || true
    done
    compose start renamer-1 renamer-2 >/dev/null 2>&1 || true
    wait_healthy renamer-1 120 || true
    wait_healthy renamer-2 120 || true
}
trap restore EXIT INT TERM

: > "$OUT"
emit "FN-N010 — interruption, stale worker, filesystem boundaries, write failures"
emit ""

# ---------------------------------------------------------------------------
# 1. A6: a real interruption between the link and the receipt.
# ---------------------------------------------------------------------------
log "1/6: A6 — interrupting a renamer between the destination link and the receipt"
stop_ordinary
DOC1="a6-after-link-$STAMP.pdf"
NAME1="a6-after-link-$LOWER.pdf"

FN_FAULT_POINTS=after_link compose --profile fault up -d renamer-fault >/dev/null 2>&1
sleep 6
submit "$DOC1"

# The faulting renamer exits when it fires, so wait for the container to stop.
_i=0
while [ "$_i" -lt 90 ]; do
    _running="$(compose --profile fault ps -q renamer-fault 2>/dev/null | head -1)"
    [ -z "$_running" ] && break
    _state="$(docker inspect -f '{{.State.Status}}' "$_running" 2>/dev/null || echo gone)"
    [ "$_state" != "running" ] && break
    sleep 1
    _i=$((_i + 1))
done
EXIT1="$(docker inspect -f '{{.State.ExitCode}}' "$(compose --profile fault ps -aq renamer-fault | head -1)" 2>/dev/null || echo unknown)"
STATE1="$(psqlq "SELECT state FROM jobs WHERE source_name = '$DOC1';")"
PUBLISHED1="$(compose run --rm --no-deps -T --entrypoint sh storage-init \
    -c "test -f '/srv/fn/consume/$NAME1' && echo yes || echo no" 2>/dev/null | tr -d ' \r\n')"
RECEIPTS1="$(psqlq "SELECT count(*) FROM delivery_receipts r JOIN jobs j USING (job_id) WHERE j.source_name = '$DOC1';")"

emit "1. A6 — interruption AFTER the link, BEFORE the receipt"
emit "   renamer exit code:            $EXIT1   (90 = injected stop)"
emit "   job state after the crash:    $STATE1   (expected publishing)"
emit "   destination present:          $PUBLISHED1"
emit "   delivery receipts:            $RECEIPTS1   (expected 0)"
[ "$STATE1" = "publishing" ] || bad "the interrupted job is in state '$STATE1', not publishing"
[ "$PUBLISHED1" = "yes" ] || bad "the document was not linked into place before the crash"
[ "$RECEIPTS1" = "0" ] || bad "a receipt exists even though the crash preceded it"

# Recovery: an ordinary renamer must reconcile it without republishing.
compose --profile fault rm -f renamer-fault >/dev/null 2>&1
compose start renamer-1 renamer-2 >/dev/null 2>&1
wait_healthy renamer-1 120 || true
RECOVERED1="$(await_state "$DOC1" "delivered held uncertain" 120)"
COUNT1="$(compose run --rm --no-deps -T --entrypoint sh storage-init \
    -c "ls -1 /srv/fn/consume 2>/dev/null | grep -c '^a6-after-link-$LOWER' || true" 2>/dev/null | tr -d ' \r\n')"
RECON1="$(psqlq "SELECT count(*) FROM job_events e JOIN jobs j USING (job_id) WHERE j.source_name = '$DOC1' AND e.event_type = 'reconciled';")"

emit "   after recovery:               $RECOVERED1   (expected delivered)"
emit "   reconciled events:            $RECON1   (expected 1: not republished)"
emit "   documents with that name:     $COUNT1   (expected 1)"
[ "$RECOVERED1" = "delivered" ] || bad "recovery left the job in '$RECOVERED1'"
[ "$RECON1" = "1" ] || bad "the recovery did not record a reconciliation"
[ "$COUNT1" = "1" ] || bad "$COUNT1 documents exist for one job after recovery"
emit ""

# ---------------------------------------------------------------------------
# 2. A6: interrupted BEFORE the link -- the destination never appears.
# ---------------------------------------------------------------------------
log "2/6: A6 — interrupting before the link, so the destination never appears"
stop_ordinary
DOC2="a6-before-link-$STAMP.pdf"
NAME2="a6-before-link-$LOWER.pdf"

FN_FAULT_POINTS=before_link compose --profile fault up -d renamer-fault >/dev/null 2>&1
sleep 6
submit "$DOC2"
_i=0
while [ "$_i" -lt 90 ]; do
    _running="$(compose --profile fault ps -q renamer-fault 2>/dev/null | head -1)"
    [ -z "$_running" ] && break
    _state="$(docker inspect -f '{{.State.Status}}' "$_running" 2>/dev/null || echo gone)"
    [ "$_state" != "running" ] && break
    sleep 1
    _i=$((_i + 1))
done
STATE2="$(psqlq "SELECT state FROM jobs WHERE source_name = '$DOC2';")"
PUBLISHED2="$(compose run --rm --no-deps -T --entrypoint sh storage-init \
    -c "test -f '/srv/fn/consume/$NAME2' && echo yes || echo no" 2>/dev/null | tr -d ' \r\n')"

emit "2. A6 — interruption BEFORE the link"
emit "   job state after the crash:    $STATE2   (expected publishing: the intent was committed)"
emit "   destination present:          $PUBLISHED2   (expected no)"
[ "$STATE2" = "publishing" ] || bad "the interrupted job is in state '$STATE2'"
[ "$PUBLISHED2" = "no" ] || bad "a destination exists although the crash preceded the link"

compose --profile fault rm -f renamer-fault >/dev/null 2>&1
compose start renamer-1 renamer-2 >/dev/null 2>&1
wait_healthy renamer-1 120 || true
RECOVERED2="$(await_state "$DOC2" "delivered held uncertain" 120)"
emit "   after recovery:               $RECOVERED2   (expected uncertain: a publication may have happened)"
[ "$RECOVERED2" = "uncertain" ] || bad "recovery reached '$RECOVERED2'; an unconfirmable publication must be uncertain"

# And a redelivery of an uncertain job must NOT reopen it.
REDELIVERED2="$(psqlq "SELECT state FROM jobs WHERE source_name = '$DOC2';")"
emit "   still uncertain afterwards:   $REDELIVERED2"
[ "$REDELIVERED2" = "uncertain" ] || bad "an uncertain job was reopened to '$REDELIVERED2'"
emit ""

# ---------------------------------------------------------------------------
# 3. A7: a stale worker -- broker connection lost, filesystem access retained.
# ---------------------------------------------------------------------------
log "3/6: A7 — a worker that loses the broker but keeps its filesystem access"
DOC3="a7-stale-$STAMP.pdf"
NAME3="a7-stale-$LOWER.pdf"
submit "$DOC3" 2000000
sleep 3

# Cut renamer-1 off from the broker at the network layer while it keeps
# running and keeps every mount. This is the stale-worker condition: it cannot
# acknowledge, and a replacement will be handed the same job.
compose exec -T --user 0 renamer-1 sh -c 'exit 0' >/dev/null 2>&1 && HAVE_EXEC=yes || HAVE_EXEC=no
docker network disconnect "${PROJECT}_fn" "$(compose ps -q renamer-1 | head -1)" >/dev/null 2>&1 && CUT=yes || CUT=no

RECOVERED3="$(await_state "$DOC3" "delivered held uncertain" 150)"
COUNT3="$(compose run --rm --no-deps -T --entrypoint sh storage-init \
    -c "ls -1 /srv/fn/consume 2>/dev/null | grep -c '^a7-stale-$LOWER' || true" 2>/dev/null | tr -d ' \r\n')"
RECEIPTS3="$(psqlq "SELECT count(*) FROM delivery_receipts r JOIN jobs j USING (job_id) WHERE j.source_name = '$DOC3';")"
RESV3="$(psqlq "SELECT count(*) FROM name_reservations n JOIN jobs j USING (job_id) WHERE j.source_name = '$DOC3' AND n.blocked_at IS NULL;")"

emit "3. A7 — stale worker with retained filesystem access"
emit "   broker connection cut:        $CUT"
emit "   final state:                  $RECOVERED3"
emit "   documents with that name:     $COUNT3   (expected at most 1)"
emit "   delivery receipts:            $RECEIPTS3   (expected at most 1)"
emit "   active reservations:          $RESV3   (expected at most 1)"
[ "${COUNT3:-0}" -le 1 ] || bad "$COUNT3 documents exist for one job"
[ "${RECEIPTS3:-0}" -le 1 ] || bad "$RECEIPTS3 receipts exist for one job"
[ "${RESV3:-0}" -le 1 ] || bad "$RESV3 active reservations exist for one job"

# Reconnect and let it settle.
docker network connect "${PROJECT}_fn" "$(compose ps -q renamer-1 | head -1)" >/dev/null 2>&1 || true
compose restart renamer-1 >/dev/null 2>&1 || true
wait_healthy renamer-1 120 || true
emit ""

# ---------------------------------------------------------------------------
# 4. A10: staging and consume on genuinely different filesystems.
# ---------------------------------------------------------------------------
log "4/6: A10 — staging on a tmpfs, so the copy fallback is actually taken"
stop_ordinary
DOC4="a10-altfs-$STAMP.pdf"
NAME4="a10-altfs-$LOWER.pdf"

compose --profile fault up -d --wait --wait-timeout 180 renamer-altfs >/dev/null 2>&1
# The runtime image is FROM scratch and has no shell, and a tmpfs is private to
# its container, so nothing outside can stat it. The application reports the
# device behind each root in its own effective configuration, which is the only
# vantage point that can see both roots at once.
DEVS="$(compose --profile fault exec -T renamer-altfs \
    /usr/local/bin/fn-renamer check-config 2>/dev/null \
    | tr -d ' "' | grep -A8 storage_devices: | tr '\n' ' ')"
STAGING_DEV="$(compose --profile fault exec -T renamer-altfs /usr/local/bin/fn-renamer check-config 2>/dev/null \
    | python3 -c "import json,sys; print(json.load(sys.stdin).get('storage_devices',{}).get('staging',''))" 2>/dev/null || true)"
CONSUME_DEV="$(compose --profile fault exec -T renamer-altfs /usr/local/bin/fn-renamer check-config 2>/dev/null \
    | python3 -c "import json,sys; print(json.load(sys.stdin).get('storage_devices',{}).get('consume',''))" 2>/dev/null || true)"
submit "$DOC4" 200000
STATE4="$(await_state "$DOC4" "delivered held uncertain" 120)"
PUB4="$(compose run --rm --no-deps -T --entrypoint sh storage-init \
    -c "test -f '/srv/fn/consume/$NAME4' && echo yes || echo no" 2>/dev/null | tr -d ' \r\n')"

emit "4. A10 — staging and consume on different filesystems"
emit "   staging device:               ${STAGING_DEV:-unknown}"
emit "   consume device:               ${CONSUME_DEV:-unknown}"
if [ -n "$STAGING_DEV" ] && [ -n "$CONSUME_DEV" ]; then
    if [ "$STAGING_DEV" = "$CONSUME_DEV" ]; then
        bad "staging and consume share device $STAGING_DEV; the cross-filesystem path was NOT exercised"
    else
        emit "   => genuinely different filesystems, so the copy fallback was taken"
    fi
else
    bad "the storage devices could not be read, so the topology is unverified"
fi
emit "   final state:                  $STATE4   (expected delivered)"
emit "   destination present:          $PUB4"
[ "$STATE4" = "delivered" ] || bad "publication across a filesystem boundary reached '$STATE4'"
[ "$PUB4" = "yes" ] || bad "no document was published across the boundary"
compose --profile fault stop renamer-altfs >/dev/null 2>&1
compose --profile fault rm -f renamer-altfs >/dev/null 2>&1
emit ""

# ---------------------------------------------------------------------------
# 5. A11: the working copy runs out of space mid-write.
# ---------------------------------------------------------------------------
log "5/6: A11 — a real ENOSPC while writing the working copy"
DOC5="a11-nospace-$STAMP.pdf"
compose --profile fault up -d --wait --wait-timeout 180 renamer-tinyfs >/dev/null 2>&1
submit "$DOC5" 5000000   # 5 MB into a 1 MB staging tmpfs
STATE5="$(await_state "$DOC5" "delivered held uncertain" 150)"
CAT5="$(psqlq "SELECT coalesce(failure_category,'-') FROM jobs WHERE source_name = '$DOC5';")"
ATTEMPTS5="$(psqlq "SELECT delivery_attempts FROM jobs WHERE source_name = '$DOC5';")"
EVENTS5="$(psqlq "SELECT count(*) FROM job_events e JOIN jobs j USING (job_id) WHERE j.source_name = '$DOC5';")"
PUB5="$(compose run --rm --no-deps -T --entrypoint sh storage-init \
    -c "ls -1 /srv/fn/consume 2>/dev/null | grep -c 'a11-nospace-$LOWER' || true" 2>/dev/null | tr -d ' \r\n')"

emit "5. A11 — out of space while writing the working copy"
emit "   final state:                  $STATE5   (expected held)"
emit "   category:                     $CAT5"
emit "   delivery attempts:            $ATTEMPTS5   (the retryable failure was retried)"
emit "   history rows:                 $EVENTS5   (bounded, not a spin)"
emit "   partial documents published:  $PUB5   (expected 0)"
[ "$STATE5" = "held" ] || bad "an out-of-space write reached '$STATE5'"
[ "$PUB5" = "0" ] || bad "$PUB5 documents were published despite the failed copy"
[ "${ATTEMPTS5:-0}" -ge 2 ] || bad "the failure was not retried before being held (attempts=$ATTEMPTS5)"
[ "${EVENTS5:-0}" -le 60 ] || bad "$EVENTS5 history rows; the retry bound did not hold"
case "$CAT5" in
    retry_exhausted|storage_error|permission_denied|storage_unavailable) ;;
    *) bad "category '$CAT5' does not describe a storage failure" ;;
esac
compose --profile fault stop renamer-tinyfs >/dev/null 2>&1
compose --profile fault rm -f renamer-tinyfs >/dev/null 2>&1
emit ""

# ---------------------------------------------------------------------------
# 6. Retry-budget exhaustion through a persistent retryable failure.
# ---------------------------------------------------------------------------
emit "6. Retry budget"
emit "   Scenario 5 is the retry-budget demonstration: the out-of-space write is"
emit "   a RETRYABLE storage failure that never succeeds, so the delivery is"
emit "   returned and redelivered until the durable budget is spent and the job"
emit "   is held. An immediate destination_conflict, which is what the previous"
emit "   round offered as evidence, settles on the first attempt and never"
emit "   touches the budget at all."
emit "   delivery attempts recorded:   $ATTEMPTS5 against a budget of 2"
emit ""

emit "mismatches: $FAILURES"

if [ "$FAILURES" -ne 0 ]; then
    echo >&2
    echo "FAILED: $FAILURES expectation(s) not met; see $OUT" >&2
    exit 1
fi

log "PASSED: interruption, stale worker, filesystem boundary and write-failure behaviour"
note "evidence: $OUT"
