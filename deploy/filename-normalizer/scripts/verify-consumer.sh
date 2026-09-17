#!/bin/sh
# A real consumer takes the published document, and the handoff survives it.
#
# # What this is for
#
# Two guarantees are about what a consumer DOES to a published file:
#
#   1. The normalizer's job is finished when the document is linked into the
#      consume directory. What happens to it next is the consumer's business,
#      and the normalizer must not mistake the file's disappearance for a
#      failure -- the delivery HAPPENED, and a successful link is the proof.
#   2. A consumer that takes the file between the link and the moment the
#      normalizer looks at it must not turn a completed delivery into a hold,
#      and must not cause the document to be published a second time.
#
# The previous round reasoned about both. Reasoning is not observation, and a
# fake consumer that deletes files on a timer is not evidence either: the
# timing that makes the test pass is chosen by whoever writes the fake. So
# this exercise runs a REAL Paperless instance against the real consume
# directory and lets it behave however it behaves.
#
# # Isolation
#
# The Paperless instance is created by this exercise and destroyed by it. It
# has its own SQLite database, its own media volume, its own Redis, and no
# port published to the host. It ingests only the synthetic documents this
# stack produces. Nothing here touches a real library, and no document of the
# user's is submitted to it.
. "$(dirname "$0")/lib.sh"

OUT="$EVIDENCE_DIR/consumer-handoff.txt"
mkdir -p "$EVIDENCE_DIR"
FAILURES=0
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
LOWER="$(printf '%s' "$STAMP" | tr 'A-Z' 'a-z')"
CREATED=""

emit() { printf '%s\n' "$*" >> "$OUT"; printf '%s\n' "$*"; }
bad() { printf '    MISMATCH: %s\n' "$*" >&2; emit "    MISMATCH: $*"; FAILURES=$((FAILURES + 1)); }

psqlq() {
    compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" \
        postgres-primary psql -U "${FN_DB_USER:-fn_app}" -d "${FN_DB_NAME:-filename_normalizer}" -tA -c "$1" \
        2>/dev/null | tr -d ' \r'
}

in_storage() {
    compose run --rm --no-deps -T --entrypoint sh storage-init -c "$1" 2>/dev/null | tr -d ' \r\n'
}

# A REAL consumer only takes a document it can actually read. The stub PDF the
# other exercises use -- four lines with no xref table -- is rejected by
# Paperless and left where it is, which would make the one observation this
# exercise exists for impossible: the consumer REMOVING the published file.
#
# So the fixture is a genuinely valid PDF, written by pikepdf inside the
# consumer's own digest-pinned image (no host tooling), into the incoming root
# through a one-off container. It is still synthetic: a blank page, produced
# here, ingested only by the throwaway instance this exercise creates.
submit() {
    _sb_name="$1"; _sb_wip="$2"
    compose run --rm --no-deps -T \
        -v "${PROJECT}_fn-incoming:/incoming" \
        --entrypoint python3 paperless -c "
import os, pikepdf
wip = '/incoming/.wip-$_sb_wip'
pdf = pikepdf.new()
pdf.add_blank_page(page_size=(612, 792))
pdf.save(wip)
# The renamer runs unprivileged and must be able to read what it finds.
os.chmod(wip, 0o644)
# Renamed into place so discovery never sees a partially written file.
os.rename(wip, '/incoming/$_sb_name')
" >/dev/null 2>&1
}

await_state() {
    _aw_name="$1"; _aw_want="$2"; _aw_secs="${3:-120}"; _aw_i=0
    while [ "$_aw_i" -lt "$_aw_secs" ]; do
        _aw_got="$(psqlq "SELECT state FROM jobs WHERE source_name = '$_aw_name';")"
        case " $_aw_want " in *" $_aw_got "*) printf '%s' "$_aw_got"; return 0 ;; esac
        sleep 1; _aw_i=$((_aw_i + 1))
    done
    printf '%s' "${_aw_got:-absent}"; return 1
}

start_consumer() {
    for _sc in paperless-redis paperless; do
        if [ -n "$(compose --profile consumer ps -aq "$_sc" 2>/dev/null)" ]; then
            echo "error: '$_sc' already exists; this invocation did not create it and will not remove it." >&2
            return 1
        fi
    done
    CREATED="paperless paperless-redis"

    # The consumer is pointed at the REAL destination directory, which holds
    # what earlier runs delivered there. Those documents are not this
    # exercise's to consume: it did not create them, it may not remove them,
    # and asking a real consumer to work through ninety of them first means it
    # does not reach this exercise's document inside any window -- which is
    # what happened, and it read as "the consumer never took the file".
    #
    # So everything already present is named in the consumer's ignore list.
    # The consumer still watches the real directory and still behaves however
    # it behaves; it is simply not handed a backlog belonging to somebody
    # else. The list is built from the directory itself, in a digest-pinned
    # container, so it is exact rather than a pattern that might also match
    # this exercise's own files.
    IGNORED_JSON="$(docker run --rm -v "${PROJECT}_fn-consume:/consume:ro" "$UTIL_PY_IMAGE" \
        python3 -c "import json,os; print(json.dumps(sorted(os.listdir('/consume'))))" 2>/dev/null)"
    IGNORED_COUNT="$(docker run --rm -v "${PROJECT}_fn-consume:/consume:ro" "$UTIL_PY_IMAGE" \
        python3 -c "import os; print(len(os.listdir('/consume')))" 2>/dev/null)"
    if [ -z "$IGNORED_JSON" ]; then
        echo "error: could not read the destination directory to build the ignore list." >&2
        return 1
    fi
    (
        export FN_PAPERLESS_IGNORE="$IGNORED_JSON"
        compose --profile consumer up -d --wait --wait-timeout 420 paperless >/dev/null 2>&1
    )
}

# claim_service starts a service this invocation owns, refusing one that
# already exists rather than adopting -- and therefore later destroying --
# something another run or an operator is using.
claim_service() {
    _cs="$1"; shift
    if [ -n "$(compose --profile consumer ps -aq "$_cs" 2>/dev/null)" ]; then
        echo "error: service '$_cs' already exists; this invocation did not create it" >&2
        echo "       and will not remove it. Refusing before anything is changed." >&2
        return 1
    fi
    CREATED="$CREATED $_cs"
    (
        for _cs_kv in "$@"; do export "$_cs_kv"; done
        compose --profile consumer up -d "$_cs" >/dev/null 2>&1
    ) || {
        echo "error: could not start '$_cs'" >&2
        compose --profile consumer logs --tail 20 "$_cs" >&2 2>/dev/null || true
        return 1
    }
    return 0
}

# The ordinary renamers are stopped for the window scenario so the holding
# instance is the only consumer of the queue. The restore trap recreates them
# from the plain environment on every exit path.
stop_ordinary() { compose stop renamer-1 renamer-2 >/dev/null 2>&1; }

RESTORED=0
restore() {
    if [ "$RESTORED" = "1" ]; then return 0; fi
    RESTORED=1
    log "removing the consumer and restoring the stack"
    _r_ok=1
    for _r in $CREATED; do
        compose --profile consumer stop "$_r" >/dev/null 2>&1 || true
        compose --profile consumer rm -f -v "$_r" >/dev/null 2>&1 || true
    done
    # The consumer's own state volumes go too. They were created by this
    # exercise and hold nothing anybody else wants; leaving them behind would
    # make a later run start against a half-ingested library.
    for _v in paperless-data paperless-media paperless-redis; do
        docker volume rm "${PROJECT}_$_v" >/dev/null 2>&1 || true
    done
    _r_left="$(compose --profile consumer ps -aq paperless paperless-redis renamer-consume-hold 2>/dev/null | wc -l | tr -d ' ')"
    if [ "${_r_left:-0}" != "0" ]; then _r_ok=0; fi

    compose up -d --force-recreate --wait --wait-timeout 180 renamer-1 renamer-2 watcher >/dev/null 2>&1 || true
    _r_ready="$(compose exec -T renamer-1 /usr/local/bin/fn-renamer healthcheck >/dev/null 2>&1 && echo yes || echo no)"
    _r_readyw="$(compose exec -T watcher /usr/local/bin/fn-watcher healthcheck >/dev/null 2>&1 && echo yes || echo no)"
    if [ "$_r_ready" != "yes" ] || [ "$_r_readyw" != "yes" ]; then _r_ok=0; fi
    {
        printf '\nrestoration (read back from the running stack):\n'
        printf '  consumer containers left: %s\n' "${_r_left:-unknown}"
        printf '  renamer-1 healthcheck:    %s\n' "$_r_ready"
        printf '  watcher   healthcheck:    %s\n' "$_r_readyw"
    } >> "$OUT"
    exercise_unlock
    if [ "$_r_ok" != "1" ]; then
        echo >&2
        echo "FAILED: the stack was NOT restored. See $OUT." >&2
        printf '\nRESTORATION FAILED — see the values above.\n' >> "$OUT"
        exit 1
    fi
    note "restored: consumer removed, application services answering their own healthcheck"
}

exercise_lock consumer || exit 1
trap 'restore' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

: > "$OUT"
emit "A real consumer takes the published document, and the handoff survives it"
emit ""

BEFORE_CONSUME="$(in_storage "ls -1 /srv/fn/consume 2>/dev/null | wc -l")"
log "1/4: starting an isolated Paperless instance against the real consume directory"
start_consumer || exit 1
PAPERLESS_IMG="$(docker inspect -f '{{.Config.Image}}' "$(compose --profile consumer ps -q paperless | head -1)" 2>/dev/null || echo unknown)"

# Its web healthcheck answering is not the same as its consumer watching the
# directory, and the difference is not cosmetic: submitting during startup
# produced a real permission denial (Paperless chowns its consumption
# directory as it comes up) and the job was truthfully held. Wait until the
# directory is actually usable by the shared runtime account from inside the
# consumer, then let it work through whatever earlier runs left there -- a
# backlog it is still chewing through is a consumer that will not reach this
# exercise's document inside any window.
_i=0
CONSUMER_READY=no
while [ "$_i" -lt 180 ]; do
    if compose --profile consumer exec -T --user "${FN_RUNTIME_UID:-65532}" paperless \
        sh -c 'test -r /usr/src/paperless/consume && test -w /usr/src/paperless/consume' >/dev/null 2>&1; then
        CONSUMER_READY=yes
        break
    fi
    sleep 2; _i=$((_i + 2))
done
emit "consumer can use the shared directory: $CONSUMER_READY (after ~${_i}s)"
[ "$CONSUMER_READY" = "yes" ] || bad "the consumer cannot read and write the shared directory, so nothing below would mean anything"

emit "already in the directory: ${IGNORED_COUNT:-unknown} entries from earlier runs, all named in the"
emit "                 consumer's ignore list. This exercise did not create them, does not"
emit "                 remove them, and does not ask the consumer to work through them."
emit "consumer:        $PAPERLESS_IMG"
emit "its inbox:       /usr/src/paperless/consume  ==  the normalizer's /srv/fn/consume"
emit "isolation:       own SQLite database, own media volume, own Redis, no host port,"
emit "                 created and destroyed by this exercise"
emit "documents in the consume directory before: $BEFORE_CONSUME"
emit ""

# ---------------------------------------------------------------------------
log "2/4: publishing a document and letting the real consumer take it"
DOC="consumer-handoff-$STAMP.pdf"
NAME="consumer-handoff-$LOWER.pdf"
submit "$DOC" "$LOWER"
STATE="$(await_state "$DOC" "delivered held uncertain" 180)"
JOB="$(psqlq "SELECT job_id FROM jobs WHERE source_name = '$DOC';")"
RECEIPTS="$(psqlq "SELECT count(*) FROM delivery_receipts WHERE job_id = '$JOB';")"
DELIVERED_NAME="$(psqlq "SELECT coalesce(delivered_name,'-') FROM delivery_receipts WHERE job_id = '$JOB';")"

emit "2. the normalizer's side of the handoff"
emit "   final state:                  $STATE   (expected delivered)"
emit "   delivery receipts:            $RECEIPTS   (expected 1)"
emit "   delivered as:                 $DELIVERED_NAME"
[ "$STATE" = "delivered" ] || bad "the job reached '$STATE' with a real consumer watching the directory"
[ "${RECEIPTS:-0}" = "1" ] || bad "$RECEIPTS receipts exist for one delivery"

# The consumer takes it when it takes it. Wait for that to actually happen
# rather than assuming it: if the file is still there, the interesting case
# has not occurred and nothing below means anything.
#
# Gated on the receipt, because "the file is not there" means two opposite
# things. With a receipt it means the consumer took a document that was
# published; without one it means nothing was ever published -- and the first
# version of this check reported "the consumer removed it: yes (after ~0s)"
# for a job that had been held before it reached the directory at all.
_i=0
TAKEN=no
if [ "${RECEIPTS:-0}" = "1" ]; then
    while [ "$_i" -lt 240 ]; do
        if [ "$(in_storage "test -e '/srv/fn/consume/$NAME' && echo yes || echo no")" = "no" ]; then TAKEN=yes; break; fi
        sleep 2; _i=$((_i + 2))
    done
else
    TAKEN="not applicable (nothing was published)"
fi
INGESTED="$(compose --profile consumer exec -T paperless sh -c \
    "python3 manage.py document_exporter --help >/dev/null 2>&1; python3 -c \"
import sqlite3
db = sqlite3.connect('/usr/src/paperless/data/db.sqlite3')
print(db.execute('select count(*) from documents_document').fetchone()[0])
\"" 2>/dev/null | tr -d ' \r\n' || echo unknown)"

emit "   the consumer removed it:      $TAKEN (after ~${_i}s)"
emit "   documents in its library:     $INGESTED"
[ "$TAKEN" = "yes" ] || bad "the real consumer never took the file, so the disappearance case was not exercised"
emit ""

# ---------------------------------------------------------------------------
log "3/4: the delivery must stand after the consumer has taken the file"
sleep 5
STATE_AFTER="$(psqlq "SELECT state FROM jobs WHERE job_id = '$JOB';")"
CAT_AFTER="$(psqlq "SELECT coalesce(failure_category,'-') FROM jobs WHERE job_id = '$JOB';")"
RECEIPTS_AFTER="$(psqlq "SELECT count(*) FROM delivery_receipts WHERE job_id = '$JOB';")"
REPUB="$(in_storage "ls -1 /srv/fn/consume 2>/dev/null | grep -c 'consumer-handoff-$LOWER' || true")"
SRC_AFTER="$(in_storage "test -f '/srv/fn/incoming/$DOC' && echo yes || echo no")"

emit "3. after the consumer took the document"
emit "   state:                        $STATE_AFTER   (expected delivered: a taken file is not a failure)"
emit "   category:                     $CAT_AFTER   (expected '-')"
emit "   delivery receipts:            $RECEIPTS_AFTER   (expected 1: not withdrawn)"
emit "   copies in the directory:      $REPUB   (expected 0: taken, and not delivered again)"
emit "   source preserved:             $SRC_AFTER"
[ "$STATE_AFTER" = "delivered" ] || bad "a delivered job became '$STATE_AFTER' once the consumer took the file"
[ "$CAT_AFTER" = "-" ] || bad "a completed delivery acquired the failure category '$CAT_AFTER'"
[ "${RECEIPTS_AFTER:-0}" = "1" ] || bad "the receipt count changed to $RECEIPTS_AFTER after the consumer acted"
# As in scenario 4: "none left" is only the right expectation once the consumer
# has actually taken it. If it did not (already a mismatch above), the property
# that still has to hold is that one publication produced one copy.
if [ "$TAKEN" = "yes" ]; then
    [ "${REPUB:-0}" = "0" ] || bad "$REPUB copies remain after the consumer took the document"
else
    [ "${REPUB:-0}" -le 1 ] || bad "$REPUB copies exist for one publication"
fi
[ "$SRC_AFTER" = "yes" ] || bad "the source was removed"

# And a redelivery afterwards must settle against the existing outcome rather
# than deciding the destination is missing.
DELIV_BEFORE="$(psqlq "SELECT count(*) FROM job_events WHERE job_id = '$JOB' AND event_type = 'delivery_received';")"
compose exec -T rabbitmq rabbitmqadmin \
    --vhost "${FN_AMQP_VHOST:-filename-normalizer}" \
    --username "${FN_AMQP_USER:-fn_app}" \
    --password "$(cat "$DEPLOY_DIR/secrets/fn_amqp_password")" \
    --non-interactive \
    publish message \
    --exchange "${FN_AMQP_EXCHANGE:-filename_normalizer.jobs}" \
    --routing-key "${FN_AMQP_ROUTING_KEY:-normalize}" \
    --properties '{"delivery_mode":2,"content_type":"application/json"}' \
    --payload "{\"contract_version\":1,\"job_id\":\"$JOB\",\"attempt\":99,\"enqueued_at\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" >/dev/null 2>&1 \
    && REPUB_MSG=yes || REPUB_MSG=no
_i=0
DELIV_AFTER="$DELIV_BEFORE"
while [ "$_i" -lt 60 ]; do
    DELIV_AFTER="$(psqlq "SELECT count(*) FROM job_events WHERE job_id = '$JOB' AND event_type = 'delivery_received';")"
    if [ "${DELIV_AFTER:-0}" -gt "${DELIV_BEFORE:-0}" ]; then break; fi
    sleep 1; _i=$((_i + 1))
done
sleep 4
STATE_REDELIV="$(psqlq "SELECT state FROM jobs WHERE job_id = '$JOB';")"
REPUB2="$(in_storage "ls -1 /srv/fn/consume 2>/dev/null | grep -c 'consumer-handoff-$LOWER' || true")"
RECEIPTS_REDELIV="$(psqlq "SELECT count(*) FROM delivery_receipts WHERE job_id = '$JOB';")"

emit "   a later delivery was published:      $REPUB_MSG"
emit "   delivery_received events:            $DELIV_BEFORE -> $DELIV_AFTER"
emit "   state after that delivery:           $STATE_REDELIV   (expected delivered)"
emit "   receipts after it:                   $RECEIPTS_REDELIV   (expected 1)"
emit "   files republished by it:             $REPUB2   (expected 0)"
[ "$REPUB_MSG" = "yes" ] || bad "the redelivery could not be published"
[ "${DELIV_AFTER:-0}" -gt "${DELIV_BEFORE:-0}" ] || bad "no further delivery arrived, so nothing was proven about redelivery after consumption"
[ "$STATE_REDELIV" = "delivered" ] || bad "a redelivery after consumption moved the job to '$STATE_REDELIV'"
[ "${RECEIPTS_REDELIV:-0}" = "1" ] || bad "a redelivery after consumption changed the receipt count to $RECEIPTS_REDELIV"
[ "${REPUB2:-0}" = "0" ] || bad "a redelivery after consumption published the document again"
emit ""

# ---------------------------------------------------------------------------
# Scenarios 2 and 3 let the consumer act on its own schedule, which is after
# the receipt exists. The case the review raised is narrower and worse: the
# consumer takes the document BETWEEN the successful link and the moment the
# publisher reads the destination back. That read then returns ENOENT for a
# publication that definitely happened.
#
# That window is microseconds wide, so this renamer holds it open. The hold is
# the only synthetic thing here: the link is real, the ingestion is real, and
# the removal is Paperless's own doing on its own schedule.
log "4/4: the consumer takes the document INSIDE the publication window"
stop_ordinary
claim_service renamer-consume-hold || exit 1

DOC4="consumer-window-$STAMP.pdf"
NAME4="consumer-window-$LOWER.pdf"
submit "$DOC4" "w$LOWER"
STATE4="$(await_state "$DOC4" "delivered held uncertain" 300)"
JOB4="$(psqlq "SELECT job_id FROM jobs WHERE source_name = '$DOC4';")"
PAUSED4="$(compose --profile consumer logs renamer-consume-hold 2>/dev/null | grep -c fault_point_paused || true)"
CONSUMED4="$(compose --profile consumer logs renamer-consume-hold 2>/dev/null | grep -c destination_consumed_immediately || true)"
RECEIPTS4="$(psqlq "SELECT count(*) FROM delivery_receipts WHERE job_id = '$JOB4';")"
ABSENT4="$(psqlq "SELECT count(*) FROM delivery_receipts WHERE job_id = '$JOB4' AND absent_observed_at IS NOT NULL;")"
CAT4="$(psqlq "SELECT coalesce(failure_category,'-') FROM jobs WHERE job_id = '$JOB4';")"
REPUB4="$(in_storage "ls -1 /srv/fn/consume 2>/dev/null | grep -c 'consumer-window-$LOWER' || true")"
SRC4="$(in_storage "test -f '/srv/fn/incoming/$DOC4' && echo yes || echo no")"

emit "4. the consumer took it between the link and the read-back"
emit "   the publisher held the window open: $PAUSED4 log line(s)"
emit "   the destination was gone when read:  $CONSUMED4 log line(s)   (expected 1)"
emit "   final state:                  $STATE4   (expected delivered: the link succeeded)"
emit "   category:                     $CAT4   (expected '-': not a missing source)"
emit "   delivery receipts:            $RECEIPTS4   (expected 1)"
emit "   receipt records the absence:  $ABSENT4   (expected 1: observed, not reinterpreted)"
emit "   copies in the directory:      $REPUB4   (expected 0: taken, and not put back)"
emit "   source preserved:             $SRC4   (expected yes)"
[ "${PAUSED4:-0}" -ge 1 ] || bad "the publisher never paused, so the window was never held open"
[ "${CONSUMED4:-0}" -ge 1 ] || bad "the consumer did not take the document inside the window; the case was NOT exercised"
[ "$STATE4" = "delivered" ] || bad "a publication whose link succeeded ended as '$STATE4'"
[ "$CAT4" = "-" ] || bad "a completed delivery acquired the failure category '$CAT4'"
[ "${RECEIPTS4:-0}" = "1" ] || bad "$RECEIPTS4 receipts exist for one delivery"
[ "${ABSENT4:-0}" = "1" ] || bad "the receipt does not record that the delivered file was already gone"
# Two different claims, so two different checks. If the consumer took the
# document, nothing may be left; if it did not (already a mismatch above),
# there must still never be more than the one copy that was published.
if [ "${CONSUMED4:-0}" -ge 1 ]; then
    [ "${REPUB4:-0}" = "0" ] || bad "$REPUB4 copies remain after the consumer took the document"
else
    [ "${REPUB4:-0}" -le 1 ] || bad "$REPUB4 copies exist for one publication"
fi
[ "$SRC4" = "yes" ] || bad "the source was removed"
emit ""

# Nothing this exercise did not create may have been consumed or removed.
LEFT_ALONE="$(in_storage "ls -1 /srv/fn/consume 2>/dev/null | grep -vc '^consumer-' || true")"
emit "documents from earlier runs still in the directory: ${LEFT_ALONE:-unknown} of ${IGNORED_COUNT:-unknown}"
[ "${LEFT_ALONE:-0}" = "${IGNORED_COUNT:-0}" ] || bad "the consumer took ${IGNORED_COUNT} - ${LEFT_ALONE} documents this exercise did not create"
emit ""

emit "The handoff is a filesystem handoff and it ends at the link. A real"
emit "consumer took the document out of the directory on its own schedule; the"
emit "delivery stood, the receipt stood, and neither the disappearance nor a"
emit "later redelivery produced a second copy or reopened a finished job."
emit ""
emit "mismatches: $FAILURES"

if [ "$FAILURES" -ne 0 ]; then
    echo >&2
    echo "FAILED: $FAILURES expectation(s) not met; see $OUT" >&2
    exit 1
fi

log "PASSED: the handoff to a real consumer holds"
note "evidence: $OUT"
