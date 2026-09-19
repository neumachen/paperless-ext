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
# Empty until start_consumer establishes which volumes it created. `set -u` is
# on, and restore runs on every exit path including a refusal before that.
CREATED_VOLUMES=""

emit() { printf '%s\n' "$*" >> "$OUT"; printf '%s\n' "$*"; }
bad() { printf '    MISMATCH: %s\n' "$*" >&2; emit "    MISMATCH: $*"; FAILURES=$((FAILURES + 1)); }

# psqlq runs one read-only query and returns its rows.
#
# It used to pipe through `tr -d ' \r'`, which deletes every SPACE in the
# result. With `psql -tA` there is no padding to strip, so the spaces it
# removed were always part of the DATA. Two things depended on values that
# contain one:
#
#   * T0, captured as `SELECT now()`, became `2026-09-1913:33:38.868306+00`.
#     Every later query comparing `delivered_at >= '$T0'` then failed to parse
#     it, returned nothing, and was read as zero through `${ARRIVED:-0}` -- so
#     the check that no document belonging to other work had been ingested was
#     answered by a query that never ran.
#   * a delivered name containing a space came back joined up, so the probe for
#     it reported the document missing when it was there.
#
# Only carriage returns are removed now. The timestamp is also taken in a
# space-free ISO form below, so the value does not depend on this at all.
psqlq() {
    compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" \
        postgres-primary psql -U "${FN_DB_USER:-fn_app}" -d "${FN_DB_NAME:-filename_normalizer}" -tA -c "$1" \
        2>/dev/null | tr -d '\r'
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
    # A Paperless library that already exists is not this exercise's to use.
    #
    # Excluding a pre-existing volume from the removal list stopped the
    # exercise DELETING somebody's library, and no more than that: it still
    # started Paperless against it, so the instance ingested documents into an
    # existing media store, wrote to an existing database, and left both
    # changed. Not-deleting is not not-touching. A library this invocation did
    # not create means this invocation does not run.
    _sc_existing=""
    for _v in paperless-data paperless-media; do
        if [ -n "$(docker volume ls -q --filter "name=^${PROJECT}_${_v}$" 2>/dev/null)" ]; then
            _sc_existing="$_sc_existing ${PROJECT}_${_v}"
        fi
    done
    if [ -n "$_sc_existing" ]; then
        echo "error: Paperless state already exists:$_sc_existing" >&2
        echo "       This invocation did not create it, will not ingest into it," >&2
        echo "       and will not remove it. Refusing before anything is changed." >&2
        return 1
    fi

    # Past this point the stack is being changed, so restoration has something
    # to restore. Before it, restoration must do nothing: see MUTATED.
    MUTATED=1
    # Only volumes that did not exist before this invocation may be removed
    # afterwards. The cleanup used to delete three fixed names unconditionally,
    # so a pre-existing Paperless instance -- the very thing the refusal above
    # protects -- lost its database and media the moment this exercise ran, and
    # so did a run that refused to start at all.
    CREATED_VOLUMES=""
    for _v in paperless-data paperless-media paperless-redis; do
        if [ -z "$(docker volume ls -q --filter "name=^${PROJECT}_${_v}$" 2>/dev/null)" ]; then
            CREATED_VOLUMES="$CREATED_VOLUMES $_v"
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
stop_ordinary() { MUTATED=1; compose stop renamer-1 renamer-2 >/dev/null 2>&1; }

RESTORED=0
# Set the moment this invocation first changes the running stack. Until then a
# restoration is not harmless: the preflight refuses a consumer service it did
# not create, and "restoring" afterwards force-recreated renamer-1, renamer-2
# and the watcher -- three services this invocation had not touched, dropping
# whatever they were doing -- while reporting that a refusal had changed
# nothing. The configuration exercise had exactly this defect and this is the
# same guard.
MUTATED=0
restore() {
    if [ "$RESTORED" = "1" ]; then return 0; fi
    RESTORED=1
    if [ "$MUTATED" = "0" ]; then
        log "nothing was changed; leaving the stack alone"
        exercise_unlock
        return 0
    fi
    log "removing the consumer and restoring the stack"
    _r_ok=1
    for _r in $CREATED; do
        compose --profile consumer stop "$_r" >/dev/null 2>&1 || true
        compose --profile consumer rm -f -v "$_r" >/dev/null 2>&1 || true
    done
    # The consumer's own state volumes go too -- but only the ones this
    # invocation created, and only when nothing belonging to other work is
    # inside them.
    #
    # The ignore list is a snapshot taken before the consumer starts, so a
    # document delivered by other work AFTER it cannot be in that list and is
    # eligible for ingestion. Destroying the media volume then destroys the only
    # remaining copy of somebody else's document: the consumer removed it from
    # the directory and this exercise would remove it from the library. So the
    # last check before the irreversible step is whether that happened, and if
    # it did the volumes stay, named, for a person to recover from.
    # Destroying these volumes is irreversible, so it happens only on a
    # definite "nothing of anyone else's is in here". `unknown` keeps them.
    _r_verdict="$(foreign_content_verdict)"
    if [ "$_r_verdict" != "none" ]; then
        if [ "$_r_verdict" = "present" ]; then
            echo "error: document(s) belonging to other work were consumed by this" >&2
            echo "       exercise's Paperless instance. Its volumes are NOT being removed:" >&2
        else
            echo "error: whether documents belonging to other work are inside this" >&2
            echo "       exercise's Paperless instance COULD NOT BE ESTABLISHED. Its" >&2
            echo "       volumes are NOT being removed:" >&2
        fi
        for _v in $CREATED_VOLUMES; do echo "         ${PROJECT}_$_v" >&2; done
        {
            printf '  volumes KEPT (foreign content: %s)\n' "$_r_verdict"
            printf '  restoration is INCOMPLETE; the copies are inside:%s\n' "$CREATED_VOLUMES"
        } >> "$OUT"
        CREATED_VOLUMES=""
        _r_ok=0
    fi
    for _v in $CREATED_VOLUMES; do
        docker volume rm "${PROJECT}_$_v" >/dev/null 2>&1 || true
        if [ -n "$(docker volume ls -q --filter "name=^${PROJECT}_${_v}$" 2>/dev/null)" ]; then
            echo "error: volume ${PROJECT}_${_v} could not be removed." >&2
            _r_ok=0
        fi
    done
    if [ -n "$(printf '%s' "$CREATED_VOLUMES" | tr -d ' ')" ]; then
        printf '  volumes created and removed by this run:%s\n' "$CREATED_VOLUMES" >> "$OUT"
    else
        printf '  volumes: none created by this run; none removed\n' >> "$OUT"
    fi
    _r_left="$(compose --profile consumer ps -aq paperless paperless-redis renamer-consume-hold 2>/dev/null | wc -l | tr -d ' ')"
    if [ "${_r_left:-0}" != "0" ]; then _r_ok=0; fi

    compose up -d --force-recreate --wait --wait-timeout 180 renamer-1 renamer-2 watcher >/dev/null 2>&1 || true
    # Every service this invocation recreated, and READINESS for each.
    # renamer-2 was recreated here and never checked, so a worker left unable
    # to reach its dependencies would have been reported as a restored stack;
    # and plain `healthcheck` reads /healthz, which a process answers while
    # every dependency it needs is unreachable.
    _r_ready="$(compose exec -T renamer-1 /usr/local/bin/fn-renamer healthcheck --require-ready >/dev/null 2>&1 && echo yes || echo no)"
    _r_ready2="$(compose exec -T renamer-2 /usr/local/bin/fn-renamer healthcheck --require-ready >/dev/null 2>&1 && echo yes || echo no)"
    _r_readyw="$(compose exec -T watcher /usr/local/bin/fn-watcher healthcheck --require-ready >/dev/null 2>&1 && echo yes || echo no)"
    if [ "$_r_ready" != "yes" ] || [ "$_r_ready2" != "yes" ] || [ "$_r_readyw" != "yes" ]; then _r_ok=0; fi
    {
        printf '\nrestoration (read back from the running stack):\n'
        printf '  consumer containers left: %s\n' "${_r_left:-unknown}"
        printf '  renamer-1 ready:          %s\n' "$_r_ready"
        printf '  renamer-2 ready:          %s\n' "$_r_ready2"
        printf '  watcher   ready:          %s\n' "$_r_readyw"
    } >> "$OUT"
    exercise_unlock
    if [ "$_r_ok" != "1" ]; then
        echo >&2
        echo "FAILED: the stack was NOT restored. See $OUT." >&2
        printf '\nRESTORATION FAILED — see the values above.\n' >> "$OUT"
        exit 1
    fi
    report_restored
    note "restored: consumer removed, every recreated service reports itself READY"
}

exercise_lock consumer || exit 1
trap 'restore; report_keep' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

report_begin "consumer" "$OUT" "$0"
emit "A real consumer takes the published document, and the handoff survives it"
emit ""

# The moment before the consumer exists, so deliveries made by other work
# while it runs can be told from what was already there.
# foreign_content_verdict answers one question -- is anything belonging to other
# work inside this consumer's library? -- with three possible answers, and
# `unknown` is one of them.
#
# # Why it cannot default to "none"
#
# The restoration path used to read the count with
#
#     _r_foreign="$(psqlq "SELECT count(*) ..." 2>/dev/null)"
#
# and test "${_r_foreign:-0}" -gt 0. PostgreSQL being unavailable during
# cleanup -- which is ordinary, since cleanup runs after fault scenarios -- made
# that zero, skipped the enumeration entirely, and went on to destroy the media
# volumes. A document another job delivered and this consumer ingested would
# have been in them. The same held for the per-name probe: anything that was not
# the literal string "no" was read as "still present", so a probe that could not
# run also cleared the way.
#
# Two sources, because the receipt query only sees what it can enumerate:
# receipts written since T0 for non-consumer sources, and separately every entry
# that was in the destination before this exercise began. A document that was
# delivered earlier and consumed now appears only in the second.
#
# Any step that cannot answer makes the whole verdict `unknown`, and `unknown`
# preserves.
foreign_content_verdict() {
    _fv_count="$(psqlq "SELECT count(*) FROM delivery_receipts r JOIN jobs j USING (job_id)
                         WHERE r.delivered_at >= '$T0' AND j.source_name NOT LIKE 'consumer-%';" 2>/dev/null | tr -d ' \r\n')"
    case "$_fv_count" in
        ''|*[!0-9]*) printf 'unknown'; return 0 ;;
    esac
    if [ "$_fv_count" -gt 0 ]; then
        _fv_names="$(psqlq "SELECT r.delivered_name FROM delivery_receipts r JOIN jobs j USING (job_id)
                             WHERE r.delivered_at >= '$T0' AND j.source_name NOT LIKE 'consumer-%';" 2>/dev/null)"
        _fv_seen=0
        for _fv_n in $_fv_names; do
            _fv_seen=$((_fv_seen + 1))
            case "$(probe_exists "/srv/fn/consume/$_fv_n")" in
                no)      printf 'present'; return 0 ;;
                unknown) printf 'unknown'; return 0 ;;
            esac
        done
        # The count said there are rows and the listing produced none: the
        # second query failed where the first succeeded.
        [ "$_fv_seen" -ge "$_fv_count" ] || { printf 'unknown'; return 0; }
    fi

    # Everything that was here before this exercise started must still be here.
    if [ -z "$IGNORED_JSON" ]; then
        printf 'unknown'; return 0
    fi
    _fv_gone="$(printf '%s' "$IGNORED_JSON" | docker run --rm -i \
        -v "${PROJECT}_fn-consume:/consume:ro" "$UTIL_PY_IMAGE" python3 -c "
import json, os, sys
before = set(json.load(sys.stdin))
now = set(os.listdir('/consume'))
missing = [n for n in sorted(before - now) if not n.startswith('consumer-')]
print(len(missing))" 2>/dev/null | tr -d ' \r\n')"
    case "$_fv_gone" in
        ''|*[!0-9]*) printf 'unknown'; return 0 ;;
        0)           printf 'none' ;;
        *)           printf 'present' ;;
    esac
}

# Space-free by construction, so no downstream trimming can corrupt it.
T0="$(psqlq "SELECT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.USZ');")"
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
        if [ "$(probe_exists "/srv/fn/consume/$NAME")" = "no" ]; then TAKEN=yes; break; fi
        sleep 2; _i=$((_i + 2))
    done
else
    TAKEN="not applicable (nothing was published)"
fi
# WHICH document it ingested, not how many it holds. A count proves only that
# something arrived; the claim is that THIS exercise's document was taken by
# the consumer, so the consumer is asked for the original filename it recorded.
INGESTED_THIS="$(compose --profile consumer exec -T paperless sh -c \
    "python3 -c \"
import sqlite3
db = sqlite3.connect('/usr/src/paperless/data/db.sqlite3')
print(db.execute(
    'select count(*) from documents_document where original_filename = ?',
    ('$NAME',)).fetchone()[0])
\"" 2>/dev/null | tr -d ' \r\n' || echo unknown)"
INGESTED="$(compose --profile consumer exec -T paperless sh -c \
    "python3 -c \"
import sqlite3
db = sqlite3.connect('/usr/src/paperless/data/db.sqlite3')
print(db.execute('select count(*) from documents_document').fetchone()[0])
\"" 2>/dev/null | tr -d ' \r\n' || echo unknown)"

emit "   the consumer removed it:      $TAKEN (after ~${_i}s)"
emit "   documents in its library:     $INGESTED"
emit "   THIS document in its library: $INGESTED_THIS   (expected 1, matched by original filename)"
[ "$TAKEN" = "yes" ] || bad "the real consumer never took the file, so the disappearance case was not exercised"
[ "${INGESTED_THIS:-0}" = "1" ] || bad "the consumer's library does not hold this exercise's document ($INGESTED_THIS); its disappearance is unexplained"
emit ""

# ---------------------------------------------------------------------------
log "3/4: the delivery must stand after the consumer has taken the file"
sleep 5
STATE_AFTER="$(psqlq "SELECT state FROM jobs WHERE job_id = '$JOB';")"
CAT_AFTER="$(psqlq "SELECT coalesce(failure_category,'-') FROM jobs WHERE job_id = '$JOB';")"
RECEIPTS_AFTER="$(psqlq "SELECT count(*) FROM delivery_receipts WHERE job_id = '$JOB';")"
REPUB="$(in_storage "ls -1 /srv/fn/consume 2>/dev/null | grep -c 'consumer-handoff-$LOWER' || true")"
SRC_AFTER="$(probe_exists "/srv/fn/incoming/$DOC")"

emit "3. after the consumer took the document"
emit "   state:                        $STATE_AFTER   (expected delivered: a taken file is not a failure)"
emit "   category:                     $CAT_AFTER   (expected '-')"
emit "   delivery receipts:            $RECEIPTS_AFTER   (expected 1: not withdrawn)"
PUBLISH_EVENTS="$(psqlq "SELECT count(*) FROM job_events WHERE job_id = '$JOB' AND event_type = 'publish_attempted';")"
emit "   copies in the directory:      $REPUB   (expected 0: taken, and not delivered again)"
emit "   publication attempts recorded:$PUBLISH_EVENTS   (expected 1: a directory count cannot"
emit "                                 prove this, because the consumer empties it)"
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
# The durable count is what rules out a second publication. An empty directory
# is equally consistent with "published once" and "published twice and both
# consumed", so it cannot carry this claim on its own.
[ "${PUBLISH_EVENTS:-0}" = "1" ] || bad "$PUBLISH_EVENTS publication attempts are recorded for one job"
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
# After the redelivery, the same two durable questions as before it: how many
# publication attempts the ledger recorded, and whether the consumer's library
# still holds exactly this exercise's fixture. A directory that is empty because
# the consumer emptied it cannot answer either.
ATTEMPTS_AFTER_REDELIVERY="$(psqlq "SELECT count(*) FROM job_events WHERE job_id = '$JOB' AND event_type = 'publish_attempted';")"
INGESTED_AFTER="$(compose --profile consumer exec -T paperless sh -c \
    "python3 -c \"
import sqlite3
db = sqlite3.connect('/usr/src/paperless/data/db.sqlite3')
print(db.execute(
    'select count(*) from documents_document where original_filename = ?',
    ('$NAME',)).fetchone()[0])
\"" 2>/dev/null | tr -d ' \r\n' || echo unknown)"
emit "   publication attempts recorded:       $ATTEMPTS_AFTER_REDELIVERY   (expected 1: still one)"
emit "   this fixture in the library:         $INGESTED_AFTER   (expected 1: still exactly one)"
[ "${ATTEMPTS_AFTER_REDELIVERY:-0}" = "1" ] || bad "$ATTEMPTS_AFTER_REDELIVERY publication attempts after the redelivery"
[ "${INGESTED_AFTER:-0}" = "1" ] || bad "the consumer's library holds $INGESTED_AFTER copies of this fixture after the redelivery"
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
SRC4="$(probe_exists "/srv/fn/incoming/$DOC4")"

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
ATTEMPTS4="$(psqlq "SELECT count(*) FROM job_events WHERE job_id = '$JOB4' AND event_type = 'publish_attempted';")"
INGESTED4="$(compose --profile consumer exec -T paperless sh -c \
    "python3 -c \"
import sqlite3
db = sqlite3.connect('/usr/src/paperless/data/db.sqlite3')
print(db.execute(
    'select count(*) from documents_document where original_filename = ?',
    ('$NAME4',)).fetchone()[0])
\"" 2>/dev/null | tr -d ' \r\n' || echo unknown)"
emit "   publication attempts recorded:$ATTEMPTS4   (expected 1: the race produced one publication)"
emit "   this fixture in the library:  $INGESTED4   (expected 1: the consumer took it once)"
[ "${ATTEMPTS4:-0}" = "1" ] || bad "$ATTEMPTS4 publication attempts recorded for the raced job"
[ "${INGESTED4:-0}" = "1" ] || bad "the consumer's library holds $INGESTED4 copies of the raced fixture"
emit ""

# Nothing this exercise did not create may have been consumed or removed.
#
# The NAMES are compared, not two counts. Counting could not carry this claim
# and quietly failed on it: the ignore list was built with os.listdir(), which
# includes the `.fn-*` temporaries other exercises strand in the destination,
# while the closing count was `ls -1 | grep -vc '^consumer-'`, which lists no
# dotfiles and also drops entries an EARLIER run of this exercise left behind.
# Both differences read as documents the consumer had eaten -- 311 before
# against 301 after, with nothing actually missing.
GONE_JSON="$(printf '%s' "$IGNORED_JSON" | docker run --rm -i \
    -v "${PROJECT}_fn-consume:/consume:ro" "$UTIL_PY_IMAGE" python3 -c "
import json, os, sys
before = set(json.load(sys.stdin))
now = set(os.listdir('/consume'))
print(json.dumps(sorted(before - now)))" 2>/dev/null || echo unreadable)"
if [ "$GONE_JSON" = "unreadable" ]; then
    bad "the destination directory could not be read back, so nothing was established about what survived"
    GONE_COUNT=unknown
else
    GONE_COUNT="$(printf '%s' "$GONE_JSON" | docker run --rm -i "$UTIL_PY_IMAGE" \
        python3 -c "import json,sys; print(len(json.load(sys.stdin)))" 2>/dev/null || echo unknown)"
fi
emit "entries present before this exercise:               ${IGNORED_COUNT:-unknown}"
emit "of those, missing afterwards:                      ${GONE_COUNT:-unknown}   (expected 0, compared by name)"
if [ "${GONE_COUNT:-1}" != "0" ]; then
    emit "   missing: $GONE_JSON"
    bad "${GONE_COUNT} entries this exercise did not create are gone from the destination"
fi

# The ignore list is a snapshot taken before the consumer started, so it cannot
# protect a document some OTHER work delivers while this exercise is running:
# that document is not in the list, and the consumer is entitled to ingest it
# into a media store this exercise destroys on the way out. Nothing here can
# stop that from a snapshot, so it is detected and reported rather than assumed
# not to happen.
ARRIVED="$(psqlq "SELECT count(*) FROM delivery_receipts r JOIN jobs j USING (job_id)
                   WHERE r.delivered_at >= '$T0' AND j.source_name NOT LIKE 'consumer-%';")"
FOREIGN_VERDICT="$(foreign_content_verdict)"
ARRIVED_GONE=0
case "$FOREIGN_VERDICT" in
    present) ARRIVED_GONE=1 ;;
    unknown) bad "whether other work's documents were ingested could not be established; \
the consumer's data is preserved and restoration is incomplete" ;;
esac
emit "documents other work delivered while this ran:     ${ARRIVED:-unknown}"
emit "anything of other work's inside this consumer:     $FOREIGN_VERDICT   (expected none)"
# A flag, not a count: the verdict establishes that something of other work's
# is inside, not how much. Claiming a number here would be inventing one.
[ "${ARRIVED_GONE:-0}" = "0" ] || bad "document(s) belonging to other work were ingested by this exercise's consumer"
# The ignore list is a snapshot and cannot name a document that had not arrived
# when it was taken. That is a real hole and it is reported as one rather than
# left to the reader: if other work delivered while this ran, those documents
# were eligible for ingestion into a media store this exercise destroys.
if [ "${ARRIVED:-0}" -gt 0 ]; then
    emit "   NOTE: those ${ARRIVED} arrived after the ignore list was taken, so the"
    emit "   list could not protect them. What protects them is the check above and"
    emit "   the refusal to destroy this consumer's volumes when any of them was"
    emit "   ingested -- not the list."
else
    emit "   This run observed NO unrelated arrivals, so it establishes the handoff"
    emit "   and NOT isolation under new arrivals: an arrival after the snapshot is"
    emit "   eligible for ingestion, and what this exercise guarantees is that its"
    emit "   media is then preserved rather than destroyed."
fi
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

report_success
log "PASSED: the handoff to a real consumer holds"
note "evidence: $OUT"
