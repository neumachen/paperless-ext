#!/bin/sh
# Fire the refusal that keeps another job's documents out of a disposable
# consumer's grave.
#
# # Why this is separate from verify-consumer.sh
#
# That exercise runs a real Paperless instance against the SHARED consume
# directory and destroys its media volumes afterwards. Anything delivered
# while it runs is not in the ignore list it took at startup, so the consumer
# may ingest it -- and then destroying those volumes would destroy the only
# remaining copy of somebody else's document. It guards that with
# `foreign_content_verdict`: on anything but a definite "none" the volumes are
# kept, named, for a person to recover from.
#
# Every run of it has reported "This run observed NO unrelated arrivals", so
# the guard has never fired. An untested refusal is a comment. This exercise
# creates the condition deliberately and asserts the refusal happens.
#
# # Why the fixture is honestly "unrelated"
#
# The detector's rule is `source_name NOT LIKE 'consumer-%'`. A fixture named
# `isolation-probe-<stamp>` satisfies it, so to every check in that exercise it
# IS an unrelated arrival -- the detector cannot tell, and that is the point.
# This exercise separately knows it created the fixture, which is the ONLY
# reason it may clean the volumes up afterwards; it proves that before doing
# it, and keeps them if it cannot.
. "$(dirname "$0")/lib.sh"

OUT="$EVIDENCE_DIR/consumer-isolation.txt"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
LOWER="$(printf '%s' "$STAMP" | tr 'A-Z' 'a-z')"
# NO PROJECT OVERRIDE HERE.
#
# lib.sh already resolves the compose project (COMPOSE_PROJECT_NAME, else the
# .env value, else fnfoundation). Setting it again here to a plausible-looking
# default got it wrong -- the real project is `fnfoundation` -- and every
# volume name built from it then named something that did not exist.
#
# `docker run -v <name>:/path` CREATES <name> when it is absent. So the ignore
# list was read from a volume this line had just created empty, came back as
# `[]`, and the consumer was handed the entire shared directory as its backlog
# instead of ignoring it. It ingested 15 documents belonging to earlier
# exercises. The refuse-if-exists guards missed it for the same reason: they
# were looking for `filename-normalizer_paperless-*`, and the volumes compose
# actually made were `fnfoundation_paperless-*`.
UTIL_PY_IMAGE="${FN_UTIL_PY_IMAGE:-python:3.12-alpine}"
FAILURES=0
CREATED_VOLUMES=""
MUTATED=0
DOC="isolation-probe-$LOWER.pdf"
report_begin consumer-isolation "$OUT" "$0"

emit() { printf '%s\n' "$*" >> "$OUT"; printf '%s\n' "$*"; }
bad() { printf '    MISMATCH: %s\n' "$*" >&2; emit "    MISMATCH: $*"; FAILURES=$((FAILURES + 1)); }
psqlq() {
    compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" \
        postgres-primary psql -U "${FN_DB_USER:-fn_app}" -d "${FN_DB_NAME:-filename_normalizer}" \
        -tA -c "$1" < /dev/null | tr -d '\r' | sed '/^$/d'
}
psqln() { psqlq "$1" | tr -d ' \n'; }
in_storage() {
    compose run --rm --no-deps -T --entrypoint sh storage-init -c "$1" 2>/dev/null < /dev/null | tr -d '\r'
}

emit "The refusal that protects another job's documents, actually fired"
emit ""

# --- claim, or refuse ------------------------------------------------------
for _sc in paperless-redis paperless; do
    if [ -n "$(compose --profile consumer ps -aq "$_sc" 2>/dev/null)" ]; then
        echo "error: '$_sc' already exists; this invocation did not create it and will not remove it." >&2
        exit 1
    fi
done
_existing=""
for _v in paperless-data paperless-media paperless-redis; do
    if [ -n "$(docker volume ls -q --filter "name=^${PROJECT}_${_v}$" 2>/dev/null)" ]; then
        _existing="$_existing ${PROJECT}_${_v}"
    fi
done
if [ -n "$_existing" ]; then
    echo "error: Paperless state already exists:$_existing" >&2
    echo "       This invocation did not create it and will not touch it." >&2
    exit 1
fi

restore() {
    [ "$MUTATED" = "1" ] || return 0
    compose --profile consumer stop paperless paperless-redis >/dev/null 2>&1 || true
    compose --profile consumer rm -f paperless paperless-redis >/dev/null 2>&1 || true
    # Volumes are removed only on proof that the single foreign-looking
    # delivery is this run's own fixture. Anything else and they stay.
    if [ "$SAFE_TO_REMOVE" = "1" ]; then
        for _v in $CREATED_VOLUMES; do docker volume rm "${PROJECT}_$_v" >/dev/null 2>&1 || true; done
        emit "   volumes created and removed by this run:$CREATED_VOLUMES"
        emit "   (removable only because the one foreign-looking delivery is this"
        emit "    run's own fixture, asserted above)"
    else
        emit "   volumes KEPT:$CREATED_VOLUMES"
        emit "   restoration is INCOMPLETE, deliberately: this run could not prove the"
        emit "   ingested documents were all its own."
    fi
    compose up -d --wait --wait-timeout 180 renamer-1 renamer-2 watcher >/dev/null 2>&1 || true
}
SAFE_TO_REMOVE=0
trap 'restore' EXIT INT TERM

for _v in paperless-data paperless-media paperless-redis; do
    CREATED_VOLUMES="$CREATED_VOLUMES $_v"
done

# Space-free by construction, so no downstream trimming can corrupt it.
T0="$(psqlq "SELECT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.USZ');" | tr -d ' ')"
# The volume must already exist. Naming a volume that does not is not a
# lookup failure here -- docker creates it, and the empty listing that follows
# is indistinguishable from a genuinely empty directory.
if [ -z "$(docker volume ls -q --filter "name=^${PROJECT}_fn-consume$" 2>/dev/null)" ]; then
    echo "error: volume ${PROJECT}_fn-consume does not exist; refusing to let" >&2
    echo "       'docker run -v' create it and read an empty ignore list from it." >&2
    exit 1
fi
IGNORED_JSON="$(docker run --rm -v "${PROJECT}_fn-consume:/consume:ro" "$UTIL_PY_IMAGE" \
    python3 -c "import json,os; print(json.dumps(sorted(os.listdir('/consume'))))" 2>/dev/null)"
[ -n "$IGNORED_JSON" ] || { echo "error: could not read the destination directory." >&2; exit 1; }
# An empty ignore list against a non-empty directory means the consumer would
# be handed everything already there. That is the condition that caused the
# damage above, so it is refused rather than reported.
IGNORED_N="$(printf '%s' "$IGNORED_JSON" | docker run --rm -i "$UTIL_PY_IMAGE" \
    python3 -c 'import json,sys; print(len(json.load(sys.stdin)))' 2>/dev/null | tr -d ' \r\n')"
DIR_N="$(in_storage "ls -1 /srv/fn/consume 2>/dev/null | wc -l" | tr -d ' ')"
case "$IGNORED_N" in ''|*[!0-9]*) echo "error: the ignore list size is unreadable." >&2; exit 1 ;; esac
case "$DIR_N" in ''|*[!0-9]*) echo "error: the destination directory size is unreadable." >&2; exit 1 ;; esac
if [ "$IGNORED_N" -lt "$DIR_N" ]; then
    echo "error: the ignore list names $IGNORED_N entries but the directory holds $DIR_N." >&2
    echo "       The consumer would be handed documents it must not ingest. Refusing." >&2
    exit 1
fi

log "1/3: an isolated consumer, ignoring everything already present"
MUTATED=1
(
    export FN_PAPERLESS_IGNORE="$IGNORED_JSON"
    compose --profile consumer up -d --wait --wait-timeout 420 paperless >/dev/null 2>&1
)
CONSUMER_UP="$(compose --profile consumer ps --format '{{.Health}}' paperless 2>/dev/null | head -1)"
emit "1. the consumer"
emit "   started and healthy:          ${CONSUMER_UP:-unknown}   (expected healthy)"
emit "   its ignore list names:        $(printf '%s' "$IGNORED_JSON" | docker run --rm -i "$UTIL_PY_IMAGE" python3 -c 'import json,sys; print(len(json.load(sys.stdin)))' 2>/dev/null) entries already in the directory"
[ "$CONSUMER_UP" = "healthy" ] || bad "the consumer did not become healthy; nothing below is about a consumer"

# --- an arrival the ignore list cannot cover -------------------------------
log "2/3: a delivery that arrives AFTER the ignore list was taken"
# A REAL PDF, built by pikepdf inside the consumer's own image -- the same
# way verify-consumer.sh makes its fixture.
#
# This used to write `printf '%%PDF-1.4 isolation probe'`, which has the PDF
# magic bytes and nothing else. Paperless dispatches on that magic, hands the
# file to ocrmypdf, and ocrmypdf rejects it:
#
#   ocrmypdf.exceptions.InputFileError
#   ConsumerError: isolation-probe-...pdf: Error occurred while consuming
#
# A failed consumption leaves the file in the consume directory and puts
# nothing in the library, so the exercise reported "the consumer never took
# the fixture" -- true, but not for the reason the message implied. The
# consumer took it and could not parse it.
compose run --rm --no-deps -T \
    -v "${PROJECT}_fn-incoming:/incoming" \
    --entrypoint python3 paperless -c "
import os, pikepdf
wip = '/incoming/.wip-$LOWER'
pdf = pikepdf.new()
pdf.add_blank_page(page_size=(612, 792))
pdf.save(wip)
os.chmod(wip, 0o644)
os.rename(wip, '/incoming/$DOC')
" >/dev/null 2>&1
JOB=""; _i=0
while [ "$_i" -lt 150 ]; do
    [ -n "$JOB" ] || JOB="$(psqln "SELECT job_id FROM jobs WHERE source_name = '$DOC';")"
    if [ -n "$JOB" ] && [ "$(psqln "SELECT state FROM jobs WHERE job_id = '$JOB';")" = "delivered" ]; then break; fi
    sleep 3; _i=$((_i + 3))
done
STATE="$(psqln "SELECT state FROM jobs WHERE job_id = '$JOB';")"
NAME="$(psqlq "SELECT delivered_name FROM delivery_receipts WHERE job_id = '$JOB';" | head -1)"
emit ""
emit "2. the unrelated arrival"
emit "   fixture:                      $DOC"
emit "   (its source name does not match the detector's 'consumer-%' rule, so"
emit "    every check below sees it exactly as it would another job's document)"
emit "   state:                        ${STATE:-none}   (expected delivered)"
emit "   delivered as:                 ${NAME:-none}"
[ "$STATE" = "delivered" ] || bad "the fixture never reached the consume directory"

# Did the consumer take it?
TAKEN=no; _i=0
while [ "$_i" -lt 240 ]; do
    if [ "$(probe_exists "/srv/fn/consume/$NAME")" = "no" ]; then TAKEN=yes; break; fi
    sleep 5; _i=$((_i + 5))
done
INGESTED_THIS="$(compose --profile consumer exec -T paperless sh -c \
    "python3 -c \"
import sqlite3
db = sqlite3.connect('/usr/src/paperless/data/db.sqlite3')
print(db.execute(
    'select count(*) from documents_document where original_filename = ?',
    ('$NAME',)).fetchone()[0])
\"" 2>/dev/null | tr -d ' \r\n' || echo unknown)"
emit "   the consumer took it:         $TAKEN (after ~${_i}s)"
emit "   it is in the consumer's library: ${INGESTED_THIS:-unknown}   (expected 1)"
[ "$TAKEN" = "yes" ] || bad "the consumer never took the fixture, so no foreign ingestion occurred to detect"
[ "${INGESTED_THIS:-0}" = "1" ] || bad "the fixture is not in the consumer's library ($INGESTED_THIS)"

# --- the detector and the refusal ------------------------------------------
log "3/3: the detector fires and the volumes are kept"
ARRIVED="$(psqln "SELECT count(*) FROM delivery_receipts r JOIN jobs j USING (job_id)
                   WHERE r.delivered_at >= '$T0' AND j.source_name NOT LIKE 'consumer-%';")"
FOREIGN_NAMES="$(psqlq "SELECT j.source_name FROM delivery_receipts r JOIN jobs j USING (job_id)
                         WHERE r.delivered_at >= '$T0' AND j.source_name NOT LIKE 'consumer-%';")"
VERDICT=none
for _n in $FOREIGN_NAMES; do
    _dn="$(psqlq "SELECT r.delivered_name FROM delivery_receipts r JOIN jobs j USING (job_id)
                   WHERE j.source_name = '$_n';" | head -1)"
    case "$(probe_exists "/srv/fn/consume/$_dn")" in
        no)      VERDICT=present; break ;;
        unknown) VERDICT=unknown; break ;;
    esac
done
VOLS_PRESENT=0
for _v in $CREATED_VOLUMES; do
    [ -n "$(docker volume ls -q --filter "name=^${PROJECT}_${_v}$" 2>/dev/null)" ] && VOLS_PRESENT=$((VOLS_PRESENT + 1))
done
emit ""
emit "3. what verify-consumer.sh's guard would now see"
emit "   deliveries it calls unrelated: ${ARRIVED:-unknown}   (expected >= 1: the guard has"
emit "                                 something to act on for the first time)"
emit "   foreign-content verdict:      $VERDICT   (expected 'present': a definite finding,"
emit "                                 not 'unknown')"
emit "   consumer volumes still present: $VOLS_PRESENT of 3   (expected 3: destroying them"
emit "                                 is what the guard refuses)"
[ "${ARRIVED:-0}" -ge 1 ] || bad "the detector counted no unrelated deliveries, so it still has not fired"
[ "$VERDICT" = "present" ] || bad "the verdict was '$VERDICT', so the refusal path was not reached"
[ "$VOLS_PRESENT" = "3" ] || bad "only $VOLS_PRESENT of 3 consumer volumes exist"

# Cleanup is permitted only on proof that every foreign-looking delivery is
# this run's own fixture. That is what makes destroying the media safe here and
# unsafe in the exercise this one is about.
OTHERS=0
for _n in $FOREIGN_NAMES; do
    [ "$_n" = "$DOC" ] || OTHERS=$((OTHERS + 1))
done
emit ""
emit "   deliveries it calls unrelated that this run did NOT create: $OTHERS   (expected 0)"
if [ "$OTHERS" = "0" ] && [ "$FAILURES" = "0" ]; then
    SAFE_TO_REMOVE=1
    emit "   => this run may remove the media it filled, because it filled all of it."
else
    emit "   => this run may NOT remove the media: it holds something it did not create."
fi

emit ""
emit "mismatches: $FAILURES"
emit ""
emit "restoration:"
restore
trap - EXIT INT TERM
MUTATED=0
LEFT="$(compose --profile consumer ps -aq paperless paperless-redis 2>/dev/null | wc -l | tr -d ' ')"
emit "   consumer containers left:     $LEFT   (expected 0)"
emit "   renamer-1 / renamer-2:        $(compose ps --format '{{.Health}}' renamer-1 2>/dev/null | head -1) / $(compose ps --format '{{.Health}}' renamer-2 2>/dev/null | head -1)"

if [ "$FAILURES" = "0" ]; then
    report_success
    log "PASSED: the guard fired on a real foreign ingestion and kept the volumes"
    note "evidence: $OUT"
    exit 0
fi
echo "FAILED: $FAILURES expectation(s) not met; see $OUT" >&2
exit 1
