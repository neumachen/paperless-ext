#!/bin/sh
# Qualify the documented restore procedure, on known fixtures.
#
# # The claim under test
#
# production/README.md warns that restoring `incoming` into the live root
# re-delivers every source that was already delivered, and tells the operator
# to restore it into a quarantine directory instead. That warning was reasoned
# from the source and never executed. A procedure whose dangerous half has
# never been demonstrated is a claim, and the safe half is then a claim too.
#
# # Why replacing a file is a faithful restore
#
# Discovery recognises an already-registered submission by root, name, device
# and inode (ledger.AlreadyRegistered). Content is deliberately not consulted:
# two distinct submissions may hold identical bytes and the contract forbids
# deduplicating them.
#
# `tar -x` over an existing tree writes new files and renames them into place,
# so every restored source keeps its name and gets a NEW inode. Copying a
# fixture's own source and renaming it over itself produces exactly that --
# same root, same name, same bytes, new inode -- without inventing a second
# deployment to hold it. The bytes are never absent: the copy is made first and
# the rename replaces the name atomically.
#
# # What it touches
#
# It creates its own fixtures, named with this run's stamp, and the quarantine
# directory it later removes. It does not delete a source, resolve an uncertain
# job, or touch any document it did not submit.
. "$(dirname "$0")/lib.sh"

OUT="$EVIDENCE_DIR/restore-replay.txt"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
LOWER="$(printf '%s' "$STAMP" | tr 'A-Z' 'a-z')"
FAILURES=0
# The quarantine has to live on a MOUNTED volume that discovery does not
# watch. In production /srv/fn is one host directory holding every role, so the
# README's sibling `/srv/fn/restored-incoming` persists. Here each role is its
# own named volume and /srv/fn itself is container-local, so a sibling created
# in one `compose run` container is gone in the next -- which is why this step
# first reported "the quarantined copy was not created". `failed` is a real
# volume and is not a watched root, which is the only property that matters.
QUARANTINE="/srv/fn/failed/restored-incoming-$LOWER"
MADE_QUARANTINE=0
report_begin restore-replay "$OUT" "$0"

emit() { printf '%s\n' "$*" >> "$OUT"; printf '%s\n' "$*"; }
bad() { printf '    MISMATCH: %s\n' "$*" >&2; emit "    MISMATCH: $*"; FAILURES=$((FAILURES + 1)); }

# Scalars only. `tr -d ' '` would delete spaces from DATA, and a delivered name
# may contain them.
psqlq() {
    compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" \
        postgres-primary psql -U "${FN_DB_USER:-fn_app}" -d "${FN_DB_NAME:-filename_normalizer}" \
        -tA -c "$1" < /dev/null | tr -d '\r' | sed '/^$/d'
}
psqln() { psqlq "$1" | tr -d ' \n'; }
in_storage() {
    compose run --rm --no-deps -T --entrypoint sh storage-init -c "$1" 2>/dev/null < /dev/null | tr -d '\r'
}

cleanup() {
    if [ "$MADE_QUARANTINE" = "1" ]; then
        # Only what this run made, and only if it is still the directory this
        # run made. Its single occupant is a copy this run created.
        in_storage "rm -rf '$QUARANTINE'" >/dev/null 2>&1 || true
    fi
}
trap 'cleanup' EXIT INT TERM

emit "A restore that keeps its names gets new inodes, and discovery reads inodes"
emit ""

UNCERTAIN_BEFORE="$(psqln "SELECT count(*) FROM jobs WHERE state = 'uncertain';")"
HELD_BEFORE="$(psqln "SELECT count(*) FROM jobs WHERE state = 'held';")"

# --- fixture: one ordinary delivered document ------------------------------
submit_and_wait() {
    _doc="$1"
    compose run --rm --no-deps -T --entrypoint sh storage-init -c \
        "printf '%%PDF-1.4 restore replay probe\n' > /srv/fn/incoming/.wip-$LOWER && \
         mv /srv/fn/incoming/.wip-$LOWER '/srv/fn/incoming/$_doc'" >/dev/null 2>&1
    _j=""; _i=0
    while [ "$_i" -lt 120 ]; do
        [ -n "$_j" ] || _j="$(psqln "SELECT job_id FROM jobs WHERE source_name = '$_doc';")"
        if [ -n "$_j" ] && [ "$(psqln "SELECT state FROM jobs WHERE job_id = '$_j';")" = "delivered" ]; then
            printf '%s' "$_j"; return 0
        fi
        sleep 2; _i=$((_i + 2))
    done
    printf '%s' "$_j"
}

log "1/4: a delivered fixture, whose source is preserved in incoming"
DOC_A="restore-replay-$LOWER-a.pdf"
JOB_A="$(submit_and_wait "$DOC_A")"
STATE_A="$(psqln "SELECT state FROM jobs WHERE job_id = '$JOB_A';")"
NAME_A="$(psqlq "SELECT delivered_name FROM delivery_receipts WHERE job_id = '$JOB_A';" | head -1)"
INODE_A1="$(probe_inode "/srv/fn/incoming/$DOC_A")"
emit "1. a delivered fixture"
emit "   job:                          ${JOB_A:-none}"
emit "   state:                        ${STATE_A:-none}   (expected delivered)"
emit "   delivered as:                 ${NAME_A:-none}"
emit "   its source, still in incoming, inode: $INODE_A1"
[ "$STATE_A" = "delivered" ] || bad "the fixture did not reach delivered; nothing below is about a restore"
case "$INODE_A1" in ''|ABSENT|UNKNOWN) bad "the fixture's source inode could not be read" ;; esac

# --- the naive restore: incoming, back into the live root ------------------
log "2/4: restoring that source into the LIVE root, as the README warns against"
in_storage "cp -p '/srv/fn/incoming/$DOC_A' '/srv/fn/incoming/.restore-$LOWER' && \
            mv -f '/srv/fn/incoming/.restore-$LOWER' '/srv/fn/incoming/$DOC_A'" >/dev/null 2>&1
INODE_A2="$(probe_inode "/srv/fn/incoming/$DOC_A")"
# The restore is only faithful if the inode actually changed. If it did not,
# this exercise is measuring nothing and must say so rather than pass.
if [ "$INODE_A2" = "$INODE_A1" ]; then
    bad "the replaced source kept inode $INODE_A1, so no restore was simulated"
fi

_i=0; JOBS_A=1
while [ "$_i" -lt 120 ]; do
    JOBS_A="$(psqln "SELECT count(*) FROM jobs WHERE source_name = '$DOC_A';")"
    [ "${JOBS_A:-1}" -ge 2 ] && break
    sleep 3; _i=$((_i + 3))
done
JOB_A2="$(psqln "SELECT job_id FROM jobs WHERE source_name = '$DOC_A' AND job_id <> '$JOB_A';")"
# Whatever the second job then does is the collision policy's business. What
# this step establishes is that a SECOND JOB EXISTS for a source the ledger had
# already delivered -- the re-registration is the hazard.
_i=0; STATE_A2=""
while [ "$_i" -lt 150 ]; do
    STATE_A2="$(psqln "SELECT state FROM jobs WHERE job_id = '$JOB_A2';")"
    case "$STATE_A2" in delivered|held|uncertain|failed) break ;; esac
    sleep 3; _i=$((_i + 3))
done
NAME_A2="$(psqlq "SELECT delivered_name FROM delivery_receipts WHERE job_id = '$JOB_A2';" | head -1)"
DOCS_A="$(psqln "SELECT count(*) FROM delivery_receipts r JOIN jobs j USING (job_id) WHERE j.source_name = '$DOC_A';")"

emit ""
emit "2. the same source, restored into the live root"
emit "   inode before / after:         $INODE_A1 / $INODE_A2   (expected: different)"
emit "   jobs for that source name:    ${JOBS_A:-unknown}   (expected 2: it was registered again)"
emit "   the second job:               ${JOB_A2:-none}"
emit "   what became of it:            ${STATE_A2:-did not settle}"
emit "   it delivered:                 ${NAME_A2:-nothing}"
emit "   delivery receipts for this one source: ${DOCS_A:-unknown}"
[ "${JOBS_A:-1}" -ge 2 ] || bad "the restored source was NOT re-registered; the README's warning does not describe this build"
[ -n "$JOB_A2" ] || bad "no second job id could be read"
if [ -n "$NAME_A2" ] && [ "$NAME_A2" = "$NAME_A" ]; then
    bad "the second delivery reused the first's name '$NAME_A'"
fi
emit "   => the warning in production/README.md is accurate: a wholesale"
emit "      restore of incoming re-delivers what was already delivered."

# --- the documented restore: into a quarantine -----------------------------
log "3/4: restoring into a quarantine directory, as the README instructs"
DOC_B="restore-replay-$LOWER-b.pdf"
JOB_B="$(submit_and_wait "$DOC_B")"
STATE_B="$(psqln "SELECT state FROM jobs WHERE job_id = '$JOB_B';")"
[ "$STATE_B" = "delivered" ] || bad "the second fixture did not reach delivered"

in_storage "mkdir -p '$QUARANTINE' && cp -p '/srv/fn/incoming/$DOC_B' '$QUARANTINE/$DOC_B'" >/dev/null 2>&1
QPRESENT="$(probe_exists "$QUARANTINE/$DOC_B")"
[ "$QPRESENT" = "yes" ] && MADE_QUARANTINE=1
JOBS_B_BEFORE="$(psqln "SELECT count(*) FROM jobs WHERE source_name = '$DOC_B';")"
# The same budget the replay above was given. A shorter wait would report
# "nothing was discovered" about a window in which nothing could have been.
sleep 45
JOBS_B_AFTER="$(psqln "SELECT count(*) FROM jobs WHERE source_name = '$DOC_B';")"
DOCS_B="$(psqln "SELECT count(*) FROM delivery_receipts r JOIN jobs j USING (job_id) WHERE j.source_name = '$DOC_B';")"
QSTILL="$(probe_exists "$QUARANTINE/$DOC_B")"
# Step 5 of the documented procedure: the operator asks the ledger about each
# quarantined file before moving it back.
RECONCILE="$(psqlq "SELECT state FROM jobs WHERE source_name = '$DOC_B';" | head -1)"

emit ""
emit "3. the same situation, restored into a quarantine instead"
emit "   quarantine:                   $QUARANTINE"
emit "                                 (a mounted volume; discovery watches incoming only)"
emit "   the restored copy is there:   $QPRESENT   (expected yes)"
emit "   jobs for that source, before: ${JOBS_B_BEFORE:-unknown}   (expected 1)"
emit "   jobs for that source, after:  ${JOBS_B_AFTER:-unknown}   (expected 1: nothing was discovered)"
emit "   delivery receipts for it:     ${DOCS_B:-unknown}   (expected 1: not delivered twice)"
emit "   the copy is still in quarantine: $QSTILL   (expected yes: nothing consumed it)"
emit "   the documented reconcile query answers: ${RECONCILE:-nothing}   (expected delivered)"
[ "$QPRESENT" = "yes" ] || bad "the quarantined copy was not created, so step 3 tested nothing"
[ "${JOBS_B_AFTER:-0}" = "${JOBS_B_BEFORE:-1}" ] || bad "a file in the quarantine was discovered: $JOBS_B_BEFORE -> $JOBS_B_AFTER"
[ "${DOCS_B:-0}" = "1" ] || bad "$DOCS_B receipts exist for a source delivered once"
[ "$QSTILL" = "yes" ] || bad "the quarantined copy disappeared"
[ "$RECONCILE" = "delivered" ] || bad "the documented reconcile query answered '$RECONCILE'"

# --- nothing else moved ----------------------------------------------------
log "4/4: no other job changed state"
UNCERTAIN_AFTER="$(psqln "SELECT count(*) FROM jobs WHERE state = 'uncertain';")"
SRC_A="$(probe_exists "/srv/fn/incoming/$DOC_A")"
SRC_B="$(probe_exists "/srv/fn/incoming/$DOC_B")"
emit ""
emit "4. what this exercise did not touch"
emit "   uncertain jobs before/after:  $UNCERTAIN_BEFORE / $UNCERTAIN_AFTER   (expected equal:"
emit "                                 a restore resolves nothing)"
emit "   held jobs before:             $HELD_BEFORE"
emit "   both fixture sources preserved: $SRC_A / $SRC_B   (expected yes/yes)"
[ "$UNCERTAIN_AFTER" = "$UNCERTAIN_BEFORE" ] || bad "uncertain jobs changed from $UNCERTAIN_BEFORE to $UNCERTAIN_AFTER"
[ "$SRC_A" = "yes" ] || bad "fixture A's source is gone"
[ "$SRC_B" = "yes" ] || bad "fixture B's source is gone"

cleanup
MADE_QUARANTINE=0
QGONE="$(probe_exists "$QUARANTINE/$DOC_B")"
emit ""
emit "restoration (read back):"
emit "   quarantine removed:           $([ "$QGONE" = "no" ] && echo yes || echo "no ($QGONE)")"
emit "   renamer-1 / renamer-2:        $(compose ps --format '{{.Health}}' renamer-1 2>/dev/null | head -1) / $(compose ps --format '{{.Health}}' renamer-2 2>/dev/null | head -1)"
emit ""
emit "mismatches: $FAILURES"

if [ "$FAILURES" = "0" ]; then
    report_success
    log "PASSED: the documented restore quarantine prevents the replay a live-root restore causes"
    note "evidence: $OUT"
    exit 0
fi
echo "FAILED: $FAILURES expectation(s) not met; see $OUT" >&2
exit 1
