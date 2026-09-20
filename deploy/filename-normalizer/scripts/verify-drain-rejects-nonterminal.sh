#!/bin/sh
# The drain phase must REJECT a real fixture that is still non-terminal when
# the identity-based assertion examines it.
#
# # Why this needs its own control
#
# The per-identity check only speaks when a fixture has not finished, and on a
# healthy run every fixture finishes. So the passing run says nothing about
# the rejection, which is the half that protects the claim.
#
# # What is real here, and what is supplied
#
# The job is REAL and genuinely non-terminal: an instance is killed by the
# `before_link` fault, which leaves the job in `publishing`, and the ordinary
# renamers stay stopped so recovery cannot resolve it while the assertion
# runs. The assertion is the real one, running in the real integration image
# against the real ledger.
#
# What is supplied is the phase's INPUT: the other preconditions are copied
# from the most recent real drain run into a fresh run id, and only the
# real-fixture id list is replaced -- with that one real non-terminal job.
# Nothing is fabricated as a RESULT; the orchestrator's own `save_state`
# mechanism provides the input, and the phase reaches its own verdict.
#
# The successful drain evidence is copied aside before the phase runs and put
# back afterwards, because `run_phase` writes to the same slots.
. "$(dirname "$0")/lib.sh"

OUT="$EVIDENCE_DIR/drain-rejects-nonterminal.txt"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
LOWER="$(printf '%s' "$STAMP" | tr 'A-Z' 'a-z')"
FAILURES=0
RESTORE_OK=1
OWNS_LOCK=0
OWNS_FAULT_SVC=0
NEW_RUN=""
SAVED=""
SLOT_RESTORED=unknown
UDOC="drainreject-$LOWER.pdf"

report_begin drain-rejects-nonterminal "$OUT" "$0"

emit() { printf '%s\n' "$*" >> "$OUT"; printf '%s\n' "$*"; }
bad() { printf '    MISMATCH: %s\n' "$*" >&2; emit "    MISMATCH: $*"; FAILURES=$((FAILURES + 1)); }

psqlq() {
    compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" \
        postgres-primary psql -U "${FN_DB_USER:-fn_app}" -d "${FN_DB_NAME:-filename_normalizer}" \
        -tA -c "$1" < /dev/null 2>/dev/null | tr -d '\r' | sed '/^$/d'
}
psqln() { psqlq "$1" | tr -d ' \n'; }

# Hash every file under a directory in ONE container run, like every other hash
# in this stack. The output is "<sha256>  ./<relative path>", sorted, so two
# listings taken before and after a phase compare with a plain diff.
slot_digests() {
    docker run --rm -v "$1":/slot:ro "$UTIL_IMAGE" \
        sh -c 'cd /slot && find . -type f -exec sha256sum {} + 2>/dev/null | sort -k2' 2>/dev/null
}

CLEANED=0
cleanup() {
    [ "$CLEANED" = "0" ] || return 0
    CLEANED=1
    if [ "$OWNS_FAULT_SVC" = "1" ]; then
        compose --profile fault stop -t 15 renamer-fault >/dev/null 2>&1 || true
        compose --profile fault rm -f renamer-fault >/dev/null 2>&1 || true
        if _fs="$(compose --profile fault ps -aq renamer-fault 2>/dev/null)"; then
            if [ -z "$_fs" ]; then OWNS_FAULT_SVC=0; fi
        fi
        [ "$OWNS_FAULT_SVC" = "0" ] || { echo "error: renamer-fault remains" >&2; RESTORE_OK=0; }
    fi
    # Put the successful drain evidence back exactly as it was.
    #
    # "Exactly" has to mean every file the phase writes, not the two that are
    # easy to name. Saving only phase-drained.log and f5-drain-under-load.txt
    # left f5-drain-readiness-summary.txt carrying THIS control's shutdown
    # instant: that summary is recomputed from renamer-2's live log, and by the
    # time this control runs, the real run's shutdown_started record has
    # rotated out of the container, so the recomputation silently substitutes
    # the wrong instant into retained evidence. Save and restore the slot whole,
    # then prove it by digest instead of trusting the copy.
    if [ -n "$SAVED" ] && [ -d "$SAVED" ]; then
        if [ -f "$SAVED/phase-drained.log" ]; then
            cp "$SAVED/phase-drained.log" "$EVIDENCE_DIR/phase-drained.log"
        fi
        if [ -d "$SAVED/drained" ]; then
            cp -R "$SAVED/drained/." "$EVIDENCE_DIR/drained/" 2>/dev/null || true
        fi
        if [ -f "$SAVED/.slot-before" ]; then
            slot_digests "$EVIDENCE_DIR/drained" > "$SAVED/.slot-after" 2>/dev/null || true
            if [ ! -s "$SAVED/.slot-after" ]; then
                SLOT_RESTORED=unknown
                echo "error: could not read back the drained evidence slot; its" >&2
                echo "       restoration is UNCONFIRMED and must not be reported as done." >&2
                RESTORE_OK=0
            elif diff "$SAVED/.slot-before" "$SAVED/.slot-after" >/dev/null 2>&1; then
                SLOT_RESTORED=yes
            else
                SLOT_RESTORED=NO
                echo "error: the drained evidence slot did not come back byte-for-byte:" >&2
                diff "$SAVED/.slot-before" "$SAVED/.slot-after" >&2 || true
                RESTORE_OK=0
            fi
        fi
        rm -rf "$SAVED" 2>/dev/null || true
    fi
    # Remove only the state copies this control made.
    if [ -n "$NEW_RUN" ]; then
        rm -f "$EVIDENCE_DIR/state/$NEW_RUN".* 2>/dev/null || true
    fi
    recreate_service renamer-1 renamer-2 watcher >/dev/null 2>&1 || true
    if [ "$OWNS_LOCK" = "1" ]; then exercise_unlock; fi
    return 0
}
on_signal() { echo "error: interrupted; cleaning up and stopping." >&2; cleanup; exit 130; }
trap 'cleanup' EXIT
trap 'on_signal' INT TERM

exercise_lock drain-rejects-nonterminal || exit 1
OWNS_LOCK=1

emit "The drain phase rejects a real fixture that is still non-terminal"
emit ""

# --- the source run whose preconditions are reused --------------------------
SRC_RUN="$(ls -t "$EVIDENCE_DIR/state" 2>/dev/null | grep 'drain-registered$' | head -1 | sed 's/\.drain-registered$//')"
[ -n "$SRC_RUN" ] || { emit "    REFUSED: no prior drain run state to build on"; exit 1; }
NEW_RUN="rejectctl-$STAMP-$$"
for _f in "$EVIDENCE_DIR/state/$SRC_RUN".*; do
    _suffix="${_f##*"$SRC_RUN".}"
    cp "$_f" "$EVIDENCE_DIR/state/$NEW_RUN.$_suffix" 2>/dev/null || true
done
emit "1. the phase's preconditions"
emit "   copied from the last real drain run: $SRC_RUN"
emit "   into a fresh run id:                 $NEW_RUN"
emit "   (the retained run's own state and evidence are not modified)"

# --- a real, genuinely non-terminal job ------------------------------------
log "1/3: producing a real job that is genuinely stuck in publishing"
compose stop -t 30 renamer-1 renamer-2 >/dev/null 2>&1
own_service renamer-fault || { emit "    REFUSED: renamer-fault already exists"; exit 1; }
OWNS_FAULT_SVC=1
( export FN_FAULT_POINTS=before_link
  compose --profile fault up -d renamer-fault >/dev/null 2>&1 ) \
    || { emit "    REFUSED: could not start renamer-fault"; exit 1; }
sleep 6
compose run --rm --no-deps -T -e FN_N="/srv/fn/incoming/$UDOC" --entrypoint sh storage-init \
    -c 'printf "%%PDF-1.4 drain reject fixture\n" > /srv/fn/incoming/.wip-dr && mv /srv/fn/incoming/.wip-dr "$FN_N"' \
    </dev/null >/dev/null 2>&1
_i=0
while [ "$_i" -lt 150 ]; do
    _r="$(compose --profile fault ps -q renamer-fault 2>/dev/null | head -1)"
    if [ -z "$_r" ]; then break; fi
    if [ "$(docker inspect -f '{{.State.Status}}' "$_r" 2>/dev/null || echo gone)" != "running" ]; then break; fi
    sleep 2; _i=$((_i + 2))
done
compose --profile fault stop -t 15 renamer-fault >/dev/null 2>&1 || true
compose --profile fault rm -f renamer-fault >/dev/null 2>&1 || true
if _fs="$(compose --profile fault ps -aq renamer-fault 2>/dev/null)"; then
    if [ -z "$_fs" ]; then OWNS_FAULT_SVC=0; fi
fi
UJOB="$(psqln "SELECT job_id FROM jobs WHERE source_name='$UDOC';")"
USTATE="$(psqln "SELECT state FROM jobs WHERE job_id='$UJOB';")"
emit ""
emit "2. the fixture"
emit "   job:                          ${UJOB:-none}"
emit "   state:                        ${USTATE:-none}   (expected publishing: non-terminal)"
emit "   the ordinary renamers are stopped, so recovery cannot resolve it while"
emit "   the assertion runs."
[ -n "$UJOB" ] || bad "no job was registered for the fixture"
[ "$USTATE" = "publishing" ] || bad "the fixture is '$USTATE', not a non-terminal publishing job"

# --- run the real assertion over it ----------------------------------------
log "2/3: running the real drained assertion with that job as its real-fixture set"
SAVED="$EVIDENCE_DIR/.drain-evidence-saved-$$"
mkdir -p "$SAVED/drained"
if [ -f "$EVIDENCE_DIR/phase-drained.log" ]; then
    cp "$EVIDENCE_DIR/phase-drained.log" "$SAVED/phase-drained.log"
fi
# The whole slot, plus a digest of every file in it, so the restore is an
# assertion rather than a hope. run_phase rewrites all of these.
if [ -d "$EVIDENCE_DIR/drained" ]; then
    cp -R "$EVIDENCE_DIR/drained/." "$SAVED/drained/" 2>/dev/null || true
    slot_digests "$EVIDENCE_DIR/drained" > "$SAVED/.slot-before" 2>/dev/null || true
    if [ ! -s "$SAVED/.slot-before" ]; then
        emit "    REFUSED: the retained drain evidence could not be hashed, so this"
        emit "             control cannot prove it put it back; nothing has been changed"
        exit 1
    fi
fi
printf '%s,\n' "$UJOB" > "$EVIDENCE_DIR/state/$NEW_RUN.drain-real-ids"
# run_phase reads both of these from the shell directly. PHASE_FAILURES is
# normally set by run-integration.sh, and under `set -u` its absence is a
# fatal unbound-variable error rather than a missing count.
RUN_ID="$NEW_RUN"
PHASE_FAILURES=0
export RUN_ID
PHASE_EXIT=0
run_phase drained "" || PHASE_EXIT=$?
REJECT_LINE="$(grep -a "is in non-terminal state" "$EVIDENCE_DIR/phase-drained.log" 2>/dev/null | head -1 | sed 's/^ *//')"
REJECT_N="$(grep -ac 'real drain fixtures did not reach a durable outcome' "$EVIDENCE_DIR/phase-drained.log" 2>/dev/null || true)"
cp "$EVIDENCE_DIR/phase-drained.log" "$EVIDENCE_DIR/drain-rejects-nonterminal-phase.log" 2>/dev/null || true
emit ""
emit "3. what the real assertion did with it"
emit "   phase exit status:            $PHASE_EXIT   (expected non-zero)"
emit "   it named the fixture:         ${REJECT_LINE:-<no rejection line>}"
emit "   it summarised the rejection:  ${REJECT_N:-0} line(s)   (expected >= 1)"
emit "   full phase output retained at .evidence/drain-rejects-nonterminal-phase.log"
[ "$PHASE_EXIT" != "0" ] || bad "the phase passed with a non-terminal real fixture"
[ -n "$REJECT_LINE" ] || bad "the assertion did not name the non-terminal fixture"
[ "${REJECT_N:-0}" -ge 1 ] || bad "the assertion did not summarise the rejection"
case "$REJECT_LINE" in
    *"$UJOB"*) : ;;
    *) bad "the rejection names a different job than the fixture ($UJOB)" ;;
esac

# --- restore ---------------------------------------------------------------
log "3/3: restoring the stack and the retained drain evidence"
cleanup
trap - EXIT INT TERM
_w=0
while [ "$_w" -lt 240 ]; do
    _s="$(psqln "SELECT state FROM jobs WHERE job_id='$UJOB';")"
    case "$_s" in delivered|held|uncertain|failed) break ;; esac
    sleep 3; _w=$((_w + 3))
done
FINAL="$(psqln "SELECT state FROM jobs WHERE job_id='$UJOB';")"
L1="$(compose ps --format '{{.Health}}' renamer-1 2>/dev/null | head -1)"
L2="$(compose ps --format '{{.Health}}' renamer-2 2>/dev/null | head -1)"
LW="$(compose ps --format '{{.Health}}' watcher 2>/dev/null | head -1)"
DRAIN_OK="$(grep -ac 'real_fixtures_terminal=' "$EVIDENCE_DIR/drained/f5-drain-under-load.txt" 2>/dev/null || true)"
emit ""
emit "restoration (read back from the running stack):"
emit "   the fixture's own outcome:    $FINAL   (recovery resolved it once the"
emit "                                 renamers came back; it is this control's"
emit "                                 own synthetic job, identified above)"
emit "   renamer-1 / renamer-2 / watcher: $L1 / $L2 / $LW   (expected healthy x3)"
emit "   retained drain evidence readable: $([ "${DRAIN_OK:-0}" -ge 1 ] && echo yes || echo NO)"
emit "   the whole drained/ slot came back byte-for-byte: $SLOT_RESTORED   (expected yes;"
emit "                                 every file the phase rewrites, compared by"
emit "                                 sha256 against the listing taken before it ran)"
emit "   state copies left behind:     $(ls "$EVIDENCE_DIR/state/$NEW_RUN".* 2>/dev/null | grep -c . || true)   (expected 0)"
{ [ "$L1" = "healthy" ] && [ "$L2" = "healthy" ] && [ "$LW" = "healthy" ]; } \
    || bad "the live stack is not healthy after this control ($L1/$L2/$LW)"
[ "${DRAIN_OK:-0}" -ge 1 ] || bad "the retained drain evidence was not restored"
[ "$SLOT_RESTORED" = "yes" ] || bad "the drained evidence slot was not restored byte-for-byte ($SLOT_RESTORED)"
[ "$RESTORE_OK" = "1" ] || bad "restoration was incomplete"

emit ""
emit "mismatches: $FAILURES"
if [ "$FAILURES" = "0" ]; then
    report_restored
    report_success
    log "PASSED: the identity assertion rejects a real non-terminal fixture"
    note "evidence: $OUT"
    exit 0
fi
echo "FAILED: $FAILURES expectation(s) not met; see $OUT" >&2
exit 1
