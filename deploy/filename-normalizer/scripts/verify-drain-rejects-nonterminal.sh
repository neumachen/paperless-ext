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
SAVED_KEPT=""
SAVE_VERIFIED=0
SLOT_RESTORED=unknown
PR_RESTORED=0
CTL_RESULT_LINE=""
# An independent pre-run copy of the ledger, kept OUTSIDE the recovery copy so
# the comparison still has something to compare against after a successful
# restore has removed that copy.
PR_BEFORE=""
LIVE_TOUCHED=0
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

# Hash the WHOLE evidence recovery set in ONE container run: every shared
# output `run_phase` rewrites, not just the drained/ slot.
#
#   phase-drained.log   the phase log, replaced
#   phase-results.txt   the cumulative ledger, appended to
#   logs/               every service log, overwritten by collect_logs
#   drained/            every file the phase itself writes
#
# The same function runs against the live evidence directory and against the
# recovery copy, which share this relative layout, so the two listings compare
# with a plain diff. Dotfiles at the root are not included, which is why the
# manifests themselves can live inside the copy.
# Emit "<sha256>  <path>" for every file in the evidence recovery set, or FAIL.
#
#   phase-drained.log   the phase log, replaced
#   phase-results.txt   the cumulative ledger, appended to
#   logs/               every service log, overwritten by collect_logs
#   drained/            every file the phase itself writes
#
# This used to end with an unconditional `exit 0` inside the container and a
# pipeline ending in `sort`, so BOTH the shell's status and the pipeline's
# status were discarded: a `find` that could not descend, or a `sha256sum`
# that could not read a file, produced a SHORTER manifest that still looked
# like a successful one. Two short manifests then compared equal and the
# completeness check passed on evidence nobody had actually hashed.
#
# Now every failure exits non-zero, the container's status is captured before
# anything is piped, and sorting happens afterwards on the captured text.
recovery_digests() {
    _rd_out="$(docker run --rm -v "$1":/r:ro "$UTIL_IMAGE" sh -c '
        set -e
        cd /r
        for f in phase-drained.log phase-results.txt; do
            if [ -f "$f" ]; then sha256sum "$f"; fi
        done
        for d in logs drained; do
            if [ -d "$d" ]; then
                find "$d" -type f > /tmp/rd-list || exit 3
                while IFS= read -r _p; do
                    [ -n "$_p" ] || continue
                    sha256sum "$_p" || exit 4
                done < /tmp/rd-list
            fi
        done')" || return 1
    printf '%s\n' "$_rd_out" | sort -k2
    return 0
}

# How many files the recovery set is EXPECTED to contain, counted
# independently of the hashing above. A manifest with fewer lines than this
# means something was enumerated but never hashed.
recovery_expected_count() {
    docker run --rm -v "$1":/r:ro "$UTIL_IMAGE" sh -c '
        set -e
        cd /r
        n=0
        for f in phase-drained.log phase-results.txt; do
            if [ -f "$f" ]; then n=$((n + 1)); fi
        done
        for d in logs drained; do
            if [ -d "$d" ]; then
                c="$(find "$d" -type f | wc -l)"
                n=$((n + c))
            fi
        done
        printf "%s\n" "$n"' 2>/dev/null | tr -d ' \r\n'
}

# A manifest is COMPLETE when it was produced without error and has exactly
# one line per expected file. Returns 0 only then.
manifest_complete() {
    _mc_manifest="$1"; _mc_expected="$2"
    [ -s "$_mc_manifest" ] || return 1
    case "$_mc_expected" in ''|*[!0-9]*) return 1 ;; esac
    [ "$_mc_expected" -ge 1 ] || return 1
    _mc_lines="$(grep -c . "$_mc_manifest" 2>/dev/null || echo 0)"
    [ "$_mc_lines" = "$_mc_expected" ] || return 1
    return 0
}

CLEANED=0
cleanup() {
    [ "$CLEANED" = "0" ] || return 0
    CLEANED=1

    # A rejected invocation restores NOTHING. The EXIT trap is armed before
    # the lock is taken, so a refused acquisition reached this function and
    # force-recreated renamer-1, renamer-2 and watcher -- disrupting the run
    # that legitimately held the lock, on the way out of a control whose
    # entire subject is not disturbing other holders. Nothing below was
    # reached in that case either.
    if [ "$OWNS_LOCK" != "1" ]; then
        return 0
    fi
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
    # Only a VERIFIED recovery copy may be written back. An unverified one --
    # a partial `cp -R`, or a refusal that happened before the copy was
    # checked -- would overwrite intact evidence with a worse version of it,
    # which is the opposite of what this block is for.
    if [ "$SAVE_VERIFIED" = "1" ] && [ -n "$SAVED" ] && [ -d "$SAVED" ]; then
        if [ -f "$SAVED/phase-drained.log" ]; then
            cp "$SAVED/phase-drained.log" "$EVIDENCE_DIR/phase-drained.log"
        fi
        if [ -d "$SAVED/drained" ]; then
            cp -R "$SAVED/drained/." "$EVIDENCE_DIR/drained/" 2>/dev/null || true
        fi
        # The other shared outputs `run_phase` rewrites, which are not part of
        # the drained/ slot: the cumulative phase result ledger it APPENDS a
        # line to, and the service logs `collect_logs` overwrites wholesale.
        # Leaving phase-results.txt alone put an unattributed
        # "drained FAIL (exit 1)" directly beneath the 21-phase baseline's
        # "drained PASS", so the retained record of a passing suite read as a
        # failing one.
        if [ -f "$SAVED/phase-results.txt" ]; then
            cp "$SAVED/phase-results.txt" "$EVIDENCE_DIR/phase-results.txt"
        fi
        if [ -d "$SAVED/logs" ]; then
            cp -R "$SAVED/logs/." "$EVIDENCE_DIR/logs/" 2>/dev/null || true
        fi
        # The WHOLE set is compared against its pre-run manifest -- the log,
        # the ledger, every service log and every file in drained/ -- not the
        # slot alone.
        if [ -f "$SAVED/.set-before" ]; then
            _set_after_expected="$(recovery_expected_count "$EVIDENCE_DIR")"
            if ! recovery_digests "$EVIDENCE_DIR" > "$SAVED/.set-after" 2>/dev/null; then
                : > "$SAVED/.set-after"
            fi
            if ! manifest_complete "$SAVED/.set-after" "$_set_after_expected"; then
                SLOT_RESTORED=unknown
                echo "error: the restored evidence could not be completely hashed" >&2
                echo "       ($_set_after_expected expected); restoration is UNCONFIRMED" >&2
                echo "       and must not be reported as done." >&2
                RESTORE_OK=0
            elif diff "$SAVED/.set-before" "$SAVED/.set-after" >/dev/null 2>&1; then
                SLOT_RESTORED=yes
            else
                SLOT_RESTORED=NO
                echo "error: the evidence recovery set did not come back byte-for-byte:" >&2
                diff "$SAVED/.set-before" "$SAVED/.set-after" >&2 || true
                RESTORE_OK=0
            fi
        else
            SLOT_RESTORED=unknown
            echo "error: no pre-run manifest; restoration cannot be confirmed." >&2
            RESTORE_OK=0
        fi
        # The recovery copy is deleted ONLY when the evidence is provably back.
        # Removing it unconditionally destroyed the last copy of the material
        # needed to recover by hand, in exactly the case that needed it, while
        # the report said restoration had failed.
        if [ "$SLOT_RESTORED" = "yes" ] && [ "$RESTORE_OK" = "1" ]; then
            rm -rf "$SAVED" 2>/dev/null || true
        else
            SAVED_KEPT="$SAVED"
            echo "error: a required restoration was unsuccessful or unknown, so the" >&2
            echo "       recovery copy is RETAINED at $SAVED" >&2
            echo "       It holds phase-drained.log, phase-results.txt, logs/ and the" >&2
            echo "       whole drained/ slot as they were before this control ran." >&2
        fi
    fi
    # Remove only the state copies this control made.
    if [ -n "$NEW_RUN" ]; then
        rm -f "$EVIDENCE_DIR/state/$NEW_RUN".* 2>/dev/null || true
    fi
    # Only what this invocation actually changed. It stops renamer-1 and
    # renamer-2 to strand the fixture; if it never got that far there is
    # nothing to put back.
    if [ "$LIVE_TOUCHED" = "1" ]; then
        recreate_service renamer-1 renamer-2 watcher >/dev/null 2>&1 || true
    fi
    # `exercise_unlock` is an unconditional `rm -rf`, so the owner file is
    # checked first: a lock that has changed hands is not this process's to
    # remove.
    if [ "$OWNS_LOCK" = "1" ]; then
        _dl="$(cat "$EVIDENCE_DIR/.exercise.lock/owner" 2>/dev/null || true)"
        case "$_dl" in
            *"pid=$$ "*|*"pid=$$") exercise_unlock ;;
            *) echo "warning: the exercise lock is not this process's; leaving it in place." >&2 ;;
        esac
        OWNS_LOCK=0
    fi
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
LIVE_TOUCHED=1
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
# --- the instrument is checked against a known-bad set first --------------
#
# A completeness check that has never been seen to fail is not evidence. On a
# disposable directory of this control's own making -- never the retained
# evidence -- it is shown to (a) accept a whole set, (b) reject a set with a
# file missing, and (c) FAIL, rather than silently shorten, when a path cannot
# be hashed.
#
# What (c) does and does not show. The fixture is a path containing a newline.
# The line-reading loop in recovery_digests cannot consume it, so sha256sum is
# handed two paths that do not exist, the loop exits 4 and the run ends
# non-zero. That is a real hashing error, really propagated, by THIS
# implementation.
#
# It is NOT a reproduction of the previous implementation's defect. Measured on
# this same fixture, the previous `find "$d" -type f -exec sha256sum {} +`
# hashed the file successfully and exited 0 -- `-exec` passes the name as one
# argument, so a newline in it is harmless there. That implementation's actual
# defect was an unconditional `exit 0` inside the container and a
# sort-terminated pipeline, which discarded both statuses; it is a property of
# that code, and no fixture in this step exercises it.
SELFTEST="$EVIDENCE_DIR/.drain-selftest-$$"
rm -rf "$SELFTEST" 2>/dev/null || true
mkdir -p "$SELFTEST/logs" "$SELFTEST/drained"
printf 'selftest\n' > "$SELFTEST/phase-drained.log"
printf 'selftest\n' > "$SELFTEST/phase-results.txt"
printf 'selftest\n' > "$SELFTEST/logs/selftest.log"
printf 'selftest\n' > "$SELFTEST/drained/selftest.txt"
ST_EXPECTED="$(recovery_expected_count "$SELFTEST")"
ST_WHOLE=0
if recovery_digests "$SELFTEST" > "$SELFTEST/.m" 2>/dev/null; then
    if manifest_complete "$SELFTEST/.m" "$ST_EXPECTED"; then ST_WHOLE=1; fi
fi
rm -f "$SELFTEST/drained/selftest.txt"
ST_SHORT=0
recovery_digests "$SELFTEST" > "$SELFTEST/.m2" 2>/dev/null || true
if ! manifest_complete "$SELFTEST/.m2" "$ST_EXPECTED"; then ST_SHORT=1; fi
# A path the hashing loop cannot consume: `find -type f` lists it, the loop
# splits it at the newline, and sha256sum cannot read either half.
printf 'selftest\n' > "$SELFTEST/drained/selftest.txt"
touch "$SELFTEST/drained/broken
name" 2>/dev/null || true
ST_FAILS=0
if ! recovery_digests "$SELFTEST" > "$SELFTEST/.m3" 2>/dev/null; then ST_FAILS=1; fi
rm -rf "$SELFTEST" 2>/dev/null || true
emit ""
emit "0. the completeness check, measured on a disposable set before borrowing"
emit "   accepts a whole set ($ST_EXPECTED files): $ST_WHOLE   (expected 1)"
emit "   rejects a set with one file missing:      $ST_SHORT   (expected 1)"
emit "   FAILS when a path cannot be hashed:       $ST_FAILS   (expected 1)"
emit "     what this shows: the fixture is a path containing a newline, which this"
emit "     implementation's line-reading loop cannot consume, so it is a REAL hashing"
emit "     error that really ends the run non-zero."
emit "     what it does NOT show: the previous implementation failing. Measured on"
emit "     this same fixture, its 'find -exec sha256sum {} +' hashed the file and"
emit "     exited 0. That implementation's defect -- an unconditional 'exit 0' and a"
emit "     sort-terminated pipeline discarding both statuses -- is a property of the"
emit "     code, not something this fixture reproduces."
[ "$ST_WHOLE" = "1" ] || bad "the completeness check rejected a whole set; it cannot be trusted"
[ "$ST_SHORT" = "1" ] || bad "the completeness check accepted a set with a file missing"
[ "$ST_FAILS" = "1" ] || bad "a path that cannot be hashed did not fail the manifest"
if [ "$ST_WHOLE" != "1" ] || [ "$ST_SHORT" != "1" ] || [ "$ST_FAILS" != "1" ]; then
    emit "    REFUSED: the completeness check does not behave as required, so this"
    emit "             control will not borrow evidence it cannot prove it restored"
    exit 1
fi

SAVED="$EVIDENCE_DIR/.drain-evidence-saved-$$"
mkdir -p "$SAVED/drained" "$SAVED/logs"
# Copy the whole recovery set, then prove the COPY is complete before a single
# byte of the original is borrowed.
#
# Verifying only drained/ left phase-drained.log, phase-results.txt and logs/
# copied on trust: `cp` failures were swallowed, and an incomplete copy would
# have been discovered only at restore time, when the originals were already
# overwritten and the bad copy was all that remained.
if [ -f "$EVIDENCE_DIR/phase-drained.log" ]; then
    cp "$EVIDENCE_DIR/phase-drained.log" "$SAVED/phase-drained.log" 2>/dev/null || true
fi
if [ -f "$EVIDENCE_DIR/phase-results.txt" ]; then
    cp "$EVIDENCE_DIR/phase-results.txt" "$SAVED/phase-results.txt" 2>/dev/null || true
fi
if [ -d "$EVIDENCE_DIR/logs" ]; then
    cp -R "$EVIDENCE_DIR/logs/." "$SAVED/logs/" 2>/dev/null || true
fi
if [ -d "$EVIDENCE_DIR/drained" ]; then
    cp -R "$EVIDENCE_DIR/drained/." "$SAVED/drained/" 2>/dev/null || true
fi
PR_BEFORE="$EVIDENCE_DIR/.drain-pr-before-$$"
if [ -f "$EVIDENCE_DIR/phase-results.txt" ]; then
    cp "$EVIDENCE_DIR/phase-results.txt" "$PR_BEFORE" 2>/dev/null || true
fi
SET_EXPECTED="$(recovery_expected_count "$EVIDENCE_DIR")"
if ! recovery_digests "$EVIDENCE_DIR" > "$SAVED/.set-before" 2>/dev/null; then
    rm -rf "$SAVED" 2>/dev/null || true
    emit "    REFUSED: hashing the retained evidence FAILED, so this control cannot"
    emit "             prove it put it back; nothing has been changed"
    exit 1
fi
if ! manifest_complete "$SAVED/.set-before" "$SET_EXPECTED"; then
    rm -rf "$SAVED" 2>/dev/null || true
    emit "    REFUSED: the retained evidence manifest is INCOMPLETE -- $SET_EXPECTED file(s)"
    emit "             expected, $(grep -c . "$SAVED/.set-before" 2>/dev/null || echo 0) hashed. Nothing has been changed."
    exit 1
fi
# The copy is hashed with the same instrument and must be complete on its own
# terms as well as identical: two short manifests can agree with each other.
SET_COPY_EXPECTED="$(recovery_expected_count "$SAVED")"
if ! recovery_digests "$SAVED" > "$SAVED/.set-copy" 2>/dev/null; then
    SAVED_KEPT="$SAVED"
    emit "    REFUSED: hashing the recovery COPY failed, so a complete recovery copy"
    emit "             cannot be proved to exist; nothing has been borrowed. The copy"
    emit "             is RETAINED at $SAVED"
    exit 1
fi
if ! manifest_complete "$SAVED/.set-copy" "$SET_COPY_EXPECTED" \
   || [ "$SET_COPY_EXPECTED" != "$SET_EXPECTED" ]; then
    SAVED_KEPT="$SAVED"
    emit "    REFUSED: the recovery copy is INCOMPLETE -- $SET_EXPECTED file(s) expected,"
    emit "             $SET_COPY_EXPECTED found in the copy. Nothing has been borrowed and the"
    emit "             copy is RETAINED at $SAVED"
    exit 1
fi
if ! diff "$SAVED/.set-before" "$SAVED/.set-copy" >/dev/null 2>&1; then
    SAVED_KEPT="$SAVED"
    emit "    REFUSED: the recovery copy is INCOMPLETE -- it does not match the"
    emit "             evidence it was taken from, so a complete recovery copy does"
    emit "             not exist and nothing has been borrowed. The partial copy is"
    emit "             RETAINED at $SAVED"
    emit "             differences (left: original, right: copy):"
    diff "$SAVED/.set-before" "$SAVED/.set-copy" 2>&1 | head -20 | while IFS= read -r _dl; do
        emit "               $_dl"
    done
    exit 1
fi
# A complete, verified recovery copy of every shared output now exists. Only
# from here may the evidence be borrowed, and only from here may cleanup write
# anything back.
SAVE_VERIFIED=1
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
# run_phase appends this control's outcome to the shared, cumulative result
# ledger. It is recorded HERE, attributed to this control, and the baseline
# ledger is put back in cleanup -- so the negative result is preserved with
# its provenance instead of sitting unattributed under the real suite's row.
CTL_RESULT_LINE="$(tail -1 "$EVIDENCE_DIR/phase-results.txt" 2>/dev/null || true)"
emit ""
emit "3. what the real assertion did with it"
emit "   phase exit status:            $PHASE_EXIT   (expected non-zero)"
emit "   it named the fixture:         ${REJECT_LINE:-<no rejection line>}"
emit "   it summarised the rejection:  ${REJECT_N:-0} line(s)   (expected >= 1)"
emit "   full phase output retained at .evidence/drain-rejects-nonterminal-phase.log"
emit "   the line it appended to the shared phase ledger, which belongs to THIS"
emit "   negative control and not to the 21-phase baseline:"
emit "       ${CTL_RESULT_LINE:-<none>}"
emit "   (phase-results.txt is restored to the baseline below; this row is the"
emit "    attributed copy)"
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
# The shared ledger is compared against its PRE-RUN CONTENTS, not merely
# checked for this control's row at the end. A last-line check passes just as
# happily on a ledger that lost rows, gained different ones, or had its
# earlier lines rewritten -- none of which is "restored".
PR_RESTORED=0
if [ -f "$PR_BEFORE" ]; then
    if diff "$PR_BEFORE" "$EVIDENCE_DIR/phase-results.txt" >/dev/null 2>&1; then
        PR_RESTORED=1
    else
        echo "error: phase-results.txt differs from its pre-run contents:" >&2
        diff "$PR_BEFORE" "$EVIDENCE_DIR/phase-results.txt" >&2 || true
    fi
fi
emit ""
emit "restoration (read back from the running stack):"
emit "   the fixture's own outcome:    $FINAL   (recovery resolved it once the"
emit "                                 renamers came back; it is this control's"
emit "                                 own synthetic job, identified above)"
emit "   renamer-1 / renamer-2 / watcher: $L1 / $L2 / $LW   (expected healthy x3)"
emit "   retained drain evidence readable: $([ "${DRAIN_OK:-0}" -ge 1 ] && echo yes || echo NO)"
emit "   the whole recovery set came back byte-for-byte: $SLOT_RESTORED   (expected yes;"
emit "                                 phase-drained.log, phase-results.txt, logs/ and"
emit "                                 drained/, compared by sha256 against the manifest"
emit "                                 taken before the phase ran)"
emit "   state copies left behind:     $(ls "$EVIDENCE_DIR/state/$NEW_RUN".* 2>/dev/null | grep -c . || true)   (expected 0)"
emit "   shared phase ledger restored: $([ "$PR_RESTORED" = "1" ] && echo yes || echo NO)   (expected yes:"
emit "                                 phase-results.txt compared line-for-line"
emit "                                 against a copy taken before the phase ran)"
emit "   recovery copy:                ${SAVED_KEPT:-removed (the evidence is provably back)}"
{ [ "$L1" = "healthy" ] && [ "$L2" = "healthy" ] && [ "$LW" = "healthy" ]; } \
    || bad "the live stack is not healthy after this control ($L1/$L2/$LW)"
[ "${DRAIN_OK:-0}" -ge 1 ] || bad "the retained drain evidence was not restored"
[ "$SLOT_RESTORED" = "yes" ] || bad "the evidence recovery set was not restored byte-for-byte ($SLOT_RESTORED)"
[ "$PR_RESTORED" = "1" ] || bad "phase-results.txt was not restored to its pre-run contents"
[ -z "$PR_BEFORE" ] || rm -f "$PR_BEFORE" 2>/dev/null || true
[ -z "$SAVED_KEPT" ] || bad "restoration was not confirmed; the recovery copy is retained at $SAVED_KEPT"
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
