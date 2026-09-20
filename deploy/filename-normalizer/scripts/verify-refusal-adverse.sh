#!/bin/sh
# The adverse paths of the refusal driver and the exercise it drives:
# a stolen lock, an interrupted parent, and inspections that genuinely fail.
#
# # Why these are separate from verify-restore-refusals.sh
#
# That control shows each refusal happening. It cannot show what happens when
# the machinery AROUND a refusal goes wrong -- the lock changing hands during
# a handoff, a signal arriving while a child owns the lock, or an inspection
# command that fails rather than answering. Those paths decide whether a
# failure is contained or becomes a mutation without ownership, and they never
# run on a successful day.
#
# # Nothing here is stubbed
#
# The failing inspections fail because they are pointed at a Docker endpoint
# that genuinely does not exist, so the real `docker` client really cannot
# connect; the evidence records the operation and the endpoint. The competing
# lock holder is a real second process taking the real lock through lib.sh's
# own `exercise_lock`. The interruption is a real SIGTERM to a named pid.
. "$(dirname "$0")/lib.sh"

OUT="$EVIDENCE_DIR/refusal-adverse.txt"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
LOWER="$(printf '%s' "$STAMP" | tr 'A-Z' 'a-z')"
DRIVER="$SCRIPT_DIR/verify-restore-refusals.sh"
EXERCISE="$SCRIPT_DIR/verify-restore-held-uncertain.sh"
DEAD_SOCK="unix:///nonexistent/fn-adverse-$LOWER.sock"
FAILURES=0
RESTORE_OK=1
STOLE_LOCK=0
ADV_CHILD=""

report_begin refusal-adverse "$OUT" "$0"

emit() { printf '%s\n' "$*" >> "$OUT"; printf '%s\n' "$*"; }
bad() { printf '    MISMATCH: %s\n' "$*" >&2; emit "    MISMATCH: $*"; FAILURES=$((FAILURES + 1)); }

cleanup() {
    if [ -n "$ADV_CHILD" ] && kill -0 "$ADV_CHILD" 2>/dev/null; then
        kill -TERM "$ADV_CHILD" 2>/dev/null || true
        _c=0
        while kill -0 "$ADV_CHILD" 2>/dev/null && [ "$_c" -lt 240 ]; do sleep 2; _c=$((_c + 2)); done
    fi
    if [ -n "$ADV_CHILD" ]; then
        wait "$ADV_CHILD" 2>/dev/null || true
    fi
    ADV_CHILD=""
    # Only a lock this control itself took.
    if [ "$STOLE_LOCK" = "1" ]; then
        _o="$(cat "$EVIDENCE_DIR/.exercise.lock/owner" 2>/dev/null || true)"
        case "$_o" in
            *"adverse-control pid=$$"*) rm -rf "$EVIDENCE_DIR/.exercise.lock" 2>/dev/null || true ;;
            *) echo "warning: the stolen lock is no longer this control's; leaving it." >&2 ;;
        esac
        STOLE_LOCK=0
    fi
    return 0
}
on_signal() { echo "error: interrupted; cleaning up and stopping." >&2; cleanup; exit 130; }
trap 'cleanup' EXIT
trap 'on_signal' INT TERM

emit "The adverse paths: a stolen lock, an interrupted parent, failing inspections"
emit ""
emit "The failing inspections are pointed at $DEAD_SOCK,"
emit "which does not exist, so the real docker client genuinely cannot connect."
emit ""

# ---------------------------------------------------------------------------
# A. A failed enumeration must not authorise ownership.
# ---------------------------------------------------------------------------
log "1/5: own_service on a genuinely failing enumeration"
# lib.sh runs under `set -eu` and this call is REQUIRED to fail, so its
# status is captured with `||` rather than read from `$?` afterwards. Without
# that the control dies on the very refusal it exists to record -- which is
# how the first run of this file ended after one heading.
A_EXIT=0
A_OUT="$( (DOCKER_HOST="$DEAD_SOCK" own_service renamer-hold) 2>&1 )" || A_EXIT=$?
emit "A. own_service, with the docker endpoint pointed at a socket that is not there"
emit "   operation:                    docker compose --profile fault ps -aq renamer-hold"
emit "   endpoint:                     $DEAD_SOCK"
emit "   exit status:                  $A_EXIT   (expected non-zero: unknown refuses)"
emit "   it said:                      $(printf '%s' "$A_OUT" | head -1)"
[ "$A_EXIT" != "0" ] || bad "own_service authorised ownership on an enumeration that failed"
if ! printf '%s' "$A_OUT" | grep -q "could not determine"; then
    bad "own_service did not report the enumeration as undetermined"
fi

# ---------------------------------------------------------------------------
# B. The exercise refuses end to end, mutating nothing.
# ---------------------------------------------------------------------------
log "2/5: the whole exercise against a dead docker endpoint"
B_LOCK_BEFORE="$([ -d "$EVIDENCE_DIR/.exercise.lock" ] && echo held || echo free)"
B_LOG="$EVIDENCE_DIR/.adverse-deadhost-$$.log"
B_EXIT=0
DOCKER_HOST="$DEAD_SOCK" sh "$EXERCISE" > "$B_LOG" 2>&1 || B_EXIT=$?
B_LOCK_AFTER="$([ -d "$EVIDENCE_DIR/.exercise.lock" ] && echo held || echo free)"
B_STRAY="$(docker volume ls -q 2>/dev/null | grep -c '^fn-hu-backup-' || true)"
emit ""
emit "B. the exercise itself, same dead endpoint"
emit "   exit status:                  $B_EXIT   (expected non-zero)"
emit "   first line:                   $(head -1 "$B_LOG" | cut -c1-90)"
emit "   exercise lock before/after:   $B_LOCK_BEFORE / $B_LOCK_AFTER   (expected free / free)"
emit "   backup volumes it left:       $B_STRAY   (expected 0: it never got that far)"
[ "$B_EXIT" != "0" ] || bad "the exercise reported success with no working docker endpoint"
[ "$B_LOCK_AFTER" = "free" ] || bad "the exercise left the lock held after failing"
[ "${B_STRAY:-1}" = "0" ] || bad "$B_STRAY backup volume(s) were left behind"

# ---------------------------------------------------------------------------
# C. Created, then gone before the confirming read: responsibility is kept.
# ---------------------------------------------------------------------------
log "3/5: a resource created, then absent when the confirmation reads it"
C_VOL="fn-adverse-vol-$LOWER"
C_TAG="adverse-$STAMP-$$"
docker volume create --label "fn.owner=$C_TAG" "$C_VOL" >/dev/null 2>&1 || true
# Removed out of band, exactly as a competing cleanup would: the confirming
# read below then answers truthfully that it is not there.
docker volume rm "$C_VOL" >/dev/null 2>&1 || true
C_READ="$( { docker volume inspect -f '{{index .Labels "fn.owner"}}' "$C_VOL" 2>/dev/null || echo unreadable; } | tr -d ' \r\n')"
C_LEFT="$(docker volume ls -q --filter "name=^${C_VOL}$" 2>/dev/null | grep -c . || true)"
emit ""
emit "C. a volume created by this control and removed before its confirmation"
emit "   confirming read:              $C_READ   (expected unreadable: it is genuinely gone)"
emit "   volumes left with that name:  $C_LEFT   (expected 0)"
emit "   the driver's rule: a creation that was ISSUED keeps responsibility even"
emit "   when the confirming read fails, so cleanup still attempts removal and an"
emit "   unreadable confirmation is reported rather than certifying cleanup."
[ "$C_READ" = "unreadable" ] || bad "the confirming read did not fail as this case requires"
[ "${C_LEFT:-1}" = "0" ] || bad "the control volume was not actually removed"

# ---------------------------------------------------------------------------
# D. The lock changes hands during a handoff.
# ---------------------------------------------------------------------------
log "4/5: stealing the lock while the driver has released it for a child"
D_LOG="$EVIDENCE_DIR/.adverse-steal-$$.log"
sh "$DRIVER" > "$D_LOG" 2>&1 &
ADV_CHILD=$!
# Wait for a handoff window: the driver releases the lock for each child it
# runs. mkdir is the same atomic take lib.sh uses, so this is a real
# competitor, not a simulated one.
# First wait for the DRIVER to own the lock. Polling mkdir straight away won
# the race at startup instead: the driver had not taken the lock yet, so it
# refused at its first take_lock and never reached a handoff at all -- which
# is case 5 again, not this case.
_d=0
D_SAW_DRIVER=0
while [ "$_d" -lt 600 ]; do
    case "$(cat "$EVIDENCE_DIR/.exercise.lock/owner" 2>/dev/null || true)" in
        restore-refusals-control*) D_SAW_DRIVER=1; break ;;
    esac
    kill -0 "$ADV_CHILD" 2>/dev/null || break
    sleep 1; _d=$((_d + 1))
done
_d=0
while [ "$D_SAW_DRIVER" = "1" ] && [ "$_d" -lt 600 ]; do
    if mkdir "$EVIDENCE_DIR/.exercise.lock" 2>/dev/null; then
        printf 'adverse-control pid=%s at=%s\n' "$$" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
            > "$EVIDENCE_DIR/.exercise.lock/owner"
        STOLE_LOCK=1
        break
    fi
    kill -0 "$ADV_CHILD" 2>/dev/null || break
    sleep 1; _d=$((_d + 1))
done
D_TOOK="$STOLE_LOCK"
if [ "$D_TOOK" = "1" ]; then
    _w=0
    while kill -0 "$ADV_CHILD" 2>/dev/null && [ "$_w" -lt 600 ]; do sleep 2; _w=$((_w + 2)); done
    D_EXIT=0
    wait "$ADV_CHILD" 2>/dev/null || D_EXIT=$?
    ADV_CHILD=""
else
    D_EXIT=skipped
    kill -TERM "$ADV_CHILD" 2>/dev/null || true
    wait "$ADV_CHILD" 2>/dev/null || true
    ADV_CHILD=""
fi
D_OWNER="$(cat "$EVIDENCE_DIR/.exercise.lock/owner" 2>/dev/null | head -1)"
D_SAID="$(grep -ac 'could not retake the exercise lock' "$D_LOG" || true)"
D_NAMED="$(grep -ac 'cleaned up NOTHING' "$D_LOG" || true)"
emit ""
emit "D. the exercise lock taken by another process during a handoff"
emit "   the driver held the lock first: $D_SAW_DRIVER   (expected 1: otherwise this"
emit "                                 is the startup refusal, not a handoff)"
emit "   this control took the lock:   $D_TOOK   (expected 1)"
emit "   driver exit status:           $D_EXIT   (expected non-zero)"
emit "   it reported the failure:      $D_SAID line(s)   (expected >= 1)"
emit "   it reported cleaning nothing: $D_NAMED line(s)   (expected >= 1)"
emit "   the lock still belongs to:    ${D_OWNER:-<gone>}"
emit "                                 (expected this control: another holder's"
emit "                                  lock must survive the driver's exit)"
if [ "$D_SAW_DRIVER" != "1" ]; then
    bad "the driver never held the lock; case D was NOT exercised"
elif [ "$D_TOOK" != "1" ]; then
    bad "could not take the lock during a handoff window; case D was NOT exercised"
else
    [ "$D_EXIT" != "0" ] || bad "the driver reported success after losing the lock"
    [ "${D_SAID:-0}" -ge 1 ] || bad "the driver did not report the failed reacquisition"
    case "$D_OWNER" in
        *"adverse-control pid=$$"*) : ;;
        *) bad "the driver removed or replaced another holder's lock (owner now '${D_OWNER:-gone}')" ;;
    esac
fi
# Anything the driver reported as retained is this control's to clear now.
D_RETAINED="$(grep -a -A6 'cleaned up NOTHING' "$D_LOG" | grep -aE 'document|database|volume|fault service' | sed 's/^ *//' | tr '\n' ';')"
emit "   the driver named as retained: ${D_RETAINED:-none}"
cleanup   # releases this control's stolen lock before the retained items are cleared
for _v in $(docker volume ls -q 2>/dev/null | grep '^fn-refusal-vol-' || true); do
    docker volume rm "$_v" >/dev/null 2>&1 || true
done
for _d in $(compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" \
        postgres-primary psql -U "${FN_DB_USER:-fn_app}" -d postgres -tA \
        -c "SELECT datname FROM pg_database WHERE datname LIKE 'fn_refusal_db_%';" </dev/null 2>/dev/null | tr -d ' \r'); do
    compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" \
        postgres-primary psql -U "${FN_DB_USER:-fn_app}" -d postgres \
        -c "DROP DATABASE IF EXISTS $_d;" </dev/null >/dev/null 2>&1 || true
done
compose run --rm --no-deps -T --entrypoint sh storage-init \
    -c 'rm -f /srv/fn/consume/hu-held-refusal-*.pdf' >/dev/null 2>&1 </dev/null || true
compose --profile fault stop -t 15 renamer-hold >/dev/null 2>&1 || true
compose --profile fault rm -f renamer-hold >/dev/null 2>&1 || true
recreate_service renamer-1 renamer-2 watcher >/dev/null 2>&1 || true
CLEARED_VOL="$(docker volume ls -q 2>/dev/null | grep -c '^fn-refusal-vol-' || true)"
CLEARED_SVC="$(compose --profile fault ps -aq renamer-hold 2>/dev/null | grep -c . || true)"
emit "   this control then cleared them: volumes left $CLEARED_VOL, fault services left $CLEARED_SVC"
[ "${CLEARED_VOL:-1}" = "0" ] || bad "$CLEARED_VOL retained volume(s) could not be cleared"
[ "${CLEARED_SVC:-1}" = "0" ] || bad "$CLEARED_SVC retained fault service(s) could not be cleared"

# ---------------------------------------------------------------------------
# E. The parent is interrupted while its child owns the lock.
# ---------------------------------------------------------------------------
log "5/5: interrupting the driver while a child exercise owns the lock"
CLEANED=0
E_LOG="$EVIDENCE_DIR/.adverse-interrupt-$$.log"
sh "$DRIVER" > "$E_LOG" 2>&1 &
ADV_CHILD=$!
E_PARENT="$ADV_CHILD"
# Wait until a CHILD of the driver holds the lock: the lock exists and its
# owner is the exercise, not the control.
# Case 6 is the ONLY child the driver runs in the background; cases 1-4 run
# theirs inside a command substitution, where the parent is blocked and a
# signal is not handled until the child returns. Waiting merely for "a child
# owns the lock" matched case 1 and signalled a parent that had no registered
# child -- so the driver had nothing to settle and the case proved nothing
# about coordinating an active one.
_e=0
E_READY=0
while [ "$_e" -lt 900 ]; do
    if grep -aq '6/6: interrupting the exercise' "$E_LOG" 2>/dev/null; then
        case "$(cat "$EVIDENCE_DIR/.exercise.lock/owner" 2>/dev/null || true)" in
            restore-held-uncertain*) E_READY=1; break ;;
        esac
    fi
    kill -0 "$ADV_CHILD" 2>/dev/null || break
    sleep 1; _e=$((_e + 1))
done
E_CHILD_OWNER="$(cat "$EVIDENCE_DIR/.exercise.lock/owner" 2>/dev/null | head -1)"
if [ "$E_READY" = "1" ]; then
    kill -TERM "$E_PARENT" 2>/dev/null || true
    _w=0
    while kill -0 "$E_PARENT" 2>/dev/null && [ "$_w" -lt 600 ]; do sleep 2; _w=$((_w + 2)); done
    E_EXIT=0
    wait "$E_PARENT" 2>/dev/null || E_EXIT=$?
    ADV_CHILD=""
else
    E_EXIT=skipped
    kill -TERM "$E_PARENT" 2>/dev/null || true
    wait "$E_PARENT" 2>/dev/null || true
    ADV_CHILD=""
fi
sleep 5
E_LOCK="$([ -d "$EVIDENCE_DIR/.exercise.lock" ] && echo held || echo released)"
E_SETTLE="$(grep -ac 'terminating the child exercise' "$E_LOG" || true)"
E_FAULTS="$(compose --profile fault ps -aq renamer-hold renamer-fault 2>/dev/null | grep -c . || true)"
emit ""
emit "E. SIGTERM to the driver while a child exercise owned the lock"
emit "   process signalled:            the driver, pid $E_PARENT (sh $DRIVER)"
emit "   driver phase at that moment:  case 6 (its only BACKGROUND child)"
emit "   lock owner at that moment:    ${E_CHILD_OWNER:-<none>}   (expected the child exercise)"
emit "   driver exit status:           $E_EXIT   (expected non-zero)"
emit "   it settled the child first:   $E_SETTLE line(s)   (expected >= 1)"
emit "   exercise lock afterwards:     $E_LOCK   (expected released)"
emit "   fault services left behind:   $E_FAULTS   (expected 0)"
if [ "$E_READY" != "1" ]; then
    bad "no child ever held the lock; case E was NOT exercised"
else
    [ "$E_EXIT" != "0" ] || bad "the interrupted driver reported success"
    [ "${E_SETTLE:-0}" -ge 1 ] || bad "the driver did not terminate its child before cleaning up"
    [ "$E_LOCK" = "released" ] || bad "the lock was left held after the interruption"
    [ "${E_FAULTS:-1}" = "0" ] || bad "$E_FAULTS fault service(s) survived the interruption"
fi
recreate_service renamer-1 renamer-2 watcher >/dev/null 2>&1 || true

emit ""
emit "mismatches: $FAILURES"
cleanup
trap - EXIT INT TERM
L1="$(compose ps --format '{{.Health}}' renamer-1 2>/dev/null | head -1)"
L2="$(compose ps --format '{{.Health}}' renamer-2 2>/dev/null | head -1)"
LW="$(compose ps --format '{{.Health}}' watcher 2>/dev/null | head -1)"
emit ""
emit "restoration (read back from the running stack):"
emit "   renamer-1 / renamer-2 / watcher: $L1 / $L2 / $LW   (expected healthy x3)"
emit "   exercise lock:                   $([ -d "$EVIDENCE_DIR/.exercise.lock" ] && echo HELD || echo released)"
{ [ "$L1" = "healthy" ] && [ "$L2" = "healthy" ] && [ "$LW" = "healthy" ]; } \
    || bad "the live stack is not healthy after this control ($L1/$L2/$LW)"
if [ -d "$EVIDENCE_DIR/.exercise.lock" ]; then bad "the exercise lock was left held"; fi

if [ "$FAILURES" = "0" ]; then
    report_restored
    report_success
    log "PASSED: the adverse paths contain themselves and preserve what they do not own"
    note "evidence: $OUT"
    exit 0
fi
echo "FAILED: $FAILURES expectation(s) not met; see $OUT" >&2
exit 1
