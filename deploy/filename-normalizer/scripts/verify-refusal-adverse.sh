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
# Exact resources THIS control created, cleared by name and nothing else.
C_FOREIGN_VOL="fn-adverse-foreign-$LOWER"
MADE_FOREIGN_VOL=0
# Exact resources the driver (this control's child) reported it retained.
# Parsed from its own report, never guessed from a prefix.
RETAINED_DOCS=""
RETAINED_DBS=""
RETAINED_VOLS=""
RETAINED_SVCS=""
RETAINED_LIVE=0

report_begin refusal-adverse "$OUT" "$0"

emit() { printf '%s\n' "$*" >> "$OUT"; printf '%s\n' "$*"; }
bad() { printf '    MISMATCH: %s\n' "$*" >&2; emit "    MISMATCH: $*"; FAILURES=$((FAILURES + 1)); }

# Does this control hold the lock RIGHT NOW? Re-read, never cached: the flag
# records what was true when it was set, not who owns the directory now.
adv_holds_lock() {
    [ "$STOLE_LOCK" = "1" ] || return 1
    _ah="$(cat "$EVIDENCE_DIR/.exercise.lock/owner" 2>/dev/null)" || return 1
    case "$_ah" in *"adverse-control pid=$$"*) return 0 ;; esac
    return 1
}
# Take the lock the same atomic way lib.sh does.
adv_take_lock() {
    if mkdir "$EVIDENCE_DIR/.exercise.lock" 2>/dev/null; then
        printf 'adverse-control pid=%s at=%s\n' "$$" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
            > "$EVIDENCE_DIR/.exercise.lock/owner"
        STOLE_LOCK=1
        return 0
    fi
    return 1
}
# Release ONLY a lock whose owner file still names this process.
adv_drop_lock() {
    [ "$STOLE_LOCK" = "1" ] || return 0
    if adv_holds_lock; then
        rm -rf "$EVIDENCE_DIR/.exercise.lock" 2>/dev/null || true
    else
        echo "warning: the lock is no longer this control's; leaving it in place." >&2
    fi
    STOLE_LOCK=0
    return 0
}

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
    # This control's own disposable volume, by exact name, under authority.
    if [ "$MADE_FOREIGN_VOL" != "0" ]; then
        if adv_holds_lock || adv_take_lock; then
            docker volume rm "$C_FOREIGN_VOL" >/dev/null 2>&1 || true
            if _fv="$(docker volume ls -q --filter "name=^${C_FOREIGN_VOL}$" 2>/dev/null)"; then
                if [ -z "$_fv" ]; then
                    MADE_FOREIGN_VOL=0
                else
                    echo "error: $C_FOREIGN_VOL remains" >&2; RESTORE_OK=0
                fi
            else
                echo "error: could not list volumes; removal of $C_FOREIGN_VOL is UNCONFIRMED" >&2
                RESTORE_OK=0
            fi
        else
            echo "error: the exercise lock is held elsewhere; $C_FOREIGN_VOL was NOT removed" >&2
            RESTORE_OK=0
        fi
    fi
    adv_drop_lock
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
# C. A positively foreign resource survives a rejected invocation.
#
# This used to create a volume, remove it, inspect it and then narrate what
# the driver's rule "would" be. None of the code under review ran: the case
# proved that `docker volume inspect` fails on a volume that is not there,
# which nobody doubted. It also swallowed its own creation failure with
# `|| true`, so if the volume was never created the case still "passed".
#
# Now the real exercise runs against a volume this control created and
# labelled as somebody else's. Its ownership machinery is what decides the
# outcome, and the assertions below are about what that machinery did.
# ---------------------------------------------------------------------------
log "3/5: a foreign resource put in the exercise's path"
C_FOREIGN_TAG="not-this-run-$STAMP"
C_LOG="$EVIDENCE_DIR/.adverse-foreign-$$.log"
# The prerequisite is ASSERTED, not assumed: a case built on a resource that
# was never created proves nothing about preserving it.
C_MADE=0
if docker volume create --label "fn.owner=$C_FOREIGN_TAG" "$C_FOREIGN_VOL" >/dev/null 2>&1; then
    MADE_FOREIGN_VOL=1
    C_MADE=1
else
    bad "could not create the foreign volume; case C was NOT exercised"
fi
C_LABEL_BEFORE=unreadable
C_SHA_BEFORE=ABSENT
if [ "$C_MADE" = "1" ]; then
    C_LABEL_BEFORE="$(docker volume inspect -f '{{index .Labels "fn.owner"}}' "$C_FOREIGN_VOL" 2>/dev/null || echo unreadable)"
    docker run --rm -v "$C_FOREIGN_VOL:/v" "$UTIL_IMAGE" \
        sh -c 'printf "belongs to somebody else\n" > /v/canary.txt' >/dev/null 2>&1 || true
    C_SHA_BEFORE="$(docker run --rm -v "$C_FOREIGN_VOL:/v:ro" "$UTIL_IMAGE" \
        sh -c 'if [ -f /v/canary.txt ]; then sha256sum /v/canary.txt | cut -d" " -f1; else echo ABSENT; fi' \
        2>/dev/null | tr -d ' \r\n')"
    if [ "$C_LABEL_BEFORE" != "$C_FOREIGN_TAG" ]; then
        bad "the foreign volume does not carry its foreign label; case C was NOT exercised"
        C_MADE=0
    fi
    case "$C_SHA_BEFORE" in ABSENT|"") bad "could not seed the foreign volume; case C was NOT exercised"; C_MADE=0 ;; esac
fi
C_EXIT=0
C_VOLS_MADE=0
if [ "$C_MADE" = "1" ]; then
    FN_HU_BACKUP_VOL="$C_FOREIGN_VOL" sh "$EXERCISE" > "$C_LOG" 2>&1 || C_EXIT=$?
    C_VOLS_MADE="$(docker volume ls -q 2>/dev/null | grep -c '^fn-hu-backup-' || true)"
fi
C_LABEL_AFTER="$(docker volume inspect -f '{{index .Labels "fn.owner"}}' "$C_FOREIGN_VOL" 2>/dev/null || echo unreadable)"
C_SHA_AFTER="$(docker run --rm -v "$C_FOREIGN_VOL:/v:ro" "$UTIL_IMAGE" \
    sh -c 'if [ -f /v/canary.txt ]; then sha256sum /v/canary.txt | cut -d" " -f1; else echo ABSENT; fi' \
    2>/dev/null | tr -d ' \r\n')"
C_PRESENT="$(docker volume ls -q --filter "name=^${C_FOREIGN_VOL}$" 2>/dev/null | grep -c . || true)"
C_NAMED="$(grep -ac "volume $C_FOREIGN_VOL already exists" "$C_LOG" 2>/dev/null || true)"
C_BOUNDARY="$(grep -ac 'ownership pre-flight' "$C_LOG" 2>/dev/null || true)"
emit ""
emit "C. a positively foreign volume in the exercise's path"
emit "   the volume this control created: $C_FOREIGN_VOL"
emit "   its label, before:            $C_LABEL_BEFORE   (expected $C_FOREIGN_TAG:"
emit "                                 the prerequisite is asserted, not assumed)"
emit "   exercise exit status:         $C_EXIT   (expected non-zero)"
emit "   it reached the ownership boundary: $C_BOUNDARY line(s)   (expected >= 1)"
emit "   it named the volume:          $C_NAMED line(s)   (expected >= 1)"
emit "   the volume still exists:      $C_PRESENT   (expected 1: a rejected"
emit "                                 invocation must not delete a stranger's resource)"
emit "   its label, after:             $C_LABEL_AFTER   (expected unchanged)"
emit "   its contents:                 $([ "$C_SHA_AFTER" = "$C_SHA_BEFORE" ] && echo unchanged || echo "CHANGED ($C_SHA_AFTER)")"
emit "   backup volumes it created:    $C_VOLS_MADE   (expected 0: it mutated nothing)"
if [ "$C_MADE" != "1" ]; then
    bad "the foreign volume was not established; case C was NOT exercised"
else
    [ "$C_EXIT" != "0" ] || bad "the exercise succeeded against a foreign volume"
    [ "${C_BOUNDARY:-0}" -ge 1 ] || bad "the exercise never reached its ownership boundary"
    [ "${C_NAMED:-0}" -ge 1 ] || bad "the exercise did not name the volume it refused"
    [ "${C_PRESENT:-0}" = "1" ] || bad "the foreign volume was removed by an invocation that refused it"
    [ "$C_LABEL_AFTER" = "$C_LABEL_BEFORE" ] || bad "the foreign volume's ownership label changed"
    [ "$C_SHA_AFTER" = "$C_SHA_BEFORE" ] || bad "the foreign volume's contents changed"
    [ "${C_VOLS_MADE:-1}" = "0" ] || bad "$C_VOLS_MADE backup volume(s) were created by a rejected invocation"
fi

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
# Anything the driver reported as retained is this control's to clear -- and
# ONLY that.
#
# This used to release the lock first and then delete by prefix: every volume
# matching `fn-refusal-vol-`, every database matching `fn_refusal_db_%`, every
# document matching `hu-held-refusal-*.pdf`, the shared `renamer-hold`, and a
# recreate of renamer-1, renamer-2 and watcher -- all with no lock held. A
# concurrent exercise that had just taken the lock would have had its
# resources destroyed underneath it by the very control that exists to prove
# resources are not destroyed. The driver names what it retained, by exact
# name, so those exact names are what get cleared, under authority held for
# the whole of it.
D_REPORT="$(grep -a -A8 'cleaned up NOTHING' "$D_LOG" 2>/dev/null | sed 's/^ *//' || true)"
RETAINED_DOCS="$(printf '%s\n' "$D_REPORT" | sed -n 's|^document /srv/fn/consume/||p')"
RETAINED_DBS="$(printf '%s\n' "$D_REPORT" | sed -n 's|^database ||p')"
RETAINED_VOLS="$(printf '%s\n' "$D_REPORT" | sed -n 's|^volume ||p')"
RETAINED_SVCS="$(printf '%s\n' "$D_REPORT" | sed -n 's|^fault service ||p')"
if printf '%s\n' "$D_REPORT" | grep -q 'live applications may still be stopped'; then
    RETAINED_LIVE=1
fi
D_RETAINED="$(printf '%s\n' "$D_REPORT" | grep -aE '^(document|database|volume|fault service) ' | tr '\n' ';')"
emit "   the driver named as retained: ${D_RETAINED:-none}"

# Authority FIRST, and kept until the clearing is finished. The stolen lock is
# still this control's at this point; if it is not, nothing is touched.
D_CLEAR_OK=1
D_LEFT=0
if adv_holds_lock || adv_take_lock; then
    for _doc in $RETAINED_DOCS; do
        compose run --rm --no-deps -T -e FN_T="/srv/fn/consume/$_doc" --entrypoint sh storage-init \
            -c 'rm -f "$FN_T"' >/dev/null 2>&1 </dev/null || true
        case "$(probe_exists "/srv/fn/consume/$_doc")" in
            no) : ;;
            *)  echo "error: retained document $_doc could not be confirmed removed" >&2
                D_CLEAR_OK=0; D_LEFT=$((D_LEFT + 1)) ;;
        esac
    done
    for _db in $RETAINED_DBS; do
        compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" \
            postgres-primary psql -U "${FN_DB_USER:-fn_app}" -d postgres \
            -c "DROP DATABASE IF EXISTS $_db;" </dev/null >/dev/null 2>&1 || true
        _dbn="$(compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" \
            postgres-primary psql -U "${FN_DB_USER:-fn_app}" -d postgres -tA \
            -c "SELECT count(*) FROM pg_database WHERE datname='$_db';" </dev/null 2>/dev/null | tr -d ' \r\n')"
        case "$_dbn" in
            0) : ;;
            *) echo "error: retained database $_db remains (read '$_dbn')" >&2
               D_CLEAR_OK=0; D_LEFT=$((D_LEFT + 1)) ;;
        esac
    done
    for _vol in $RETAINED_VOLS; do
        docker volume rm "$_vol" >/dev/null 2>&1 || true
        if _vn="$(docker volume ls -q --filter "name=^${_vol}$" 2>/dev/null)"; then
            [ -z "$_vn" ] || { echo "error: retained volume $_vol remains" >&2
                               D_CLEAR_OK=0; D_LEFT=$((D_LEFT + 1)); }
        else
            echo "error: could not list volumes; removal of $_vol is UNCONFIRMED" >&2
            D_CLEAR_OK=0; D_LEFT=$((D_LEFT + 1))
        fi
    done
    for _svc in $RETAINED_SVCS; do
        compose --profile fault stop -t 15 "$_svc" >/dev/null 2>&1 || true
        compose --profile fault rm -f "$_svc" >/dev/null 2>&1 || true
        if _sn="$(compose --profile fault ps -aq "$_svc" 2>/dev/null)"; then
            [ -z "$_sn" ] || { echo "error: retained fault service $_svc remains" >&2
                               D_CLEAR_OK=0; D_LEFT=$((D_LEFT + 1)); }
        else
            echo "error: could not query $_svc; removal is UNCONFIRMED" >&2
            D_CLEAR_OK=0; D_LEFT=$((D_LEFT + 1))
        fi
    done
    # Only if the driver said it had left them stopped.
    if [ "$RETAINED_LIVE" = "1" ]; then
        recreate_service renamer-1 renamer-2 watcher >/dev/null 2>&1 || true
    fi
    D_CLEARED=yes
else
    D_CLEARED=NO
    D_CLEAR_OK=0
    echo "error: the exercise lock is held elsewhere; the retained resources were NOT cleared." >&2
fi
adv_drop_lock
emit "   this control then cleared them: $D_CLEARED   (exact names only, under the lock;"
emit "                                 unconfirmed or remaining: $D_LEFT)"
[ "$D_CLEAR_OK" = "1" ] || bad "$D_LEFT retained resource(s) could not be confirmed cleared"

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
# Recreating shared services is a mutation like any other, so it happens under
# the lock or not at all. The interrupted driver's own cleanup has already
# restored what it stopped; this is the backstop, and a backstop that runs
# without authority is just another way to disrupt whoever holds it now.
if adv_holds_lock || adv_take_lock; then
    recreate_service renamer-1 renamer-2 watcher >/dev/null 2>&1 || true
    adv_drop_lock
else
    echo "warning: the exercise lock is held elsewhere; services were left as the driver restored them." >&2
fi

# Cleanup runs BEFORE the count is emitted. It can discover that a resource
# could not be removed or confirmed, and a "mismatches: 0" printed beforehand
# would be contradicted by the exit status rather than agreeing with it.
cleanup
trap - EXIT INT TERM
[ "$RESTORE_OK" = "1" ] || bad "restoration was incomplete or could not be confirmed"
L1="$(compose ps --format '{{.Health}}' renamer-1 2>/dev/null | head -1)"
L2="$(compose ps --format '{{.Health}}' renamer-2 2>/dev/null | head -1)"
LW="$(compose ps --format '{{.Health}}' watcher 2>/dev/null | head -1)"
FOREIGN_LEFT="$(docker volume ls -q --filter "name=^${C_FOREIGN_VOL}$" 2>/dev/null | grep -c . || true)"
emit ""
emit "restoration (read back from the running stack):"
emit "   renamer-1 / renamer-2 / watcher: $L1 / $L2 / $LW   (expected healthy x3)"
emit "   exercise lock:                   $([ -d "$EVIDENCE_DIR/.exercise.lock" ] && echo HELD || echo released)"
emit "   this control's own volume:       $([ "${FOREIGN_LEFT:-1}" = "0" ] && echo removed || echo "LEFT BEHIND")   (expected removed)"
{ [ "$L1" = "healthy" ] && [ "$L2" = "healthy" ] && [ "$LW" = "healthy" ]; } \
    || bad "the live stack is not healthy after this control ($L1/$L2/$LW)"
if [ -d "$EVIDENCE_DIR/.exercise.lock" ]; then bad "the exercise lock was left held"; fi
[ "${FOREIGN_LEFT:-1}" = "0" ] || bad "this control's own volume $C_FOREIGN_VOL was left behind"

emit ""
emit "mismatches: $FAILURES"

if [ "$FAILURES" = "0" ]; then
    report_restored
    report_success
    log "PASSED: the adverse paths contain themselves and preserve what they do not own"
    note "evidence: $OUT"
    exit 0
fi
echo "FAILED: $FAILURES expectation(s) not met; see $OUT" >&2
exit 1
