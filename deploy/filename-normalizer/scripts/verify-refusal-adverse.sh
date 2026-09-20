#!/bin/sh
# The adverse paths of the refusal driver and the exercise it drives:
# inspections that genuinely fail, a resource that appears after a pre-flight
# has passed, a stolen lock, an interrupted parent, the driver's prerequisite
# guards driven against names that are really occupied, and a measurement of
# what duplicate creation does on the pinned broker.
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
# Case F's occupied names, put in front of the driver's prerequisite guards.
F_DB="fn_refusal_db_advf$LOWER"
F_VOL="fn-refusal-vol-advf-$LOWER"
F_TAG="adverse-case-f-$STAMP"
MADE_F_DB=0
MADE_F_VOL=0
# Case G's disposable vhost.
G_VHOST="fn-adverse-vhost-$LOWER"
G_TAG_A="adverse-G-original-$STAMP"
G_TAG_B="adverse-G-imposter-$STAMP"
MADE_G_VHOST=0
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
    # This control's own disposable volume, by exact name, under authority --
    # and only when the label proved this control created it. Ambiguous
    # ownership is retained and reported, never deleted by name.
    if [ "$MADE_FOREIGN_VOL" = "1" ]; then
        echo "error: volume $C_FOREIGN_VOL is AMBIGUOUS -- this control may or may not" >&2
        echo "       have created it. It is RETAINED, not removed by name." >&2
        RESTORE_OK=0
    fi
    if [ "$MADE_FOREIGN_VOL" = "2" ]; then
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
    # Case F's occupied names, same rules: only what this control provably
    # created, by exact name, under authority, each removal confirmed.
    if [ "$MADE_F_DB" = "1" ] || [ "$MADE_F_VOL" = "1" ]; then
        echo "error: case F resources are AMBIGUOUS; they are RETAINED, not removed by name." >&2
        RESTORE_OK=0
    fi
    if [ "$MADE_F_DB" = "2" ] || [ "$MADE_F_VOL" = "2" ]; then
        if adv_holds_lock || adv_take_lock; then
            if [ "$MADE_F_DB" = "2" ]; then
                compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" \
                    postgres-primary psql -U "${FN_DB_USER:-fn_app}" -d postgres \
                    -c "DROP DATABASE IF EXISTS $F_DB;" </dev/null >/dev/null 2>&1 || true
                case "$(compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" \
                        postgres-primary psql -U "${FN_DB_USER:-fn_app}" -d postgres -tA \
                        -c "SELECT count(*) FROM pg_database WHERE datname='$F_DB';" </dev/null 2>/dev/null | tr -d ' \r\n')" in
                    0) MADE_F_DB=0 ;;
                    *) echo "error: case F database $F_DB remains" >&2; RESTORE_OK=0 ;;
                esac
            fi
            if [ "$MADE_F_VOL" = "2" ]; then
                docker volume rm "$F_VOL" >/dev/null 2>&1 || true
                if _fvv="$(docker volume ls -q --filter "name=^${F_VOL}$" 2>/dev/null)"; then
                    if [ -z "$_fvv" ]; then MADE_F_VOL=0
                    else echo "error: case F volume $F_VOL remains" >&2; RESTORE_OK=0; fi
                else
                    echo "error: could not list volumes; removal of $F_VOL is UNCONFIRMED" >&2
                    RESTORE_OK=0
                fi
            fi
        else
            echo "error: the exercise lock is held elsewhere; case F resources were NOT removed" >&2
            RESTORE_OK=0
        fi
    fi
    # Case G's vhost, removed only because its description proves this control
    # created it. Deleting a vhost takes every queue in it, so an unproven one
    # is retained and named.
    if [ "$MADE_G_VHOST" = "1" ]; then
        echo "error: broker vhost $G_VHOST is AMBIGUOUS; it is RETAINED, not deleted by name." >&2
        RESTORE_OK=0
    fi
    if [ "$MADE_G_VHOST" = "2" ]; then
        compose exec -T rabbitmq rabbitmqctl delete_vhost "$G_VHOST" >/dev/null 2>&1 || true
        if _gv="$(compose exec -T rabbitmq rabbitmqctl list_vhosts --no-table-headers name 2>/dev/null)"; then
            if printf '%s\n' "$_gv" | grep -qx "$G_VHOST"; then
                echo "error: case G vhost $G_VHOST remains" >&2; RESTORE_OK=0
            else
                MADE_G_VHOST=0
            fi
        else
            echo "error: could not list vhosts; removal of $G_VHOST is UNCONFIRMED" >&2
            RESTORE_OK=0
        fi
    fi
    adv_drop_lock
    return 0
}
on_signal() { echo "error: interrupted; cleaning up and stopping." >&2; cleanup; exit 130; }
trap 'cleanup' EXIT
trap 'on_signal' INT TERM

emit "The adverse paths: failing inspections, a resource that appears mid-run, a"
emit "stolen lock, an interrupted parent, prerequisite guards against real occupied"
emit "names, and what duplicate creation on the real broker actually does"
emit ""
emit "The failing inspections are pointed at $DEAD_SOCK,"
emit "which does not exist, so the real docker client genuinely cannot connect."
emit ""

# ---------------------------------------------------------------------------
# A. A failed enumeration must not authorise ownership.
# ---------------------------------------------------------------------------
log "1/7: own_service on a genuinely failing enumeration"
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
log "2/7: the whole exercise against a dead docker endpoint"
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
log "3/7: a volume that appears after the pre-flight has passed"
C_FOREIGN_TAG="not-this-run-$STAMP"
C_LOG="$EVIDENCE_DIR/.adverse-foreign-$$.log"

# The exercise runs FIRST, and the volume appears while it is busy producing
# its fixtures -- after 0/8 said the name was free, before 3/8 creates it.
#
# The previous version created the volume up front, so the exercise refused at
# 0/8 and the evidence reached "0/8: ownership pre-flight" and stopped. The
# branch under review is the one AFTER the creation, where `docker volume
# create` is a no-op on an existing volume and the label read back is somebody
# else's. That branch was never entered, and a refusal at 0/8 is not evidence
# for it.
FN_HU_BACKUP_VOL="$C_FOREIGN_VOL" sh "$EXERCISE" > "$C_LOG" 2>&1 &
ADV_CHILD=$!
C_PID="$ADV_CHILD"
# 1/8 is logged only once the 0/8 pre-flight has passed, so the volume is
# planted strictly inside the window the branch exists for. The window is the
# two fixture steps -- minutes -- so this is a plain ordering, not a race.
_c=0
C_PASSED_PREFLIGHT=0
while [ "$_c" -lt 900 ]; do
    if grep -aq '1/8: producing a held fixture' "$C_LOG" 2>/dev/null; then
        C_PASSED_PREFLIGHT=1; break
    fi
    kill -0 "$C_PID" 2>/dev/null || break
    sleep 1; _c=$((_c + 1))
done

C_MADE=0
C_LABEL_BEFORE=unreadable
C_SHA_BEFORE=ABSENT
if [ "$C_PASSED_PREFLIGHT" = "1" ]; then
    # Ownership is established BEFORE anything is written into this volume or
    # recorded as removable.
    #
    # `docker volume create` is idempotent: it returns 0 on a volume that
    # already exists and applies no labels to it. Recording ownership from
    # that exit status -- which is what this did -- claimed any volume that
    # happened to carry this name, wrote a canary into it before the label was
    # ever checked, and on a foreign label cleared only the local case flag
    # while the cleanup flag stayed set. A stranger's volume would have been
    # written to and then deleted, by the control that exists to show neither
    # happening. The label asked for here comes back only if this create is
    # what made the volume.
    _c_exit=0
    docker volume create --label "fn.owner=$C_FOREIGN_TAG" "$C_FOREIGN_VOL" >/dev/null 2>&1 || _c_exit=$?
    if [ "$_c_exit" != "0" ]; then
        bad "could not plant the volume; case C was NOT exercised and nothing was written"
    else
        C_LABEL_BEFORE="$(docker volume inspect -f '{{index .Labels "fn.owner"}}' "$C_FOREIGN_VOL" 2>/dev/null || echo unreadable)"
        case "$C_LABEL_BEFORE" in
            "$C_FOREIGN_TAG") MADE_FOREIGN_VOL=2; C_MADE=1 ;;
            unreadable)       MADE_FOREIGN_VOL=1
                              bad "could not read $C_FOREIGN_VOL's label; ownership is AMBIGUOUS, nothing was written to it and it will NOT be removed by name" ;;
            *)                MADE_FOREIGN_VOL=0
                              bad "$C_FOREIGN_VOL already existed carrying owner '$C_LABEL_BEFORE'; it is not this control's, nothing was written to it and it will NOT be removed" ;;
        esac
    fi
fi
if [ "$C_MADE" = "1" ]; then
    docker run --rm -v "$C_FOREIGN_VOL:/v" "$UTIL_IMAGE" \
        sh -c 'printf "belongs to somebody else\n" > /v/canary.txt' >/dev/null 2>&1 || true
    C_SHA_BEFORE="$(docker run --rm -v "$C_FOREIGN_VOL:/v:ro" "$UTIL_IMAGE" \
        sh -c 'if [ -f /v/canary.txt ]; then sha256sum /v/canary.txt | cut -d" " -f1; else echo ABSENT; fi' \
        2>/dev/null | tr -d ' \r\n')"
    case "$C_SHA_BEFORE" in ABSENT|"") bad "could not seed the planted volume; case C was NOT exercised"; C_MADE=0 ;; esac
fi
C_EXIT=0
wait "$C_PID" 2>/dev/null || C_EXIT=$?
ADV_CHILD=""
C_VOLS_MADE="$(docker volume ls -q 2>/dev/null | grep -c '^fn-hu-backup-' || true)"
C_LABEL_AFTER="$(docker volume inspect -f '{{index .Labels "fn.owner"}}' "$C_FOREIGN_VOL" 2>/dev/null || echo unreadable)"
C_SHA_AFTER="$(docker run --rm -v "$C_FOREIGN_VOL:/v:ro" "$UTIL_IMAGE" \
    sh -c 'if [ -f /v/canary.txt ]; then sha256sum /v/canary.txt | cut -d" " -f1; else echo ABSENT; fi' \
    2>/dev/null | tr -d ' \r\n')"
C_PRESENT="$(docker volume ls -q --filter "name=^${C_FOREIGN_VOL}$" 2>/dev/null | grep -c . || true)"
# The boundary that had to be reached: the creation site at 3/8, not the
# pre-flight at 0/8.
C_REACHED="$(grep -ac '3/8: backing up the ledger' "$C_LOG" 2>/dev/null || true)"
# The branch itself, quoted from the exercise's own refusal.
C_BRANCH="$(grep -a "carries owner '$C_FOREIGN_TAG'" "$C_LOG" 2>/dev/null | head -1 | sed 's/^ *//')"
C_PREFLIGHT_REFUSAL="$(grep -ac "volume $C_FOREIGN_VOL already exists" "$C_LOG" 2>/dev/null || true)"
emit ""
emit "C. a volume that appears after the pre-flight, refused at the creation site"
emit "   the volume this control planted: $C_FOREIGN_VOL"
emit "   planted only after:           the exercise logged 1/8, i.e. 0/8 had"
emit "                                 already found the name free ($C_PASSED_PREFLIGHT)"
emit "   its label, before:            $C_LABEL_BEFORE   (expected $C_FOREIGN_TAG:"
emit "                                 ownership proved before anything was written)"
emit "   exercise exit status:         $C_EXIT   (expected non-zero)"
emit "   it reached the creation site: $C_REACHED line(s) of '3/8'   (expected >= 1:"
emit "                                 0/8 alone would be the pre-flight, not this branch)"
emit "   refusals at the 0/8 pre-flight: $C_PREFLIGHT_REFUSAL   (expected 0)"
emit "   the branch it took:           ${C_BRANCH:-<not the foreign-label branch>}"
emit "   the volume still exists:      $C_PRESENT   (expected 1)"
emit "   its label, after:             $C_LABEL_AFTER   (expected unchanged)"
emit "   its contents:                 $([ "$C_SHA_AFTER" = "$C_SHA_BEFORE" ] && echo unchanged || echo "CHANGED ($C_SHA_AFTER)")"
emit "   backup volumes it created:    $C_VOLS_MADE   (expected 0)"
if [ "$C_MADE" != "1" ]; then
    bad "the volume was not planted under proven ownership; case C was NOT exercised"
else
    [ "$C_EXIT" != "0" ] || bad "the exercise succeeded against a foreign volume"
    [ "${C_REACHED:-0}" -ge 1 ] || bad "the exercise never reached the 3/8 creation site; the post-creation branch was NOT exercised"
    [ "${C_PREFLIGHT_REFUSAL:-0}" = "0" ] || bad "the exercise refused at the 0/8 pre-flight; the post-creation branch was NOT exercised"
    [ -n "$C_BRANCH" ] || bad "the exercise did not take the post-creation foreign-label branch"
    [ "${C_PRESENT:-0}" = "1" ] || bad "the planted volume was removed by an invocation that refused it"
    [ "$C_LABEL_AFTER" = "$C_LABEL_BEFORE" ] || bad "the planted volume's ownership label changed"
    [ "$C_SHA_AFTER" = "$C_SHA_BEFORE" ] || bad "the planted volume's contents changed"
    [ "${C_VOLS_MADE:-1}" = "0" ] || bad "$C_VOLS_MADE backup volume(s) were created by a rejected invocation"
fi

# ---------------------------------------------------------------------------
# D. The lock changes hands during a handoff.
# ---------------------------------------------------------------------------
log "4/7: stealing the lock while the driver has released it for a child"
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
log "5/7: interrupting the driver while a child exercise owns the lock"
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
# ---------------------------------------------------------------------------
# F. The driver's failed-prerequisite guards, driven by real occupied names.
#
# The guards refuse to create over a name that is already taken and refuse to
# run the case at all. Nothing previously reached them: the driver picks
# per-invocation names, so on an ordinary run the names are always free and
# the guarded branches are dead code from the evidence's point of view. The
# driver's disposable names are now overridable -- the same mechanism the
# exercise already offers it -- so this control can occupy them for real.
# ---------------------------------------------------------------------------
log "6/7: the driver's prerequisite guards, against names that are already taken"
F_LOG="$EVIDENCE_DIR/.adverse-guards-$$.log"
fpsql() {
    compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" \
        postgres-primary psql -U "${FN_DB_USER:-fn_app}" -d "${2:-postgres}" -tA \
        -c "$1" </dev/null 2>/dev/null | tr -d ' \r\n'
}
# The database: CREATE's own outcome is the ownership proof, as everywhere else.
_f_exit=0
compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" \
    postgres-primary psql -U "${FN_DB_USER:-fn_app}" -d postgres \
    -c "CREATE DATABASE $F_DB;" </dev/null >/dev/null 2>&1 || _f_exit=$?
if [ "$_f_exit" = "0" ]; then
    MADE_F_DB=2
    fpsql "CREATE TABLE advf_keep(id int); INSERT INTO advf_keep VALUES (7);" "$F_DB" >/dev/null 2>&1 || true
else
    case "$(fpsql "SELECT count(*) FROM pg_database WHERE datname='$F_DB';")" in
        0) bad "could not create case F's database and it does not exist; case F was NOT exercised" ;;
        1) bad "$F_DB already exists and this control's CREATE failed against it; it is not this control's and will NOT be dropped" ;;
        *) MADE_F_DB=1; bad "could not establish whether $F_DB exists; ownership is AMBIGUOUS and it will NOT be dropped by name" ;;
    esac
fi
# The volume: the label is the ownership proof.
_f_exit=0
docker volume create --label "fn.owner=$F_TAG" "$F_VOL" >/dev/null 2>&1 || _f_exit=$?
if [ "$_f_exit" != "0" ]; then
    bad "could not create case F's volume; case F was NOT exercised"
else
    case "$(docker volume inspect -f '{{index .Labels "fn.owner"}}' "$F_VOL" 2>/dev/null || echo unreadable)" in
        "$F_TAG")   MADE_F_VOL=2
                    docker run --rm -v "$F_VOL:/v" "$UTIL_IMAGE" \
                        sh -c 'printf "case F canary\n" > /v/canary.txt' >/dev/null 2>&1 || true ;;
        unreadable) MADE_F_VOL=1
                    bad "could not read $F_VOL's label; ownership is AMBIGUOUS, nothing was written and it will NOT be removed by name" ;;
        *)          MADE_F_VOL=0
                    bad "$F_VOL already existed under another owner; nothing was written and it will NOT be removed" ;;
    esac
fi
F_ROWS_BEFORE="$(fpsql "SELECT count(*) FROM advf_keep;" "$F_DB")"
F_SHA_BEFORE="$(docker run --rm -v "$F_VOL:/v:ro" "$UTIL_IMAGE" \
    sh -c 'if [ -f /v/canary.txt ]; then sha256sum /v/canary.txt | cut -d" " -f1; else echo ABSENT; fi' \
    2>/dev/null | tr -d ' \r\n')"
F_READY=0
if [ "$MADE_F_DB" = "2" ] && [ "$MADE_F_VOL" = "2" ] && [ "$F_ROWS_BEFORE" = "1" ]; then
    case "$F_SHA_BEFORE" in ABSENT|"") : ;; *) F_READY=1 ;; esac
fi
F_EXIT=0
if [ "$F_READY" = "1" ]; then
    FN_REFUSAL_DB="$F_DB" FN_REFUSAL_VOL="$F_VOL" sh "$DRIVER" > "$F_LOG" 2>&1 || F_EXIT=$?
fi
F_DB_GUARD="$(grep -a "case 3 not exercised, nothing was created" "$F_LOG" 2>/dev/null | head -1 | sed 's/^ *//')"
F_VOL_GUARD="$(grep -a "case 4 not exercised, nothing was created" "$F_LOG" 2>/dev/null | head -1 | sed 's/^ *//')"
F_DB_EXISTS="$(fpsql "SELECT count(*) FROM pg_database WHERE datname='$F_DB';")"
F_ROWS_AFTER="$(fpsql "SELECT count(*) FROM advf_keep;" "$F_DB")"
# The driver seeds `keepme` only on the path the guard refuses. Its absence is
# the positive evidence that the guarded CREATE and seed never ran.
F_KEEPME="$(fpsql "SELECT count(*) FROM information_schema.tables WHERE table_name='keepme';" "$F_DB")"
F_VOL_PRESENT="$(docker volume ls -q --filter "name=^${F_VOL}$" 2>/dev/null | grep -c . || true)"
F_SHA_AFTER="$(docker run --rm -v "$F_VOL:/v:ro" "$UTIL_IMAGE" \
    sh -c 'if [ -f /v/canary.txt ]; then sha256sum /v/canary.txt | cut -d" " -f1; else echo ABSENT; fi' \
    2>/dev/null | tr -d ' \r\n')"
F_VOL_FILES="$(docker run --rm -v "$F_VOL:/v:ro" "$UTIL_IMAGE" \
    sh -c 'find /v -type f | wc -l' 2>/dev/null | tr -d ' \r\n')"
emit ""
emit "F. the driver's prerequisite guards, against names already taken"
emit "   occupied by this control:     database $F_DB, volume $F_VOL"
emit "   both established under proven ownership: $F_READY   (expected 1)"
emit "   driver exit status:           $F_EXIT   (expected non-zero)"
emit "   the database guard said:      ${F_DB_GUARD:-<the guard was not reached>}"
emit "   the volume guard said:        ${F_VOL_GUARD:-<the guard was not reached>}"
emit "   the database still exists:    $F_DB_EXISTS   (expected 1)"
emit "   its rows:                     $F_ROWS_BEFORE -> $F_ROWS_AFTER   (expected 1 -> 1)"
emit "   tables named 'keepme' in it:  $F_KEEPME   (expected 0: the driver seeds one"
emit "                                 only on the path the guard refused, so its"
emit "                                 absence is what proves the CREATE never ran)"
emit "   the volume still exists:      $F_VOL_PRESENT   (expected 1)"
emit "   its contents:                 $([ "$F_SHA_AFTER" = "$F_SHA_BEFORE" ] && echo unchanged || echo "CHANGED ($F_SHA_AFTER)")"
emit "   files in it:                  $F_VOL_FILES   (expected 1: the driver seeds a"
emit "                                 canary of its own only past the guard)"
if [ "$F_READY" != "1" ]; then
    bad "case F's occupied names were not established under proven ownership; case F was NOT exercised"
else
    [ "$F_EXIT" != "0" ] || bad "the driver reported success although both prerequisites were occupied"
    [ -n "$F_DB_GUARD" ] || bad "the driver's database prerequisite guard was not reached"
    [ -n "$F_VOL_GUARD" ] || bad "the driver's volume prerequisite guard was not reached"
    [ "$F_DB_EXISTS" = "1" ] || bad "the occupied database was dropped by a run that refused it"
    [ "$F_ROWS_AFTER" = "$F_ROWS_BEFORE" ] || bad "the occupied database's contents changed"
    [ "${F_KEEPME:-1}" = "0" ] || bad "the driver created its table in a database it had refused"
    [ "${F_VOL_PRESENT:-0}" = "1" ] || bad "the occupied volume was removed by a run that refused it"
    [ "$F_SHA_AFTER" = "$F_SHA_BEFORE" ] || bad "the occupied volume's contents changed"
    [ "${F_VOL_FILES:-0}" = "1" ] || bad "the driver wrote into a volume it had refused ($F_VOL_FILES files)"
fi

# ---------------------------------------------------------------------------
# G. What duplicate creation on the real broker actually does.
#
# The restore exercise used to determine vhost ownership from `add_vhost`'s
# exit status, on the stated grounds that it "fails on a vhost that already
# exists". That was asserted and never measured, and it is false: the call is
# idempotent. A vhost somebody else made would have been adopted on success
# and then deleted, taking every queue in it.
#
# This case measures both halves of the replacement mechanism against the
# pinned broker every time it runs, on a vhost of its own making, so the
# claim in that exercise cannot quietly go stale again.
# ---------------------------------------------------------------------------
log "7/7: what duplicate vhost creation really does on the pinned broker"
rmq() { compose exec -T rabbitmq rabbitmqctl "$@"; }
g_desc() {
    if _gd="$(rmq list_vhosts --no-table-headers name description 2>/dev/null)"; then
        printf '%s\n' "$_gd" | awk -F'\t' -v v="$G_VHOST" \
            'BEGIN{f=0} $1==v{print $2; f=1} END{if(!f) print "__ABSENT__"}' | head -1
    else
        printf '%s\n' "__UNREADABLE__"
    fi
}
G_BROKER="$(docker inspect -f '{{.Config.Image}}' "$(compose ps -q rabbitmq 2>/dev/null | head -1)" 2>/dev/null || echo unknown)"
G_FIRST_EXIT=0
rmq add_vhost "$G_VHOST" --description "$G_TAG_A" >/dev/null 2>&1 || G_FIRST_EXIT=$?
G_DESC_1=unmeasured
if [ "$G_FIRST_EXIT" = "0" ]; then
    G_DESC_1="$(g_desc)"
    case "$G_DESC_1" in
        "$G_TAG_A")      MADE_G_VHOST=2 ;;
        __UNREADABLE__)  MADE_G_VHOST=1
                         bad "could not read vhost metadata; case G's vhost ownership is AMBIGUOUS and it will NOT be deleted" ;;
        *)               MADE_G_VHOST=0
                         bad "$G_VHOST already existed (description '$G_DESC_1'); it is not this control's and will NOT be deleted" ;;
    esac
else
    bad "could not create case G's disposable vhost; case G was NOT exercised"
fi
G_DUP_EXIT=unmeasured
G_DESC_2=unmeasured
if [ "$MADE_G_VHOST" = "2" ]; then
    # The same call again, on a vhost that now exists, asking for a DIFFERENT
    # description. Both results matter.
    G_DUP_EXIT=0
    rmq add_vhost "$G_VHOST" --description "$G_TAG_B" >/dev/null 2>&1 || G_DUP_EXIT=$?
    G_DESC_2="$(g_desc)"
fi
emit ""
emit "G. duplicate vhost creation, measured on the pinned broker"
emit "   broker image:                 $G_BROKER"
emit "   first add_vhost, fresh name:  exit $G_FIRST_EXIT   (expected 0)"
emit "   its description reads:        $G_DESC_1   (expected $G_TAG_A)"
emit "   second add_vhost, SAME name,  exit $G_DUP_EXIT   (expected 0: add_vhost is"
emit "     asking for description B:   IDEMPOTENT, so its exit status is NOT evidence"
emit "                                 of who created the vhost)"
emit "   its description now reads:    $G_DESC_2   (expected $G_TAG_A, unchanged: the"
emit "                                 description is written only at genuine creation"
emit "                                 and is left alone otherwise, which is what makes"
emit "                                 it a usable ownership marker)"
if [ "$MADE_G_VHOST" != "2" ]; then
    bad "case G's vhost was not established under proven ownership; case G was NOT exercised"
else
    [ "$G_DUP_EXIT" = "0" ] || bad "add_vhost on an existing vhost returned $G_DUP_EXIT; the exercise's ownership rule assumes idempotence and must be re-derived"
    [ "$G_DESC_2" = "$G_TAG_A" ] || bad "the description changed to '$G_DESC_2'; it cannot be used as an ownership marker and the exercise's rule must be re-derived"
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
