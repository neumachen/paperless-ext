#!/bin/sh
# Drive verify-restore-held-uncertain.sh into each of its refusals, and show
# that the resource it refused is still there, unchanged, afterwards.
#
# # Why a separate exercise
#
# The refusals in that script are the parts that run when something is already
# there -- a fault service, a database, a volume, a compose project, a file at
# the destination. Those paths never execute on a successful run, so a
# successful run says nothing about them. It is exactly where a "refusal" that
# records a mismatch and then falls through into a write would hide, which is
# the defect this pass is correcting.
#
# # It drives the real code path
#
# Nothing here reimplements the refusal logic. Each case pre-creates a
# DISPOSABLE resource, points the real exercise at that name through the
# documented overrides, runs it, and then checks three things: it exited
# non-zero, it said it was refusing, and the pre-existing resource still has
# the content it had before.
. "$(dirname "$0")/lib.sh"

OUT="$EVIDENCE_DIR/restore-refusals.txt"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
LOWER="$(printf '%s' "$STAMP" | tr 'A-Z' 'a-z')"
UTIL_IMAGE="${FN_UTIL_IMAGE:-alpine:3}"
SCRIPT="$SCRIPT_DIR/verify-restore-held-uncertain.sh"
FAILURES=0
RESTORE_OK=1

# Disposable resources this control creates, and must get back unchanged.
CTL_VOL="fn-refusal-vol-$LOWER"
CTL_DB="fn_refusal_db_$LOWER"
CTL_DOC="hu-held-refusal-$LOWER.pdf"
MADE_VOL=0
MADE_DB=0
MADE_DOC=0
MADE_HOLD=0
CTL_TAG="restore-refusals-$STAMP-$$"

report_begin restore-refusals "$OUT" "$0"

emit() { printf '%s\n' "$*" >> "$OUT"; printf '%s\n' "$*"; }
bad() { printf '    MISMATCH: %s\n' "$*" >&2; emit "    MISMATCH: $*"; FAILURES=$((FAILURES + 1)); }

psqlq() {
    compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" \
        postgres-primary psql -U "${FN_DB_USER:-fn_app}" -d "${2:-${FN_DB_NAME:-filename_normalizer}}" \
        -tA -c "$1" < /dev/null 2>/dev/null | tr -d '\r' | sed '/^$/d'
}
psqln() { psqlq "$1" "${2:-}" | tr -d ' \n'; }
live_sha() {
    compose run --rm --no-deps -T -e FN_P="$1" --entrypoint sh storage-init \
        -c 'if [ -f "$FN_P" ]; then sha256sum "$FN_P" | cut -d" " -f1; else echo ABSENT; fi' \
        </dev/null 2>/dev/null | tr -d ' \r\n'
}
vol_sha() {
    docker run --rm -v "$1:/v:ro" "$UTIL_IMAGE" \
        sh -c 'if [ -f /v/canary.txt ]; then sha256sum /v/canary.txt | cut -d" " -f1; else echo ABSENT; fi' \
        2>/dev/null | tr -d ' \r\n'
}

CLEANED=0
LIVE_TOUCHED=0
cleanup() {
    [ "$CLEANED" = "0" ] || return 0
    CLEANED=1
    if [ "$MADE_DOC" = "1" ]; then
        compose run --rm --no-deps -T -e FN_T="/srv/fn/consume/$CTL_DOC" \
            --entrypoint sh storage-init -c 'rm -f "$FN_T"' >/dev/null 2>&1 </dev/null || true
        case "$(probe_exists "/srv/fn/consume/$CTL_DOC")" in
            no) : ;;
            *)  echo "error: control document $CTL_DOC could not be confirmed removed" >&2; RESTORE_OK=0 ;;
        esac
    fi
    if [ "$MADE_DB" = "1" ]; then
        psqlq "DROP DATABASE IF EXISTS $CTL_DB;" postgres >/dev/null 2>&1 || true
        case "$(psqln "SELECT count(*) FROM pg_database WHERE datname='$CTL_DB';" postgres)" in
            0) : ;;
            *)  echo "error: control database $CTL_DB remains" >&2; RESTORE_OK=0 ;;
        esac
    fi
    if [ "$MADE_VOL" = "1" ]; then
        docker volume rm "$CTL_VOL" >/dev/null 2>&1 || true
        if _cv="$(docker volume ls -q --filter "name=^${CTL_VOL}$" 2>/dev/null)"; then
            [ -z "$_cv" ] || { echo "error: control volume $CTL_VOL remains" >&2; RESTORE_OK=0; }
        else
            echo "error: could not list volumes; removal of $CTL_VOL unconfirmed" >&2
            RESTORE_OK=0
        fi
    fi
    if [ "$MADE_HOLD" = "1" ]; then
        compose --profile fault stop -t 15 renamer-hold >/dev/null 2>&1 || true
        compose --profile fault rm -f renamer-hold >/dev/null 2>&1 || true
        if _ch="$(compose --profile fault ps -aq renamer-hold 2>/dev/null)"; then
            [ -z "$_ch" ] || { echo "error: control fault service renamer-hold remains" >&2; RESTORE_OK=0; }
        else
            echo "error: could not query renamer-hold; removal unconfirmed" >&2
            RESTORE_OK=0
        fi
    fi
    # Only if this control actually disturbed them. The exercise it drives
    # refuses in its pre-flight, before touching the live applications, so on
    # the ordinary path there is nothing here to put back.
    # Restoring the shared applications is itself a mutation, so it happens
    # only while holding the lock. Case 6 releases the lock for the child it
    # interrupts; a failure in that window used to reach here and recreate the
    # renamers and watcher with no lock held at all.
    if [ "$LIVE_TOUCHED" = "1" ]; then
        if [ "$DRIVER_LOCK" = "1" ] || take_lock; then
            recreate_service renamer-1 renamer-2 watcher >/dev/null 2>&1 || true
        else
            echo "error: the exercise lock is held elsewhere; the live applications" >&2
            echo "       were NOT restored by this control. They may still be stopped." >&2
            RESTORE_OK=0
        fi
    fi
    drop_lock
    return 0
}
on_signal() {
    echo "error: interrupted; cleaning up and stopping." >&2
    cleanup
    exit 130
}
trap 'cleanup' EXIT
trap 'on_signal' INT TERM

# The driver mutates the same stack the exercise does -- a document in the
# consume root, a fault service, a database, a volume -- so it holds the
# exercise lock while it does, and releases it only for the moment the child
# needs it. Without that, a concurrent exercise could be running while this
# one plants and removes fixtures under it.
DRIVER_LOCK=0
take_lock() {
    exercise_lock restore-refusals-control || return 1
    DRIVER_LOCK=1
    return 0
}
# Releases ONLY a lock this process owns.
#
# `exercise_unlock` is an unconditional `rm -rf`. Combined with a stale
# DRIVER_LOCK -- which is what a failed reacquisition inside a command
# substitution leaves behind, since the subshell's assignment never reaches
# the parent -- it would delete a lock another exercise had legitimately
# taken. The owner file records this pid, so it is checked first.
drop_lock() {
    [ "$DRIVER_LOCK" = "1" ] || return 0
    _dl_owner="$(cat "$EVIDENCE_DIR/.exercise.lock/owner" 2>/dev/null || true)"
    case "$_dl_owner" in
        *"pid=$$ "*|*"pid=$$") exercise_unlock ;;
        *) echo "warning: the exercise lock is not this process's; leaving it in place." >&2 ;;
    esac
    DRIVER_LOCK=0
    return 0
}
# The lock handoff around a child run, performed by the PARENT.
#
# It used to live inside run_case, which every caller invokes through `$( )`.
# A command substitution is a subshell: `DRIVER_LOCK=0` and the later `=1`
# were set in a process that then exited, so the parent's view never changed,
# and `exit 1` on a failed reacquisition ended only the subshell. The parent
# carried on believing it held a lock it did not.
release_for_child() { drop_lock; }
reacquire_after_child() {
    take_lock || {
        echo "error: could not retake the exercise lock after the child ran." >&2
        echo "       Another exercise now holds it; stopping without touching it." >&2
        DRIVER_LOCK=0
        exit 1
    }
}
take_lock || { echo "error: another exercise holds the lock; nothing was changed." >&2; exit 1; }

emit "Every refusal stops the work that depended on it, and preserves what it refused"
emit ""

# A refusal case: run the real exercise with one name pointed at a resource
# this control already created, and require a non-zero exit with a REFUSED
# line. The exercise holds the exercise lock, so these run one at a time.
run_case() {
    _rc_name="$1"; _rc_want="$2"; shift 2
    _rc_log="$EVIDENCE_DIR/.refusal-$_rc_name-$$.log"
    # lib.sh runs under `set -eu`, and this invocation is REQUIRED to fail.
    # Without the `||` the control aborts on the very refusal it exists to
    # produce -- which is how the first run of this file ended after printing
    # one heading, while the refusal underneath it had worked correctly.
    _rc_exit=0
    ( for _kv in "$@"; do export "$_kv"; done
      sh "$SCRIPT" ) > "$_rc_log" 2>&1 || _rc_exit=$?
    # The INTENDED refusal, not merely some refusal. An unrelated failure with
    # the fixture left untouched would otherwise count as having exercised
    # this boundary.
    _rc_refused="$(grep -a 'REFUSED:' "$_rc_log" | grep -ac -- "$_rc_want" || true)"
    _rc_text="$(grep -a 'REFUSED:' "$_rc_log" | head -1 | sed 's/^ *//')"
    printf '%s|%s|%s\n' "$_rc_exit" "${_rc_refused:-0}" "$_rc_text"
}

# --- 1. a document already at the destination ------------------------------
log "1/6: a file already at the held fixture's destination"
# Exclusive create: this control must not truncate a file it did not make,
# which is the very defect it exists to check for in the exercise.
CTL_PLANT="$(compose run --rm --no-deps -T -e FN_T="/srv/fn/consume/$CTL_DOC" \
    --entrypoint sh storage-init -c '
        if [ -e "$FN_T" ]; then echo OCCUPIED; exit 0; fi
        set -C
        if printf "pre-existing document, not this run\n" > "$FN_T" 2>/dev/null
        then echo CREATED; else echo OCCUPIED; fi' </dev/null 2>/dev/null | tr -d ' \r\n')"
case "$CTL_PLANT" in
    CREATED)  MADE_DOC=1 ;;
    OCCUPIED) bad "$CTL_DOC already exists; case 1 not exercised and nothing was written" ;;
    *)        bad "could not establish whether $CTL_DOC exists (read '$CTL_PLANT'); case 1 not exercised" ;;
esac
DOC_SHA_BEFORE="$(live_sha "/srv/fn/consume/$CTL_DOC")"
release_for_child
R1="$(run_case destination "already occupies /srv/fn/consume/$CTL_DOC" "FN_HU_HELD_NAME=$CTL_DOC")"
reacquire_after_child
DOC_SHA_AFTER="$(live_sha "/srv/fn/consume/$CTL_DOC")"
emit "1. a pre-existing document occupies the destination"
emit "   exercise exit / REFUSED lines: $(echo "$R1" | cut -d'|' -f1) / $(echo "$R1" | cut -d'|' -f2)   (expected non-zero / >= 1)"
emit "   it said:                       $(echo "$R1" | cut -d'|' -f3-)"
emit "   the document's bytes:          $([ "$DOC_SHA_AFTER" = "$DOC_SHA_BEFORE" ] && echo unchanged || echo "CHANGED ($DOC_SHA_AFTER)")"
[ "$(echo "$R1" | cut -d'|' -f1)" != "0" ] || bad "the exercise succeeded although the destination was occupied"
[ "$(echo "$R1" | cut -d'|' -f2)" -ge 1 ] || bad "the INTENDED refusal (occupied destination) was not the one recorded"
[ "$DOC_SHA_AFTER" = "$DOC_SHA_BEFORE" ] || bad "the pre-existing document was modified or removed"

# --- 2. a fault service that this invocation did not create ----------------
log "2/6: a fault service is already running"
# Refuse to adopt. `compose up -d` on an existing service returns success and
# this used to record it as created -- so a service somebody else was using
# would have been removed by this control's own cleanup.
if _pre="$(compose --profile fault ps -aq renamer-hold 2>/dev/null)"; then
    if [ -n "$_pre" ]; then
        bad "renamer-hold already exists; case 2 not exercised and it was left alone"
    else
        compose --profile fault up -d renamer-hold >/dev/null 2>&1
        if [ -n "$(compose --profile fault ps -aq renamer-hold 2>/dev/null)" ]; then
            MADE_HOLD=1
        else
            bad "could not create the control fault service; case 2 not exercised"
        fi
    fi
else
    bad "could not determine whether renamer-hold exists; case 2 not exercised"
fi
HOLD_ID_BEFORE="$(compose --profile fault ps -q renamer-hold 2>/dev/null | head -1)"
release_for_child
R2="$(run_case service "fault service 'renamer-hold' already exists")"
reacquire_after_child
HOLD_ID_AFTER="$(compose --profile fault ps -q renamer-hold 2>/dev/null | head -1)"
emit ""
emit "2. a fault service exists that this invocation did not create"
emit "   exercise exit / REFUSED lines: $(echo "$R2" | cut -d'|' -f1) / $(echo "$R2" | cut -d'|' -f2)   (expected non-zero / >= 1)"
emit "   it said:                       $(echo "$R2" | cut -d'|' -f3-)"
emit "   the service's container:       $([ -n "$HOLD_ID_AFTER" ] && [ "$HOLD_ID_AFTER" = "$HOLD_ID_BEFORE" ] && echo "the same one, still running" || echo "CHANGED OR REMOVED")"
[ "$(echo "$R2" | cut -d'|' -f2)" -ge 1 ] || bad "the INTENDED refusal (fault service) was not the one recorded"
[ "$(echo "$R2" | cut -d'|' -f1)" != "0" ] || bad "the exercise succeeded although a fault service already existed"
[ -n "$HOLD_ID_AFTER" ] || bad "the pre-existing fault service was removed by a run that refused it"
[ "$HOLD_ID_AFTER" = "$HOLD_ID_BEFORE" ] || bad "the pre-existing fault service was replaced"
# Only if THIS control created it. This used to run unconditionally, so a
# renamer-hold the control had just refused -- because somebody else owned it
# -- was stopped and removed anyway.
if [ "$MADE_HOLD" = "1" ]; then
    compose --profile fault stop -t 15 renamer-hold >/dev/null 2>&1 || true
    compose --profile fault rm -f renamer-hold >/dev/null 2>&1 || true
    if _mh="$(compose --profile fault ps -aq renamer-hold 2>/dev/null)"; then
        [ -z "$_mh" ] && MADE_HOLD=0
    fi
    recreate_service renamer-1 renamer-2 >/dev/null 2>&1 || true
fi

# --- 3. a database that this invocation did not create ---------------------
log "3/6: the restore database name is already taken"
compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" postgres-primary \
    psql -U "${FN_DB_USER:-fn_app}" -d postgres -c "CREATE DATABASE $CTL_DB;" >/dev/null 2>&1 </dev/null
if [ "$(psqln "SELECT count(*) FROM pg_database WHERE datname='$CTL_DB';" postgres)" = "1" ]; then
    MADE_DB=1
    psqlq "CREATE TABLE keepme(id int); INSERT INTO keepme VALUES (42);" "$CTL_DB" >/dev/null 2>&1
else
    bad "could not create the control database; case 3 not exercised"
fi
DB_ROWS_BEFORE="$(psqln "SELECT count(*) FROM keepme;" "$CTL_DB")"
release_for_child
R3="$(run_case database "database $CTL_DB already exists" "FN_HU_RESTORE_DB=$CTL_DB")"
reacquire_after_child
DB_EXISTS_AFTER="$(psqln "SELECT count(*) FROM pg_database WHERE datname='$CTL_DB';" postgres)"
DB_ROWS_AFTER="$(psqln "SELECT count(*) FROM keepme;" "$CTL_DB")"
emit ""
emit "3. the restore database name is already taken"
emit "   exercise exit / REFUSED lines: $(echo "$R3" | cut -d'|' -f1) / $(echo "$R3" | cut -d'|' -f2)   (expected non-zero / >= 1)"
emit "   it said:                       $(echo "$R3" | cut -d'|' -f3-)"
emit "   the database still exists:     $DB_EXISTS_AFTER   (expected 1)"
emit "   its rows:                      $DB_ROWS_BEFORE -> $DB_ROWS_AFTER   (expected 1 -> 1)"
[ "$(echo "$R3" | cut -d'|' -f2)" -ge 1 ] || bad "the INTENDED refusal (database) was not the one recorded"
[ "$(echo "$R3" | cut -d'|' -f1)" != "0" ] || bad "the exercise succeeded although the database already existed"
[ "$DB_EXISTS_AFTER" = "1" ] || bad "the pre-existing database was dropped by a run that refused it"
[ "$DB_ROWS_AFTER" = "$DB_ROWS_BEFORE" ] || bad "the pre-existing database's contents changed"

# --- 4. a volume that this invocation did not create -----------------------
log "4/6: the backup volume name is already taken"
# `docker volume create` returns 0 on a volume that already exists and does
# not apply the label, so only the label coming back proves this control
# created it -- and only then may its cleanup remove it.
docker volume create --label "fn.owner=$CTL_TAG" "$CTL_VOL" >/dev/null 2>&1 || true
_cv_owner="$(docker volume inspect -f '{{index .Labels "fn.owner"}}' "$CTL_VOL" 2>/dev/null || echo unreadable)"
case "$_cv_owner" in
    "$CTL_TAG") MADE_VOL=1 ;;
    unreadable) bad "could not read $CTL_VOL's ownership label; case 4 not exercised" ;;
    *)          bad "$CTL_VOL exists and is not this control's (owner '$_cv_owner'); case 4 not exercised" ;;
esac
# Seeded ONLY when the label proved this control created it. Writing into a
# volume whose ownership was just rejected is the mutation-after-refusal this
# whole exercise is about.
VOL_SHA_BEFORE=ABSENT
if [ "$MADE_VOL" = "1" ]; then
    docker run --rm -v "$CTL_VOL:/v" "$UTIL_IMAGE" \
        sh -c 'printf "do not delete\n" > /v/canary.txt' >/dev/null 2>&1
    VOL_SHA_BEFORE="$(vol_sha "$CTL_VOL")"
    case "$VOL_SHA_BEFORE" in ABSENT|"") bad "could not seed the control volume; case 4 not exercised" ;; esac
fi
release_for_child
R4="$(run_case volume "volume $CTL_VOL already exists" "FN_HU_BACKUP_VOL=$CTL_VOL")"
reacquire_after_child
VOL_PRESENT_AFTER="$(docker volume ls -q --filter "name=^${CTL_VOL}$" 2>/dev/null | wc -l | tr -d ' ')"
VOL_SHA_AFTER="$(vol_sha "$CTL_VOL")"
emit ""
emit "4. the backup volume name is already taken"
emit "   exercise exit / REFUSED lines: $(echo "$R4" | cut -d'|' -f1) / $(echo "$R4" | cut -d'|' -f2)   (expected non-zero / >= 1)"
emit "   it said:                       $(echo "$R4" | cut -d'|' -f3-)"
emit "   the volume still exists:       $VOL_PRESENT_AFTER   (expected 1)"
emit "   its contents:                  $([ "$VOL_SHA_AFTER" = "$VOL_SHA_BEFORE" ] && echo unchanged || echo "CHANGED ($VOL_SHA_AFTER)")"
[ "$(echo "$R4" | cut -d'|' -f2)" -ge 1 ] || bad "the INTENDED refusal (volume) was not the one recorded"
[ "$(echo "$R4" | cut -d'|' -f1)" != "0" ] || bad "the exercise succeeded although the volume already existed"
[ "$VOL_PRESENT_AFTER" = "1" ] || bad "the pre-existing volume was removed by a run that refused it"
[ "$VOL_SHA_AFTER" = "$VOL_SHA_BEFORE" ] || bad "the pre-existing volume's contents changed"

# --- 5. the lock is held: a rejected invocation restores NOTHING -----------
log "5/6: the exercise lock is held by somebody else"
# This control already holds the lock for its own mutations, so the child
# simply runs without it being released -- no lock directory is fabricated.
R1_ID_BEFORE="$(compose ps -q renamer-1 2>/dev/null | head -1)"
R2_ID_BEFORE="$(compose ps -q renamer-2 2>/dev/null | head -1)"
W_ID_BEFORE="$(compose ps -q watcher 2>/dev/null | head -1)"
_lk_exit=0
sh "$SCRIPT" > "$EVIDENCE_DIR/.refusal-lock-$$.log" 2>&1 || _lk_exit=$?
R1_ID_AFTER="$(compose ps -q renamer-1 2>/dev/null | head -1)"
R2_ID_AFTER="$(compose ps -q renamer-2 2>/dev/null | head -1)"
W_ID_AFTER="$(compose ps -q watcher 2>/dev/null | head -1)"
LOCK_MSG="$(grep -ac 'holds the lock' "$EVIDENCE_DIR/.refusal-lock-$$.log" || true)"
emit ""
emit "5. the exercise lock is held by another invocation"
emit "   exercise exit:                 $_lk_exit   (expected non-zero)"
emit "   it named the lock:             ${LOCK_MSG:-0} line(s)   (expected >= 1)"
emit "   the live containers:           $([ "$R1_ID_AFTER" = "$R1_ID_BEFORE" ] && [ "$R2_ID_AFTER" = "$R2_ID_BEFORE" ] && [ "$W_ID_AFTER" = "$W_ID_BEFORE" ] && echo "the same three, untouched" || echo "RECREATED")"
emit "                                  (a rejected invocation must not restore"
emit "                                   an active exercise's applications)"
[ "$_lk_exit" != "0" ] || bad "the exercise ran although the lock was held"
[ "${LOCK_MSG:-0}" -ge 1 ] || bad "the exercise did not report the held lock"
[ -n "$R1_ID_AFTER" ] && [ "$R1_ID_AFTER" = "$R1_ID_BEFORE" ] || bad "renamer-1 was recreated by an invocation that never acquired the lock"
[ -n "$R2_ID_AFTER" ] && [ "$R2_ID_AFTER" = "$R2_ID_BEFORE" ] || bad "renamer-2 was recreated by an invocation that never acquired the lock"
[ -n "$W_ID_AFTER" ] && [ "$W_ID_AFTER" = "$W_ID_BEFORE" ] || bad "watcher was recreated by an invocation that never acquired the lock"

# --- 6. a catchable interruption ends the exercise -------------------------
log "6/6: interrupting the exercise once it has started changing things"
LIVE_TOUCHED=1
INT_LOG="$EVIDENCE_DIR/.refusal-interrupt-$$.log"
release_for_child
sh "$SCRIPT" > "$INT_LOG" 2>&1 &
INT_PID=$!
# Wait for an actual MUTATION, not for the heading that precedes one. The
# "1/8" line is printed before the child stops the renamers or creates any
# service, so signalling on it proved only that an idle process can exit.
# The fault service existing is a change to the stack.
_i=0
INT_READY=0
while [ "$_i" -lt 240 ]; do
    if _ir="$(compose --profile fault ps -aq renamer-hold 2>/dev/null)"; then
        [ -n "$_ir" ] && { INT_READY=1; break; }
    fi
    kill -0 "$INT_PID" 2>/dev/null || break
    sleep 2; _i=$((_i + 2))
done
if [ "$INT_READY" = "1" ]; then
    kill -TERM "$INT_PID" 2>/dev/null || true
    _w=0
    while kill -0 "$INT_PID" 2>/dev/null && [ "$_w" -lt 180 ]; do sleep 2; _w=$((_w + 2)); done
    # `wait` returns the child's non-zero status, and under lib.sh's `set -e`
    # that aborts this control before it can record it -- which is exactly
    # what happened the first time: the child interrupted and cleaned up
    # correctly, and the control died reporting 130 as its own exit.
    INT_EXIT=0
    wait "$INT_PID" 2>/dev/null || INT_EXIT=$?
else
    INT_EXIT=unknown
    bad "the exercise never reached a mutating step; case 6 not exercised"
    kill -TERM "$INT_PID" 2>/dev/null || true
    wait "$INT_PID" 2>/dev/null || true
fi
sleep 10
INT_STOPPED="$(grep -ac 'interrupted; cleaning up and stopping' "$INT_LOG" || true)"
# A failed enumeration piped into `wc -l` is 0, and 0 is the answer that says
# nothing was left behind.
if _if="$(compose --profile fault ps -aq renamer-hold renamer-fault 2>/dev/null)"; then
    INT_FAULTS="$(printf '%s' "$_if" | grep -c . || true)"
else
    INT_FAULTS=unreadable
fi
INT_LOCK="$([ -d "$EVIDENCE_DIR/.exercise.lock" ] && echo held || echo released)"
# Exactly what the evidence excludes: every step after the one interrupted.
# The child was signalled while step 1 held the fault service, so none of
# steps 2 through 8 -- the uncertain fixture, the backup, the restore, the
# restored stack, the redelivery, the quarantine, the live-system checks --
# may appear after it.
INT_PAST=0
for _st in '2/8: producing an uncertain' '3/8: backing up' '4/8: restoring into' \
           '5/8: starting real applications' '6/8: redelivering' \
           '7/8: restored sources' '8/8: the live system'; do
    _n="$(grep -ac -- "$_st" "$INT_LOG" || true)"
    INT_PAST=$((INT_PAST + ${_n:-0}))
done
reacquire_after_child
recreate_service renamer-1 renamer-2 watcher >/dev/null 2>&1 || true
emit ""
emit "6. a catchable interruption (SIGTERM) part-way through"
emit "   exercise exit:                 $INT_EXIT   (expected non-zero)"
emit "   it said it was stopping:       ${INT_STOPPED:-0} line(s)   (expected >= 1)"
emit "   fault services left behind:    $INT_FAULTS   (expected 0)"
emit "   exercise lock:                 $INT_LOCK   (expected released)"
emit "   the fault service existed before signalling: yes (a real mutation, not a heading)"
emit "   later steps (2/8 through 8/8) executed: ${INT_PAST:-0}   (expected 0)"
emit "                                   the evidence excludes the uncertain fixture,"
emit "                                   the backup, the restore, the restored stack,"
emit "                                   the redelivery, the quarantine and the"
emit "                                   live-system checks -- none ran after the signal"
[ "$INT_EXIT" != "0" ] || bad "an interrupted exercise reported success"
[ "${INT_STOPPED:-0}" -ge 1 ] || bad "the exercise did not report stopping on the signal"
[ "${INT_FAULTS:-1}" = "0" ] || bad "fault services after the interruption: $INT_FAULTS (expected 0; 'unreadable' is not absence)"
[ "$INT_LOCK" = "released" ] || bad "the exercise lock was not released after the interruption"
[ "${INT_PAST:-1}" = "0" ] || bad "the exercise resumed mutating after the signal"

emit ""
emit "In every case the exercise stopped at the refusal and the resource it"
emit "refused was still there afterwards with the same content."
emit ""
emit "mismatches: $FAILURES"
cleanup
trap - EXIT INT TERM
L1="$(compose ps --format '{{.Health}}' renamer-1 2>/dev/null | head -1)"
L2="$(compose ps --format '{{.Health}}' renamer-2 2>/dev/null | head -1)"
LW="$(compose ps --format '{{.Health}}' watcher 2>/dev/null | head -1)"
emit ""
emit "restoration (read back from the running stack):"
emit "   control resources removed and confirmed: $([ "$RESTORE_OK" = "1" ] && echo yes || echo NO)"
emit "   renamer-1 / renamer-2 / watcher:         $L1 / $L2 / $LW   (expected healthy x3)"
[ "$RESTORE_OK" = "1" ] || bad "restoration was incomplete"
{ [ "$L1" = "healthy" ] && [ "$L2" = "healthy" ] && [ "$LW" = "healthy" ]; } \
    || bad "the live stack is not healthy after this control ($L1/$L2/$LW)"

if [ "$FAILURES" = "0" ]; then
    report_restored
    report_success
    log "PASSED: each refusal stopped the dependent work and preserved what it refused"
    note "evidence: $OUT"
    exit 0
fi
echo "FAILED: $FAILURES expectation(s) not met; see $OUT" >&2
exit 1
