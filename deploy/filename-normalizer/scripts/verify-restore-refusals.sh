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
#
# Overridable for the same reason the exercise's names are (see its header):
# so verify-refusal-adverse.sh can put a REAL occupied name in front of the
# failed-prerequisite guards below and drive them against this exact code
# path, instead of asserting that a guard it never reached would have worked.
# Defaults are per-invocation and unique.
CTL_VOL="${FN_REFUSAL_VOL:-fn-refusal-vol-$LOWER}"
CTL_DB="${FN_REFUSAL_DB:-fn_refusal_db_$LOWER}"
CTL_DOC="${FN_REFUSAL_DOC:-hu-held-refusal-$LOWER.pdf}"
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
CHILD_PID=""

# Ownership is three-valued, not boolean:
#   0  never attempted -- no responsibility, never touch it
#   1  ATTEMPTED       -- creation was issued; disposition unconfirmed
#   2  confirmed       -- creation was issued and verified
#
# The flags used to go straight from 0 to "confirmed" on a post-creation
# inspection, so a creation that SUCCEEDED followed by an inspection that
# failed or came back empty left responsibility unrecorded and cleanup walked
# past the resource. Responsibility is now taken before the creation command
# and only ever upgraded, so a resource this invocation may have made is
# always cleaned up -- and a failed confirmation is reported rather than being
# allowed to certify cleanup.
# Only CONFIRMED ownership authorises a destructive action. Ambiguous
# ownership -- a creation was issued and its outcome could not be established
# -- preserves the resource, names it, and reports restoration incomplete.
# `owned()` covered both and was used for every removal below, so "we might
# have made this" was enough to drop a database or remove a volume by name.
owned_confirmed() { [ "$1" = "2" ]; }
owned_ambiguous() { [ "$1" = "1" ]; }
# For the retained-resources report only: anything this invocation may have
# created is worth naming when cleanup cannot run at all.
owned_any()       { [ "$1" = "1" ] || [ "$1" = "2" ]; }

# Does this process hold the lock RIGHT NOW?
#
# The cached flag is not enough: it says what we believed when we last acted,
# not who owns the directory now. A cached 1 must never authorise a mutation
# after the lock has in fact been released or taken by somebody else.
hold_confirmed() {
    [ "$DRIVER_LOCK" = "1" ] || return 1
    _hc_owner="$(cat "$EVIDENCE_DIR/.exercise.lock/owner" 2>/dev/null)" || return 1
    case "$_hc_owner" in
        *"pid=$$ "*|*"pid=$$") return 0 ;;
    esac
    return 1
}

# Wait for a child that may still own the lock, so the parent never cleans up
# underneath a running exercise.
settle_child() {
    [ -n "$CHILD_PID" ] || return 0
    if kill -0 "$CHILD_PID" 2>/dev/null; then
        echo "note: terminating the child exercise ($CHILD_PID) before cleanup." >&2
        kill -TERM "$CHILD_PID" 2>/dev/null || true
        _sc=0
        while kill -0 "$CHILD_PID" 2>/dev/null && [ "$_sc" -lt 180 ]; do
            sleep 2; _sc=$((_sc + 2))
        done
        if kill -0 "$CHILD_PID" 2>/dev/null; then
            echo "error: the child exercise did not exit; it may still hold the lock." >&2
            RESTORE_OK=0
        fi
    fi
    wait "$CHILD_PID" 2>/dev/null || true
    CHILD_PID=""
    return 0
}

cleanup() {
    [ "$CLEANED" = "0" ] || return 0
    CLEANED=1

    # An active child may own the lock and be mid-mutation. Finish it first.
    settle_child

    # EVERY protected action below needs ownership, not just the service
    # restore at the end. Documents, databases, volumes and fault services
    # were removed before the lock was ever checked.
    if hold_confirmed || take_lock; then
        : # ownership established for the whole protected section
    else
        echo "error: the exercise lock is held elsewhere; this control cleaned up NOTHING." >&2
        echo "       Retained by this invocation and NOT removed:" >&2
        owned_any "$MADE_DOC"  && echo "         document /srv/fn/consume/$CTL_DOC" >&2
        owned_any "$MADE_DB"   && echo "         database $CTL_DB" >&2
        owned_any "$MADE_VOL"  && echo "         volume $CTL_VOL" >&2
        owned_any "$MADE_HOLD" && echo "         fault service renamer-hold" >&2
        [ "$LIVE_TOUCHED" = "1" ] && echo "         live applications may still be stopped" >&2
        RESTORE_OK=0
        DRIVER_LOCK=0
        return 0
    fi

    if owned_ambiguous "$MADE_DOC"; then
        echo "error: document /srv/fn/consume/$CTL_DOC is AMBIGUOUS -- this invocation" >&2
        echo "       may or may not have created it. It is RETAINED, not removed by name." >&2
        RESTORE_OK=0
    fi
    if owned_confirmed "$MADE_DOC"; then
        compose run --rm --no-deps -T -e FN_T="/srv/fn/consume/$CTL_DOC" \
            --entrypoint sh storage-init -c 'rm -f "$FN_T"' >/dev/null 2>&1 </dev/null || true
        case "$(probe_exists "/srv/fn/consume/$CTL_DOC")" in
            no) : ;;
            *)  echo "error: control document $CTL_DOC could not be confirmed removed" >&2; RESTORE_OK=0 ;;
        esac
    fi
    if owned_ambiguous "$MADE_DB"; then
        echo "error: database $CTL_DB is AMBIGUOUS -- this invocation may or may not" >&2
        echo "       have created it. It is RETAINED, not dropped by name." >&2
        RESTORE_OK=0
    fi
    if owned_confirmed "$MADE_DB"; then
        psqlq "DROP DATABASE IF EXISTS $CTL_DB;" postgres >/dev/null 2>&1 || true
        case "$(psqln "SELECT count(*) FROM pg_database WHERE datname='$CTL_DB';" postgres)" in
            0) : ;;
            *)  echo "error: control database $CTL_DB remains" >&2; RESTORE_OK=0 ;;
        esac
    fi
    # A volume carries its own answer, so an ambiguous one is disambiguated by
    # re-reading the label rather than removed on a guess.
    if owned_ambiguous "$MADE_VOL"; then
        case "$(docker volume inspect -f '{{index .Labels "fn.owner"}}' "$CTL_VOL" 2>/dev/null || echo unreadable)" in
            "$CTL_TAG") MADE_VOL=2 ;;
            unreadable) echo "error: volume $CTL_VOL's label is still unreadable; it is RETAINED, not removed by name." >&2
                        RESTORE_OK=0 ;;
            *)          MADE_VOL=0
                        echo "note: volume $CTL_VOL belongs to somebody else; leaving it untouched." >&2 ;;
        esac
    fi
    if owned_confirmed "$MADE_VOL"; then
        docker volume rm "$CTL_VOL" >/dev/null 2>&1 || true
        if _cv="$(docker volume ls -q --filter "name=^${CTL_VOL}$" 2>/dev/null)"; then
            [ -z "$_cv" ] || { echo "error: control volume $CTL_VOL remains" >&2; RESTORE_OK=0; }
        else
            echo "error: could not list volumes; removal of $CTL_VOL unconfirmed" >&2
            RESTORE_OK=0
        fi
    fi
    # A compose service carries no per-invocation marker, so an ambiguous one
    # cannot be disambiguated later. It is retained and named; the next run's
    # pre-flight refuses on it rather than adopting it.
    if owned_ambiguous "$MADE_HOLD"; then
        echo "error: fault service renamer-hold is AMBIGUOUS -- this invocation may or" >&2
        echo "       may not have created it. It is RETAINED, not removed." >&2
        RESTORE_OK=0
    fi
    if owned_confirmed "$MADE_HOLD"; then
        compose --profile fault stop -t 15 renamer-hold >/dev/null 2>&1 || true
        compose --profile fault rm -f renamer-hold >/dev/null 2>&1 || true
        if _ch="$(compose --profile fault ps -aq renamer-hold 2>/dev/null)"; then
            [ -z "$_ch" ] || { echo "error: control fault service renamer-hold remains" >&2; RESTORE_OK=0; }
        else
            echo "error: could not query renamer-hold; removal unconfirmed" >&2
            RESTORE_OK=0
        fi
    fi
    # Ownership was established once at the top of this function, so the
    # restore is inside the same protected section as every removal above.
    if [ "$LIVE_TOUCHED" = "1" ]; then
        recreate_service renamer-1 renamer-2 watcher >/dev/null 2>&1 || true
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
# Responsibility BEFORE the write. `set -C` guarantees nothing is written when
# the path is occupied, so clearing back to 0 on OCCUPIED cannot orphan a file.
MADE_DOC=1
CTL_PLANT="$(compose run --rm --no-deps -T -e FN_T="/srv/fn/consume/$CTL_DOC" \
    --entrypoint sh storage-init -c '
        if [ -e "$FN_T" ]; then echo OCCUPIED; exit 0; fi
        set -C
        if printf "pre-existing document, not this run\n" > "$FN_T" 2>/dev/null
        then echo CREATED; else echo OCCUPIED; fi' </dev/null 2>/dev/null | tr -d ' \r\n')"
case "$CTL_PLANT" in
    CREATED)  MADE_DOC=2 ;;
    OCCUPIED) MADE_DOC=0
              bad "$CTL_DOC already exists; case 1 not exercised and nothing was written" ;;
    *)        bad "could not establish whether $CTL_DOC was created (read '$CTL_PLANT');
        responsibility is retained and cleanup will still try to remove it" ;;
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
        # Responsibility BEFORE the creation. `compose up -d` can leave a
        # container behind and still report failure, and the confirming read
        # can fail on its own; either way this invocation may have made it.
        MADE_HOLD=1
        compose --profile fault up -d renamer-hold >/dev/null 2>&1
        if _post="$(compose --profile fault ps -aq renamer-hold 2>/dev/null)"; then
            if [ -n "$_post" ]; then
                MADE_HOLD=2
            else
                bad "renamer-hold was not created; case 2 not exercised"
            fi
        else
            bad "could not confirm whether renamer-hold was created;
        responsibility is retained and cleanup will still try to remove it"
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
if owned_confirmed "$MADE_HOLD"; then
    compose --profile fault stop -t 15 renamer-hold >/dev/null 2>&1 || true
    compose --profile fault rm -f renamer-hold >/dev/null 2>&1 || true
    if _mh="$(compose --profile fault ps -aq renamer-hold 2>/dev/null)"; then
        [ -z "$_mh" ] && MADE_HOLD=0
    fi
    recreate_service renamer-1 renamer-2 >/dev/null 2>&1 || true
fi

# --- 3. a database that this invocation did not create ---------------------
log "3/6: the restore database name is already taken"
# Refuse on an unreadable pre-check, then take responsibility BEFORE creating.
# The prerequisite STOPS the CREATE it guards. Falling through meant that a
# name already in use was created over: the CREATE failed, the post-check then
# read 1 because somebody else's database was there, ownership was recorded as
# confirmed, a table was written into it and cleanup dropped it.
CASE3_READY=0
_db_pre="$(psqln "SELECT count(*) FROM pg_database WHERE datname='$CTL_DB';" postgres)"
case "$_db_pre" in
    0) MADE_DB=1; CASE3_READY=1 ;;
    "") bad "could not establish whether $CTL_DB is free; case 3 not exercised and nothing was created" ;;
    *) bad "$CTL_DB already exists (read '$_db_pre'); case 3 not exercised, nothing was created and it was left alone" ;;
esac
if [ "$CASE3_READY" = "1" ]; then
    # The CREATE's own exit status decides ownership, and it was being thrown
    # away. A database appearing between the check above and this line makes
    # the CREATE fail; the post-check then read 1 because somebody ELSE's
    # database was there, ownership was recorded as confirmed, a table was
    # written into it and cleanup dropped it.
    _db_exit=0
    compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" postgres-primary \
        psql -U "${FN_DB_USER:-fn_app}" -d postgres -c "CREATE DATABASE $CTL_DB;" >/dev/null 2>&1 </dev/null \
        || _db_exit=$?
    _db_post="$(psqln "SELECT count(*) FROM pg_database WHERE datname='$CTL_DB';" postgres)"
    if [ "$_db_exit" = "0" ]; then
        case "$_db_post" in
            1) MADE_DB=2
               psqlq "CREATE TABLE keepme(id int); INSERT INTO keepme VALUES (42);" "$CTL_DB" >/dev/null 2>&1 ;;
            0) MADE_DB=0
               bad "CREATE DATABASE $CTL_DB reported success but the database is absent; case 3 not exercised" ;;
            *) bad "could not confirm whether $CTL_DB was created (read '$_db_post');
        responsibility is AMBIGUOUS, it will NOT be dropped by name, and restoration is reported unconfirmed" ;;
        esac
    else
        case "$_db_post" in
            0) MADE_DB=0
               bad "CREATE DATABASE $CTL_DB failed and the database is absent; case 3 not exercised and nothing was created" ;;
            1) MADE_DB=0
               bad "$CTL_DB exists but this control's CREATE failed against it; it is not this control's,
        nothing was written into it and it will NOT be dropped" ;;
            *) bad "CREATE DATABASE $CTL_DB failed and its existence could not be established (read '$_db_post');
        responsibility is AMBIGUOUS, it will NOT be dropped by name, and restoration is reported unconfirmed" ;;
        esac
    fi
fi
DB_ROWS_BEFORE="$(psqln "SELECT count(*) FROM keepme;" "$CTL_DB")"
if [ "$MADE_DB" = "2" ]; then
    release_for_child
    R3="$(run_case database "database $CTL_DB already exists" "FN_HU_RESTORE_DB=$CTL_DB")"
    reacquire_after_child
else
    R3="skipped|0|case 3 was not set up, so the exercise was not run against it"
fi
DB_EXISTS_AFTER="$(psqln "SELECT count(*) FROM pg_database WHERE datname='$CTL_DB';" postgres)"
DB_ROWS_AFTER="$(psqln "SELECT count(*) FROM keepme;" "$CTL_DB")"
emit ""
emit "3. the restore database name is already taken"
emit "   exercise exit / REFUSED lines: $(echo "$R3" | cut -d'|' -f1) / $(echo "$R3" | cut -d'|' -f2)   (expected non-zero / >= 1)"
emit "   it said:                       $(echo "$R3" | cut -d'|' -f3-)"
emit "   the database still exists:     $DB_EXISTS_AFTER   (expected 1)"
emit "   its rows:                      $DB_ROWS_BEFORE -> $DB_ROWS_AFTER   (expected 1 -> 1)"
if [ "$MADE_DB" = "2" ]; then
    [ "$(echo "$R3" | cut -d'|' -f2)" -ge 1 ] || bad "the INTENDED refusal (database) was not the one recorded"
    [ "$(echo "$R3" | cut -d'|' -f1)" != "0" ] || bad "the exercise succeeded although the database already existed"
    [ "$DB_EXISTS_AFTER" = "1" ] || bad "the pre-existing database was dropped by a run that refused it"
    [ "$DB_ROWS_AFTER" = "$DB_ROWS_BEFORE" ] || bad "the pre-existing database's contents changed"
fi

# --- 4. a volume that this invocation did not create -----------------------
log "4/6: the backup volume name is already taken"
# `docker volume create` returns 0 on a volume that already exists and does
# not apply the label, so only the label coming back proves this control
# created it -- and only then may its cleanup remove it.
# Establish the name is free, then take responsibility BEFORE creating.
# A failed prerequisite STOPS the creation it guards. Recording the mismatch
# and falling through into `docker volume create` anyway is how this control
# came to create over a name it had just found occupied.
CASE4_READY=0
if _cv_pre="$(docker volume ls -q --filter "name=^${CTL_VOL}$" 2>/dev/null)"; then
    if [ -z "$_cv_pre" ]; then
        MADE_VOL=1
        CASE4_READY=1
    else
        bad "$CTL_VOL already exists; case 4 not exercised, nothing was created and it was left alone"
    fi
else
    bad "could not establish whether $CTL_VOL exists; case 4 not exercised and nothing was created"
fi
if [ "$CASE4_READY" = "1" ]; then
    docker volume create --label "fn.owner=$CTL_TAG" "$CTL_VOL" >/dev/null 2>&1 || true
    _cv_owner="$(docker volume inspect -f '{{index .Labels "fn.owner"}}' "$CTL_VOL" 2>/dev/null || echo unreadable)"
    case "$_cv_owner" in
        "$CTL_TAG") MADE_VOL=2 ;;
        unreadable) bad "could not confirm $CTL_VOL's ownership label;
        responsibility is retained and cleanup will still try to remove it" ;;
        # Positively somebody else's: created between the check and now, and
        # `docker volume create` applies no label to an existing volume.
        # Responsibility for an ATTEMPT is not permission to delete a
        # stranger's resource, so it is dropped back to "not ours".
        *)          MADE_VOL=0
                    CASE4_READY=0
                    bad "$CTL_VOL carries owner '$_cv_owner', not this control's;
        it will NOT be removed by this control's cleanup" ;;
    esac
fi
# Seeded ONLY when the label proved this control created it. Writing into a
# volume whose ownership was just rejected is the mutation-after-refusal this
# whole exercise is about.
VOL_SHA_BEFORE=ABSENT
if [ "$MADE_VOL" = "2" ]; then
    docker run --rm -v "$CTL_VOL:/v" "$UTIL_IMAGE" \
        sh -c 'printf "do not delete\n" > /v/canary.txt' >/dev/null 2>&1
    VOL_SHA_BEFORE="$(vol_sha "$CTL_VOL")"
    case "$VOL_SHA_BEFORE" in ABSENT|"") bad "could not seed the control volume; case 4 not exercised" ;; esac
fi
if [ "$MADE_VOL" = "2" ]; then
    release_for_child
    R4="$(run_case volume "volume $CTL_VOL already exists" "FN_HU_BACKUP_VOL=$CTL_VOL")"
    reacquire_after_child
else
    R4="skipped|0|case 4 was not set up, so the exercise was not run against it"
fi
VOL_PRESENT_AFTER="$(docker volume ls -q --filter "name=^${CTL_VOL}$" 2>/dev/null | wc -l | tr -d ' ')"
VOL_SHA_AFTER="$(vol_sha "$CTL_VOL")"
emit ""
emit "4. the backup volume name is already taken"
emit "   exercise exit / REFUSED lines: $(echo "$R4" | cut -d'|' -f1) / $(echo "$R4" | cut -d'|' -f2)   (expected non-zero / >= 1)"
emit "   it said:                       $(echo "$R4" | cut -d'|' -f3-)"
emit "   the volume still exists:       $VOL_PRESENT_AFTER   (expected 1)"
emit "   its contents:                  $([ "$VOL_SHA_AFTER" = "$VOL_SHA_BEFORE" ] && echo unchanged || echo "CHANGED ($VOL_SHA_AFTER)")"
if [ "$MADE_VOL" = "2" ]; then
    [ "$(echo "$R4" | cut -d'|' -f2)" -ge 1 ] || bad "the INTENDED refusal (volume) was not the one recorded"
    [ "$(echo "$R4" | cut -d'|' -f1)" != "0" ] || bad "the exercise succeeded although the volume already existed"
    [ "$VOL_PRESENT_AFTER" = "1" ] || bad "the pre-existing volume was removed by a run that refused it"
    [ "$VOL_SHA_AFTER" = "$VOL_SHA_BEFORE" ] || bad "the pre-existing volume's contents changed"
fi

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
# Registered so that a signal to THIS process terminates and reaps the child
# before cleanup touches anything the child may still own -- including the
# lock, which the child holds for the whole of its run.
CHILD_PID="$INT_PID"
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
    CHILD_PID=""
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
