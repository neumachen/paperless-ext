#!/bin/sh
# Qualify backup and restore for the two outcomes a restore must NOT change:
# held work and uncertain work -- by RUNNING the restored system, not by
# reading it.
#
# # What this establishes
#
# `verify-restore-replay.sh` covered delivered work and sources. This covers
# the outcomes that carry an operator decision, and it does so against real
# application processes started on the restored database and the restored
# filesystem, in their own compose project, database and broker vhost.
#
# # The fixtures are produced, not written
#
#   held      a foreign file arrives at the reserved name AFTER the receipt is
#             committed and while the document is still invisible, so
#             publication refuses and the job is held with category
#             `destination_conflict`. Planting it earlier is a different
#             thing: the naming policy then reserves a suffixed name and the
#             job is delivered.
#   uncertain an instance is killed BEFORE the link, leaving `publishing`;
#             recovery cannot confirm whether a publication happened and
#             records `uncertain`.
#
# No ledger row is edited to manufacture either state.
#
# # Ownership
#
# Every resource is established as created by THIS invocation before anything
# depends on it, and only such resources are cleaned up. A refusal stops the
# work that would have depended on it; it never falls through into a write. An
# inspection that could not run is `unknown`, and unknown authorises nothing.
. "$(dirname "$0")/lib.sh"

OUT="$EVIDENCE_DIR/restore-held-uncertain.txt"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
LOWER="$(printf '%s' "$STAMP" | tr 'A-Z' 'a-z')"
UTIL_IMAGE="${FN_UTIL_IMAGE:-alpine:3}"
PROD="$DEPLOY_DIR/production"
FAILURES=0

# Ownership flags. Three-valued, because "might have created it" and "did not
# create it" are different answers and only one of them authorises a delete:
#
#   0  not this invocation's -- never attempted, or POSITIVELY foreign
#   1  AMBIGUOUS             -- a creation was issued and its outcome could
#                              not be established either way
#   2  confirmed ours        -- the creation demonstrably made the resource
#
# Only 2 authorises a delete. 1 is retained and REPORTED, not deleted: with
# nothing to prove the resource is this invocation's, removing it by name is
# a coin toss with somebody else's data, and "cleanup tried" would read as
# "cleanup succeeded". An ambiguous resource fails restoration and is named in
# the report so a person can settle it.
#
# Ownership is decided by the creating command's own outcome, not by a check
# taken beforehand. A freeness check answers "was the name free a moment ago",
# which a resource appearing in between makes false; the creation answers "did
# I make this", which is the question cleanup actually needs.
owned_confirmed() { [ "$1" = "2" ]; }
owned_ambiguous() { [ "$1" = "1" ]; }
OWNS_LOCK=0
OWNS_HOLD_SVC=0
OWNS_FAULT_SVC=0
OWNS_BACKUP_VOL=0
OWNS_RESTORE_DB=0
OWNS_RESTORE_VHOST=0
OWNS_RESTORE_PROJECT=0
FOREIGN_PLANTED=0

# Overridable so verify-restore-refusals.sh can drive a REAL collision on each
# of these boundaries against this exact code path, rather than testing a copy
# of the refusal logic. Defaults are per-invocation and unique.
HDOC="${FN_HU_HELD_NAME:-hu-held-$LOWER.pdf}"
BACKUP_VOL="${FN_HU_BACKUP_VOL:-fn-hu-backup-$$}"
RESTORE_DB="${FN_HU_RESTORE_DB:-fn_restore_hu_$$}"
RESTORE_VHOST="${FN_HU_RESTORE_VHOST:-fn-hu-$$}"
RESTORE_PROJECT="${FN_HU_RESTORE_PROJECT:-fnhu$$}"
SCRATCH=""
UDOC=""
FOREIGN_INODE=""
RESTORE_OK=1
HU_RUN_TAG="restore-held-uncertain-$STAMP-$$"

report_begin restore-held-uncertain "$OUT" "$0"

emit() { printf '%s\n' "$*" >> "$OUT"; printf '%s\n' "$*"; }
bad() { printf '    MISMATCH: %s\n' "$*" >&2; emit "    MISMATCH: $*"; FAILURES=$((FAILURES + 1)); }
fatal() {
    printf '    REFUSED: %s\n' "$*" >&2
    emit "    REFUSED: $*"
    FAILURES=$((FAILURES + 1))
    exit 1
}

psqlq() {
    compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" \
        postgres-primary psql -U "${FN_DB_USER:-fn_app}" -d "${2:-${FN_DB_NAME:-filename_normalizer}}" \
        -tA -c "$1" < /dev/null 2>/dev/null | tr -d '\r' | sed '/^$/d'
}
psqln() { psqlq "$1" "${2:-}" | tr -d ' \n'; }
in_storage() {
    compose run --rm --no-deps -T --entrypoint sh storage-init -c "$1" 2>/dev/null < /dev/null | tr -d '\r'
}
hu() { docker compose -p "$RESTORE_PROJECT" -f "$PROD/compose.prod.yml" \
        -f "$PROD/compose.local-rehearsal.yml" --env-file "$SCRATCH/prod.env" "$@"; }

# Creation ONLY. The refusal check is a separate step the caller performs
# before it claims ownership: `own_service` inside here ran after the flag was
# already set, so refusing a service somebody else owns still left this
# invocation's cleanup willing to remove it.
start_fault() {
    _sf="$1"; shift
    ( for _kv in "$@"; do export "$_kv"; done
      compose --profile fault up -d "$_sf" >/dev/null 2>&1 ) || return 1
    return 0
}
submit() {
    compose run --rm --no-deps -T -e FN_N="/srv/fn/incoming/$1" --entrypoint sh storage-init \
        -c 'printf "%%PDF-1.4 held uncertain fixture\n" > /srv/fn/incoming/.wip-hu && mv /srv/fn/incoming/.wip-hu "$FN_N"' \
        </dev/null >/dev/null 2>&1
}
await_committed_invisible() {
    _job=""; _i=0
    while [ "$_i" -lt 180 ]; do
        [ -n "$_job" ] || _job="$(psqln "SELECT job_id FROM jobs WHERE source_name = '$1';")"
        if [ -n "$_job" ]; then
            if [ "$(psqln "SELECT count(*) FROM delivery_receipts WHERE job_id = '$_job';")" = "1" ] \
               && [ "$(probe_exists "/srv/fn/consume/$2")" = "no" ]; then
                printf '%s' "$_job"; return 0
            fi
        fi
        sleep 2; _i=$((_i + 2))
    done
    printf '%s' "$_job"; return 1
}
# sha256 of a path inside the live storage roots, or a definite token.
live_sha() {
    compose run --rm --no-deps -T -e FN_P="$1" --entrypoint sh storage-init \
        -c 'if [ -f "$FN_P" ]; then sha256sum "$FN_P" | cut -d" " -f1; else echo ABSENT; fi' \
        </dev/null 2>/dev/null | tr -d ' \r\n'
}
# sha256 of a path inside the restored host tree.
restored_sha() {
    docker run --rm -v "$SCRATCH:/s:ro" -e FN_P="/s/$1" "$UTIL_IMAGE" \
        sh -c 'if [ -f "$FN_P" ]; then sha256sum "$FN_P" | cut -d" " -f1; else echo ABSENT; fi' \
        2>/dev/null | tr -d ' \r\n'
}
short() { printf '%s' "$1" | cut -c1-12; }

# Event ids for one job, from a query that is REQUIRED to have succeeded.
#
# `psqlq ... > file` pipes through tr and sed, so the pipeline's status is the
# last command's and a failed query left an EMPTY file. An empty baseline then
# compared clean against anything, and the exercise reported "0 original
# records lost" having read none. A job that has reached a terminal outcome
# always has history, so zero rows is a failed read here, not an answer.
capture_event_ids() {
    _ce_job="$1"; _ce_db="$2"; _ce_out="$3"
    _ce_rows="$(compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" \
        postgres-primary psql -U "${FN_DB_USER:-fn_app}" \
        -d "${_ce_db:-${FN_DB_NAME:-filename_normalizer}}" -tA \
        -c "SELECT event_id FROM job_events WHERE job_id='$_ce_job' ORDER BY event_id;" \
        </dev/null 2>/dev/null)" || return 1
    printf '%s\n' "$_ce_rows" | tr -d '\r' | sed '/^$/d' > "$_ce_out"
    [ -s "$_ce_out" ] || return 1
    return 0
}

CLEANED=0
LIVE_TOUCHED=0
cleanup() {
    # Runs at most once, however it is reached.
    [ "$CLEANED" = "0" ] || return 0
    CLEANED=1

    # A rejected invocation restores NOTHING. Without the lock another
    # exercise owns this stack, and recreating its renamers and watcher -- as
    # this used to do unconditionally -- would disrupt the run that legitimately
    # holds it. Nothing below was reached in that case either.
    if [ "$OWNS_LOCK" != "1" ]; then
        return 0
    fi

    # Only what this invocation established, and every removal is confirmed.
    if [ "$OWNS_RESTORE_PROJECT" = "1" ] && [ -n "$SCRATCH" ]; then
        hu down -v --remove-orphans >/dev/null 2>&1 || true
        if _leftout="$(docker ps -aq --filter "label=com.docker.compose.project=$RESTORE_PROJECT" 2>/dev/null)"; then
            _left="$(printf '%s' "$_leftout" | grep -c . || true)"
            [ "$_left" = "0" ] || { echo "error: $_left container(s) of project $RESTORE_PROJECT remain" >&2; RESTORE_OK=0; }
        else
            echo "error: could not enumerate project $RESTORE_PROJECT; removal unconfirmed" >&2
            RESTORE_OK=0
        fi
    fi
    if owned_ambiguous "$OWNS_RESTORE_VHOST"; then
        echo "error: broker vhost $RESTORE_VHOST is AMBIGUOUS -- this invocation may or" >&2
        echo "       may not have created it. It is RETAINED, not deleted by name." >&2
        RESTORE_OK=0
    fi
    if owned_confirmed "$OWNS_RESTORE_VHOST"; then
        compose exec -T rabbitmq rabbitmqctl delete_vhost "$RESTORE_VHOST" >/dev/null 2>&1 || true
        if _vh="$(compose exec -T rabbitmq rabbitmqctl list_vhosts 2>/dev/null)"; then
            printf '%s\n' "$_vh" | grep -qx "$RESTORE_VHOST" && {
                echo "error: broker vhost $RESTORE_VHOST remains" >&2; RESTORE_OK=0; }
        else
            echo "error: could not list broker vhosts; removal of $RESTORE_VHOST unconfirmed" >&2
            RESTORE_OK=0
        fi
    fi
    if owned_ambiguous "$OWNS_RESTORE_DB"; then
        echo "error: database $RESTORE_DB is AMBIGUOUS -- this invocation may or may not" >&2
        echo "       have created it. It is RETAINED, not dropped by name." >&2
        RESTORE_OK=0
    fi
    if owned_confirmed "$OWNS_RESTORE_DB"; then
        psqlq "DROP DATABASE IF EXISTS $RESTORE_DB;" postgres >/dev/null 2>&1 || true
        _db="$(psqln "SELECT count(*) FROM pg_database WHERE datname='$RESTORE_DB';" postgres)"
        case "$_db" in
            0) : ;;
            *) echo "error: database $RESTORE_DB remains (read '$_db')" >&2; RESTORE_OK=0 ;;
        esac
    fi
    # A volume, unlike a database or a vhost, carries its own answer. When
    # responsibility is ambiguous the label is re-read here rather than the
    # name being deleted on a guess.
    if owned_ambiguous "$OWNS_BACKUP_VOL"; then
        case "$(docker volume inspect -f '{{index .Labels "fn.owner"}}' "$BACKUP_VOL" 2>/dev/null || echo unreadable)" in
            "$HU_RUN_TAG") OWNS_BACKUP_VOL=2 ;;
            unreadable)    echo "error: volume $BACKUP_VOL's label is still unreadable; it is RETAINED, not removed by name." >&2
                           RESTORE_OK=0 ;;
            *)             OWNS_BACKUP_VOL=0
                           echo "note: volume $BACKUP_VOL belongs to somebody else; leaving it untouched." >&2 ;;
        esac
    fi
    if owned_confirmed "$OWNS_BACKUP_VOL"; then
        docker volume rm "$BACKUP_VOL" >/dev/null 2>&1 || true
        if _vl="$(docker volume ls -q --filter "name=^${BACKUP_VOL}$" 2>/dev/null)"; then
            [ -z "$_vl" ] || { echo "error: volume $BACKUP_VOL remains" >&2; RESTORE_OK=0; }
        else
            echo "error: could not list volumes; removal of $BACKUP_VOL unconfirmed" >&2
            RESTORE_OK=0
        fi
    fi
    # The stranger is removed BY INODE, and only if this run planted it.
    if [ "$FOREIGN_PLANTED" = "1" ] && [ -n "$FOREIGN_INODE" ] && [ -n "$HDOC" ]; then
        compose run --rm --no-deps -T -e FN_T="/srv/fn/consume/$HDOC" -e FN_I="$FOREIGN_INODE" \
            --entrypoint sh storage-init -c '
                if [ -e "$FN_T" ] && [ "$(stat -c %i "$FN_T")" = "$FN_I" ]; then rm -f "$FN_T"; fi' \
            >/dev/null 2>&1 </dev/null || true
        case "$(probe_exists "/srv/fn/consume/$HDOC")" in
            no) : ;;
            *)  echo "error: the planted occupant at $HDOC could not be confirmed removed" >&2
                RESTORE_OK=0 ;;
        esac
    fi
    for _f in renamer-hold renamer-fault; do
        case "$_f" in
            renamer-hold)  [ "$OWNS_HOLD_SVC" = "1" ]  || continue ;;
            renamer-fault) [ "$OWNS_FAULT_SVC" = "1" ] || continue ;;
        esac
        compose --profile fault stop -t 15 "$_f" >/dev/null 2>&1 || true
        compose --profile fault rm -f "$_f" >/dev/null 2>&1 || true
        if _fq="$(compose --profile fault ps -aq "$_f" 2>/dev/null)"; then
            [ -z "$_fq" ] || { echo "error: fault service $_f remains" >&2; RESTORE_OK=0; }
        else
            echo "error: could not query fault service $_f; removal unconfirmed" >&2
            RESTORE_OK=0
        fi
    done
    [ -n "$SCRATCH" ] && rm -rf "$SCRATCH" 2>/dev/null
    rm -f "$EVIDENCE_DIR/.hu-backup-$$.dump" 2>/dev/null || true
    rm -f "$EVIDENCE_DIR/.hu-ev-"*"-$$" 2>/dev/null || true
    # The live applications are put back only if this invocation stopped them.
    if [ "$LIVE_TOUCHED" = "1" ]; then
        recreate_service renamer-1 renamer-2 watcher >/dev/null 2>&1 || true
    fi
    # `exercise_unlock` is an unconditional `rm -rf`, so the owner file is
    # checked first. A lock that has changed hands belongs to somebody else
    # and removing it would hand this stack to a third invocation.
    _xu="$(cat "$EVIDENCE_DIR/.exercise.lock/owner" 2>/dev/null || true)"
    case "$_xu" in
        *"pid=$$ "*|*"pid=$$") exercise_unlock ;;
        *) echo "warning: the exercise lock is not this process's; leaving it in place." >&2 ;;
    esac
    OWNS_LOCK=0
    return 0
}
# On a catchable interruption the exercise ENDS after cleanup, unsuccessfully.
# A plain `trap cleanup INT` returns to where the signal arrived and the
# remaining mutations run on a stack that has already been restored and a lock
# that has already been released.
on_signal() {
    echo "error: interrupted; cleaning up and stopping." >&2
    cleanup
    exit 130
}
trap 'cleanup' EXIT
trap 'on_signal' INT TERM

# --- exclusive coordination, before anything is touched --------------------
exercise_lock restore-held-uncertain || exit 1
OWNS_LOCK=1

emit "A restore must not resolve an uncertain job or forget why one was held"
emit ""
emit "orchestration provenance (separate from the application/test digest above):"
emit "  production manifest: compose.prod.yml sha256=$(cksum_sha256 "$PROD/compose.prod.yml")"
emit "  rehearsal overlay:   compose.local-rehearsal.yml sha256=$(cksum_sha256 "$PROD/compose.local-rehearsal.yml")"
emit ""

# --- ownership pre-flight, before ANY mutation -----------------------------
#
# Every resource this exercise will need is established as free here, while
# nothing has been changed yet. Checking a name at the point of use meant the
# fixtures, the fault services and a full backup had already been produced
# before a collision was discovered -- so a refusal arrived after the work it
# was supposed to prevent.
log "0/8: ownership pre-flight"
for _svc in renamer-hold renamer-fault; do
    own_service "$_svc" || fatal "fault service '$_svc' already exists; nothing has been changed"
done
if docker volume inspect "$BACKUP_VOL" >/dev/null 2>&1; then
    fatal "volume $BACKUP_VOL already exists; this invocation did not create it, and nothing has been changed"
fi
_pf_db="$(psqln "SELECT count(*) FROM pg_database WHERE datname='$RESTORE_DB';" postgres)"
case "$_pf_db" in
    0) : ;;
    ''|*[!0-9]*) fatal "could not establish whether database $RESTORE_DB exists (read '$_pf_db'); refusing" ;;
    *) fatal "database $RESTORE_DB already exists; this invocation did not create it, and nothing has been changed" ;;
esac
if ! compose exec -T rabbitmq rabbitmqctl list_vhosts >/dev/null 2>&1; then
    fatal "could not list broker vhosts; refusing rather than assuming $RESTORE_VHOST is free"
fi
if compose exec -T rabbitmq rabbitmqctl list_vhosts 2>/dev/null | grep -qx "$RESTORE_VHOST"; then
    fatal "broker vhost $RESTORE_VHOST already exists; nothing has been changed"
fi
if _pf_proj="$(docker ps -aq --filter "label=com.docker.compose.project=$RESTORE_PROJECT" 2>/dev/null)"; then
    [ -z "$_pf_proj" ] || fatal "compose project $RESTORE_PROJECT already exists; nothing has been changed"
else
    fatal "could not enumerate compose projects; refusing rather than assuming $RESTORE_PROJECT is free"
fi
# The destination this run will plant at must be free BEFORE the fixture is
# produced. It is checked again at the moment of the write, because the window
# between the two is exactly what this exercise is about.
case "$(probe_exists "/srv/fn/consume/$HDOC")" in
    no) : ;;
    yes) fatal "a document already occupies /srv/fn/consume/$HDOC; refusing to overwrite it, and nothing has been changed" ;;
    *)   fatal "could not establish whether /srv/fn/consume/$HDOC is occupied; refusing" ;;
esac
note "fault services, volume, database, vhost, project and destination are all free"

UNCERTAIN_BEFORE="$(psqln "SELECT count(*) FROM jobs WHERE state='uncertain';")"
HELD_BEFORE="$(psqln "SELECT count(*) FROM jobs WHERE state='held';")"
UNCERTAIN_IDS_BEFORE="$EVIDENCE_DIR/.hu-uncertain-before-$$"
psqlq "SELECT job_id FROM jobs WHERE state='uncertain' ORDER BY job_id;" > "$UNCERTAIN_IDS_BEFORE"

# ---------------------------------------------------------------------------
# 1. A HELD fixture. The prerequisite gates the write that follows it.
# ---------------------------------------------------------------------------
log "1/8: producing a held fixture (destination_conflict)"
LIVE_TOUCHED=1
compose stop -t 30 renamer-1 renamer-2 >/dev/null 2>&1
# Ownership is taken BEFORE the creation, not after it. `compose up` can leave
# a container behind and still fail, and ownership recorded only on success
# would have left it for somebody else to trip over.
# Refuse first, claim second, create third: ownership is never assigned to a
# service this invocation refused, and it IS assigned before the creation that
# could half-succeed.
own_service renamer-hold || fatal "fault service 'renamer-hold' already exists; it was left alone"
OWNS_HOLD_SVC=1
start_fault renamer-hold FN_FAULT_POINTS=hold_before_reveal FN_FAULT_HOLD=120s \
    FN_RENAMER_PREFETCH=1 || fatal "could not start renamer-hold"

sleep 6
submit "$HDOC"

# The prerequisite GATES the write. If the publisher did not reach
# committed-and-invisible, the window this fixture needs does not exist, and
# the write below would land on whatever IS at that name -- possibly a
# delivered document. This used to record a mismatch and carry on into a
# truncating write.
if ! HJOB="$(await_committed_invisible "$HDOC" "$HDOC")"; then
    emit "1. the held fixture"
    emit "   publisher reached committed-and-invisible: no"
    emit "   destination at that name now: $(probe_exists "/srv/fn/consume/$HDOC")   (left untouched)"
    fatal "the publisher never reached committed-and-invisible; refusing to write to $HDOC"
fi

# Exclusive create. `> file` truncates whatever is there; `set -C` refuses,
# so an occupied destination is preserved. FOREIGN_PLANTED is set only when
# this invocation actually created the file, so cleanup can never delete one
# it found.
PLANT="$(compose run --rm --no-deps -T -e FN_T="/srv/fn/consume/$HDOC" \
    --entrypoint sh storage-init -c '
        if [ -e "$FN_T" ]; then echo OCCUPIED; exit 0; fi
        set -C
        if printf "foreign occupant\n" > "$FN_T" 2>/dev/null; then
            chown 0:0 "$FN_T" 2>/dev/null || true
            echo "PLANTED $(stat -c %i "$FN_T")"
        else
            echo OCCUPIED
        fi' </dev/null 2>/dev/null | tr -d '\r')"
case "$PLANT" in
    PLANTED\ *) FOREIGN_PLANTED=1; FOREIGN_INODE="${PLANT#PLANTED }" ;;
    OCCUPIED)   fatal "the destination $HDOC is already occupied; refusing to overwrite it" ;;
    *)          fatal "could not establish whether $HDOC is occupied (read '$PLANT'); refusing to write" ;;
esac

_i=0
while [ "$_i" -lt 300 ]; do
    _s="$(psqln "SELECT state FROM jobs WHERE job_id='$HJOB';")"
    case "$_s" in delivered|held|uncertain|failed) break ;; esac
    sleep 3; _i=$((_i + 3))
done
compose --profile fault stop -t 15 renamer-hold >/dev/null 2>&1 || true
compose --profile fault rm -f renamer-hold >/dev/null 2>&1 || true
# Kept owned while removal is unconfirmed, so cleanup tries again and reports.
if _hs="$(compose --profile fault ps -aq renamer-hold 2>/dev/null)"; then
    [ -z "$_hs" ] && OWNS_HOLD_SVC=0
fi
recreate_service renamer-1 renamer-2 >/dev/null 2>&1 || true

HSTATE="$(psqln "SELECT state FROM jobs WHERE job_id='$HJOB';")"
HCAT="$(psqln "SELECT coalesce(failure_category::text,'-') FROM jobs WHERE job_id='$HJOB';")"
HRECEIPTS="$(psqln "SELECT count(*) FROM delivery_receipts WHERE job_id='$HJOB';")"
HEVENTS="$(psqln "SELECT count(*) FROM job_events WHERE job_id='$HJOB';")"
HSRC_SHA="$(live_sha "/srv/fn/incoming/$HDOC")"
FOREIGN_SHA="$(live_sha "/srv/fn/consume/$HDOC")"
emit "1. the held fixture"
emit "   job:                          ${HJOB:-none}"
emit "   state:                        ${HSTATE:-none}   (expected held)"
emit "   recorded reason:              ${HCAT:-none}   (expected destination_conflict)"
emit "   delivery receipts:            $HRECEIPTS   (expected 0: the receipt was withdrawn)"
emit "   recorded events:              $HEVENTS"
emit "   occupant planted by this run: inode $FOREIGN_INODE sha256 $(short "$FOREIGN_SHA")"
emit "   source sha256:                $(short "$HSRC_SHA")"
[ "$HSTATE" = "held" ] || bad "the held fixture reached '$HSTATE', not held"
[ "$HCAT" = "destination_conflict" ] || bad "the held fixture's reason is '$HCAT'"
case "$HSRC_SHA" in ABSENT|"") bad "the held fixture's source is missing" ;; esac

# ---------------------------------------------------------------------------
# 2. An UNCERTAIN fixture.
# ---------------------------------------------------------------------------
log "2/8: producing an uncertain fixture (interrupted before the link)"
UDOC="hu-uncertain-$LOWER.pdf"
LIVE_TOUCHED=1
compose stop -t 30 renamer-1 renamer-2 >/dev/null 2>&1
# Refuse first, claim second, create third: ownership is never assigned to a
# service this invocation refused, and it IS assigned before the creation that
# could half-succeed.
own_service renamer-fault || fatal "fault service 'renamer-fault' already exists; it was left alone"
OWNS_FAULT_SVC=1
start_fault renamer-fault FN_FAULT_POINTS=before_link \
    || fatal "could not start renamer-fault"

sleep 6
submit "$UDOC"
_i=0
while [ "$_i" -lt 120 ]; do
    _r="$(compose --profile fault ps -q renamer-fault 2>/dev/null | head -1)"
    [ -z "$_r" ] && break
    [ "$(docker inspect -f '{{.State.Status}}' "$_r" 2>/dev/null || echo gone)" != "running" ] && break
    sleep 2; _i=$((_i + 2))
done
UMID="$(psqln "SELECT state FROM jobs WHERE source_name='$UDOC';")"
compose --profile fault stop -t 15 renamer-fault >/dev/null 2>&1 || true
compose --profile fault rm -f renamer-fault >/dev/null 2>&1 || true
if _fs="$(compose --profile fault ps -aq renamer-fault 2>/dev/null)"; then
    [ -z "$_fs" ] && OWNS_FAULT_SVC=0
fi
recreate_service renamer-1 renamer-2 >/dev/null 2>&1 || true
UJOB="$(psqln "SELECT job_id FROM jobs WHERE source_name='$UDOC';")"
_i=0
while [ "$_i" -lt 240 ]; do
    _s="$(psqln "SELECT state FROM jobs WHERE job_id='$UJOB';")"
    case "$_s" in uncertain|delivered|held|failed) break ;; esac
    sleep 3; _i=$((_i + 3))
done
USTATE="$(psqln "SELECT state FROM jobs WHERE job_id='$UJOB';")"
UCAT="$(psqln "SELECT coalesce(failure_category::text,'-') FROM jobs WHERE job_id='$UJOB';")"
URECEIPTS="$(psqln "SELECT count(*) FROM delivery_receipts WHERE job_id='$UJOB';")"
UEVENTS="$(psqln "SELECT count(*) FROM job_events WHERE job_id='$UJOB';")"
USRC_SHA="$(live_sha "/srv/fn/incoming/$UDOC")"
emit ""
emit "2. the uncertain fixture"
emit "   job:                          ${UJOB:-none}"
emit "   state at the crash:           ${UMID:-none}   (expected publishing)"
emit "   state after recovery:         ${USTATE:-none}   (expected uncertain)"
emit "   recorded reason:              ${UCAT:-none}"
emit "   delivery receipts:            $URECEIPTS"
emit "   recorded events:              $UEVENTS"
emit "   source sha256:                $(short "$USRC_SHA")"
[ "$USTATE" = "uncertain" ] || bad "the uncertain fixture reached '$USTATE', not uncertain"
case "$USRC_SHA" in ABSENT|"") bad "the uncertain fixture's source is missing" ;; esac

# ---------------------------------------------------------------------------
# 3. Back both halves up. A refusal here stops the restore that depends on it.
# ---------------------------------------------------------------------------
# The original history, by record identity. A count that is unchanged or
# larger says nothing about whether the ORIGINAL rows survived: a restore that
# dropped two and a redelivery that added two would satisfy it.
HEV_IDS="$EVIDENCE_DIR/.hu-ev-held-$$"
UEV_IDS="$EVIDENCE_DIR/.hu-ev-uncertain-$$"
capture_event_ids "$HJOB" "" "$HEV_IDS" \
    || fatal "could not read the held job's history; refusing to compare against an unread baseline"
capture_event_ids "$UJOB" "" "$UEV_IDS" \
    || fatal "could not read the uncertain job's history; refusing to compare against an unread baseline"

log "3/8: backing up the ledger and the document roots"
# `docker volume create` is idempotent: it returns 0 on a volume that already
# exists and does NOT apply the labels asked for. So success proves nothing
# about who created it. The label does: if it comes back, this invocation
# created the volume; if it does not, something else owns it and it is left
# alone.
# The LABEL decides, and nothing else.
#
# There used to be a freeness check on this line, re-reading what 0/8 had
# already read. It was strictly weaker than the label and it shadowed it:
# `docker volume create` is idempotent and applies NO labels to a volume that
# already exists, so the label coming back is the one and only proof of who
# made this volume -- and the check in front of it meant that proof was never
# consulted on the path where it mattered. A volume appearing between 0/8 and
# here now reaches the label check, which is where it belongs.
OWNS_BACKUP_VOL=1
docker volume create --label "fn.owner=$HU_RUN_TAG" "$BACKUP_VOL" >/dev/null 2>&1 \
    || fatal "could not create the backup volume"
_bv_owner="$(docker volume inspect -f '{{index .Labels "fn.owner"}}' "$BACKUP_VOL" 2>/dev/null || echo unreadable)"
case "$_bv_owner" in
    "$HU_RUN_TAG") OWNS_BACKUP_VOL=2 ;;
    # Positively somebody else's: the volume existed, so the create was a
    # no-op and the label is theirs. Hands off it entirely.
    unreadable)    fatal "could not read volume $BACKUP_VOL's ownership label; responsibility is AMBIGUOUS, it will NOT be removed by name, and restoration is reported unconfirmed" ;;
    *)             OWNS_BACKUP_VOL=0
                   fatal "volume $BACKUP_VOL carries owner '$_bv_owner'; it is not this invocation's and will NOT be removed" ;;
esac
compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" postgres-primary \
    pg_dump -U "${FN_DB_USER:-fn_app}" -d "${FN_DB_NAME:-filename_normalizer}" -Fc \
    </dev/null > "$EVIDENCE_DIR/.hu-backup-$$.dump" 2>/dev/null \
    || fatal "the ledger dump failed; refusing to restore from an incomplete backup"
for _role in consume incoming staging failed; do
    docker run --rm -v "${PROJECT}_fn-${_role}:/role:ro" -v "$BACKUP_VOL:/backup" \
        "$UTIL_IMAGE" sh -c "tar -cf /backup/${_role}.tar -C /role ." >/dev/null 2>&1 \
        || fatal "archiving role '$_role' failed; refusing to restore from an incomplete backup"
done
emit ""
emit "3. the backup"
emit "   ledger dump and four role archives written"

# ---------------------------------------------------------------------------
# 4. Restore into a throwaway ledger and a host tree the restored stack mounts.
# ---------------------------------------------------------------------------
log "4/8: restoring into a throwaway ledger and filesystem"
# A database carries no ownership label, so the CREATE's own outcome is the
# proof. It is also a better one than a freeness check taken beforehand: the
# check says the name was free a moment ago, which a database appearing in
# between makes false, and the old order then let a failed CREATE leave
# responsibility claimed and cleanup DROP a stranger's database.
#
#   CREATE succeeded            -> this invocation made it        (ours, 2)
#   CREATE failed, it exists    -> somebody else made it          (not ours, 0)
#   CREATE failed, it is absent -> nothing was made               (not ours, 0)
#   CREATE failed, unreadable   -> cannot tell                    (ambiguous, 1)
OWNS_RESTORE_DB=1
_rdb_exit=0
compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" postgres-primary \
    psql -U "${FN_DB_USER:-fn_app}" -d postgres -c "CREATE DATABASE $RESTORE_DB;" >/dev/null 2>&1 </dev/null \
    || _rdb_exit=$?
if [ "$_rdb_exit" = "0" ]; then
    OWNS_RESTORE_DB=2
else
    _rdb_post="$(psqln "SELECT count(*) FROM pg_database WHERE datname='$RESTORE_DB';" postgres)"
    case "$_rdb_post" in
        0)  OWNS_RESTORE_DB=0
            fatal "could not create the throwaway restore database $RESTORE_DB, and it does not exist; nothing has been changed" ;;
        1)  OWNS_RESTORE_DB=0
            fatal "database $RESTORE_DB already exists and this invocation's CREATE failed against it; it is not this invocation's and will NOT be dropped" ;;
        *)  fatal "could not create database $RESTORE_DB and could not establish whether it exists (read '$_rdb_post'); responsibility is AMBIGUOUS, it will NOT be dropped by name, and restoration is reported unconfirmed" ;;
    esac
fi

compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" postgres-primary \
    pg_restore -U "${FN_DB_USER:-fn_app}" -d "$RESTORE_DB" --no-owner \
    < "$EVIDENCE_DIR/.hu-backup-$$.dump" >/dev/null 2>&1 \
    || fatal "the ledger restore failed"

SCRATCH="$(mktemp -d)"
mkdir -p "$SCRATCH"/incoming "$SCRATCH"/queued "$SCRATCH"/staging "$SCRATCH"/consume "$SCRATCH"/failed
chmod 0777 "$SCRATCH" "$SCRATCH"/incoming "$SCRATCH"/queued "$SCRATCH"/staging "$SCRATCH"/consume "$SCRATCH"/failed
# `incoming` is restored into a QUARANTINE, not into the watched root -- the
# procedure production/README.md documents and verify-restore-replay.sh
# demonstrated the need for. Restored files get new inodes, so restoring them
# into the live root re-registers every one of them as a new submission. The
# quarantine sits under `failed`, which the stack MOUNTS but does not watch,
# so the copies are genuinely reachable by the running applications and still
# must not be discovered.
QUARANTINE_REL="failed/restored-incoming"
mkdir -p "$SCRATCH/$QUARANTINE_REL"
chmod 0777 "$SCRATCH/$QUARANTINE_REL"
docker run --rm -v "$BACKUP_VOL:/backup:ro" -v "$SCRATCH:/restored" "$UTIL_IMAGE" \
    sh -c 'tar -xf /backup/consume.tar  -C /restored/consume  &&
           tar -xf /backup/incoming.tar -C /restored/failed/restored-incoming &&
           tar -xf /backup/staging.tar  -C /restored/staging  &&
           tar -xf /backup/failed.tar   -C /restored/failed' >/dev/null 2>&1 \
    || fatal "the document restore failed"

R_HSRC_SHA="$(restored_sha "$QUARANTINE_REL/$HDOC")"
R_USRC_SHA="$(restored_sha "$QUARANTINE_REL/$UDOC")"
R_FOREIGN_SHA="$(restored_sha "consume/$HDOC")"
emit ""
emit "4. the restore, into a target this run created"
emit "   ledger:                       $RESTORE_DB"
emit "   filesystem:                   a host tree the restored stack mounts"
emit "   incoming restored into:       $QUARANTINE_REL (mounted, NOT watched)"
emit "   held source bytes:            $([ "$R_HSRC_SHA" = "$HSRC_SHA" ] && echo identical || echo "DIFFER ($(short "$R_HSRC_SHA"))")"
emit "   uncertain source bytes:       $([ "$R_USRC_SHA" = "$USRC_SHA" ] && echo identical || echo "DIFFER ($(short "$R_USRC_SHA"))")"
emit "   foreign occupant bytes:       $([ "$R_FOREIGN_SHA" = "$FOREIGN_SHA" ] && echo identical || echo "DIFFER ($(short "$R_FOREIGN_SHA"))")"
[ "$R_HSRC_SHA" = "$HSRC_SHA" ] || bad "the held fixture's source bytes changed across the restore"
[ "$R_USRC_SHA" = "$USRC_SHA" ] || bad "the uncertain fixture's source bytes changed across the restore"
[ "$R_FOREIGN_SHA" = "$FOREIGN_SHA" ] || bad "the foreign occupant's bytes changed across the restore"

# ---------------------------------------------------------------------------
# 5. Start REAL applications on the restored ledger and filesystem.
# ---------------------------------------------------------------------------
log "5/8: starting real applications against the restored system"
# Same rule for the broker, and for the same reason: `rabbitmqctl add_vhost`
# fails on a vhost that already exists, so its outcome says who made this one.
# Deleting a vhost takes every queue in it with it, which is the last thing
# that should rest on a freeness check taken minutes earlier.
#
# The listing below is tested inside `if`, not as `... | grep -qx X && ...`:
# under `set -e` that list aborts the script when grep finds nothing, which is
# the ordinary case.
OWNS_RESTORE_VHOST=1
_vh_exit=0
compose exec -T rabbitmq rabbitmqctl add_vhost "$RESTORE_VHOST" >/dev/null 2>&1 || _vh_exit=$?
if [ "$_vh_exit" = "0" ]; then
    OWNS_RESTORE_VHOST=2
else
    if _vh_post="$(compose exec -T rabbitmq rabbitmqctl list_vhosts 2>/dev/null)"; then
        if printf '%s\n' "$_vh_post" | grep -qx "$RESTORE_VHOST"; then
            OWNS_RESTORE_VHOST=0
            fatal "broker vhost $RESTORE_VHOST already exists and this invocation's add_vhost failed against it; it is not this invocation's and will NOT be deleted"
        fi
        OWNS_RESTORE_VHOST=0
        fatal "could not create broker vhost $RESTORE_VHOST, and it does not exist; nothing has been changed"
    else
        fatal "could not create broker vhost $RESTORE_VHOST and could not list vhosts; responsibility is AMBIGUOUS, it will NOT be deleted by name, and restoration is reported unconfirmed"
    fi
fi

compose exec -T rabbitmq rabbitmqctl set_permissions -p "$RESTORE_VHOST" \
    "${FN_AMQP_USER:-fn_app}" '.*' '.*' '.*' >/dev/null 2>&1 \
    || fatal "could not grant permissions on the restored stack's vhost"
cp "$DEPLOY_DIR/config/normalizer.json" "$SCRATCH/normalizer.json"
cp "$DEPLOY_DIR/secrets/fn_db_password" "$SCRATCH/db_password"
cp "$DEPLOY_DIR/secrets/fn_amqp_password" "$SCRATCH/amqp_password"
chmod 0444 "$SCRATCH/db_password" "$SCRATCH/amqp_password"
cat > "$SCRATCH/prod.env" <<ENV
FN_PROJECT=$RESTORE_PROJECT
FN_EXTERNAL_NETWORK=$(docker network ls --format '{{.Name}}' | grep -m1 "_fn$")
FN_WATCHER_IMAGE=${FN_IMAGE_PREFIX:-filename-normalizer}/watcher:${FN_VERSION:-0.1.0-foundation}
FN_RENAMER_IMAGE=${FN_IMAGE_PREFIX:-filename-normalizer}/renamer:${FN_VERSION:-0.1.0-foundation}
FN_DB_PRIMARY_HOST=postgres-primary
FN_DB_NAME=$RESTORE_DB
FN_DB_SSLMODE=disable
FN_AMQP_HOST=rabbitmq
FN_AMQP_VHOST=$RESTORE_VHOST
FN_HOST_INCOMING=$SCRATCH/incoming
FN_HOST_QUEUED=$SCRATCH/queued
FN_HOST_STAGING=$SCRATCH/staging
FN_HOST_CONSUME=$SCRATCH/consume
FN_HOST_FAILED=$SCRATCH/failed
FN_HOST_CONFIG=$SCRATCH/normalizer.json
FN_SECRET_DB_PASSWORD=$SCRATCH/db_password
FN_SECRET_AMQP_PASSWORD=$SCRATCH/amqp_password
ENV
OWNS_RESTORE_PROJECT=1
hu up -d >/dev/null 2>&1 || fatal "the restored stack did not start"
_i=0; R_READY=no
while [ "$_i" -lt 300 ]; do
    _w="$(hu ps --format '{{.Health}}' watcher 2>/dev/null | head -1)"
    _r="$(hu ps --format '{{.Health}}' renamer-1 2>/dev/null | head -1)"
    if [ "$_w" = "healthy" ] && [ "$_r" = "healthy" ]; then R_READY=yes; break; fi
    sleep 3; _i=$((_i + 3))
done
# WHICH restored resources those processes use, read from the running
# processes rather than from the env file written for them.
R_BACKENDS="$(psqln "SELECT count(*) FROM pg_stat_activity WHERE datname='$RESTORE_DB';" postgres)"
R_CONSUME_SRC="$(docker inspect "$(hu ps -q renamer-1 2>/dev/null | head -1)" \
    --format '{{range .Mounts}}{{if eq .Destination "/srv/fn/consume"}}{{.Source}}{{end}}{{end}}' 2>/dev/null)"
emit ""
emit "5. real applications on the restored system"
emit "   watcher + renamer healthy:    $R_READY   (expected yes)"
emit "   backends on $RESTORE_DB: ${R_BACKENDS:-unknown}   (expected >= 1)"
emit "   their consume mount:          ${R_CONSUME_SRC:-unknown}"
emit "                                 (expected the restored tree, not the live volume)"
[ "$R_READY" = "yes" ] || fatal "the restored stack never became healthy; nothing below would be about it"
[ "${R_BACKENDS:-0}" -ge 1 ] || bad "no backend is connected to $RESTORE_DB"
case "${R_CONSUME_SRC:-}" in
    "$SCRATCH"/consume) : ;;
    *) bad "the restored renamer's consume mount is '${R_CONSUME_SRC:-unknown}', not the restored tree" ;;
esac

# ---------------------------------------------------------------------------
# 6. Redeliver both jobs INTO the restored stack and observe a worker take them.
# ---------------------------------------------------------------------------
log "6/8: redelivering both jobs into the restored stack"
republish() {
    compose exec -T rabbitmq rabbitmqadmin \
        --vhost "$RESTORE_VHOST" --username "${FN_AMQP_USER:-fn_app}" \
        --password "$(cat "$DEPLOY_DIR/secrets/fn_amqp_password")" --non-interactive \
        publish message --exchange "${FN_AMQP_EXCHANGE:-filename_normalizer.jobs}" \
        --routing-key "${FN_AMQP_ROUTING_KEY:-normalize}" \
        --properties '{"delivery_mode":2,"content_type":"application/json"}' \
        --payload "{\"contract_version\":1,\"job_id\":\"$1\",\"attempt\":9,\"enqueued_at\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" \
        >/dev/null 2>&1 && echo yes || echo no
}
HPUB="$(republish "$HJOB")"
UPUB="$(republish "$UJOB")"
# OBSERVED reaching a worker, not inferred from a sleep. The earlier version
# recorded delivery-event baselines and never compared them, so publishing
# into a vhost nothing consumed would have passed.
_i=0; HSEEN=0; USEEN=0
while [ "$_i" -lt 180 ]; do
    _logs="$(hu logs 2>/dev/null)"
    HSEEN="$(printf '%s' "$_logs" | grep "$HJOB" | grep -c 'delivery_received' || true)"
    USEEN="$(printf '%s' "$_logs" | grep "$UJOB" | grep -c 'delivery_received' || true)"
    [ "${HSEEN:-0}" -ge 1 ] && [ "${USEEN:-0}" -ge 1 ] && break
    sleep 3; _i=$((_i + 3))
done
sleep 20
RH_STATE="$(psqln "SELECT state FROM jobs WHERE job_id='$HJOB';" "$RESTORE_DB")"
RH_CAT="$(psqln "SELECT coalesce(failure_category::text,'-') FROM jobs WHERE job_id='$HJOB';" "$RESTORE_DB")"
RH_RCPT="$(psqln "SELECT count(*) FROM delivery_receipts WHERE job_id='$HJOB';" "$RESTORE_DB")"
RH_EV="$(psqln "SELECT count(*) FROM job_events WHERE job_id='$HJOB';" "$RESTORE_DB")"
RU_STATE="$(psqln "SELECT state FROM jobs WHERE job_id='$UJOB';" "$RESTORE_DB")"
RU_CAT="$(psqln "SELECT coalesce(failure_category::text,'-') FROM jobs WHERE job_id='$UJOB';" "$RESTORE_DB")"
RU_RCPT="$(psqln "SELECT count(*) FROM delivery_receipts WHERE job_id='$UJOB';" "$RESTORE_DB")"
RU_EV="$(psqln "SELECT count(*) FROM job_events WHERE job_id='$UJOB';" "$RESTORE_DB")"
R_FOREIGN_AFTER="$(restored_sha "consume/$HDOC")"
R_UDOC_AFTER="$(restored_sha "consume/$UDOC")"
# Every original record must still be there. New redelivery events are
# allowed; losing an original one is not.
_rh_now="$EVIDENCE_DIR/.hu-ev-held-after-$$"
_ru_now="$EVIDENCE_DIR/.hu-ev-uncertain-after-$$"
HIST_READ=yes
capture_event_ids "$HJOB" "$RESTORE_DB" "$_rh_now" || HIST_READ=no
capture_event_ids "$UJOB" "$RESTORE_DB" "$_ru_now" || HIST_READ=no
HEV_LOST="$(comm -23 "$HEV_IDS" "$_rh_now" | wc -l | tr -d ' ')"
UEV_LOST="$(comm -23 "$UEV_IDS" "$_ru_now" | wc -l | tr -d ' ')"
HEV_ADDED="$(comm -13 "$HEV_IDS" "$_rh_now" | wc -l | tr -d ' ')"
UEV_ADDED="$(comm -13 "$UEV_IDS" "$_ru_now" | wc -l | tr -d ' ')"
rm -f "$_rh_now" "$_ru_now" 2>/dev/null || true
emit ""
emit "6. after a real redelivery inside the restored stack"
emit "   redeliveries published:       $HPUB / $UPUB   (expected yes/yes)"
emit "   a worker received them:       $HSEEN / $USEEN delivery_received line(s)   (expected >= 1 each)"
emit "   held      state:              $RH_STATE   (expected held: not reopened)"
emit "             reason:             $RH_CAT   (expected destination_conflict)"
emit "             receipts:           $RH_RCPT   (expected $HRECEIPTS)"
emit "             original history records lost: $HEV_LOST   (expected 0; $HEV_ADDED added by the redelivery)"
emit "             restored history readable: $HIST_READ   (expected yes)"
emit "   uncertain state:              $RU_STATE   (expected uncertain: not resolved)"
emit "             reason:             $RU_CAT"
emit "             receipts:           $RU_RCPT   (expected $URECEIPTS)"
emit "             original history records lost: $UEV_LOST   (expected 0; $UEV_ADDED added by the redelivery)"
emit "   foreign occupant bytes:       $([ "$R_FOREIGN_AFTER" = "$FOREIGN_SHA" ] && echo unchanged || echo CHANGED)"
emit "   a document at the uncertain name: $([ "$R_UDOC_AFTER" = "ABSENT" ] && echo no || echo YES)   (expected no)"
[ "$HPUB" = "yes" ] || bad "the held job's redelivery could not be published"
[ "$UPUB" = "yes" ] || bad "the uncertain job's redelivery could not be published"
[ "${HSEEN:-0}" -ge 1 ] || bad "no worker in the restored stack received the held job"
[ "${USEEN:-0}" -ge 1 ] || bad "no worker in the restored stack received the uncertain job"
[ "$RH_STATE" = "held" ] || bad "the restored held job became '$RH_STATE'"
[ "$RH_CAT" = "destination_conflict" ] || bad "the restored held job's reason became '$RH_CAT'"
[ "$RH_RCPT" = "$HRECEIPTS" ] || bad "restored held receipts changed: $HRECEIPTS -> $RH_RCPT"
# A comparison that could not be read establishes nothing either way.
[ "$HIST_READ" = "yes" ] || bad "the restored history could not be read; preservation is unestablished"
[ "${HEV_LOST:-1}" = "0" ] || bad "$HEV_LOST original history record(s) of the held job did not survive"
[ "$RH_CAT" = "$HCAT" ] || bad "the held job's recorded reason changed: $HCAT -> $RH_CAT"
[ "$RU_STATE" = "uncertain" ] || bad "the restored uncertain job was resolved to '$RU_STATE'"
[ "$RU_RCPT" = "$URECEIPTS" ] || bad "restored uncertain receipts changed: $URECEIPTS -> $RU_RCPT"
[ "${UEV_LOST:-1}" = "0" ] || bad "$UEV_LOST original history record(s) of the uncertain job did not survive"
# The reason was printed and never asserted, so it could have changed freely.
[ "$RU_CAT" = "$UCAT" ] || bad "the uncertain job's recorded reason changed: $UCAT -> $RU_CAT"
[ "$R_FOREIGN_AFTER" = "$FOREIGN_SHA" ] || bad "the foreign occupant's bytes changed inside the restored stack"
[ "$R_UDOC_AFTER" = "ABSENT" ] || bad "a redelivery of an uncertain job published a document"

# ---------------------------------------------------------------------------
# 7. Quarantine, with discovery demonstrably operating.
# ---------------------------------------------------------------------------
log "7/8: restored sources in a quarantine, while discovery is running"
# The restored sources are already in the quarantine (step 4). What remains is
# to show that discovery is LIVE while they sit there -- otherwise "nothing was
# discovered" would be satisfied by a watcher that was not looking.
QSRC_H="$(restored_sha "$QUARANTINE_REL/$HDOC")"
QSRC_U="$(restored_sha "$QUARANTINE_REL/$UDOC")"
CANARY="hu-canary-$LOWER.pdf"
printf '%%PDF-1.4 canary\n' > "$SCRATCH/incoming/.wip-canary"
mv "$SCRATCH/incoming/.wip-canary" "$SCRATCH/incoming/$CANARY"
_i=0; CANARY_SEEN=0
while [ "$_i" -lt 180 ]; do
    CANARY_SEEN="$(psqln "SELECT count(*) FROM jobs WHERE source_name='$CANARY';" "$RESTORE_DB")"
    [ "${CANARY_SEEN:-0}" -ge 1 ] && break
    sleep 3; _i=$((_i + 3))
done
QH="$(psqln "SELECT count(*) FROM jobs WHERE source_name='$HDOC';" "$RESTORE_DB")"
QU="$(psqln "SELECT count(*) FROM jobs WHERE source_name='$UDOC';" "$RESTORE_DB")"
emit ""
emit "7. the quarantine, with discovery proven live"
emit "   quarantined held source bytes:      $([ "$QSRC_H" = "$HSRC_SHA" ] && echo identical || echo DIFFER)"
emit "   quarantined uncertain source bytes: $([ "$QSRC_U" = "$USRC_SHA" ] && echo identical || echo DIFFER)"
emit "   canary in the watched root discovered: ${CANARY_SEEN:-0}   (expected >= 1: discovery IS running)"
emit "   jobs for the held source:     $QH   (expected 1: the quarantined copy is not a new submission)"
emit "   jobs for the uncertain source:$QU   (expected 1)"
[ "$QSRC_H" = "$HSRC_SHA" ] || bad "the quarantined held source's bytes differ from the original"
[ "$QSRC_U" = "$USRC_SHA" ] || bad "the quarantined uncertain source's bytes differ from the original"
[ "${CANARY_SEEN:-0}" -ge 1 ] || bad "discovery never registered the canary, so 'nothing was discovered' establishes nothing"
[ "${QH:-0}" = "1" ] || bad "the quarantined held source became $QH job(s)"
[ "${QU:-0}" = "1" ] || bad "the quarantined uncertain source became $QU job(s)"

# ---------------------------------------------------------------------------
# 8. The live system is unchanged, and production stays fault-free.
# ---------------------------------------------------------------------------
log "8/8: the live system, and production fault settings"
UNCERTAIN_AFTER="$(psqln "SELECT count(*) FROM jobs WHERE state='uncertain';")"
UNCERTAIN_IDS_AFTER="$EVIDENCE_DIR/.hu-uncertain-after-$$"
psqlq "SELECT job_id FROM jobs WHERE state='uncertain' ORDER BY job_id;" > "$UNCERTAIN_IDS_AFTER"
RESOLVED="$(comm -23 "$UNCERTAIN_IDS_BEFORE" "$UNCERTAIN_IDS_AFTER" | wc -l | tr -d ' ')"
NEWLY="$(comm -13 "$UNCERTAIN_IDS_BEFORE" "$UNCERTAIN_IDS_AFTER" | tr '\n' ' ')"
PRODFAULT="$(find "$PROD" -type f \( -name '*.yml' -o -name '*.yaml' -o -name '*.env' \) \
    -exec sed 's/#.*//' {} + 2>/dev/null | grep -c 'FN_FAULT' || true)"
emit ""
emit "8. the live system"
emit "   uncertain jobs:               $UNCERTAIN_BEFORE -> $UNCERTAIN_AFTER   (counts for THIS run)"
emit "   previously uncertain, now resolved: $RESOLVED   (expected 0)"
emit "   newly uncertain, by id:       ${NEWLY:-none}"
emit "   held jobs before:             $HELD_BEFORE"
emit "   FN_FAULT settings in the production package: $PRODFAULT   (expected 0;"
emit "     it is named there only in a comment saying it is never set)"
[ "${RESOLVED:-1}" = "0" ] || bad "$RESOLVED previously uncertain job(s) were resolved"
[ "$PRODFAULT" = "0" ] || bad "the production package sets fault injection"
rm -f "$UNCERTAIN_IDS_BEFORE" "$UNCERTAIN_IDS_AFTER" 2>/dev/null || true

emit ""
emit "mismatches: $FAILURES"
cleanup
trap - EXIT INT TERM
L1="$(compose ps --format '{{.Health}}' renamer-1 2>/dev/null | head -1)"
L2="$(compose ps --format '{{.Health}}' renamer-2 2>/dev/null | head -1)"
LW="$(compose ps --format '{{.Health}}' watcher 2>/dev/null | head -1)"
emit ""
emit "restoration (read back from the running stack):"
emit "   owned resources removed and confirmed: $([ "$RESTORE_OK" = "1" ] && echo yes || echo NO)"
emit "   renamer-1 / renamer-2 / watcher:       $L1 / $L2 / $LW   (expected healthy x3)"
[ "$RESTORE_OK" = "1" ] || bad "restoration was incomplete; see the errors above"
{ [ "$L1" = "healthy" ] && [ "$L2" = "healthy" ] && [ "$LW" = "healthy" ]; } \
    || bad "the live stack is not healthy after this exercise ($L1/$L2/$LW)"

if [ "$FAILURES" = "0" ]; then
    report_restored
    report_success
    log "PASSED: the restored system preserves held and uncertain outcomes under real redelivery"
    note "evidence: $OUT"
    exit 0
fi
echo "FAILED: $FAILURES expectation(s) not met; see $OUT" >&2
exit 1
