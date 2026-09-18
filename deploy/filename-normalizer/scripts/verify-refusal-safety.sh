#!/bin/sh
# A refusal must change nothing, and cleanup must touch only what it created.
#
# # Why this exists
#
# Every exercise in this directory refuses to run in situations it cannot make
# safe: another exercise holds the lock, a service it would later destroy
# already exists, a file it would later delete is already there. Those refusals
# were printed correctly and then followed by mutation anyway:
#
#   * the consumer exercise deleted three fixed volume names unconditionally,
#     so a pre-existing Paperless instance -- exactly what the refusal is
#     about -- lost its database and media;
#   * the configuration exercise installed its restore trap BEFORE its
#     preflight, so a refusal was followed by force-recreating all three
#     application services onto hardcoded defaults, and by deleting the file it
#     had just refused to overwrite;
#   * the dry-run exercise took no lock at all and adopted, then destroyed, a
#     service it had not created.
#
# Those are fixed. This exercise is the evidence, and it is deliberately
# adversarial: it puts each exercise in the situation it must refuse, and then
# checks that the stack is byte-for-byte the way it was found.
. "$(dirname "$0")/lib.sh"

OUT="$EVIDENCE_DIR/refusal-safety.txt"
mkdir -p "$EVIDENCE_DIR"
FAILURES=0
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
DECOY_VOL="${PROJECT}_paperless-data"
DECOY_CREATED=0
LOCK_HELD=0

emit() { printf '%s\n' "$*" >> "$OUT"; printf '%s\n' "$*"; }
bad() { printf '    MISMATCH: %s\n' "$*" >&2; emit "    MISMATCH: $*"; FAILURES=$((FAILURES + 1)); }

in_storage() {
    compose run --rm --no-deps -T --entrypoint sh storage-init -c "$1" 2>/dev/null | tr -d ' \r\n'
}

# The state that must survive a refusal, read from the running processes and
# from the filesystem rather than from what the scripts say they did.
snapshot() {
    printf 'renamer1_env=%s\n' "$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' \
        "$(compose ps -q renamer-1 2>/dev/null | head -1)" 2>/dev/null \
        | sed -n 's/^FN_CONFIG_FILE=//p' | head -1)"
    printf 'renamer1_consume=%s\n' "$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' \
        "$(compose ps -q renamer-1 2>/dev/null | head -1)" 2>/dev/null \
        | sed -n 's/^FN_STORAGE_CONSUME=//p' | head -1)"
    printf 'watcher_env=%s\n' "$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' \
        "$(compose ps -q watcher 2>/dev/null | head -1)" 2>/dev/null \
        | sed -n 's/^FN_CONFIG_FILE=//p' | head -1)"
    printf 'renamer1_id=%s\n' "$(compose ps -q renamer-1 2>/dev/null | head -1)"
    printf 'renamer2_id=%s\n' "$(compose ps -q renamer-2 2>/dev/null | head -1)"
    printf 'watcher_id=%s\n' "$(compose ps -q watcher 2>/dev/null | head -1)"
    printf 'volumes=%s\n' "$(docker volume ls -q --filter "name=^${PROJECT}_" 2>/dev/null | sort | tr '\n' ',')"
    printf 'consume_perm=%s\n' "$(in_storage "stat -c '%a:%u:%g' /srv/fn/consume")"
    printf 'consume_entries=%s\n' "$(in_storage "ls -1 /srv/fn/consume 2>/dev/null | wc -l")"
    printf 'live_config=%s\n' "$(cksum "$DEPLOY_DIR/config/normalizer.json" 2>/dev/null | awk '{print $1}')"
}

cleanup() {
    # A child exercise outlives its driver otherwise.
    #
    # Case 3 starts verify-recovery in the background and stops it deliberately.
    # If THIS script is interrupted while that child is running, the child keeps
    # going: it holds the exercise lock, it is mid-way through creating fault
    # services and revoking permissions, and nothing is left watching it. It is
    # sent the same signal its own trap handles, so its restoration runs.
    if [ -n "${PART_PID:-}" ] && kill -0 "$PART_PID" 2>/dev/null; then
        echo "stopping the child exercise ($PART_PID) so it restores what it changed" >&2
        kill -TERM "$PART_PID" 2>/dev/null || true
        wait "$PART_PID" 2>/dev/null || true
        PART_PID=""
    fi
    if [ -n "${INT_PID:-}" ] && kill -0 "$INT_PID" 2>/dev/null; then
        echo "stopping the child exercise ($INT_PID) so it restores what it changed" >&2
        kill -TERM "$INT_PID" 2>/dev/null || true
        wait "$INT_PID" 2>/dev/null || true
        INT_PID=""
    fi
    if [ "$LOCK_HELD" = "1" ]; then
        rm -f "$EVIDENCE_DIR/.exercise.lock/owner" 2>/dev/null || true
        rmdir "$EVIDENCE_DIR/.exercise.lock" 2>/dev/null || true
        LOCK_HELD=0
    fi
    if [ "$DECOY_CREATED" = "1" ]; then
        docker volume rm "$DECOY_VOL" >/dev/null 2>&1 || true
        DECOY_CREATED=0
    fi
    # Interrupted between creating the decoy container and removing it.
    if [ "${REDIS_CREATED:-0}" = "1" ]; then
        compose --profile consumer rm -f -v paperless-redis >/dev/null 2>&1 || true
        REDIS_CREATED=0
    fi
    if [ "${REDIS_VOL_CREATED:-0}" = "1" ]; then
        docker volume rm "${PROJECT}_paperless-redis" >/dev/null 2>&1 || true
        REDIS_VOL_CREATED=0
    fi
}
trap 'report_keep; cleanup' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

report_begin "refusal-safety" "$OUT" "$0"
emit "A refusal changes nothing"
emit ""

BEFORE="$(snapshot)"

# ---------------------------------------------------------------------------
# 1. Every exercise refuses while another holds the lock, and mutates nothing.
# ---------------------------------------------------------------------------
log "1/4: each exercise must refuse while the lock is held, without touching anything"
# `mkdir`, not `mkdir -p`. With -p this succeeds against a lock another
# exercise is holding, and the cleanup at the end of this script would then
# remove somebody else's lock -- an ownership failure in the exercise whose
# whole subject is ownership.
if ! mkdir "$EVIDENCE_DIR/.exercise.lock" 2>/dev/null; then
    echo "error: another exercise ($(cat "$EVIDENCE_DIR/.exercise.lock/owner" 2>/dev/null || echo unknown))" >&2
    echo "       already holds the lock. Nothing has been changed." >&2
    exit 1
fi
printf 'refusal-safety pid=%s at=%s\n' "$$" "$STAMP" > "$EVIDENCE_DIR/.exercise.lock/owner"
LOCK_HELD=1
emit "1. the lock is held by this exercise; each of the four is invoked"

for _s in verify-recovery verify-consumer verify-config-restart verify-dry-run; do
    _log="$EVIDENCE_DIR/refusal-$_s.log"
    if sh "$SCRIPT_DIR/$_s.sh" > "$_log" 2>&1; then
        _rc=0
    else
        _rc=$?
    fi
    _refused="$(grep -c 'holds the lock' "$_log" 2>/dev/null || true)"
    emit "   $_s: exit $_rc, lock refusal messages: ${_refused:-0}"
    [ "$_rc" != "0" ] || bad "$_s ran to completion while another exercise held the lock"
    [ "${_refused:-0}" -ge 1 ] || bad "$_s did not say it was refused because of the lock"
done

rm -f "$EVIDENCE_DIR/.exercise.lock/owner"
rmdir "$EVIDENCE_DIR/.exercise.lock"
LOCK_HELD=0

AFTER_LOCK="$(snapshot)"
if [ "$BEFORE" = "$AFTER_LOCK" ]; then
    emit "   the stack is unchanged: same containers, same environments, same"
    emit "   volumes, same destination permissions, same live configuration"
else
    bad "the stack changed while four exercises were refusing to run"
    emit "   BEFORE:"; printf '%s\n' "$BEFORE" | sed 's/^/     /' >> "$OUT"
    emit "   AFTER:";  printf '%s\n' "$AFTER_LOCK" | sed 's/^/     /' >> "$OUT"
fi
emit ""

# ---------------------------------------------------------------------------
# 2. The consumer exercise refuses a pre-existing instance AND leaves its state
#    alone. This is the one that used to destroy a database on the way past.
# ---------------------------------------------------------------------------
log "2/4: a pre-existing consumer volume must survive the refusal"
if [ -n "$(docker volume ls -q --filter "name=^${DECOY_VOL}$" 2>/dev/null)" ]; then
    emit "2. SKIPPED: $DECOY_VOL already exists and is not this exercise's to use"
else
    docker volume create "$DECOY_VOL" >/dev/null 2>&1 && DECOY_CREATED=1
    docker run --rm -v "$DECOY_VOL:/v" "$UTIL_IMAGE" \
        sh -c 'printf "a pre-existing library\n" > /v/precious.txt' >/dev/null 2>&1 || true
    MARKER_BEFORE="$(docker run --rm -v "$DECOY_VOL:/v" "$UTIL_IMAGE" \
        sh -c 'cat /v/precious.txt 2>/dev/null' 2>/dev/null | tr -d '\r\n')"

    # A service container that exists is what the consumer exercise refuses on.
    # Created only if there is none: adopting one and removing it afterwards is
    # the exact hazard this exercise exists to demonstrate, and an exercise
    # that commits it while proving others do not is worthless.
    REDIS_CREATED=0
    REDIS_VOL_CREATED=0
    if [ -n "$(compose --profile consumer ps -aq paperless-redis 2>/dev/null)" ]; then
        emit "2. SKIPPED: a paperless-redis container already exists and is not this"
        emit "   exercise's to create, use or remove."
        SKIP_CASE2=1
    else
        # Creating the container also creates its named volume. That volume is
        # this invocation's too, and it has to go back -- the first run of this
        # exercise left ${PROJECT}_paperless-redis behind and its own
        # end-as-it-began check caught it.
        if [ -z "$(docker volume ls -q --filter "name=^${PROJECT}_paperless-redis$" 2>/dev/null)" ]; then
            REDIS_VOL_CREATED=1
        fi
        compose --profile consumer create paperless-redis >/dev/null 2>&1 && REDIS_CREATED=1
    fi

    if [ "${SKIP_CASE2:-0}" != "1" ]; then
        _log="$EVIDENCE_DIR/refusal-consumer-preexisting.log"
        if sh "$SCRIPT_DIR/verify-consumer.sh" > "$_log" 2>&1; then _rc=0; else _rc=$?; fi
        _refused="$(grep -c 'did not create it' "$_log" 2>/dev/null || true)"

        MARKER_AFTER="$(docker run --rm -v "$DECOY_VOL:/v" "$UTIL_IMAGE" \
            sh -c 'cat /v/precious.txt 2>/dev/null' 2>/dev/null | tr -d '\r\n')"
        VOL_AFTER="$(docker volume ls -q --filter "name=^${DECOY_VOL}$" 2>/dev/null)"

        emit "2. a pre-existing consumer service and volume"
        emit "   verify-consumer exit:         $_rc   (expected non-zero: refused)"
        emit "   refusal messages:             ${_refused:-0}   (expected >= 1)"
        emit "   the volume still exists:      $([ -n "$VOL_AFTER" ] && echo yes || echo NO)"
        emit "   its contents survived:        $([ "$MARKER_AFTER" = "$MARKER_BEFORE" ] && [ -n "$MARKER_BEFORE" ] && echo yes || echo NO)"
        [ "$_rc" != "0" ] || bad "the consumer exercise adopted a service it did not create"
        [ "${_refused:-0}" -ge 1 ] || bad "the consumer exercise did not report a refusal"
        [ -n "$VOL_AFTER" ] || bad "the pre-existing volume was deleted by an exercise that refused to run"
        [ -n "$MARKER_BEFORE" ] && [ "$MARKER_AFTER" = "$MARKER_BEFORE" ] || bad "the pre-existing volume's contents were destroyed"
    fi
    # Removed only if this invocation created it.
    if [ "${REDIS_CREATED:-0}" = "1" ]; then
        compose --profile consumer rm -f -v paperless-redis >/dev/null 2>&1 || true
        if [ -n "$(compose --profile consumer ps -aq paperless-redis 2>/dev/null)" ]; then
            bad "the decoy paperless-redis container this exercise created could not be removed"
        fi
    fi
    if [ "${REDIS_VOL_CREATED:-0}" = "1" ]; then
        docker volume rm "${PROJECT}_paperless-redis" >/dev/null 2>&1 || true
        if [ -n "$(docker volume ls -q --filter "name=^${PROJECT}_paperless-redis$" 2>/dev/null)" ]; then
            bad "the decoy paperless-redis volume this exercise created could not be removed"
        fi
    fi
    docker volume rm "$DECOY_VOL" >/dev/null 2>&1 && DECOY_CREATED=0
    if [ -n "$(docker volume ls -q --filter "name=^${DECOY_VOL}$" 2>/dev/null)" ]; then
        bad "the decoy volume this exercise created could not be removed"
    fi
fi
emit ""

# ---------------------------------------------------------------------------
# 3. An exercise INTERRUPTED mid-run restores what it changed.
#
# Refusal is the easy path: nothing has happened yet. The hard one is a run
# that has already started fault services, cut containers off the network and
# revoked directory permissions when it is stopped. Every exercise installs
# `trap 'exit 130' INT` and `trap 'exit 143' TERM` so that its EXIT trap -- the
# restoration -- still runs, and that arrangement had never been exercised.
#
# SIGTERM, not SIGINT, and the difference is not cosmetic. A command started
# asynchronously by a NON-INTERACTIVE shell has SIGINT set to ignore, and a
# signal inherited as ignored cannot be trapped -- so `kill -INT` at a child
# started with `&` from a script is silently a no-op. The first version of this
# case did exactly that: it sent SIGINT, the recovery exercise carried on
# through its next scenario, and `wait` simply blocked until the run finished
# on its own. SIGTERM is delivered, the TERM trap fires, and the EXIT trap runs
# the same restoration an operator's ^C would reach -- because an interactive
# ^C goes to the foreground process GROUP, where SIGINT is not ignored.
# ---------------------------------------------------------------------------
log "3/4: an exercise stopped mid-run must restore what it changed"
INT_LOG="$EVIDENCE_DIR/refusal-interrupted-recovery.log"
PERM_BEFORE_INT="$(in_storage "stat -c '%a:%u:%g' /srv/fn/consume")"
VOLS_BEFORE_INT="$(docker volume ls -q --filter "name=^${PROJECT}_" 2>/dev/null | sort | tr '\n' ',')"

sh "$SCRIPT_DIR/verify-recovery.sh" > "$INT_LOG" 2>&1 &
INT_PID=$!

# Stop it only once it has actually changed something. Stopping before the
# first mutation would demonstrate the refusal path again, not this one.
_i=0
FAULT_SEEN=no
while [ "$_i" -lt 240 ]; do
    if [ -n "$(compose --profile fault ps -aq renamer-fault 2>/dev/null)" ]; then
        FAULT_SEEN=yes
        break
    fi
    kill -0 "$INT_PID" 2>/dev/null || break
    sleep 2
    _i=$((_i + 2))
done

if [ "$FAULT_SEEN" != "yes" ]; then
    kill -TERM "$INT_PID" 2>/dev/null || true
    wait "$INT_PID" 2>/dev/null || true
    bad "the recovery exercise never started a fault service, so there was nothing to stop"
else
    kill -TERM "$INT_PID" 2>/dev/null || true
    INT_RC=0
    wait "$INT_PID" 2>/dev/null || INT_RC=$?

    FAULTS_LEFT="$(compose --profile fault ps -aq renamer-fault renamer-hold renamer-altfs \
        renamer-tinyfs renamer-permdenied renamer-tmpfsdest 2>/dev/null | wc -l | tr -d ' ')"
    RDY1="$(compose exec -T renamer-1 /usr/local/bin/fn-renamer healthcheck --require-ready >/dev/null 2>&1 && echo yes || echo no)"
    RDY2="$(compose exec -T renamer-2 /usr/local/bin/fn-renamer healthcheck --require-ready >/dev/null 2>&1 && echo yes || echo no)"
    RDYW="$(compose exec -T watcher /usr/local/bin/fn-watcher healthcheck --require-ready >/dev/null 2>&1 && echo yes || echo no)"
    NET_ON="$(for _c in watcher renamer-1 renamer-2; do
        _cid="$(compose ps -q "$_c" 2>/dev/null | head -1)"
        docker inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}' "$_cid" 2>/dev/null
    done | grep -c "${PROJECT}_fn" || true)"
    PERM_AFTER_INT="$(in_storage "stat -c '%a:%u:%g' /srv/fn/consume")"
    VOLS_AFTER_INT="$(docker volume ls -q --filter "name=^${PROJECT}_" 2>/dev/null | sort | tr '\n' ',')"
    LOCK_LEFT="$([ -d "$EVIDENCE_DIR/.exercise.lock" ] && echo yes || echo no)"
    TEMPS_LEFT="$(in_storage "ls -1a /srv/fn/consume 2>/dev/null | grep -c '^\.fn-' || true")"

    emit "3. the recovery exercise, stopped after it started a fault service"
    emit "   exit status:                  $INT_RC   (expected 143: the TERM trap ran)"
    emit "   fault services left behind:   $FAULTS_LEFT   (expected 0)"
    emit "   application services ready:   $RDY1 / $RDY2 / $RDYW   (expected yes/yes/yes)"
    emit "   still on the application net: $NET_ON of 3   (expected 3)"
    emit "   destination permissions:      $PERM_AFTER_INT   (expected $PERM_BEFORE_INT)"
    emit "   project volumes:              $([ "$VOLS_AFTER_INT" = "$VOLS_BEFORE_INT" ] && echo unchanged || echo CHANGED)"
    emit "   exercise lock left held:      $LOCK_LEFT   (expected no)"
    emit "   staged temporaries present:   $TEMPS_LEFT   (reported: a publication stopped"
    emit "                                 cannot unlink what it staged, so this is a"
    emit "                                 consequence of the interruption, not a leak"
    emit "                                 of the ordinary path)"
    [ "${INT_RC:-0}" = "143" ] || bad "the stopped exercise exited $INT_RC, not 143: the TERM trap did not run"
    [ "${FAULTS_LEFT:-1}" = "0" ] || bad "$FAULTS_LEFT fault service(s) survived the termination"
    [ "$RDY1" = "yes" ] && [ "$RDY2" = "yes" ] && [ "$RDYW" = "yes" ] || bad "an application service is not ready after the termination"
    [ "${NET_ON:-0}" = "3" ] || bad "only $NET_ON of 3 application services are on the application network"
    [ "$PERM_AFTER_INT" = "$PERM_BEFORE_INT" ] || bad "destination permissions are $PERM_AFTER_INT, were $PERM_BEFORE_INT"
    [ "$VOLS_AFTER_INT" = "$VOLS_BEFORE_INT" ] || bad "the set of project volumes changed across the stopped run"
    [ "$LOCK_LEFT" = "no" ] || bad "the stopped exercise left the exercise lock held"
fi
emit ""

# ---------------------------------------------------------------------------
# 4. A run stopped in its PARTIAL-MUTATION window.
#
# Case 3 stops an exercise that has finished changing something. This one stops
# one in the gap between its first change and the point at which it used to
# admit having changed anything: verify-config-restart stopped both renamers
# and only set its mutation flag when it later recreated them, so a stop in
# between left two workers down and a restoration that declined to act. The
# window is a second or two wide, so the exercise is stopped the moment the
# renamers are observed stopped -- which is inside it by construction.
# ---------------------------------------------------------------------------
log "4/4: a run stopped between its first change and its restoration"
PARTIAL_LOG="$EVIDENCE_DIR/refusal-partial-config-restart.log"
sh "$SCRIPT_DIR/verify-config-restart.sh" > "$PARTIAL_LOG" 2>&1 &
PART_PID=$!

_i=0
STOPPED_SEEN=no
while [ "$_i" -lt 240 ]; do
    _r1="$(docker inspect -f '{{.State.Status}}' "$(compose ps -aq renamer-1 2>/dev/null | head -1)" 2>/dev/null || echo unknown)"
    if [ "$_r1" = "exited" ] || [ "$_r1" = "created" ]; then
        STOPPED_SEEN=yes
        break
    fi
    kill -0 "$PART_PID" 2>/dev/null || break
    sleep 1
    _i=$((_i + 1))
done

if [ "$STOPPED_SEEN" != "yes" ]; then
    kill -TERM "$PART_PID" 2>/dev/null || true
    wait "$PART_PID" 2>/dev/null || true
    emit "4. SKIPPED: the partial-mutation window was not observed in this run"
    emit "   (the renamers were never seen stopped; nothing was interrupted)"
else
    kill -TERM "$PART_PID" 2>/dev/null || true
    PART_RC=0
    wait "$PART_PID" 2>/dev/null || PART_RC=$?

    P_RDY1="$(compose exec -T renamer-1 /usr/local/bin/fn-renamer healthcheck --require-ready >/dev/null 2>&1 && echo yes || echo no)"
    P_RDY2="$(compose exec -T renamer-2 /usr/local/bin/fn-renamer healthcheck --require-ready >/dev/null 2>&1 && echo yes || echo no)"
    P_RDYW="$(compose exec -T watcher /usr/local/bin/fn-watcher healthcheck --require-ready >/dev/null 2>&1 && echo yes || echo no)"
    P_CFG="$(effective_value renamer-1 "d['config_file']")"
    P_CONSUME="$(effective_value renamer-1 "d['storage']['consume']")"
    P_LOCK="$([ -d "$EVIDENCE_DIR/.exercise.lock" ] && echo yes || echo no)"
    P_EXFILES="$(ls "$DEPLOY_DIR/config"/.exercise-* 2>/dev/null | wc -l | tr -d ' ')"

    emit "4. the configuration exercise, stopped after it stopped the renamers"
    emit "   renamers were seen stopped:   $STOPPED_SEEN   (the window was entered)"
    emit "   exit status:                  $PART_RC   (non-zero: it was stopped)"
    emit "   renamer-1 / renamer-2 ready:  $P_RDY1 / $P_RDY2   (expected yes/yes: brought back)"
    emit "   watcher ready:                $P_RDYW   (expected yes)"
    emit "   configuration in use:         $P_CFG"
    emit "   consume root in use:          $P_CONSUME"
    emit "   exercise lock left held:      $P_LOCK   (expected no)"
    emit "   exercise config files left:   $P_EXFILES   (expected 0)"
    [ "${PART_RC:-0}" != "0" ] || bad "the interrupted configuration exercise exited 0"
    [ "$P_RDY1" = "yes" ] || bad "renamer-1 was left stopped or unready by a run interrupted mid-change"
    [ "$P_RDY2" = "yes" ] || bad "renamer-2 was left stopped or unready by a run interrupted mid-change"
    [ "$P_RDYW" = "yes" ] || bad "the watcher was left unready by a run interrupted mid-change"
    [ "$P_LOCK" = "no" ] || bad "the interrupted configuration exercise left the lock held"
    [ "${P_EXFILES:-0}" = "0" ] || bad "$P_EXFILES exercise configuration file(s) were left behind"
fi
emit ""

AFTER="$(snapshot)"
# Two fields are compared differently here, and only here.
#
# Container ids: case 3 interrupts a run that had already changed the stack, and
# restoring it RECREATES the three application services. New container ids are
# what a working restoration looks like, so they are excluded from this
# comparison -- case 1, where nothing may change at all, compares them strictly.
#
# The destination's entry count is compared as an inequality, because unrelated
# work delivering a document while this runs is not a change this exercise
# made; failing on it would report somebody else's successful delivery as a
# mutation. A count that FELL is still a failure: that is something destroyed.
BEFORE_FIXED="$(printf '%s\n' "$BEFORE" | grep -v '^consume_entries=' | grep -v '_id=')"
AFTER_FIXED="$(printf '%s\n' "$AFTER" | grep -v '^consume_entries=' | grep -v '_id=')"
BEFORE_N="$(printf '%s\n' "$BEFORE" | sed -n 's/^consume_entries=//p')"
AFTER_N="$(printf '%s\n' "$AFTER" | sed -n 's/^consume_entries=//p')"
if [ "$BEFORE_FIXED" = "$AFTER_FIXED" ] && [ "${AFTER_N:-0}" -ge "${BEFORE_N:-0}" ]; then
    emit "The stack ends as it began: same configuration, same volumes, same"
    emit "destination permissions, nothing removed."
    emit "  (destination entries ${BEFORE_N:-?} -> ${AFTER_N:-?}: nothing was removed)"
else
    bad "the stack did not end as it began"
    emit "   BEFORE:"; printf '%s\n' "$BEFORE" | sed 's/^/     /' >> "$OUT"
    emit "   AFTER:";  printf '%s\n' "$AFTER" | sed 's/^/     /' >> "$OUT"
fi
emit ""
emit "mismatches: $FAILURES"

if [ "$FAILURES" -ne 0 ]; then
    echo >&2
    echo "FAILED: $FAILURES expectation(s) not met; see $OUT" >&2
    exit 1
fi

report_success
log "PASSED: refusals are non-mutating and cleanup respects ownership"
note "evidence: $OUT"
