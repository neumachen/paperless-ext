#!/bin/sh
# FN-F002 (third round) — prove that verify-cleanup-ownership.sh, the OUTER
# harness, also preserves resources predating its invocation.
#
# The defect this covers: that harness installed its cleanup trap before
# creating anything, and the trap removed three fixed volume names
# unconditionally. So a pre-existing volume with one of those names was
# refused by make_decoy — and then deleted by the trap on the way out. It also
# ran `down --remove-orphans --volumes` against a whole throwaway project while
# having established ownership of only one volume in it, leaving that project's
# other volumes and its containers inside a destructive scope nobody had
# checked.
#
# This driver exercises the real entry point, `make test-cleanup-ownership`,
# with real disposable resources planted inside each destructive scope.
#
# Its own destructive surface is deliberately tiny and reviewable by
# inspection: it creates volumes and one container under names it has just
# confirmed absent, records them, and removes only those. It never runs
# `docker compose down`, so it cannot delete anything by project name.
. "$(dirname "$0")/lib.sh"

PREMISE_PROJECT="${FN_OWNERSHIP_PREMISE_PROJECT:-fnf002premise}"
PROBE_PROJECT="${FN_OWNERSHIP_PROBE_PROJECT:-fnf002probe}"
FOREIGN_VOLUME="${FN_OWNERSHIP_FOREIGN_VOLUME:-fnf002foreign_sentinel}"

OUT="$EVIDENCE_DIR/f002-harness-ownership.txt"
LOG_DIR="$EVIDENCE_DIR"
mkdir -p "$EVIDENCE_DIR"
FAILURES=0
SENTINEL="OUTER-SENTINEL-DO-NOT-DELETE"

: > "$OUT"
emit() { printf '%s\n' "$*" >> "$OUT"; printf '%s\n' "$*"; }
bad() {
    printf '    MISMATCH: %s\n' "$*" >&2
    emit "    MISMATCH: $*"
    FAILURES=$((FAILURES + 1))
}

# --- ledger: only what this driver created may be removed --------------------
OWNED_VOLUMES=""
OWNED_CONTAINERS=""

plant_volume() {
    _pv="$1"; shift
    if docker volume inspect "$_pv" >/dev/null 2>&1; then
        echo "error: '$_pv' already exists; this driver will not adopt or remove it." >&2
        exit 1
    fi
    docker volume create "$@" "$_pv" >/dev/null
    OWNED_VOLUMES="${OWNED_VOLUMES}${OWNED_VOLUMES:+ }$_pv"
    docker run --rm -v "$_pv":/d "$UTIL_IMAGE" \
        sh -c "printf '%s\n' '$SENTINEL' > /d/sentinel" >/dev/null 2>&1
}

plant_container() {
    _pc="$1"; shift
    if docker ps -aq --filter "name=^${_pc}$" | grep -q .; then
        echo "error: container '$_pc' already exists; this driver will not remove it." >&2
        exit 1
    fi
    docker run -d --name "$_pc" "$@" "$UTIL_IMAGE" sleep 900 >/dev/null
    OWNED_CONTAINERS="${OWNED_CONTAINERS}${OWNED_CONTAINERS:+ }$_pc"
}

volume_survived() {
    docker volume inspect "$1" >/dev/null 2>&1 || return 1
    _vs="$(docker run --rm -v "$1":/d "$UTIL_IMAGE" cat /d/sentinel 2>/dev/null || true)"
    [ "$_vs" = "$SENTINEL" ]
}

container_survived() {
    docker ps -aq --filter "name=^${1}$" | grep -q .
}

# Remove only what this driver planted, by exact name.
reap() {
    for _r in $OWNED_CONTAINERS; do
        docker rm -f "$_r" >/dev/null 2>&1 || true
    done
    for _r in $OWNED_VOLUMES; do
        docker volume rm "$_r" >/dev/null 2>&1 || true
    done
    OWNED_CONTAINERS=""
    OWNED_VOLUMES=""
}
trap reap EXIT INT TERM

# Any fnf002* volume that is NOT one of ours indicates the harness created
# something during a run it should have refused outright.
foreign_leftovers() {
    _fl=0
    for _v in $(docker volume ls -q | grep -E "^($PREMISE_PROJECT|$PROBE_PROJECT)_" || true); do
        case " $OWNED_VOLUMES " in
            *" $_v "*) ;;
            *) _fl=$((_fl + 1)) ;;
        esac
    done
    echo "$_fl"
}

run_harness() {
    _rh_log="$1"
    set +e
    ( cd "$DEPLOY_DIR" && make test-cleanup-ownership ) > "$_rh_log" 2>&1
    _rh_status=$?
    set -e
    echo "$_rh_status"
}

# A scenario that plants something inside the harness's destructive scope and
# requires the harness to refuse without destroying it.
refusal_case() {
    _rc_num="$1"; _rc_desc="$2"; _rc_kind="$3"; _rc_name="$4"
    _rc_log="$LOG_DIR/f002-harness-case${_rc_num}.log"

    log "case $_rc_num: $_rc_desc"
    _rc_status="$(run_harness "$_rc_log")"
    _rc_created="$(foreign_leftovers)"

    emit "case $_rc_num — $_rc_desc"
    emit "  planted $_rc_kind:        $_rc_name"
    emit "  harness exit status:      $_rc_status   expected non-zero"
    if [ "$_rc_kind" = volume ]; then
        if volume_survived "$_rc_name"; then
            emit "  planted volume:           intact, sentinel readable"
        else
            emit "  planted volume:           LOST OR ALTERED"
            bad "case $_rc_num destroyed or altered the planted volume"
        fi
    else
        if container_survived "$_rc_name"; then
            emit "  planted container:        still present"
        else
            emit "  planted container:        REMOVED"
            bad "case $_rc_num removed the planted container"
        fi
    fi
    emit "  resources the harness created anyway: $_rc_created   expected 0"
    [ "$_rc_status" -ne 0 ] || bad "case $_rc_num: the harness exited 0 despite a pre-existing resource in its scope"
    [ "$_rc_created" = "0" ] || bad "case $_rc_num: the harness created $_rc_created resource(s) during a run it should have refused"
    if grep -q 'REFUSED' "$_rc_log"; then
        emit "  the harness refused at its ownership preflight"
    else
        bad "case $_rc_num: the harness output does not show an ownership refusal"
    fi
    emit ""
}

emit "FN-F002 — outer harness ownership (verify-cleanup-ownership.sh)"
emit ""
emit "Entry point exercised: make test-cleanup-ownership"
emit "Every resource named below was created and removed by this driver."
emit ""

# ---------------------------------------------------------------------------
# 1. The name the harness itself wants to create.
#    Old behaviour: make_decoy refused it, then the EXIT trap deleted it.
# ---------------------------------------------------------------------------
V1="${PREMISE_PROJECT}_pg-replica-data"
plant_volume "$V1"
refusal_case 1 "a pre-existing volume under the name the harness creates" volume "$V1"
reap

# ---------------------------------------------------------------------------
# 2. A DIFFERENT volume of the premise project: never adopted by the old
#    harness, but inside the scope of its `down --remove-orphans --volumes`.
# ---------------------------------------------------------------------------
V2="${PREMISE_PROJECT}_rabbitmq-data"
plant_volume "$V2"
refusal_case 2 "another premise-project volume, inside the down --volumes scope" volume "$V2"
reap

# ---------------------------------------------------------------------------
# 3. A pre-existing CONTAINER of the premise project, inside the scope of
#    `down --remove-orphans`.
# ---------------------------------------------------------------------------
C3="${PREMISE_PROJECT}-bystander-1"
plant_container "$C3" --label "com.docker.compose.project=$PREMISE_PROJECT"
refusal_case 3 "a pre-existing premise-project container, inside the down scope" container "$C3"
reap

# ---------------------------------------------------------------------------
# 4. A probe-project volume: the scope of cleanup's own project teardown.
# ---------------------------------------------------------------------------
V4="${PROBE_PROJECT}_fn-queued"
plant_volume "$V4"
refusal_case 4 "a pre-existing probe-project volume, inside the cleanup scope" volume "$V4"
reap

# ---------------------------------------------------------------------------
# 5. The foreign-labelled bystander the harness plants in its own case 4.
# ---------------------------------------------------------------------------
plant_volume "$FOREIGN_VOLUME" --label com.docker.compose.project=someoneelsesproject
refusal_case 5 "a pre-existing foreign-labelled bystander volume" volume "$FOREIGN_VOLUME"
reap

# ---------------------------------------------------------------------------
# 6. A clean invocation must still do its job.
# ---------------------------------------------------------------------------
log "case 6: a clean invocation must still pass and leave nothing behind"
CLEAN_LOG="$LOG_DIR/f002-harness-case6-clean.log"
CLEAN_STATUS="$(run_harness "$CLEAN_LOG")"
CLEAN_LEFT="$(foreign_leftovers)"

emit "case 6 — clean invocation"
emit "  harness exit status:      $CLEAN_STATUS   expected 0"
emit "  fnf002* resources left:   $CLEAN_LEFT   expected 0"
[ "$CLEAN_STATUS" = "0" ] || bad "case 6: a clean invocation failed with status $CLEAN_STATUS"
[ "$CLEAN_LEFT" = "0" ] || bad "case 6: the harness left $CLEAN_LEFT resource(s) behind"

if grep -q 'result: owned' "$CLEAN_LOG"; then
    emit "  ownership preflight:      owned (scope confirmed empty)"
else
    bad "case 6: the clean run does not record an ownership result"
fi
if grep -qE "after 'compose down --remove-orphans --volumes': DELETED" "$CLEAN_LOG"; then
    emit "  positive control:         still demonstrated (DELETED)"
else
    bad "case 6: the positive control no longer demonstrates the hazard"
fi
if grep -q 'mismatches: 0' "$CLEAN_LOG"; then
    emit "  wrapper cases:            all four still pass (mismatches: 0)"
else
    bad "case 6: the harness's own cases did not all pass"
fi
emit ""

emit "Source conclusion vs observed behaviour: cases 1-5 observe the corrected"
emit "harness refusing and preserving. They do not re-observe the old harness"
emit "deleting these resources; the defect was established by source inspection."
emit ""
emit "mismatches: $FAILURES"

if [ "$FAILURES" -ne 0 ]; then
    echo >&2
    echo "FAILED: $FAILURES outer-harness ownership check(s) did not hold; see $OUT" >&2
    exit 1
fi

log "PASSED: the outer harness preserves resources predating its invocation"
note "evidence: $OUT"
