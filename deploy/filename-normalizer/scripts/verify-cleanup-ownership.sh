#!/bin/sh
# FN-F002 (second round) — prove that the isolation wrapper's OWN cleanup
# cannot destroy resources that predate the invocation.
#
# The defect this covers: the preflight counted only resources carrying the
# requested Compose project label, then installed a cleanup trap running
# `docker compose down --volumes`. Compose removes a project's volumes by the
# NAME it would give them and does not consult labels, so a pre-existing
# volume named '<probe>_pg-replica-data' with absent or foreign labels sat in
# cleanup's range while both label counts read zero. Because the trap was
# installed before the default-volume prerequisite was checked, an early
# failure was enough to delete it.
#
# Four real cases, all against synthetic volumes created and destroyed by this
# script. Nothing else is touched: every volume named here is either created
# here or belongs to a throwaway project name used nowhere else.
#
#   1. The premise, as a positive control. `down --volumes` really does remove
#      an unlabelled name-colliding volume. Without this, cases 2 and 3 would
#      be asserting against a hazard nobody had shown to exist.
#   2. The finding's exact scenario: an unlabelled colliding volume AND a
#      failing early prerequisite. Must refuse before installing the trap.
#   3. The same with a foreign project label rather than no labels.
#   4. No collision, failing early prerequisite: the trap does run, and must
#      remove only what carries the probe project's own label.
#
# Cases 2-4 also record the two label-filtered counts the old preflight used,
# which read 0/0 in exactly the situations where cleanup was dangerous.
. "$(dirname "$0")/lib.sh"

PROBE_PROJECT="${FN_OWNERSHIP_PROBE_PROJECT:-fnf002probe}"
PREMISE_PROJECT="${FN_OWNERSHIP_PREMISE_PROJECT:-fnf002premise}"
# A default project name that owns nothing, so the wrapper's step-1 standby
# volume prerequisite fails and the early-failure path is taken.
ABSENT_DEFAULT="${FN_OWNERSHIP_ABSENT_DEFAULT:-fnf002nodefault}"
# An unrelated volume carrying a foreign project label, used in case 4 as a
# bystander that must survive. Named here so the preflight can cover it too.
FOREIGN_SENTINEL_NAME="${FN_OWNERSHIP_FOREIGN_VOLUME:-fnf002foreign_sentinel}"

OUT="$EVIDENCE_DIR/f002-cleanup-ownership.txt"
mkdir -p "$EVIDENCE_DIR"
FAILURES=0
SENTINEL="SENTINEL-DO-NOT-DELETE"

: > "$OUT"
emit() { printf '%s\n' "$*" >> "$OUT"; printf '%s\n' "$*"; }

fail_case() {
    printf '    MISMATCH: %s\n' "$*" >&2
    emit "    MISMATCH: $*"
    FAILURES=$((FAILURES + 1))
}

# --- ownership ledger --------------------------------------------------------
# Only resources recorded here may be destroyed. The ledger is append-only and
# is populated after a successful create, so a resource this invocation
# refused to adopt can never end up in it.
CREATED_VOLUMES=""

record_created() { CREATED_VOLUMES="${CREATED_VOLUMES}${CREATED_VOLUMES:+ }$1"; }

is_created() {
    for _ic_v in $CREATED_VOLUMES; do
        [ "$_ic_v" = "$1" ] && return 0
    done
    return 1
}

# --- synthetic volume helpers ------------------------------------------------
# Refuse to reuse a name that already exists: this script must never adopt, or
# destroy, a volume it did not create. The preflight below has already proved
# every name absent, so this is a defensive assertion against a name appearing
# mid-run; if it fires, the name is NOT recorded and cleanup will not touch it.
make_decoy() {
    _md_name="$1"; shift
    if docker volume inspect "$_md_name" >/dev/null 2>&1; then
        echo "error: '$_md_name' already exists; this script will not touch a volume it" >&2
        echo "       did not create, and will not remove it on the way out." >&2
        exit 1
    fi
    docker volume create "$@" "$_md_name" >/dev/null
    record_created "$_md_name"
    docker run --rm -v "$_md_name":/d "$UTIL_IMAGE" \
        sh -c "printf '%s\n' '$SENTINEL' > /d/sentinel" >/dev/null 2>&1
}

decoy_intact() {
    _di_name="$1"
    docker volume inspect "$_di_name" >/dev/null 2>&1 || return 1
    _di_got="$(docker run --rm -v "$_di_name":/d "$UTIL_IMAGE" cat /d/sentinel 2>/dev/null || true)"
    [ "$_di_got" = "$SENTINEL" ]
}

# drop_decoy removes a volume only if this invocation created it.
drop_decoy() {
    if ! is_created "$1"; then
        return 0
    fi
    docker volume rm "$1" >/dev/null 2>&1 || true
}

label_counts() {
    _lc_project="$1"
    _lc_c="$(docker ps -aq --filter "label=com.docker.compose.project=$_lc_project" | wc -l | tr -d ' ')"
    _lc_v="$(docker volume ls -q --filter "label=com.docker.compose.project=$_lc_project" | wc -l | tr -d ' ')"
    echo "$_lc_c/$_lc_v"
}

probe_resource_count() {
    _prc_c="$(docker ps -aq --filter "label=com.docker.compose.project=$PROBE_PROJECT" | wc -l | tr -d ' ')"
    _prc_v="$(docker volume ls -q --filter "label=com.docker.compose.project=$PROBE_PROJECT" | wc -l | tr -d ' ')"
    echo "$((_prc_c + _prc_v))"
}

# ---------------------------------------------------------------------------
# Ownership preflight, BEFORE any destructive trap exists.
#
# The destructive scope of this harness is wider than the three volumes it
# creates: case 1 runs `down --remove-orphans --volumes` against the whole
# premise project, and cleanup runs `down --remove-orphans` against the whole
# probe project. Compose addresses project volumes by NAME and ignores labels,
# so that scope is every configured volume name of both projects, plus their
# containers and networks -- not just the names this script means to create.
#
# So ownership is established for all of it up front. If anything in that
# scope already exists, this invocation refuses and exits WITHOUT installing a
# trap, so nothing is removed -- including the resource that caused the
# refusal.
# ---------------------------------------------------------------------------
log "step 0: confirm this invocation may own everything its destructive steps can remove"

VOLUME_KEYS="$(COMPOSE_PROJECT_NAME="$PREMISE_PROJECT" docker compose \
    --profile test --profile fault config --volumes 2>/dev/null)"
if [ -z "$VOLUME_KEYS" ]; then
    echo "error: could not enumerate the configured volume names, so this invocation" >&2
    echo "       cannot establish what its destructive steps would be able to remove." >&2
    exit 1
fi

SCOPE_VOLUMES=""
for _pf_project in "$PREMISE_PROJECT" "$PROBE_PROJECT"; do
    for _pf_key in $VOLUME_KEYS; do
        SCOPE_VOLUMES="${SCOPE_VOLUMES}${SCOPE_VOLUMES:+ }${_pf_project}_${_pf_key}"
    done
done
SCOPE_VOLUMES="${SCOPE_VOLUMES} $FOREIGN_SENTINEL_NAME"

PRE_EXISTING=""
for _pf_vol in $SCOPE_VOLUMES; do
    if docker volume inspect "$_pf_vol" >/dev/null 2>&1; then
        PRE_EXISTING="${PRE_EXISTING}${PRE_EXISTING:+ }$_pf_vol"
    fi
done

PRE_CONTAINERS=""
for _pf_project in "$PREMISE_PROJECT" "$PROBE_PROJECT"; do
    for _pf_cid in $(docker ps -aq \
            --filter "label=com.docker.compose.project=$_pf_project" 2>/dev/null); do
        PRE_CONTAINERS="${PRE_CONTAINERS}${PRE_CONTAINERS:+ }$(docker inspect -f '{{.Name}}' "$_pf_cid" 2>/dev/null | sed 's|^/||')"
    done
    for _pf_cid in $(docker ps -aq --filter "name=^${_pf_project}[-_]" 2>/dev/null); do
        _pf_nm="$(docker inspect -f '{{.Name}}' "$_pf_cid" 2>/dev/null | sed 's|^/||')"
        case " $PRE_CONTAINERS " in
            *" $_pf_nm "*) ;;
            *) PRE_CONTAINERS="${PRE_CONTAINERS}${PRE_CONTAINERS:+ }$_pf_nm" ;;
        esac
    done
done

{
    echo "FN-F002 — cleanup ownership for resources predating the invocation"
    echo
    echo "step 0 — ownership of the whole destructive scope"
    echo "  throwaway projects: $PREMISE_PROJECT (positive control), $PROBE_PROJECT (wrapper cases)"
    echo "  volume names in scope ($(echo $SCOPE_VOLUMES | wc -w | tr -d ' ')):"
    for _pf_vol in $SCOPE_VOLUMES; do
        if docker volume inspect "$_pf_vol" >/dev/null 2>&1; then
            printf '    %-46s PRE-EXISTING\n' "$_pf_vol"
        else
            printf '    %-46s absent\n' "$_pf_vol"
        fi
    done
    echo "  containers in scope: ${PRE_CONTAINERS:-none}"
    echo
    if [ -n "$PRE_EXISTING" ] || [ -n "$PRE_CONTAINERS" ]; then
        echo "  result: REFUSED — this invocation did not create these resources, and"
        echo "          exits without installing a cleanup trap, so none is removed."
    else
        echo "  result: owned — every volume name and container in the destructive scope"
        echo "          was absent, so anything under these names afterwards is ours."
    fi
} > "$OUT"
cat "$OUT"

if [ -n "$PRE_EXISTING" ] || [ -n "$PRE_CONTAINERS" ]; then
    echo >&2
    echo "error: resources already exist inside this harness's destructive scope:" >&2
    for _pf_vol in $PRE_EXISTING;    do echo "         volume    $_pf_vol" >&2; done
    for _pf_nm  in $PRE_CONTAINERS;  do echo "         container $_pf_nm"  >&2; done
    echo "       This invocation did not create them. Nothing has been created or" >&2
    echo "       destroyed, and no cleanup trap was installed. Remove them" >&2
    echo "       deliberately, or set FN_OWNERSHIP_PREMISE_PROJECT /" >&2
    echo "       FN_OWNERSHIP_PROBE_PROJECT / FN_OWNERSHIP_FOREIGN_VOLUME." >&2
    exit 1
fi

# Only now is a destructive trap safe. Every configured name in both projects
# was absent a moment ago, so the whole-project `down` calls below can only
# reach resources this invocation created.
OWNS_SCOPE=yes
cleanup() {
    [ "${OWNS_SCOPE:-no}" = "yes" ] || return 0
    # Volumes: only ones recorded as created here.
    for _cl_vol in $CREATED_VOLUMES; do
        drop_decoy "$_cl_vol"
    done
    # Containers and networks of the probe project. The preflight proved the
    # project empty, so this can only remove what the wrapper cases started.
    COMPOSE_PROJECT_NAME="$PROBE_PROJECT" docker compose \
        --profile test --profile fault down --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

emit ""

# ---------------------------------------------------------------------------
# 1. The premise: compose removes project volumes by name, ignoring labels.
# ---------------------------------------------------------------------------
log "1/4: positive control — 'down --volumes' removes an unlabelled name collision"
PREMISE_VOLUME="${PREMISE_PROJECT}_pg-replica-data"
make_decoy "$PREMISE_VOLUME"
PREMISE_LABELS="$(docker volume inspect -f '{{if .Labels}}{{.Labels}}{{else}}<none>{{end}}' "$PREMISE_VOLUME")"
PREMISE_COUNTS="$(label_counts "$PREMISE_PROJECT")"
COMPOSE_PROJECT_NAME="$PREMISE_PROJECT" docker compose \
    --profile test --profile fault down --remove-orphans --volumes >/dev/null 2>&1 || true
if docker volume inspect "$PREMISE_VOLUME" >/dev/null 2>&1; then
    PREMISE_RESULT=PRESERVED
else
    PREMISE_RESULT=DELETED
fi
emit "case 1 — the hazard is real (positive control)"
emit "  synthetic volume:            $PREMISE_VOLUME"
emit "  its labels:                  $PREMISE_LABELS"
emit "  old preflight would have read (containers/volumes by label): $PREMISE_COUNTS"
emit "  after 'compose down --remove-orphans --volumes': $PREMISE_RESULT"
emit "  => a label-only preflight reads 0/0 while cleanup can still remove the volume."
if [ "$PREMISE_RESULT" != "DELETED" ]; then
    fail_case "expected the unguarded cleanup to delete the collision, so the rest of this file would be proving nothing"
fi
drop_decoy "$PREMISE_VOLUME"
emit ""

# ---------------------------------------------------------------------------
# 2. Collision with absent labels, plus a failing early prerequisite.
# ---------------------------------------------------------------------------
log "2/4: unlabelled collision + failing prerequisite must be refused, not cleaned"
PROBE_VOLUME="${PROBE_PROJECT}_pg-replica-data"
make_decoy "$PROBE_VOLUME"
CASE2_CREATED="$(docker volume inspect -f '{{.CreatedAt}}' "$PROBE_VOLUME")"
CASE2_COUNTS="$(label_counts "$PROBE_PROJECT")"
set +e
COMPOSE_PROJECT_NAME="$ABSENT_DEFAULT" FN_ISOLATION_PROJECT="$PROBE_PROJECT" \
    "$SCRIPT_DIR/verify-project-isolation.sh" > "$EVIDENCE_DIR/f002-case2-unlabelled.log" 2>&1
CASE2_STATUS=$?
set -e
CASE2_AFTER="$(docker volume inspect -f '{{.CreatedAt}}' "$PROBE_VOLUME" 2>/dev/null || echo absent)"
CASE2_RESOURCES="$(probe_resource_count)"
emit "case 2 — unlabelled collision, and the default-volume prerequisite fails"
emit "  synthetic volume:            $PROBE_VOLUME (labels: <none>)"
emit "  default project used:        $ABSENT_DEFAULT (owns no standby volume, so step 1 fails)"
emit "  old preflight would have read (containers/volumes by label): $CASE2_COUNTS"
emit "  wrapper exit status:         $CASE2_STATUS   expected non-zero"
emit "  volume created before:       $CASE2_CREATED"
emit "  volume created after:        $CASE2_AFTER"
if decoy_intact "$PROBE_VOLUME"; then
    emit "  sentinel content:            intact"
else
    emit "  sentinel content:            LOST"
    fail_case "case 2 lost the synthetic volume's contents"
fi
emit "  probe-labelled resources created: $CASE2_RESOURCES   expected 0"
[ "$CASE2_STATUS" -ne 0 ] || fail_case "case 2: the wrapper exited 0 despite a pre-existing colliding volume"
[ "$CASE2_AFTER" = "$CASE2_CREATED" ] || fail_case "case 2: the colliding volume was replaced (creation time changed)"
[ "$CASE2_RESOURCES" = "0" ] || fail_case "case 2: the refused run left $CASE2_RESOURCES probe resource(s) behind"
if grep -q 'REFUSED' "$EVIDENCE_DIR/f002-case2-unlabelled.log"; then
    emit "  the run refused at the ownership preflight, before installing any trap"
else
    fail_case "case 2: the log does not show an ownership refusal"
fi
drop_decoy "$PROBE_VOLUME"
emit ""

# ---------------------------------------------------------------------------
# 3. Collision carrying a DIFFERENT project's label.
# ---------------------------------------------------------------------------
log "3/4: foreign-labelled collision must be refused as well"
make_decoy "$PROBE_VOLUME" \
    --label com.docker.compose.project=someoneelsesproject \
    --label com.docker.compose.volume=pg-replica-data
CASE3_CREATED="$(docker volume inspect -f '{{.CreatedAt}}' "$PROBE_VOLUME")"
CASE3_COUNTS="$(label_counts "$PROBE_PROJECT")"
set +e
COMPOSE_PROJECT_NAME="$ABSENT_DEFAULT" FN_ISOLATION_PROJECT="$PROBE_PROJECT" \
    "$SCRIPT_DIR/verify-project-isolation.sh" > "$EVIDENCE_DIR/f002-case3-foreign-label.log" 2>&1
CASE3_STATUS=$?
set -e
CASE3_AFTER="$(docker volume inspect -f '{{.CreatedAt}}' "$PROBE_VOLUME" 2>/dev/null || echo absent)"
emit "case 3 — collision labelled for another project, prerequisite still fails"
emit "  synthetic volume:            $PROBE_VOLUME"
emit "  its labels:                  com.docker.compose.project=someoneelsesproject"
emit "  old preflight would have read (containers/volumes by label): $CASE3_COUNTS"
emit "  wrapper exit status:         $CASE3_STATUS   expected non-zero"
emit "  volume created before:       $CASE3_CREATED"
emit "  volume created after:        $CASE3_AFTER"
if decoy_intact "$PROBE_VOLUME"; then
    emit "  sentinel content:            intact"
else
    emit "  sentinel content:            LOST"
    fail_case "case 3 lost the synthetic volume's contents"
fi
[ "$CASE3_STATUS" -ne 0 ] || fail_case "case 3: the wrapper exited 0 despite a foreign-labelled colliding volume"
[ "$CASE3_AFTER" = "$CASE3_CREATED" ] || fail_case "case 3: the foreign-labelled volume was replaced"
drop_decoy "$PROBE_VOLUME"
emit ""

# ---------------------------------------------------------------------------
# 4. No collision, failing early prerequisite: the trap runs and must be safe.
# ---------------------------------------------------------------------------
log "4/4: early prerequisite failure with no collision must remove only owned resources"
FOREIGN_SENTINEL="$FOREIGN_SENTINEL_NAME"
make_decoy "$FOREIGN_SENTINEL" --label com.docker.compose.project=someoneelsesproject
CASE4_COUNTS="$(label_counts "$PROBE_PROJECT")"
set +e
COMPOSE_PROJECT_NAME="$ABSENT_DEFAULT" FN_ISOLATION_PROJECT="$PROBE_PROJECT" \
    "$SCRIPT_DIR/verify-project-isolation.sh" > "$EVIDENCE_DIR/f002-case4-early-failure.log" 2>&1
CASE4_STATUS=$?
set -e
CASE4_RESOURCES="$(probe_resource_count)"
emit "case 4 — no collision; the default-volume prerequisite fails after the trap exists"
emit "  old preflight would have read (containers/volumes by label): $CASE4_COUNTS"
emit "  wrapper exit status:         $CASE4_STATUS   expected non-zero"
emit "  probe-labelled resources remaining: $CASE4_RESOURCES   expected 0"
if decoy_intact "$FOREIGN_SENTINEL"; then
    emit "  unrelated foreign-labelled volume: intact"
else
    emit "  unrelated foreign-labelled volume: LOST"
    fail_case "case 4 destroyed an unrelated volume"
fi
[ "$CASE4_STATUS" -ne 0 ] || fail_case "case 4: the wrapper exited 0 with no default standby volume to test against"
[ "$CASE4_RESOURCES" = "0" ] || fail_case "case 4: $CASE4_RESOURCES probe resource(s) survived cleanup"
if grep -qE 'step 1|does not exist' "$EVIDENCE_DIR/f002-case4-early-failure.log"; then
    emit "  the run reached the step-1 prerequisite, so the trap was installed and ran"
else
    fail_case "case 4: the run did not reach the step-1 prerequisite, so the trap path was not exercised"
fi
drop_decoy "$FOREIGN_SENTINEL"
emit ""

emit "Scope: cleanup now removes volumes only through the label-verified guard,"
emit "never through 'down --volumes'. Cases 2 and 3 are refused before any trap"
emit "is installed; case 4 exercises the trap itself. All volumes named above"
emit "were created and removed by this script."
emit ""
emit "mismatches: $FAILURES"

if [ "$FAILURES" -ne 0 ]; then
    echo >&2
    echo "FAILED: $FAILURES ownership check(s) did not hold; see $OUT" >&2
    exit 1
fi

log "PASSED: cleanup cannot remove resources predating the invocation"
note "evidence: $OUT"
