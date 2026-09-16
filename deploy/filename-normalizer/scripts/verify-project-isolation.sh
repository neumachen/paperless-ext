#!/bin/sh
# FN-F002 — prove that a destructive test operation run under an overridden
# project name removes only its own project's volume.
#
# The scenario the finding describes:
#
#   1. The default project is brought down with its volumes retained, so its
#      standby volume exists with no container references. A merely stopped
#      container would block removal; a retained volume would not.
#   2. Its .env still names the default project.
#   3. A second project is run through a COMPOSE_PROJECT_NAME shell override.
#   4. The failover exercise's guarded cleanup runs inside that second project.
#
# Three things must hold, and all three are asserted rather than inferred:
#
#   * The nested exercise must actually COMPLETE. An earlier version of this
#     script started the probe project with only its database services, so the
#     nested failover died at its marker INSERT before reaching promotion or
#     the guarded removal — and the wrapper still reported PASS because the
#     default volume happened to survive. The probe project is now started as a
#     full stack (its watcher applies the schema) and the nested exit status is
#     required to be zero.
#   * The guarded removal must be shown to have executed against the probe's
#     OWN volume, and the guard must be shown to REFUSE a volume belonging to
#     another project.
#   * The default project's retained volume must survive, unchanged.
#
# Ownership and cleanup: this script refuses to run if any resource cleanup
# could remove already exists, because it would otherwise destroy resources it
# did not create. Ownership is established by NAME as well as by label, since
# `compose down --volumes` removes a project's volumes by name and ignores
# labels entirely — an empty label query is not ownership proof. Cleanup itself
# removes volumes only through the label-verified guard, never with
# `down --volumes`. The trap is installed only after the preflight passes, so
# an early prerequisite failure removes nothing. See verify-cleanup-ownership.sh
# for the synthetic collision and early-failure evidence.
. "$(dirname "$0")/lib.sh"

OTHER_PROJECT="${FN_ISOLATION_PROJECT:-fnisolationprobe}"
OUT="$EVIDENCE_DIR/project-isolation-$(date -u +%Y-%m-%dT%H%M%SZ)"
mkdir -p "$OUT"

DEFAULT_PROJECT="$PROJECT"
DEFAULT_VOLUME="${DEFAULT_PROJECT}_pg-replica-data"
PROBE_VOLUME="${OTHER_PROJECT}_pg-replica-data"

fail() {
    echo >&2
    echo "FAILED: $*" >&2
    echo "        evidence: $OUT" >&2
    exit 1
}

if [ "$OTHER_PROJECT" = "$DEFAULT_PROJECT" ]; then
    fail "the probe project must differ from the default project '$DEFAULT_PROJECT'"
fi

# ---------------------------------------------------------------------------
# Ownership check, BEFORE any cleanup trap is installed.
# ---------------------------------------------------------------------------
log "step 0: confirm this invocation may own the probe project '$OTHER_PROJECT'"

# Label counts are recorded, but they are NOT the ownership test. `docker
# compose down --volumes` removes a project's volumes by the NAME compose would
# give them, without consulting labels: an unlabelled volume called
# '<project>_pg-replica-data' is removed just the same. So a label-only
# preflight can read 0/0 while cleanup still has a pre-existing volume in
# range. Verified directly, and retained as the positive control in
# f002-cleanup-ownership.txt.
PRE_CONTAINERS="$(docker ps -aq --filter "label=com.docker.compose.project=$OTHER_PROJECT" | wc -l | tr -d ' ')"
PRE_VOLUMES="$(docker volume ls -q --filter "label=com.docker.compose.project=$OTHER_PROJECT" | wc -l | tr -d ' ')"

# Every volume cleanup could remove, addressed the way compose addresses it.
PROBE_VOLUME_KEYS="$(COMPOSE_PROJECT_NAME="$OTHER_PROJECT" docker compose \
    --profile test --profile fault config --volumes 2>/dev/null)"
if [ -z "$PROBE_VOLUME_KEYS" ]; then
    echo "error: could not enumerate the probe project's configured volumes, so this" >&2
    echo "       invocation cannot establish what its cleanup would be able to remove." >&2
    exit 1
fi

COLLIDING=""
for _iso_key in $PROBE_VOLUME_KEYS; do
    _iso_full="${OTHER_PROJECT}_${_iso_key}"
    if docker volume inspect "$_iso_full" >/dev/null 2>&1; then
        COLLIDING="${COLLIDING}${COLLIDING:+ }$_iso_full"
    fi
done
PRE_NAMED_CONTAINERS="$(docker ps -aq --filter "name=^${OTHER_PROJECT}-" | wc -l | tr -d ' ')"

{
    echo "probe project: $OTHER_PROJECT"
    echo "pre-existing containers with that project label: $PRE_CONTAINERS"
    echo "pre-existing volumes with that project label:    $PRE_VOLUMES"
    echo "containers matching the project's name prefix:   $PRE_NAMED_CONTAINERS"
    echo
    echo "--- ownership by name, for every volume cleanup could remove ---"
    echo "compose removes project volumes by name, not by label, so each"
    echo "configured volume name is checked regardless of its labels:"
    for _iso_key in $PROBE_VOLUME_KEYS; do
        _iso_full="${OTHER_PROJECT}_${_iso_key}"
        if docker volume inspect "$_iso_full" >/dev/null 2>&1; then
            printf '  %-46s PRE-EXISTING (labels: %s)\n' "$_iso_full" \
                "$(docker volume inspect -f '{{if .Labels}}{{range $k,$v := .Labels}}{{$k}}={{$v}} {{end}}{{else}}<none>{{end}}' "$_iso_full" 2>/dev/null)"
        else
            printf '  %-46s absent\n' "$_iso_full"
        fi
    done
    echo
    if [ -n "$COLLIDING" ]; then
        echo "result: REFUSED — this invocation did not create:$COLLIDING"
    else
        echo "result: owned — no configured probe volume name existed beforehand"
    fi
} > "$OUT/00-ownership.txt"
cat "$OUT/00-ownership.txt"

if [ -n "$COLLIDING" ]; then
    echo "error: these volumes already exist under the probe project's configured names:" >&2
    for _iso_full in $COLLIDING; do echo "         $_iso_full" >&2; done
    echo "       This invocation did not create them, and \`compose down --volumes\` would" >&2
    echo "       remove them by name whatever their labels say. Choose another name with" >&2
    echo "       FN_ISOLATION_PROJECT, or remove those volumes deliberately first." >&2
    echo "       Nothing has been created or destroyed by this run." >&2
    exit 1
fi

if [ "$PRE_CONTAINERS" != "0" ] || [ "$PRE_VOLUMES" != "0" ] || [ "$PRE_NAMED_CONTAINERS" != "0" ]; then
    echo "error: project '$OTHER_PROJECT' already has $PRE_CONTAINERS labelled container(s)," >&2
    echo "       $PRE_VOLUMES labelled volume(s) and $PRE_NAMED_CONTAINERS container(s) matching its" >&2
    echo "       name prefix. This invocation did not create them, so it will not remove" >&2
    echo "       them. Choose another name with FN_ISOLATION_PROJECT, or remove that" >&2
    echo "       project deliberately first." >&2
    echo "       Nothing has been created or destroyed by this run." >&2
    exit 1
fi

# Only now is cleanup safe, and even now it does not use `down --volumes`:
# that removes by name. Each volume is removed through the same label-verified
# guard the failover exercise uses, so cleanup can only destroy a volume
# carrying this project's label and the expected volume key. Combined with the
# preflight above — which proved no such name and no such label existed — every
# resource cleanup can remove was created by this invocation.
OWNS_PROBE=yes
cleanup() {
    [ "${OWNS_PROBE:-no}" = "yes" ] || return 0
    log "removing the probe project and the volumes this invocation created"
    COMPOSE_PROJECT_NAME="$OTHER_PROJECT" docker compose \
        --profile test --profile fault down --remove-orphans >/dev/null 2>&1 || true

    # The expected project is passed explicitly. $PROJECT in this shell is the
    # DEFAULT project, and by this point the probe project has no containers
    # left for effective_project() to read, so letting the guard infer the
    # project would make it refuse every volume this invocation created --
    # leaving them all behind while the run still reported success.
    _cl_left=""
    for _cl_key in $PROBE_VOLUME_KEYS; do
        _cl_full="${OTHER_PROJECT}_${_cl_key}"
        docker volume inspect "$_cl_full" >/dev/null 2>&1 || continue
        if safe_remove_volume "$_cl_full" "$_cl_key" "$OTHER_PROJECT" 2>&1; then
            :
        else
            _cl_left="${_cl_left}${_cl_left:+ }$_cl_full"
        fi
    done

    # A volume this invocation created that cleanup could not remove is a leak,
    # and it must be visible rather than swallowed: the next run would refuse
    # to start on the collision it leaves behind.
    if [ -n "$_cl_left" ]; then
        echo >&2
        echo "WARNING: cleanup could not remove volume(s) created by this run:" >&2
        for _cl_full in $_cl_left; do
            echo "           $_cl_full" >&2
        done
        echo "         They are still present. Remove them deliberately before the next run." >&2
        # Fail the run. A cleanup that quietly declines to remove what it owns
        # is how this check produced a false pass in the first place.
        exit 1
    else
        note "probe project removed; no volumes left behind"
    fi
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# The default project's retained volume.
# ---------------------------------------------------------------------------
log "step 1: confirm the default project has a retained standby volume"
if ! docker volume inspect "$DEFAULT_VOLUME" >/dev/null 2>&1; then
    fail "$DEFAULT_VOLUME does not exist; bring the default stack up once first (make up)"
fi
DEFAULT_CREATED_BEFORE="$(docker volume inspect -f '{{.CreatedAt}}' "$DEFAULT_VOLUME")"
{
    echo "default project: $DEFAULT_PROJECT"
    echo "--- default project's standby volume, before ---"
    docker volume inspect -f 'name={{.Name}} project={{index .Labels "com.docker.compose.project"}} volume={{index .Labels "com.docker.compose.volume"}} created={{.CreatedAt}}' "$DEFAULT_VOLUME"
} > "$OUT/01-before.txt" 2>&1
cat "$OUT/01-before.txt"

log "step 2: bring the default project down, RETAINING its volumes"
compose --profile test --profile fault down --remove-orphans >/dev/null
if ! docker volume inspect "$DEFAULT_VOLUME" >/dev/null 2>&1; then
    fail "the default standby volume was removed by a non-destructive down"
fi
DEFAULT_REFS="$(docker ps -a --filter "volume=$DEFAULT_VOLUME" -q | wc -l | tr -d ' ')"
{
    echo "default project brought down with volumes retained"
    echo "containers still referencing $DEFAULT_VOLUME: $DEFAULT_REFS"
    echo ".env still names: $(sed -n 's/^COMPOSE_PROJECT_NAME=//p' .env)"
} > "$OUT/02-retained.txt" 2>&1
cat "$OUT/02-retained.txt"
if [ "$DEFAULT_REFS" != "0" ]; then
    fail "$DEFAULT_REFS container(s) still reference $DEFAULT_VOLUME, so its removal would be blocked anyway and this scenario proves nothing"
fi

# ---------------------------------------------------------------------------
# The probe project, as a complete stack so the nested exercise can finish.
# ---------------------------------------------------------------------------
log "step 3: start the probe project as a full stack under a COMPOSE_PROJECT_NAME override"
if ! COMPOSE_PROJECT_NAME="$OTHER_PROJECT" docker compose up -d --wait --wait-timeout 600 \
        > "$OUT/03-probe-up.txt" 2>&1; then
    cat "$OUT/03-probe-up.txt" >&2
    fail "the probe project's stack did not start"
fi
note "probe project stack is up (its watcher applies the ledger schema)"
PROBE_CREATED_BEFORE="$(docker volume inspect -f '{{.CreatedAt}}' "$PROBE_VOLUME" 2>/dev/null || echo absent)"
note "probe standby volume created at: $PROBE_CREATED_BEFORE"

# The guard must refuse a volume belonging to another project. This calls the
# real guard, from inside the probe project, against the DEFAULT project's
# volume: it must refuse and the volume must survive.
log "step 4: the guard must refuse a volume owned by another project"
# The guard function is already defined in this shell from lib.sh. It is
# called in a subshell with only COMPOSE_PROJECT_NAME overridden, so the
# project it resolves is the probe's — exactly as it would be during a real
# overridden-project run. Re-sourcing lib.sh from `sh -c` would not work: its
# paths derive from $0.
set +e
GUARD_REFUSAL="$(
    export COMPOSE_PROJECT_NAME="$OTHER_PROJECT"
    safe_remove_volume "$DEFAULT_VOLUME" pg-replica-data 2>&1
)"
GUARD_STATUS=$?
set -e
{
    echo "attempted: safe_remove_volume $DEFAULT_VOLUME pg-replica-data"
    echo "while operating as project: $OTHER_PROJECT"
    echo "exit status: $GUARD_STATUS"
    echo "output:"
    echo "$GUARD_REFUSAL"
} > "$OUT/04-guard-refusal.txt"
cat "$OUT/04-guard-refusal.txt"
if [ "$GUARD_STATUS" -eq 0 ]; then
    fail "the guard REMOVED a volume belonging to project '$DEFAULT_PROJECT' while operating as '$OTHER_PROJECT'"
fi
case "$GUARD_REFUSAL" in
    *REFUSING*) note "the guard refused, as required" ;;
    *) fail "the guard did not remove the volume but also did not report a refusal; the reason is unclear" ;;
esac
if ! docker volume inspect "$DEFAULT_VOLUME" >/dev/null 2>&1; then
    fail "$DEFAULT_VOLUME no longer exists after the refused removal"
fi

# ---------------------------------------------------------------------------
# The nested destructive exercise, which must COMPLETE.
# ---------------------------------------------------------------------------
log "step 5: run the guarded destructive exercise inside the probe project"
set +e
COMPOSE_PROJECT_NAME="$OTHER_PROJECT" FN_TEST_RUN_ID="${FN_TEST_RUN_ID:-isolation}" \
    "$SCRIPT_DIR/run-failover.sh" > "$OUT/05-probe-failover.txt" 2>&1
FAILOVER_STATUS=$?
set -e
note "probe project's failover exercise exited $FAILOVER_STATUS"
tail -15 "$OUT/05-probe-failover.txt" || true

if [ "$FAILOVER_STATUS" -ne 0 ]; then
    fail "the nested failover exercise exited $FAILOVER_STATUS, so the guarded removal was never reached; see $OUT/05-probe-failover.txt"
fi

# The guarded removal must be visible in the nested run, naming the probe's own
# volume, and the volume must have been recreated by the rebuild.
if ! grep -q "removing volume '$PROBE_VOLUME'" "$OUT/05-probe-failover.txt"; then
    fail "the nested run does not show the guard removing '$PROBE_VOLUME'; see $OUT/05-probe-failover.txt"
fi
PROBE_CREATED_AFTER="$(docker volume inspect -f '{{.CreatedAt}}' "$PROBE_VOLUME" 2>/dev/null || echo absent)"
if [ "$PROBE_CREATED_AFTER" = "$PROBE_CREATED_BEFORE" ]; then
    fail "the probe's standby volume was not actually replaced (creation time unchanged: $PROBE_CREATED_BEFORE)"
fi

# ---------------------------------------------------------------------------
# The default project's volume must be untouched.
# ---------------------------------------------------------------------------
log "step 6: the default project's retained volume must have survived, unchanged"
if docker volume inspect "$DEFAULT_VOLUME" >/dev/null 2>&1; then
    RESULT=SURVIVED
    DEFAULT_CREATED_AFTER="$(docker volume inspect -f '{{.CreatedAt}}' "$DEFAULT_VOLUME")"
else
    RESULT=DELETED
    DEFAULT_CREATED_AFTER=absent
fi
{
    echo "--- default project's standby volume, after ---"
    echo "result: $RESULT"
    echo "created before: $DEFAULT_CREATED_BEFORE"
    echo "created after:  $DEFAULT_CREATED_AFTER"
    echo
    echo "--- probe project's own standby volume ---"
    echo "created before nested exercise: $PROBE_CREATED_BEFORE"
    echo "created after nested exercise:  $PROBE_CREATED_AFTER"
    echo "(a changed creation time shows the guard removed it and the rebuild recreated it)"
    echo
    echo "--- guarded removal lines from the probe project's run ---"
    grep -E "standby data volume identified|removing volume|REFUSING to remove" "$OUT/05-probe-failover.txt" || echo "(none found)"
    echo
    echo "--- nested exercise exit status ---"
    echo "$FAILOVER_STATUS"
} > "$OUT/06-after.txt" 2>&1
cat "$OUT/06-after.txt"

if [ "$RESULT" != "SURVIVED" ]; then
    fail "the default project's retained volume $DEFAULT_VOLUME was deleted by a destructive operation run under project '$OTHER_PROJECT'"
fi
if [ "$DEFAULT_CREATED_AFTER" != "$DEFAULT_CREATED_BEFORE" ]; then
    fail "$DEFAULT_VOLUME survived by name but its creation time changed ($DEFAULT_CREATED_BEFORE -> $DEFAULT_CREATED_AFTER): it was replaced, not preserved"
fi

# The probe project must come down before the default project comes back up:
# both publish the same host ports, so they cannot run at once.
cleanup
OWNS_PROBE=no

log "restoring the default project"
compose up -d --wait --wait-timeout 600 >/dev/null 2>&1 || \
    echo "warning: the default project did not fully come back up; run 'make up'" >&2

log "PASSED: the guard removed only '$PROBE_VOLUME'; '$DEFAULT_VOLUME' survived unchanged"
echo "evidence: $OUT"
