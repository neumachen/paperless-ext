#!/bin/sh
# Exercise the recovery behaviour this increment actually supports:
# manual promotion of the standby. There is no automatic failover, and this
# script does not pretend otherwise.
#
# What it does:
#   1. Records the pre-promotion cluster state.
#   2. Stops the primary.
#   3. Promotes the standby with pg_ctl promote and proves it accepts writes
#      and still holds the data replicated from the primary.
#   4. Leaves the cluster in the documented post-promotion state, then rebuilds
#      it so the stack is usable again.
#
# The rebuild removes the standby's data volume only. The volume is identified
# from the running standby container's own mounts and then verified against its
# Compose project labels before removal, so a name that merely looks right — or
# a same-named volume in another project — is refused rather than deleted. The
# primary's volume and every other project volume are untouched.
. "$(dirname "$0")/lib.sh"

OUT="$EVIDENCE_DIR/failover-$(date -u +%Y-%m-%dT%H%M%SZ)"
mkdir -p "$OUT"

PGUSER_VAL="$(sed -n 's/^FN_DB_USER=//p' .env)"; PGUSER_VAL="${PGUSER_VAL:-fn_app}"
PGDB_VAL="$(sed -n 's/^FN_DB_NAME=//p' .env)"; PGDB_VAL="${PGDB_VAL:-filename_normalizer}"

PGPASS_VAL="$(sed -n 's/^FN_DB_PASSWORD=//p' .env)"

primary_sql() {
    compose exec -T -e PGPASSWORD="$PGPASS_VAL" postgres-primary \
        psql -h 127.0.0.1 -U "$PGUSER_VAL" -d "$PGDB_VAL" -tAc "$1" 2>&1
}
standby_sql() {
    compose exec -T -e PGPASSWORD="$PGPASS_VAL" postgres-replica \
        psql -h 127.0.0.1 -U "$PGUSER_VAL" -d "$PGDB_VAL" -tAc "$1" 2>&1
}

log "pre-promotion state"
compose up -d --wait --wait-timeout 300 postgres-primary postgres-replica >/dev/null
wait_healthy postgres-primary 240
wait_healthy postgres-replica 240

{
    echo '=== pre-promotion ==='
    echo "primary in_recovery: $(primary_sql 'SELECT pg_is_in_recovery()')"
    echo "standby in_recovery: $(standby_sql 'SELECT pg_is_in_recovery()')"
    echo "walsender state: $(primary_sql "SELECT concat_ws(' / ', state, sync_state) FROM pg_stat_replication")"
    echo "jobs on primary: $(primary_sql 'SELECT count(*) FROM jobs')"
    echo "jobs on standby: $(standby_sql 'SELECT count(*) FROM jobs')"
} > "$OUT/01-pre-promotion.txt" 2>&1
cat "$OUT/01-pre-promotion.txt"

BEFORE="$(primary_sql 'SELECT count(*) FROM jobs' | tr -d ' \r')"

log "writing a marker row on the primary and waiting for it to replicate"
MARKER="failover-marker-$(date -u +%s)"
primary_sql "INSERT INTO jobs (job_id, contract_version, state, source_root, source_name, policy_version)
             VALUES (gen_random_uuid(), 1, 'pending_dispatch', '/srv/fn/incoming', '$MARKER', 'unimplemented')" > /dev/null
waited=0
while [ "$waited" -lt 60 ]; do
    found="$(standby_sql "SELECT count(*) FROM jobs WHERE source_name = '$MARKER'" | tr -d ' \r')"
    [ "$found" = "1" ] && break
    sleep 1; waited=$((waited + 1))
done
if [ "${found:-0}" != "1" ]; then
    echo "error: the marker row never replicated to the standby" >&2
    exit 1
fi
note "marker replicated to the standby after ${waited}s"

log "stopping the primary"
compose stop -t 30 postgres-primary
wait_stopped postgres-primary

log "promoting the standby (manual promotion; there is no automatic failover)"
compose exec -T postgres-replica pg_ctl -D /var/lib/postgresql/data promote > "$OUT/02-promote.txt" 2>&1 || true
cat "$OUT/02-promote.txt"

waited=0
while [ "$waited" -lt 60 ]; do
    rec="$(standby_sql 'SELECT pg_is_in_recovery()' | tr -d ' \r')"
    [ "$rec" = "f" ] && break
    sleep 1; waited=$((waited + 1))
done

{
    echo '=== post-promotion ==='
    echo "promotion completed after ${waited}s"
    echo "promoted node in_recovery: $(standby_sql 'SELECT pg_is_in_recovery()')"
    echo "marker row present after promotion: $(standby_sql "SELECT count(*) FROM jobs WHERE source_name = '$MARKER'")"
    echo "total jobs before promotion: $BEFORE (plus the marker)"
    echo "total jobs on the promoted node: $(standby_sql 'SELECT count(*) FROM jobs')"
    echo '--- the promoted node must now accept writes ---'
    standby_sql "INSERT INTO jobs (job_id, contract_version, state, source_root, source_name, policy_version)
                 VALUES (gen_random_uuid(), 1, 'pending_dispatch', '/srv/fn/incoming', 'post-promotion-write', 'unimplemented')"
    echo "write accepted: $(standby_sql "SELECT count(*) FROM jobs WHERE source_name = 'post-promotion-write'")"
} > "$OUT/03-post-promotion.txt" 2>&1
cat "$OUT/03-post-promotion.txt"

# An application readiness document taken while the standby is promoted.
#
# A promoted standby still answers queries, so its check passes while carrying
# a category an operator has to see. The health server no longer discards a
# category just because the probe succeeded, and this is the artefact that
# shows it survives to the readiness document.
log "recording application readiness while the standby is promoted"
mkdir -p "$EVIDENCE_DIR/state"
PROMOTED_READYZ="$OUT/04-promoted-standby-readyz.json"
: > "$PROMOTED_READYZ"

# The applications re-probe on an interval, so the promotion is not visible in
# a readiness document taken the instant pg_ctl returns. This polls until the
# standby check reports the promoted category, and records the last document
# either way so a run in which it never appears is visible rather than silent.
promoted_seen=no
attempt=0
while [ "$attempt" -lt 20 ]; do
    attempt=$((attempt + 1))
    compose run --rm --no-deps \
        --entrypoint /usr/local/bin/fn-renamer renamer-1 \
        probe http://renamer-1:8080 2>/dev/null | tail -1 > "$PROMOTED_READYZ.tmp" || true
    if [ -s "$PROMOTED_READYZ.tmp" ]; then
        mv "$PROMOTED_READYZ.tmp" "$PROMOTED_READYZ"
        if grep -q 'promoted_not_in_recovery' "$PROMOTED_READYZ"; then
            promoted_seen=yes
            note "the application reported the standby as promoted after ${attempt} probe(s)"
            break
        fi
    fi
    sleep 2
done
rm -f "$PROMOTED_READYZ.tmp"

if [ -s "$PROMOTED_READYZ" ]; then
    cat "$PROMOTED_READYZ"
    # Published as cross-phase state so the integration suite can assert on it.
    if [ -n "${FN_TEST_RUN_ID:-}" ]; then
        cp "$PROMOTED_READYZ" "$EVIDENCE_DIR/state/$FN_TEST_RUN_ID.promoted-readyz"
    fi
    cp "$PROMOTED_READYZ" "$EVIDENCE_DIR/state/latest.promoted-readyz"
else
    echo "warning: could not record a readiness document during promotion" >&2
fi
if [ "$promoted_seen" != "yes" ]; then
    echo "warning: the application never reported promoted_not_in_recovery within $((attempt * 2))s;" >&2
    echo "         the recorded document is the last one observed" >&2
fi
printf 'promoted_category_observed=%s probes=%s\n' "$promoted_seen" "$attempt" \
    > "$OUT/04-promoted-standby-observation.txt"

{
    echo 'Supported recovery behaviour for this increment:'
    echo
    echo '  * Asynchronous streaming replication from one primary to one standby.'
    echo '  * Manual promotion of the standby with pg_ctl promote, exercised above.'
    echo '  * NO automatic failover. Nothing watches the primary and nothing'
    echo '    repoints the applications; FN_DB_PRIMARY_HOST is static, so a'
    echo '    promoted standby only serves the applications after an operator'
    echo '    changes that configuration.'
    echo '  * Because replication is asynchronous, a promotion can lose commits'
    echo '    the primary had not yet shipped. The marker row above proves the'
    echo '    replicated data survived; it does not prove zero data loss.'
} > "$OUT/04-supported-behaviour.txt"
cat "$OUT/04-supported-behaviour.txt"

log "rebuilding the cluster: the promoted node cannot stream from the old primary again"

# The volume is identified from the standby container's own mounts and then
# verified against its Compose labels, while the container still exists. A name
# assembled from a project string could name another project's volume, and a
# retained volume with no remaining container references would be removable.
STANDBY_VOLUME="$(service_volume postgres-replica /var/lib/postgresql/data)"
if [ -z "$STANDBY_VOLUME" ]; then
    echo "error: could not identify the standby data volume; refusing to remove anything" >&2
    exit 1
fi
note "standby data volume identified as '$STANDBY_VOLUME' (project '$(effective_project)')"
{
    echo "effective compose project: $(effective_project)"
    echo "PROJECT as resolved by the scripts: $PROJECT"
    echo "standby volume identified from the container: $STANDBY_VOLUME"
    docker volume inspect -f 'labels: project={{index .Labels "com.docker.compose.project"}} volume={{index .Labels "com.docker.compose.volume"}}' "$STANDBY_VOLUME"
} > "$OUT/05-volume-identification.txt" 2>&1
cat "$OUT/05-volume-identification.txt"

compose stop -t 30 postgres-replica >/dev/null
compose rm -f postgres-replica >/dev/null
safe_remove_volume "$STANDBY_VOLUME" pg-replica-data
compose start postgres-primary >/dev/null
wait_healthy postgres-primary 240
# Drop and recreate the slot so the fresh base backup starts from a clean one.
primary_sql "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '$(sed -n 's/^FN_REPLICATION_SLOT=//p' .env)'" >/dev/null 2>&1 || true
primary_sql "SELECT pg_create_physical_replication_slot('$(sed -n 's/^FN_REPLICATION_SLOT=//p' .env)')" >/dev/null 2>&1 || true
compose up -d postgres-replica >/dev/null
wait_healthy postgres-replica 300

{
    echo '=== after rebuild ==='
    echo "primary in_recovery: $(primary_sql 'SELECT pg_is_in_recovery()')"
    echo "standby in_recovery: $(standby_sql 'SELECT pg_is_in_recovery()')"
    echo "walsender state: $(primary_sql "SELECT concat_ws(' / ', state, sync_state) FROM pg_stat_replication")"
    echo "note: the post-promotion write above lived only on the promoted node and is gone after the rebuild, which is the expected consequence of discarding a diverged standby."
} > "$OUT/06-after-rebuild.txt" 2>&1
cat "$OUT/06-after-rebuild.txt"

log "failover exercise complete; evidence in $OUT"
