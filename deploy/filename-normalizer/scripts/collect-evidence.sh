#!/bin/sh
# Collect logs, readiness documents, metrics and real cluster/broker state.
#
# Everything is read from inside the project network using containers already
# part of the stack. Nothing is written outside the extension's .evidence
# directory.
. "$(dirname "$0")/lib.sh"

STAMP="$(date -u +%Y-%m-%dT%H%M%SZ)"
OUT="$EVIDENCE_DIR/snapshot-$STAMP"
mkdir -p "$OUT"

log "collecting evidence into $OUT"

# --- container state -------------------------------------------------------
compose --profile test --profile fault ps -a > "$OUT/compose-ps.txt" 2>&1 || true
compose --profile test --profile fault config --images > "$OUT/images.txt" 2>&1 || true
# Resolved digests for the pinned third-party images, so the run is reproducible.
for img in $(compose config --images 2>/dev/null | sort -u); do
    docker image inspect -f '{{.RepoTags}} {{if .RepoDigests}}{{index .RepoDigests 0}}{{else}}local-build{{end}}' "$img" \
      >> "$OUT/image-digests.txt" 2>&1 || true
done
for svc in $ALL_LOG_SERVICES; do
    cid="$(compose ps -q "$svc" 2>/dev/null || true)"
    if [ -n "$cid" ]; then
        # A container inspect has no RepoDigests field, so the image digest is
        # read from the image itself and the two lines are reported together.
        docker inspect -f \
          '{{.Name}} image={{.Config.Image}} user={{if .Config.User}}{{.Config.User}}{{else}}image-default{{end}} readonly={{.HostConfig.ReadonlyRootfs}} capdrop={{.HostConfig.CapDrop}} security_opt={{.HostConfig.SecurityOpt}} restarts={{.RestartCount}} status={{.State.Status}}' \
          "$cid" >> "$OUT/container-hardening.txt" 2>&1 \
          || echo "$svc: container inspect failed" >> "$OUT/container-hardening.txt"
    fi
done

# --- application HTTP surfaces --------------------------------------------
# Fetched from inside the network by a throwaway container built from the same
# image as the suite, so no host tooling is involved.
# busybox wget suppresses the body on a non-2xx status, and /readyz answers 503
# while an application is not ready, so the body is captured with the response
# header retained either way.
fetch() {
    target="$1"; path="$2"; dest="$3"
    compose run --rm --no-deps --entrypoint /bin/sh integration -c \
      "wget -S -q -O - '$target$path' 2>&1 || wget -S -q -O - --content-on-error '$target$path' 2>&1 || echo 'unreachable'" \
      > "$dest" 2>/dev/null || true
}
for pair in "watcher http://watcher:8080" "renamer-1 http://renamer-1:8080" "renamer-2 http://renamer-2:8080"; do
    name="${pair% *}"; url="${pair#* }"
    fetch "$url" /healthz "$OUT/$name-healthz.txt"
    fetch "$url" /readyz  "$OUT/$name-readyz.txt"
    fetch "$url" /metrics "$OUT/$name-metrics.txt"
done

# --- real PostgreSQL cluster state ----------------------------------------
PGUSER_VAL="$(sed -n 's/^FN_DB_USER=//p' .env)"; PGUSER_VAL="${PGUSER_VAL:-fn_app}"
PGDB_VAL="$(sed -n 's/^FN_DB_NAME=//p' .env)"; PGDB_VAL="${PGDB_VAL:-filename_normalizer}"
PGPASS_VAL="$(sed -n 's/^FN_DB_PASSWORD=//p' .env)"

# The cluster uses scram for local connections too, so psql is given the
# password explicitly and connects over TCP inside the container.
psql_node() {
    node="$1"; sql="$2"
    compose exec -T -e PGPASSWORD="$PGPASS_VAL" "$node" \
        psql -h 127.0.0.1 -U "$PGUSER_VAL" -d "$PGDB_VAL" -P pager=off -c "$sql" 2>&1 || true
}
psql_primary() { psql_node postgres-primary "$1"; }
psql_standby() { psql_node postgres-replica "$1"; }
{
    echo '--- server version and role ---'
    psql_primary "SELECT version(), pg_is_in_recovery() AS in_recovery;"
    echo '--- replication state (walsenders) ---'
    psql_primary "SELECT application_name, state, sync_state, sent_lsn, replay_lsn, pg_wal_lsn_diff(sent_lsn, replay_lsn) AS replay_lag_bytes FROM pg_stat_replication;"
    echo '--- replication slots ---'
    psql_primary "SELECT slot_name, slot_type, active, wal_status FROM pg_replication_slots;"
    echo '--- applied schema ---'
    psql_primary "SELECT version, name, applied_at FROM schema_migrations ORDER BY version;"
    echo '--- job counts by state ---'
    psql_primary "SELECT state, count(*) FROM jobs GROUP BY state ORDER BY state;"
    echo '--- job outcome categories ---'
    psql_primary "SELECT failure_category, count(*) FROM jobs GROUP BY failure_category ORDER BY 2 DESC;"
    echo '--- retained history event counts ---'
    psql_primary "SELECT event_type, count(*) FROM job_events GROUP BY event_type ORDER BY 2 DESC;"
} > "$OUT/postgres-cluster.txt"

{
    echo '--- standby role and replay position ---'
    psql_standby "SELECT pg_is_in_recovery() AS in_recovery, pg_last_wal_receive_lsn() AS receive_lsn, pg_last_wal_replay_lsn() AS replay_lsn;"
    echo '--- job counts as visible on the standby ---'
    psql_standby "SELECT state, count(*) FROM jobs GROUP BY state ORDER BY state;"
    echo '--- the standby must reject writes (this error is the expected result) ---'
    psql_standby "INSERT INTO schema_migrations (version, name) VALUES (-1, 'evidence-must-fail');"
} > "$OUT/postgres-standby.txt"

# --- real broker state ----------------------------------------------------
VHOST_VAL="$(sed -n 's/^FN_AMQP_VHOST=//p' .env)"; VHOST_VAL="${VHOST_VAL:-filename-normalizer}"
{
    echo '--- broker version and status ---'
    compose exec -T rabbitmq rabbitmqctl --formatter=plaintext status 2>&1 | head -40 || true
    echo '--- queues ---'
    compose exec -T rabbitmq rabbitmqctl --formatter=plaintext list_queues -p "$VHOST_VAL" \
      name type durable messages messages_ready messages_unacknowledged consumers 2>&1 || true
    echo '--- exchanges ---'
    compose exec -T rabbitmq rabbitmqctl --formatter=plaintext list_exchanges -p "$VHOST_VAL" name type durable 2>&1 || true
    echo '--- consumers ---'
    compose exec -T rabbitmq rabbitmqctl --formatter=plaintext list_consumers -p "$VHOST_VAL" \
      queue_name consumer_tag ack_required prefetch_count 2>&1 || true
    echo '--- connections ---'
    compose exec -T rabbitmq rabbitmqctl --formatter=plaintext list_connections \
      name user vhost state client_properties 2>&1 | head -30 || true
} > "$OUT/rabbitmq.txt"

# --- storage roles, read from inside the containers -----------------------
{
    echo '--- incoming root as the watcher sees it ---'
    compose run --rm --no-deps --entrypoint /bin/sh integration -c \
      'ls -la /srv/fn/incoming /srv/fn/incoming/synthetic 2>&1 | head -40; echo; echo "file count: $(find /srv/fn/incoming -type f | wc -l)"' 2>/dev/null || true
    echo
    echo '--- consume root (must be empty: nothing in this increment publishes) ---'
    compose run --rm --no-deps --entrypoint /bin/sh integration -c \
      'ls -la /srv/fn/consume 2>&1; echo "entry count: $(ls -A /srv/fn/consume | wc -l)"' 2>/dev/null || true
    echo
    echo '--- queued, staging and failed roots ---'
    compose run --rm --no-deps --entrypoint /bin/sh integration -c \
      'for d in queued staging failed; do echo "== $d =="; ls -la /srv/fn/$d; done' 2>/dev/null || true
} > "$OUT/storage-roles.txt"

# --- logs -----------------------------------------------------------------
mkdir -p "$OUT/logs"
for svc in $ALL_LOG_SERVICES; do
    compose logs --no-log-prefix --no-color "$svc" > "$OUT/logs/$svc.log" 2>/dev/null || true
done

note "wrote $(find "$OUT" -type f | wc -l | tr -d ' ') evidence files"
printf '%s\n' "$OUT" > "$EVIDENCE_DIR/latest-snapshot.txt"
