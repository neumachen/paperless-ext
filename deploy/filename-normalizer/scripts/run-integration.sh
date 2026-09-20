#!/bin/sh
# Run the full phased integration suite, F1 through F6.
#
# Each phase is a real, isolated stack state produced by starting or stopping a
# single container in this project. The suite runs inside a container for every
# phase, and the phase declares which dependencies it has interrupted so a
# fault that failed to take effect cannot be mistaken for a pass.
#
# Only this project's containers are touched. Unrelated containers, volumes and
# development data are never stopped or removed.
. "$(dirname "$0")/lib.sh"

RUN_ID="${FN_TEST_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"

# A single scalar out of the ledger. Only the drain phase needs one, to wait
# for discovery to register its real documents before the termination.
# `tr -d ' '` is applied to the RESULT of a count, never to a name: deleting
# spaces from data corrupted a timestamp in an earlier exercise.
# renamer-2 carries a qualification-only override during the drain phase, and
# it must come off on EVERY exit path.
#
# It used to be undone only after `run_phase drained` returned, so a run that
# stopped earlier -- the drain guard refusing to terminate an instance holding
# nothing, an interrupt, any `exit 1` above -- left renamer-2 armed with
# hold_after_claim and a 45s budget. That was observed: a later exercise on
# this stack then ran against an instance that pauses on every claim.
DRAIN_OVERRIDE_ACTIVE=0
RESTORE_FAILED=0

restore_drain_overrides() {
    [ "$DRAIN_OVERRIDE_ACTIVE" = "1" ] || return 0
    DRAIN_OVERRIDE_ACTIVE=0
    log "restoring renamer-2 to its ordinary configuration"
    # Recreated, not restarted: a restart keeps the environment the container
    # was created with, so the override would survive it.
    recreate_service renamer-2 || true

    # Verified from the RUNNING service, not assumed from the compose file.
    _rd_ready="$(compose ps --format '{{.Health}}' renamer-2 2>/dev/null | head -1)"
    [ -n "$_rd_ready" ] || _rd_ready=unreadable

    # A failed inspection and a zero count are different answers. `grep -c`
    # exits non-zero on no matches, so `|| true` turned "docker inspect could
    # not run" and "the variable is absent" into the same 0 -- and 0 is the
    # value that says restoration succeeded.
    if _rd_envout="$(docker inspect "$(compose ps -q renamer-2 2>/dev/null)" \
        --format '{{range .Config.Env}}{{println .}}{{end}}' 2>/dev/null)"; then
        _rd_env="$(printf '%s\n' "$_rd_envout" | grep -c '^FN_FAULT_POINTS=hold_after_claim' || true)"
    else
        _rd_env=unreadable
    fi
    if _rd_logout="$(compose logs renamer-2 2>/dev/null)"; then
        _rd_armed="$(printf '%s\n' "$_rd_logout" | grep -c 'fault_points_armed' || true)"
    else
        _rd_armed=unreadable
    fi
    _rd_takeover="$(effective_value renamer-2 "d['processing']['publish_takeover_ms']")"

    note "renamer-2 readiness:            ${_rd_ready:-unknown}"
    note "renamer-2 fault env present:    ${_rd_env:-unknown} (expected 0)"
    note "renamer-2 armed since recreate: ${_rd_armed:-unknown} (expected 0)"
    note "renamer-2 publish_takeover_ms:  ${_rd_takeover:-unreadable} (expected 20000)"

    # An inspection that could not run is UNKNOWN, and unknown is not restored.
    if [ "$_rd_ready" != "healthy" ] \
       || [ "${_rd_env:-1}" != "0" ] \
       || [ "${_rd_armed:-1}" != "0" ] \
       || [ "${_rd_takeover:-unreadable}" != "20000" ]; then
        echo "error: renamer-2 was NOT restored to its ordinary configuration." >&2
        echo "       It may still carry the drain phase's injected hold." >&2
        RESTORE_FAILED=1
    fi
}
trap 'restore_drain_overrides' EXIT INT TERM

psql_scalar() {
    compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" \
        postgres-primary psql -U "${FN_DB_USER:-fn_app}" \
        -d "${FN_DB_NAME:-filename_normalizer}" -tA -c "$1" \
        </dev/null 2>/dev/null | tr -d ' \r\n'
}
PHASE_FAILURES=0
export RUN_ID

: > "$EVIDENCE_DIR/phase-results.txt"
printf 'run_id=%s\nproject=%s\nstarted=%s\n' \
    "$RUN_ID" "$PROJECT" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$EVIDENCE_DIR/run-metadata.txt"

log "integration run $RUN_ID (project $PROJECT)"

# --- ensure the stack is up and healthy ------------------------------------
log "bringing up the stack"
compose up -d --wait --wait-timeout 300
for svc in postgres-primary postgres-replica rabbitmq $APP_SERVICES; do
    wait_healthy "$svc"
done

# Give the applications a moment to complete a first readiness pass and a
# first accounting pass before asserting on their steady state.
sleep 8

# --- F1, F2, F3, F4, F6 steady state --------------------------------------
run_phase baseline "" || true

# --- A1, A3, A4, A10: real documents through the real pipeline -------------
# Placed early, on a healthy stack, because these are the acceptance criteria
# about ordinary correct operation. The fault phases that follow deliberately
# run against a system that has already published real documents, so their
# recovery assertions have something to recover.
run_phase normalization "" || true

# --- FN-F004: the public entry points' own exit statuses ------------------
# Executed here because the suite's container holds the test binary, not the
# application binaries. Both the positive and the negative outcome are
# recorded, and the suite asserts on them.
log "recording the public commands' exit statuses"
record_public_command_evidence

# --- Watcher restart reconciliation ---------------------------------------
# Documents are placed while the watcher container is stopped, so nothing is
# watching when they arrive. The next scan after it restarts must pick them up.
log "stopping the watcher, then submitting documents while nothing is watching"
compose stop watcher >/dev/null 2>&1 || true

_offline=""
for i in 1 2 3; do
    # Lowercase, because the naming policy lowercases the stem and the test
    # matches the published name.
    _name="offline-$(printf '%s' "$RUN_ID" | tr 'A-Z' 'a-z')-${i}.pdf"
    compose run --rm --no-deps --entrypoint sh storage-init -c \
        "printf '%%PDF-1.4 arrived while the watcher was down\n' > '/srv/fn/incoming/.wip-${i}' && \
         mv '/srv/fn/incoming/.wip-${i}' '/srv/fn/incoming/${_name}'" >/dev/null 2>&1 || true
    _offline="${_offline}${_offline:+,}${_name}"
done
save_state offline-submissions "$_offline"
note "submitted while the watcher was down: $_offline"

# Older than the stability interval by the time the watcher returns.
sleep 4
compose start watcher >/dev/null 2>&1 || true
wait_healthy watcher || true
run_phase watcher_restarted "" || true

# --- F6: log assertions, with the logs baseline produced now collected -----
# run_phase re-collects logs before it runs, so this phase sees the records the
# baseline phase caused the applications to emit.
run_phase telemetry "" || true

# --- F6: stack-wide privacy, after the telemetry phase forced a slow
#     database statement. Logs are re-collected again so the PostgreSQL slow
#     statement produced above is inside what the suite scans.
run_phase stack_privacy "" || true

# --- F3/F5: the standby is stopped and returns ----------------------------
log "stopping the standby"
mark_since replica_down
compose stop -t 30 postgres-replica
wait_stopped postgres-replica
sleep 6
run_phase replica_down "" || true

log "starting the standby again"
compose start postgres-replica
wait_healthy postgres-replica 240
sleep 8
run_phase replica_recovered "" || true

# --- F3: persistence across an ORDERLY primary restart --------------------
# SIGTERM, so PostgreSQL shuts down cleanly. This establishes restart
# persistence; the ungraceful kill below is what establishes crash recovery.
log "restarting the primary (orderly, SIGTERM)"
mark_since primary_restarted
compose restart -t 30 postgres-primary
wait_healthy postgres-primary 240
wait_healthy postgres-replica 240
sleep 10
run_phase primary_restarted "" || true

# --- F3: crash recovery, via an ungraceful kill ---------------------------
log "killing the primary with SIGKILL"
# The container id is captured first so its exit status can be read after the
# kill. Ignoring the kill's own status, as this once did, would let a failed
# injection leave the assertions free to pass on an earlier crash's records.
PRIMARY_CID="$(compose ps -q postgres-primary)"
if [ -z "$PRIMARY_CID" ]; then
    echo "error: postgres-primary is not running; cannot inject a crash" >&2
    exit 1
fi
mark_since primary_killed
if ! compose kill -s SIGKILL postgres-primary; then
    echo "error: the SIGKILL injection failed" >&2
    exit 1
fi
wait_stopped postgres-primary 60
# 137 is 128+SIGKILL, and it is what proves the postmaster was killed rather
# than asked to stop. The suite requires this value.
PRIMARY_KILL_CODE="$(docker inspect -f '{{.State.ExitCode}}' "$PRIMARY_CID" 2>&1 || echo unknown)"
save_state "primary-kill-exit-code" "$PRIMARY_KILL_CODE"
note "postgres-primary exited with code $PRIMARY_KILL_CODE after SIGKILL"
if [ "$PRIMARY_KILL_CODE" != "137" ]; then
    echo "error: expected exit code 137 after SIGKILL, got '$PRIMARY_KILL_CODE'" >&2
    exit 1
fi
# restart: unless-stopped does not resurrect a container stopped by compose
# kill in every configuration, so it is started explicitly; it must then
# recover its WAL.
compose start postgres-primary
wait_healthy postgres-primary 300
sleep 12
run_phase primary_killed "" || true

# --- F5: the primary is down, then returns --------------------------------
log "stopping the primary"
mark_since primary_down
compose stop -t 30 postgres-primary
wait_stopped postgres-primary
sleep 10
run_phase primary_down postgres_primary || true

log "starting the primary again"
compose start postgres-primary
wait_healthy postgres-primary 240
sleep 12
run_phase primary_recovered "" || true

# --- F4/F5: the broker is down, then returns ------------------------------
log "stopping the broker"
mark_since rabbit_down
compose stop -t 30 rabbitmq
wait_stopped rabbitmq
sleep 8
run_phase rabbit_down rabbitmq || true

log "starting the broker again"
compose start rabbitmq
wait_healthy rabbitmq 300
sleep 15
run_phase rabbit_restarted "" || true
run_phase rabbit_recovered "" || true

# --- FN-F001: the broker is torn down under the running watcher -----------
# The publisher's stale-return drain used to spin forever once the AMQP client
# closed its notification channel, which would strand the dispatch worker and,
# with it, stranded-claim recovery. This phase tears the broker down repeatedly
# while the watcher runs, then requires the same watcher process to resume both
# dispatch and claim recovery.
log "tearing the broker down under the running watcher"
mark_since broker_torn
save_state "watcher-restart-count-before" "$(restart_count watcher)"
for round in 1 2 3; do
    note "interruption round $round"
    compose stop -t 5 rabbitmq >/dev/null
    wait_stopped rabbitmq
    sleep 4
    compose start rabbitmq >/dev/null
    wait_healthy rabbitmq 300
    sleep 6
done
# The watcher must not have been restarted: otherwise "the same worker resumed"
# would prove nothing.
save_state "watcher-restart-count" "$(restart_count watcher)"
note "watcher container restart count: $(restart_count watcher)"
sleep 10
run_phase broker_torn "" || true

# --- FN-F001: the watcher's own shutdown must stay bounded afterwards -----
log "terminating the watcher with SIGTERM after the interruptions"
stop_timed watcher 40 watcher
run_phase watcher_drained "" || true
log "restarting the watcher"
compose start watcher >/dev/null
wait_healthy watcher 180
sleep 8

# --- F5/F6: unavailable storage -------------------------------------------
log "starting a watcher whose incoming root was never mounted"
mark_since storage_fault
compose --profile fault up -d watcher-storage-fault
sleep 12
run_phase storage_fault "" || true
compose --profile fault stop -t 15 watcher-storage-fault >/dev/null 2>&1 || true
compose --profile fault rm -f watcher-storage-fault >/dev/null 2>&1 || true

# --- FN-F004: a reachable database with no ledger schema -----------------
log "starting a renamer pointed at a reachable database that has no ledger schema"
mark_since schema_fault
compose --profile fault up -d renamer-no-schema
sleep 15
run_phase schema_fault "" || true
compose --profile fault stop -t 15 renamer-no-schema >/dev/null 2>&1 || true
compose --profile fault rm -f renamer-no-schema >/dev/null 2>&1 || true

# --- FN-F005: graceful termination with real work in flight --------------
# The suite registers a large batch and returns without waiting, so the batch
# is still draining when SIGTERM arrives. Readiness is sampled throughout, by
# a container using the application's own probe subcommand.
# The load phase runs in the BACKGROUND: it registers work continuously for a
# window, so the renamers stay saturated across the interval in which
# renamer-2 is terminated. Stopping an instance after a fixed batch had already
# drained would exercise an idle exit, not a drain.
# Sampling starts BEFORE the load, so it spans the whole termination; it is
# also kept running for a few seconds afterwards, because the drain window
# itself is milliseconds and the observable change is the endpoint going away.
# Filling the queue with the consumers stopped is what makes the backlog
# deterministic. Publishing into a queue that has live consumers hands each
# message straight to one of them, so no depth accumulates however fast the
# publisher runs — an earlier attempt published 1200 messages over 75 seconds
# and the queue never exceeded zero.
# renamer-2 is rebuilt HERE, before the consumers are stopped, so it holds
# each delivery it later claims and the drain below is deterministic.
#
# Without a hold the window is one or two durable writes wide, and the phase
# terminated an idle instance while still reporting green: `drained` passed on
# three of four runs by catching a single delivery by chance, twice with
# byte-identical evidence (stop_duration_seconds=0). That is not a property
# being asserted, it is a coin flip.
#
# Arming must happen before the stop below, not after the restart: recreating
# a container and waiting for health takes tens of seconds, and renamer-1
# drains the backlog in less than that, so an instance armed afterwards sits
# armed and idle -- observed as `fault_points_armed` twice and
# `fault_point_paused` never.
#
# Qualification only. These variables exist in this development compose file;
# production/compose.prod.yml has no fault plumbing.
# Armed BEFORE the command that mutates, not after it. `compose up
# --force-recreate` can remove the old container and fail while creating the
# new one; arming afterwards meant that partial change was never undone.
DRAIN_OVERRIDE_ACTIVE=1
(
    export FN_RENAMER2_FAULT_POINTS=hold_after_claim
    export FN_RENAMER2_FAULT_HOLD=15s
    # The handler budget is the shutdown timeout, so a held delivery must be
    # able to finish inside it; otherwise the drain records shutdown_timeout.
    export FN_RENAMER2_SHUTDOWN_TIMEOUT=45s
    # Pinned, so raising the budget does not move the claim-staleness rule.
    export FN_RENAMER2_TAKEOVER_AFTER=20s
    compose up -d --force-recreate renamer-2
)
wait_healthy renamer-2 180

log "stopping both renamers so the work queue can accumulate"
compose stop -t 30 renamer-1 renamer-2
wait_stopped renamer-1
wait_stopped renamer-2

log "filling the work queue"
run_phase drain_under_load "" || true

# The synthetic batch above registers ledger rows for files that DO NOT EXIST
# and builds depth by republishing their references. Those deliveries are
# rejected at source validation and held, so they never reach the publication
# path -- and every injected fault point lives inside it. An instance armed
# with hold_after_claim took 5213 of them and paused zero times.
#
# So the drain also needs work that genuinely publishes. These are real files
# in the incoming root: the watcher registers them, the renamers claim them,
# and renamer-2 pauses in the claim it is armed to hold.
log "submitting real documents so the drain has publishable work"
_dq_stamp="$(date -u +%Y%m%dT%H%M%SZ | tr 'A-Z' 'a-z')"
DRAIN_REAL_PREFIX="drainq-$_dq_stamp"
_dq=1
while [ "$_dq" -le "${FN_DRAIN_REAL_DOCS:-24}" ]; do
    compose run --rm --no-deps -T --entrypoint sh storage-init -c \
        "printf '%%PDF-1.4 drain fixture $_dq\n' > /srv/fn/incoming/.wip-$DRAIN_REAL_PREFIX-$_dq && \
         mv /srv/fn/incoming/.wip-$DRAIN_REAL_PREFIX-$_dq /srv/fn/incoming/$DRAIN_REAL_PREFIX-$_dq.pdf" \
        >/dev/null 2>&1
    _dq=$((_dq + 1))
done
# Discovery is asynchronous: wait for the watcher rather than sampling once.
_dq_want="${FN_DRAIN_REAL_DOCS:-24}"
_dq_i=0
_dq_have=0
while [ "$_dq_i" -lt 120 ]; do
    _dq_have="$(psql_scalar "SELECT count(*) FROM jobs WHERE source_name LIKE '$DRAIN_REAL_PREFIX-%';")"
    [ "${_dq_have:-0}" = "$_dq_want" ] && break
    sleep 2; _dq_i=$((_dq_i + 2))
done
save_state drain-real-prefix "$DRAIN_REAL_PREFIX"
note "registered $_dq_have of $_dq_want real drain documents"
if [ "${_dq_have:-0}" != "$_dq_want" ]; then
    echo "error: only $_dq_have of $_dq_want real drain documents were registered;" >&2
    echo "       the drain would run without publishable work." >&2
    exit 1
fi

log "starting the renamers again"
compose start renamer-1 renamer-2
wait_healthy renamer-1 180
wait_healthy renamer-2 180

# The fault interval and the readiness sampling both begin HERE, with
# renamer-2 up and serving. Marking earlier would have pulled the deliberate
# stop that emptied the consumers into the interval, so the assertions would
# have measured that shutdown instead of the one under test, and the sampler
# would have recorded the fill window as the instance being unreachable.
mark_since drain
log "sampling renamer-2 readiness"
watch_readiness http://renamer-2:8080 150 "$EVIDENCE_DIR/state/$RUN_ID.drain-readiness-observations"
# Give the sampler a moment to record the instance serving before it is stopped.
sleep 1

log "terminating renamer-2 once the broker confirms work is in flight"
# Deterministic rather than timed. A fixed delay was wrong in both directions:
# too early and the restarted renamer has not attached its consumer yet, too
# late and the backlog has drained. Both were observed. The broker's own
# unacknowledged count is the condition the drain assertion actually needs.
if ! wait_for_unacked "${FN_AMQP_QUEUE:-filename_normalizer.jobs.v1}" 1 90; then
    echo "error: cannot terminate renamer-2 during a drain that is not happening" >&2
    exit 1
fi
# The broker's count is queue-wide, and the assertion is not: it reads
# renamer-2's own in-flight count, its own draining log and the deliveries it
# settled itself. A run satisfied the queue-wide condition with four
# unacknowledged deliveries that were all on renamer-1, terminated renamer-2
# idle, and the phase reported that no drain had been exercised. The instance
# under test has to be the instance holding work.
if ! wait_for_instance_in_flight renamer-2 1 120; then
    echo "error: renamer-2 is holding no delivery; terminating it now would exercise an idle exit, not a drain" >&2
    exit 1
fi
# ...and then wait until the injected hold has actually FIRED, and stop it
# there. An armed fault is not evidence that it fired: the run that proved
# that was armed, took 5213 deliveries and paused none of them.
#
# `in_flight >= 1` is necessary and not sufficient. Every step between the
# restart and the stop costs seconds and the reading can go stale, which is
# how runs reached this line with the last delivery already settled and
# terminated an idle instance. A `fault_point_paused` line younger than this
# poll window says renamer-2 is holding work right now, with most of the 15s
# hold still ahead of the signal.
_held=0
_hi=0
while [ "$_hi" -lt 150 ]; do
    if compose logs --since 4s renamer-2 2>/dev/null | grep -q 'fault_point_paused'; then
        _held=1
        break
    fi
    sleep 1; _hi=$((_hi + 1))
done
if [ "$_held" != "1" ]; then
    echo "error: renamer-2 never entered its injected hold within 150s." >&2
    echo "       Terminating now would exercise an idle exit, not a drain." >&2
    echo "       Check that the arming recreate applied FN_FAULT_POINTS to this" >&2
    echo "       instance, and that the real drain documents above were published." >&2
    exit 1
fi
note "renamer-2 is inside an injected hold; terminating it there"
# Grace exceeds the 15s hold plus the 45s budget's worst case, so a bounded
# exit is what is asserted -- not a particular duration.
stop_timed renamer-2 90 renamer2
# Keep sampling past the termination so the endpoint's disappearance is
# recorded rather than inferred.
sleep 6
stop_readiness_watch

run_phase drained "" || true

log "restarting renamer-2"
# The same verified restoration the trap performs, so the successful path and
# every failure path undo the override identically.
restore_drain_overrides
wait_healthy renamer-2 180
sleep 6

# --- FN-F006: privacy over the WHOLE run's logs ---------------------------
# The mid-run pass could only scan what existed then. This one runs last, so
# the collected logs include every outage, recovery, kill and termination
# above, across all six services.
run_phase final_privacy "" || true

# --- final evidence -------------------------------------------------------
collect_logs
"$SCRIPT_DIR/collect-evidence.sh" || true

printf 'finished=%s\nphase_failures=%s\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$PHASE_FAILURES" >> "$EVIDENCE_DIR/run-metadata.txt"

log "phase results"
cat "$EVIDENCE_DIR/phase-results.txt"

if [ "${RESTORE_FAILED:-0}" -ne 0 ]; then
    echo
    echo "renamer-2 was not restored to its ordinary configuration; see above." >&2
    exit 1
fi
if [ "$PHASE_FAILURES" -ne 0 ]; then
    echo
    echo "$PHASE_FAILURES phase(s) failed. Evidence is under $EVIDENCE_DIR" >&2
    exit 1
fi
log "all phases passed; evidence is under $EVIDENCE_DIR"
