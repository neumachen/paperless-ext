# Shared helpers for the host-side orchestration scripts.
#
# These scripts only orchestrate containers: start, stop, restart, collect
# logs, and run the containerized test suite. Nothing is compiled or executed
# on the host.

set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEPLOY_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
EXT_DIR="$(cd "$DEPLOY_DIR/../../extensions/filename-normalizer" && pwd)"
EVIDENCE_DIR="$EXT_DIR/.evidence"

# Pinned by digest, like every other image in this stack: nothing is pulled by
# a floating tag. Kept identical to UTIL_IMAGE in the Makefile; used only for
# host-side container utility work (reading a volume, generating a secret).
UTIL_IMAGE="${UTIL_IMAGE:-postgres:17.11-alpine@sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73}"

# Pinned by digest like every other image. The exercises parse JSON, and that
# is development tooling: it runs in a container, not on the host.
UTIL_PY_IMAGE="${UTIL_PY_IMAGE:-python:3.13-alpine@sha256:7415fbc3c9e4979cc717d92377ab2bc7b2b4a2af1ac03cc52b5f3f88efedaf3a}"

APP_SERVICES="watcher renamer-1 renamer-2"
ALL_LOG_SERVICES="watcher renamer-1 renamer-2 postgres-primary postgres-replica rabbitmq storage-init"

cd "$DEPLOY_DIR"

if [ ! -f .env ]; then
    echo "error: $DEPLOY_DIR/.env is missing. Run 'make env' first." >&2
    exit 1
fi

# The Compose project name, resolved the way Compose itself resolves it.
#
# A shell COMPOSE_PROJECT_NAME takes precedence over the .env value, so reading
# .env alone can name a different project than the one the compose commands in
# this script actually operate on. Any destructive step keyed off the wrong
# name would target another project's resources.
PROJECT="${COMPOSE_PROJECT_NAME:-$(sed -n 's/^COMPOSE_PROJECT_NAME=//p' .env)}"
PROJECT="${PROJECT:-fnfoundation}"

mkdir -p "$EVIDENCE_DIR/logs" "$EVIDENCE_DIR/state"

compose() { docker compose "$@"; }

# restart_count reports how many times Docker has restarted a service's
# container. Assertions about "the same process resumed" depend on it.
restart_count() {
    _rc_cid="$(compose ps -aq "$1" 2>/dev/null | head -1)"
    if [ -z "$_rc_cid" ]; then echo "unknown"; return 0; fi
    docker inspect -f '{{.RestartCount}}' "$_rc_cid" 2>/dev/null || echo unknown
}

# save_state writes a value the containerized suite reads back through the
# shared evidence directory.
save_state() {
    printf '%s' "$2" > "$EVIDENCE_DIR/state/$RUN_ID.$1"
}

# collect_logs and run_phase also assign globals; keep their names distinct.

# stop_timed sends SIGTERM to a service, waits for it, and records how long it
# took plus its exit code, so "bounded exit" is measured rather than assumed.
stop_timed() {
    _st_svc="$1"; _st_grace="${2:-40}"; _st_prefix="${3:-$_st_svc}"
    _st_cid="$(compose ps -q "$_st_svc")"
    if [ -z "$_st_cid" ]; then
        echo "error: $_st_svc has no running container to stop" >&2
        save_state "$_st_prefix-exit-code" "no-container"
        save_state "$_st_prefix-stop-seconds" "0"
        return 1
    fi
    _st_started="$(date +%s)"
    compose stop -t "$_st_grace" "$_st_svc"
    wait_stopped "$_st_svc" $((_st_grace + 20))
    _st_ended="$(date +%s)"
    _st_code="$(docker inspect -f '{{.State.ExitCode}}' "$_st_cid" 2>&1 || echo unknown)"
    case "$_st_code" in
        ''|*[!0-9]*) echo "warning: could not read the exit code of $_st_svc ($_st_cid): $_st_code" >&2
                     _st_code=unknown ;;
    esac
    save_state "$_st_prefix-exit-code" "$_st_code"
    save_state "$_st_prefix-stop-seconds" "$((_st_ended - _st_started))"
    note "$_st_svc stopped in $((_st_ended - _st_started))s with exit code $_st_code"
}

# watch_readiness starts a background container that samples a service's
# readiness endpoint and appends JSON lines to a file. It uses the application
# binary's own probe subcommand, because busybox wget suppresses the body of a
# 503 response and /readyz answers 503 precisely while an instance is draining.
READINESS_WATCH_CID=""
watch_readiness() {
    _wr_target="$1"; _wr_duration="${2:-60}"; _wr_out="$3"
    : > "$_wr_out"
    # readiness-probe is a dedicated service: it runs the renamer binary (for
    # its probe subcommand) and mounts the evidence directory, which the
    # application services deliberately do not.
    READINESS_WATCH_CID="$(compose --profile test run -d --no-deps readiness-probe \
        probe --interval 200ms --duration "${_wr_duration}s" --quiet \
              --output "/evidence/state/$(basename "$_wr_out")" "$_wr_target" 2>/dev/null || true)"
    if [ -n "$READINESS_WATCH_CID" ]; then
        note "sampling $_wr_target readiness every 200ms for up to ${_wr_duration}s"
    else
        echo "warning: could not start the readiness watcher" >&2
    fi
}

stop_readiness_watch() {
    if [ -n "$READINESS_WATCH_CID" ]; then
        docker stop "$READINESS_WATCH_CID" >/dev/null 2>&1 || true
        docker rm -f "$READINESS_WATCH_CID" >/dev/null 2>&1 || true
        READINESS_WATCH_CID=""
    fi
}

# effective_project reads the project name off a container this compose
# invocation created, which is the authoritative answer. It falls back to
# PROJECT before anything has been started.
effective_project() {
    _ep_cid="$(compose ps -aq 2>/dev/null | head -1)"
    if [ -n "$_ep_cid" ]; then
        _ep_name="$(docker inspect -f '{{index .Config.Labels "com.docker.compose.project"}}' "$_ep_cid" 2>/dev/null || true)"
        if [ -n "$_ep_name" ]; then
            printf '%s' "$_ep_name"
            return 0
        fi
    fi
    printf '%s' "$PROJECT"
}

# service_volume prints the volume name backing a mount point inside a
# service's container, read from the container itself rather than assembled
# from a project name and a suffix.
service_volume() {
    _sv_svc="$1"; _sv_mount="$2"
    _sv_cid="$(compose ps -aq "$_sv_svc" 2>/dev/null | head -1)"
    if [ -z "$_sv_cid" ]; then
        echo "error: service '$_sv_svc' has no container, so its volume cannot be identified" >&2
        return 1
    fi
    docker inspect -f \
      "{{range .Mounts}}{{if eq .Destination \"$_sv_mount\"}}{{.Name}}{{end}}{{end}}" \
      "$_sv_cid" 2>/dev/null
}

# safe_remove_volume removes a volume only after verifying, from the volume's
# own Compose labels, that it belongs to the project this script is operating
# on and is the expected volume of that project.
#
# This is the guard for the destructive step in the failover exercise: a volume
# name is never trusted because it looks right, and a volume belonging to any
# other project is refused rather than removed.
safe_remove_volume() {
    _srv_vol="$1"; _srv_key="$2"; _srv_expect="${3:-}"
    if [ -z "$_srv_vol" ]; then
        echo "error: refusing to remove an empty volume name" >&2
        return 1
    fi

    # The expected project may be passed explicitly. effective_project() reads
    # it off a live container and falls back to $PROJECT, which is resolved
    # when this file is sourced — so a caller operating on a DIFFERENT project
    # (an overridden COMPOSE_PROJECT_NAME, or a project whose containers are
    # already gone) must say which project it means. Exporting
    # COMPOSE_PROJECT_NAME in a subshell does not change $PROJECT here.
    if [ -n "$_srv_expect" ]; then
        _srv_want="$_srv_expect"
    else
        _srv_want="$(effective_project)"
    fi
    _srv_got_project="$(docker volume inspect -f '{{index .Labels "com.docker.compose.project"}}' "$_srv_vol" 2>/dev/null || true)"
    _srv_got_key="$(docker volume inspect -f '{{index .Labels "com.docker.compose.volume"}}' "$_srv_vol" 2>/dev/null || true)"

    if [ "$_srv_got_project" != "$_srv_want" ]; then
        echo "REFUSING to remove volume '$_srv_vol': its compose project is '${_srv_got_project:-<unlabelled>}', not '$_srv_want'" >&2
        return 1
    fi
    if [ "$_srv_got_key" != "$_srv_key" ]; then
        echo "REFUSING to remove volume '$_srv_vol': its compose volume key is '${_srv_got_key:-<unlabelled>}', not '$_srv_key'" >&2
        return 1
    fi

    note "removing volume '$_srv_vol' (project '$_srv_got_project', volume '$_srv_got_key')"
    docker volume rm "$_srv_vol" >/dev/null
}

# wait_for_unacked blocks until the broker reports at least N unacknowledged
# deliveries on the work queue, i.e. until a delivery is genuinely in flight.
#
# This replaces a fixed sleep before terminating a renamer. The sleep was
# guesswork in both directions: too early and the instance has not attached its
# consumer yet, too late and the backlog has drained. The broker's own
# unacknowledged count is the actual condition the drain assertion needs.
wait_for_unacked() {
    _wu_queue="$1"; _wu_min="${2:-1}"; _wu_limit="${3:-90}"; _wu_waited=0
    _wu_user="$(sed -n 's/^FN_AMQP_USER=//p' .env)"; _wu_user="${_wu_user:-fn_app}"
    _wu_pass="$(sed -n 's/^FN_AMQP_PASSWORD=//p' .env)"
    _wu_vhost="$(sed -n 's/^FN_AMQP_VHOST=//p' .env)"; _wu_vhost="${_wu_vhost:-filename-normalizer}"
    _wu_auth="$(printf '%s:%s' "$_wu_user" "$_wu_pass" | base64 | tr -d '\n')"

    while [ "$_wu_waited" -lt "$_wu_limit" ]; do
        _wu_body="$(compose exec -T rabbitmq sh -c \
            "wget -q -O - --header='Authorization: Basic $_wu_auth' 'http://127.0.0.1:15672/api/queues/$_wu_vhost/$_wu_queue'" \
            2>/dev/null || true)"
        _wu_unacked="$(printf '%s' "$_wu_body" | sed -n 's/.*"messages_unacknowledged":\([0-9]*\).*/\1/p')"
        _wu_consumers="$(printf '%s' "$_wu_body" | sed -n 's/.*"consumers":\([0-9]*\).*/\1/p')"
        if [ -n "$_wu_unacked" ] && [ "$_wu_unacked" -ge "$_wu_min" ]; then
            note "broker reports $_wu_unacked unacknowledged delivery/deliveries and ${_wu_consumers:-?} consumer(s): work is in flight"
            return 0
        fi
        sleep 1
        _wu_waited=$((_wu_waited + 1))
    done
    echo "error: the broker never reported $_wu_min unacknowledged delivery/deliveries on $_wu_queue within ${_wu_limit}s" >&2
    return 1
}

# record_public_command_evidence executes the public entry points whose exit
# status the contract depends on, and records both the positive and the
# negative outcome for the suite to assert on.
#
# These have to be run from here: the suite's container holds the test binary,
# not the application binaries, and it cannot exec into another container.
record_public_command_evidence() {
    _pc_out="$EVIDENCE_DIR/public-commands"
    mkdir -p "$_pc_out"

    # 1. The real healthcheck subcommand against a RUNNING process, executed
    #    inside that process's own container — the same command Compose runs.
    for _pc_svc in watcher renamer-1; do
        if compose exec -T "$_pc_svc" /usr/local/bin/fn-"${_pc_svc%%-*}" healthcheck \
                > "$_pc_out/healthcheck-$_pc_svc-running.txt" 2>&1; then
            _pc_code=0
        else
            _pc_code=$?
        fi
        save_state "healthcheck-$_pc_svc-running-exit" "$_pc_code"
        note "healthcheck against running $_pc_svc exited $_pc_code"
    done

    # 2. The same subcommand pointed at an address with no listener, so the
    #    negative case is the command's own exit status rather than an
    #    inference about it.
    if compose exec -T watcher /usr/local/bin/fn-watcher healthcheck --addr :19999 \
            > "$_pc_out/healthcheck-no-listener.txt" 2>&1; then
        _pc_code=0
    else
        _pc_code=$?
    fi
    save_state "healthcheck-no-listener-exit" "$_pc_code"
    note "healthcheck against a port with no listener exited $_pc_code"

    # 3. make ready with every application up: must succeed.
    if (cd "$DEPLOY_DIR" && make ready) > "$_pc_out/make-ready-all-up.txt" 2>&1; then
        _pc_code=0
    else
        _pc_code=$?
    fi
    save_state "make-ready-all-up-exit" "$_pc_code"
    note "make ready with all applications up exited $_pc_code"

    # 4. make ready with one application stopped: must FAIL. This is the
    #    finding's point — it used to print ready:false and exit 0.
    compose stop -t 20 renamer-2 >/dev/null
    wait_stopped renamer-2 60
    if (cd "$DEPLOY_DIR" && make ready) > "$_pc_out/make-ready-one-down.txt" 2>&1; then
        _pc_code=0
    else
        _pc_code=$?
    fi
    save_state "make-ready-one-down-exit" "$_pc_code"
    note "make ready with renamer-2 stopped exited $_pc_code"
    compose start renamer-2 >/dev/null
    wait_healthy renamer-2 180
}

# mark_since records the instant just before a fault is injected, so
# assertions about that fault read only log records from its own interval
# rather than from the whole run.
mark_since() {
    _ms_label="$1"
    date -u +%Y-%m-%dT%H:%M:%S.000000000Z > "$EVIDENCE_DIR/state/$RUN_ID.since-$_ms_label"
    note "fault interval for '$_ms_label' starts at $(cat "$EVIDENCE_DIR/state/$RUN_ID.since-$_ms_label")"
}

log() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
note() { printf '    %s\n' "$*"; }

# collect_logs writes each service's current log to the shared evidence
# directory. The test container reads those files, which keeps it unprivileged:
# it never needs access to the Docker socket.
collect_logs() {
    for _cl_svc in $ALL_LOG_SERVICES; do
        compose logs --no-log-prefix --no-color "$_cl_svc" > "$EVIDENCE_DIR/logs/$_cl_svc.log" 2>/dev/null \
            || : > "$EVIDENCE_DIR/logs/$_cl_svc.log"
    done
}

# NOTE ON VARIABLE NAMES
#
# POSIX sh functions have no local scope: every assignment is global. These
# helpers call each other, so each uses a distinct prefix. Sharing a name here
# once let wait_stopped clear the container id that stop_timed had captured,
# which silently turned a measured exit code into "unknown".

# wait_healthy blocks until a service reports healthy, or fails loudly.
wait_healthy() {
    _wh_svc="$1"; _wh_limit="${2:-180}"; _wh_waited=0
    while [ "$_wh_waited" -lt "$_wh_limit" ]; do
        _wh_cid="$(compose ps -q "$_wh_svc" 2>/dev/null || true)"
        if [ -n "$_wh_cid" ]; then
            _wh_state="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$_wh_cid" 2>/dev/null || echo unknown)"
            case "$_wh_state" in
                healthy|running) note "$_wh_svc is $_wh_state"; return 0 ;;
            esac
        fi
        sleep 2
        _wh_waited=$((_wh_waited + 2))
    done
    echo "error: $_wh_svc did not become healthy within ${_wh_limit}s" >&2
    compose ps -a
    return 1
}

# wait_stopped blocks until a service is no longer running, used after a stop.
wait_stopped() {
    _ws_svc="$1"; _ws_limit="${2:-60}"; _ws_waited=0
    while [ "$_ws_waited" -lt "$_ws_limit" ]; do
        _ws_cid="$(compose ps -q "$_ws_svc" 2>/dev/null || true)"
        if [ -z "$_ws_cid" ]; then return 0; fi
        _ws_state="$(docker inspect -f '{{.State.Status}}' "$_ws_cid" 2>/dev/null || echo gone)"
        case "$_ws_state" in
            exited|dead|gone|created) return 0 ;;
        esac
        sleep 1
        _ws_waited=$((_ws_waited + 1))
    done
    echo "error: $_ws_svc did not stop within ${_ws_limit}s" >&2
    return 1
}

# run_phase collects logs and then runs the suite for one orchestrated phase.
#
# FN_TEST_EXPECT_DOWN names the dependencies the phase deliberately interrupts;
# the suite fails if one of them is actually reachable.
#
# The phase environment is passed with explicit `-e` flags. Exporting it into
# this shell would not reach the container: `compose run` only forwards
# variables the service declares or the command names.
#
# The exit status is captured inside the pipeline rather than taken from the
# pipeline itself, because a plain `cmd | tee file` reports tee's status and
# would turn a failing suite into a reported pass.
run_phase() {
    _rp_phase="$1"; _rp_expect="${2:-}"
    log "phase: $_rp_phase  (expect_down='${_rp_expect}')"
    collect_logs
    _rp_out="$EVIDENCE_DIR/phase-$_rp_phase.log"
    _rp_status_file="$EVIDENCE_DIR/.phase-$_rp_phase.status"
    rm -f "$_rp_status_file"

    {
        if compose run --rm --no-deps \
            -e "FN_TEST_PHASE=$_rp_phase" \
            -e "FN_TEST_RUN_ID=$RUN_ID" \
            -e "FN_TEST_EXPECT_DOWN=$_rp_expect" \
            integration -test.v -test.timeout 25m 2>&1
        then echo 0 > "$_rp_status_file"
        else echo "$?" > "$_rp_status_file"
        fi
    } | tee "$_rp_out"

    status="$(cat "$_rp_status_file" 2>/dev/null || echo 127)"
    rm -f "$_rp_status_file"
    out="$_rp_out"; phase="$_rp_phase"

    # A suite that exits before running anything must never count as a pass.
    # Every phase has at least one applicable test, so the output has to
    # contain go test result lines.
    if ! grep -qE '^(=== RUN|--- (PASS|FAIL|SKIP)|ok|PASS|FAIL)' "$out"; then
        note "phase $phase FAILED: the suite produced no test results (exit $status)"
        echo "$phase FAIL (no test results; exit $status)" >> "$EVIDENCE_DIR/phase-results.txt"
        PHASE_FAILURES=$((PHASE_FAILURES + 1))
        return 1
    fi

    if [ "$status" = "0" ]; then
        passed="$(grep -c '^--- PASS' "$out" 2>/dev/null | head -1)"
        skipped="$(grep -c '^    f[0-9]_.*not applicable in phase' "$out" 2>/dev/null | head -1)"
        passed="${passed:-0}"; skipped="${skipped:-0}"
        note "phase $phase PASSED  ($passed assertions passed, $skipped not applicable to this phase)"
        note "log: $out"
        echo "$phase PASS ($passed passed, $skipped not applicable)" >> "$EVIDENCE_DIR/phase-results.txt"
        return 0
    fi

    note "phase $phase FAILED (exit $status)  (log: $out)"
    echo "$phase FAIL (exit $status)" >> "$EVIDENCE_DIR/phase-results.txt"
    PHASE_FAILURES=$((PHASE_FAILURES + 1))
    return 1
}


# ---------------------------------------------------------------------------
# Exercise safety: ownership, isolation and restoration.
#
# The exercises in this directory stop services, rewrite the shared
# configuration and disconnect containers. Each of those is a mutation of state
# somebody else may be relying on, so three rules apply to all of them:
#
#   * take an exclusive lock before mutating, so two exercises cannot
#     interleave on one stack;
#   * never overwrite or remove something this invocation did not create;
#   * restore on EVERY exit path, and fail the target if restoration did not
#     take effect -- verified against the RUNNING state, not against a file.
# ---------------------------------------------------------------------------

# exercise_lock takes an exclusive lock for the duration of this invocation.
# A second exercise is refused before it mutates anything, rather than racing.
exercise_lock() {
    _xl_name="$1"
    _xl_dir="$EVIDENCE_DIR/.exercise.lock"
    if ! mkdir "$_xl_dir" 2>/dev/null; then
        _xl_owner="$(cat "$_xl_dir/owner" 2>/dev/null || echo unknown)"
        echo "error: another exercise ($_xl_owner) holds the lock at $_xl_dir." >&2
        echo "       Concurrent exercises mutate the same stack and configuration," >&2
        echo "       so this invocation refuses rather than interleaving with it." >&2
        echo "       Nothing has been changed. Remove the directory if it is stale." >&2
        return 1
    fi
    printf '%s pid=%s at=%s\n' "$_xl_name" "$$" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$_xl_dir/owner"
    return 0
}

exercise_unlock() { rm -rf "$EVIDENCE_DIR/.exercise.lock" 2>/dev/null || true; }

# backup_once copies a file to a per-invocation backup, refusing to clobber an
# existing one. A fixed backup name shared between runs is how a recovery copy
# gets overwritten by the very failure it was meant to survive.
backup_once() {
    _bo_src="$1"; _bo_dst="$2"
    if [ -e "$_bo_dst" ]; then
        echo "error: a backup already exists at $_bo_dst." >&2
        echo "       It belongs to another invocation and will not be overwritten." >&2
        return 1
    fi
    cp "$_bo_src" "$_bo_dst"
}

# own_service refuses to act on a service this invocation did not create.
# The fault services are removed on the way out, and removing one that was
# already running would destroy somebody else's work.
own_service() {
    _os_svc="$1"
    if [ -n "$(compose --profile fault ps -aq "$_os_svc" 2>/dev/null)" ]; then
        echo "error: service '$_os_svc' already exists." >&2
        echo "       This invocation did not create it and will not remove it." >&2
        return 1
    fi
    return 0
}

# recreate_service brings a service back with the CURRENT environment.
#
# `compose restart` restarts the existing container, which keeps whatever
# environment it was created with -- so an exercise that recreated a service
# with an override could not undo it by restarting. Recreation is what applies
# the current configuration.
recreate_service() {
    compose up -d --force-recreate --wait --wait-timeout 180 "$@" >/dev/null 2>&1
}

# effective_value reads one field from a running application's own effective
# configuration, so restoration is verified against what the process is
# actually using rather than against a file on disk. A file can be correct
# while the process that read it minutes ago is still running with the old
# one -- which is exactly the failure this whole helper exists to catch.
#
# check-config prints JSON followed by a human line, so the JSON is decoded
# with raw_decode rather than json.load: json.load would raise on the trailing
# text and every reading would silently come back "unknown", which reads as a
# difference and would make restoration checks pass or fail for the wrong
# reason.
effective_value() {
    _ev_svc="$1"; _ev_expr="$2"
    compose exec -T "$_ev_svc" /usr/local/bin/fn-renamer check-config 2>/dev/null \
        | run_py "import json,sys
try:
    d, _ = json.JSONDecoder().raw_decode(sys.stdin.read().lstrip())
    print($_ev_expr)
except Exception:
    print('unreadable')"
}

# watcher_effective_value is the same for the watcher, which ships its own
# binary name.
watcher_effective_value() {
    compose exec -T watcher /usr/local/bin/fn-watcher check-config 2>/dev/null \
        | run_py "import json,sys
try:
    d, _ = json.JSONDecoder().raw_decode(sys.stdin.read().lstrip())
    print($1)
except Exception:
    print('unreadable')"
}

# run_py runs Python inside a container. Development tooling does not run on
# the host; an exercise that shells out to a host interpreter has quietly
# stepped outside the container-only rule the rest of the project follows.
run_py() {
    docker run --rm -i "$UTIL_PY_IMAGE" python3 -c "$1"
}
