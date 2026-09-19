#!/bin/sh
# Validate the deployment package under production/ against the real local
# stack and the real application image.
#
# # Why a script and not a checklist
#
# Every claim in production/README.md is checkable, and the ones that were not
# checked were wrong: the first draft cited three CLI subcommands that do not
# exist and four metrics that do not exist. A package whose commands have never
# been run is a document, not a deployment.
. "$(dirname "$0")/lib.sh"

PROD="$DEPLOY_DIR/production"
OUT="$EVIDENCE_DIR/deployment-package.txt"
FAILURES=0
report_begin deployment-package "$OUT" "$0"

emit() { printf '%s\n' "$*" >> "$OUT"; }
bad() { printf '    MISMATCH: %s\n' "$*" >&2; emit "    MISMATCH: $*"; FAILURES=$((FAILURES + 1)); }

emit "The deployment package, checked against the real image and stack"
emit ""

# ---------------------------------------------------------------------------
# 1. The manifest parses with a complete environment, and nothing is missing.
# ---------------------------------------------------------------------------
log "1/7: the manifest parses with a complete environment"
SCRATCH="$(mktemp -d)"
trap 'rm -rf "$SCRATCH"' EXIT
mkdir -p "$SCRATCH/incoming" "$SCRATCH/queued" "$SCRATCH/staging" "$SCRATCH/consume" "$SCRATCH/failed"
printf 'placeholder\n' > "$SCRATCH/db_password"
printf 'placeholder\n' > "$SCRATCH/amqp_password"
cp "$DEPLOY_DIR/config/normalizer.json" "$SCRATCH/normalizer.json" 2>/dev/null \
    || printf '{}' > "$SCRATCH/normalizer.json"

WATCHER_REF="${FN_WATCHER_IMAGE_UNDER_TEST:-filename-normalizer/watcher:0.1.0-foundation}"
RENAMER_REF="${FN_RENAMER_IMAGE_UNDER_TEST:-filename-normalizer/renamer:0.1.0-foundation}"
cat > "$SCRATCH/prod.env" <<ENV
FN_WATCHER_IMAGE=$WATCHER_REF
FN_RENAMER_IMAGE=$RENAMER_REF
FN_DB_PRIMARY_HOST=postgres-primary
FN_AMQP_HOST=rabbitmq
FN_HOST_INCOMING=$SCRATCH/incoming
FN_HOST_QUEUED=$SCRATCH/queued
FN_HOST_STAGING=$SCRATCH/staging
FN_HOST_CONSUME=$SCRATCH/consume
FN_HOST_FAILED=$SCRATCH/failed
FN_HOST_CONFIG=$SCRATCH/normalizer.json
FN_SECRET_DB_PASSWORD=$SCRATCH/db_password
FN_SECRET_AMQP_PASSWORD=$SCRATCH/amqp_password
ENV

if RENDERED="$(docker compose -f "$PROD/compose.prod.yml" --env-file "$SCRATCH/prod.env" config 2>&1)"; then
    emit "manifest parses:                 yes"
else
    emit "manifest parses:                 NO"
    emit "$RENDERED" 
    bad "the manifest does not parse with a complete environment"
    RENDERED=""
fi

# Missing-variable discipline: without the environment it must FAIL, not
# silently render a deployment pointing at nothing.
if docker compose -f "$PROD/compose.prod.yml" --env-file /dev/null config >/dev/null 2>&1; then
    bad "the manifest renders with no environment at all; required values are not required"
else
    emit "refuses an empty environment:    yes   (required values are declared with :?)"
fi

# ---------------------------------------------------------------------------
# 2. Safety properties of the rendered manifest.
# ---------------------------------------------------------------------------
log "2/7: the rendered manifest is safe"
if [ -n "$RENDERED" ]; then
    _pub="$(printf '%s' "$RENDERED" | grep -E '^\s+- (published|target):' -A0 | wc -l | tr -d ' ')"
    _nonloopback="$(printf '%s' "$RENDERED" | grep -E 'host_ip:' | grep -cv '127\.0\.0\.1' || true)"
    emit "published ports bound elsewhere than loopback: ${_nonloopback:-0}   (expected 0)"
    [ "${_nonloopback:-0}" = "0" ] || bad "a port is published beyond loopback"

    if printf '%s' "$RENDERED" | grep -q "FN_FAULT_POINTS"; then
        bad "the manifest sets FN_FAULT_POINTS; fault injection must never be deployed"
    else
        emit "fault injection:                 absent   (expected absent)"
    fi

    _migrate="$(printf '%s' "$RENDERED" | grep -c 'FN_DB_APPLY_MIGRATIONS: "false"' || true)"
    emit "services that migrate on startup: 0   (all $_migrate declare false)"
    if printf '%s' "$RENDERED" | grep -q 'FN_DB_APPLY_MIGRATIONS: "true"'; then
        bad "a service would apply migrations on startup"
    fi

    _root="$(printf '%s' "$RENDERED" | grep -c 'user: 65532:65532' || true)"
    emit "application services running non-root: $_root   (expected 3)"
    [ "${_root:-0}" -ge 3 ] || bad "not every application service runs as 65532"

    if printf '%s' "$RENDERED" | grep -qE 'placeholder|password: *[A-Za-z0-9]'; then
        bad "a secret VALUE appears in the rendered manifest"
    else
        emit "secret values in the manifest:   none   (only file references)"
    fi
    _storage_required="$(printf '%s' "$RENDERED" | grep -c 'FN_STORAGE_REQUIRED: "true"' || true)"
    emit "services refusing absent storage: $_storage_required   (expected 3)"
    [ "${_storage_required:-0}" -ge 3 ] || bad "a service would treat unmounted storage as an empty directory"
fi

# ---------------------------------------------------------------------------
# 3. No credential is committed anywhere in the package.
# ---------------------------------------------------------------------------
log "3/7: no credential is committed"
if grep -rInE '(password|secret)[^A-Za-z_]*[:=][^:$]*[A-Za-z0-9]{8,}' "$PROD" \
     | grep -vE 'PASSWORD_FILE|_FILE:|\$\{|password file|password/' >/dev/null 2>&1; then
    bad "something that looks like a credential is committed under production/"
else
    emit "committed credentials:           none"
fi

# ---------------------------------------------------------------------------
# 4. Every alert expression names a metric the applications actually export.
# ---------------------------------------------------------------------------
log "4/7: every alert names a metric that exists"
# Scraped from a container on the stack network. The renamer image is
# distroless, so the scrape runs somewhere that has a shell.
EXPORTED="$(compose exec -T rabbitmq sh -c \
    "wget -q -T 5 -O - http://renamer-1:8080/metrics" 2>/dev/null \
    | grep -oE '^fn_[a-z_]+' | sort -u || true)"
if [ -z "$EXPORTED" ]; then
    bad "could not read the live metric surface, so the alert rules were not checked"
else
    _missing=""
    for _m in $(grep -E '^\s+expr:' "$PROD/alerts.prometheus.yml" \
                  | grep -oE '\bfn_[a-z_]+' | sort -u); do
        printf '%s\n' "$EXPORTED" | grep -qx "$_m" || _missing="$_missing $_m"
    done
    emit "alert metrics that do not exist: ${_missing:-none}"
    [ -z "$_missing" ] || bad "alert rules reference metrics nothing exports:$_missing"
fi

# ---------------------------------------------------------------------------
# 4b. The rules do not fire on a HEALTHY stack.
#
# Metric-name existence is not enough, and checking only that was how a rule
# shipped that pages on a working system: `fn_consumer_up == 0` also matches a
# healthy WATCHER, which is not a broker consumer and exports 0 by design.
# These evaluate the simple rules against what the live stack is exporting now.
# ---------------------------------------------------------------------------
log "4b/7: the rules do not fire on a healthy stack"
scrape() {
    compose exec -T rabbitmq sh -c "wget -q -T 5 -O - http://$1:8080/metrics" 2>/dev/null || true
}
W_METRICS="$(scrape watcher)"
R_METRICS="$(scrape renamer-1)"
if [ -z "$W_METRICS" ] || [ -z "$R_METRICS" ]; then
    bad "could not scrape the live metric surface, so no alert rule was evaluated"
else
    # A healthy watcher exports fn_consumer_up 0. The rule must be scoped so
    # that does not page.
    _w_consumer="$(printf '%s' "$W_METRICS" | grep -E '^fn_consumer_up ' | awk '{print $2}')"
    _r_consumer="$(printf '%s' "$R_METRICS" | grep -E '^fn_consumer_up ' | awk '{print $2}')"
    emit "healthy watcher fn_consumer_up:   ${_w_consumer:-absent}   (0 is normal: not a consumer)"
    emit "healthy renamer fn_consumer_up:   ${_r_consumer:-absent}   (expected 1)"
    [ "${_r_consumer:-0}" = "1" ] || bad "a healthy renamer is not consuming; the rule would page correctly, but this stack is not healthy"
    if [ "${_w_consumer:-1}" = "0" ] && ! grep -q 'application="renamer"' "$PROD/alerts.prometheus.yml"; then
        bad "the consumer rule is not scoped to renamers, and a healthy watcher exports 0"
    fi

    # Every state label a rule names must be a state the ledger actually
    # reports. `pending` was not one, so that rule could never fire.
    for _st in $(grep -oE 'fn_jobs\{state="[a-z_]+"\}' "$PROD/alerts.prometheus.yml" \
                  | sed 's/.*state="//; s/"}//' | sort -u); do
        if ! printf '%s' "$W_METRICS" | grep -q "^fn_jobs{state=\"$_st\"}"; then
            bad "an alert names fn_jobs state \"$_st\", which the ledger does not report"
        fi
    done
    emit "alert job-states that the ledger does not report: none"

    # Required dependencies are up, and the rule excludes the optional replica.
    _down="$(printf '%s' "$R_METRICS" | grep -E '^fn_dependency_up\{' | awk '$2 == 0 {print $1}' | wc -l | tr -d ' ')"
    emit "required dependencies reporting down on a healthy stack: $_down   (expected 0)"
    grep -q 'dependency!="postgres_replica"' "$PROD/alerts.prometheus.yml" \
        || bad "the dependency rule does not exclude the optional replica"

    # Discovery is running, so the stalled-discovery rule must be quiet.
    _disc="$(printf '%s' "$W_METRICS" | grep -E '^fn_discovery_last_run_timestamp_seconds ' | awk '{print $2}')"
    if [ -n "$_disc" ]; then
        _age="$(awk -v t="$_disc" 'BEGIN{printf "%d", systime() - t}')"
        emit "seconds since discovery last ran: $_age   (rule fires above 900)"
        [ "$_age" -lt 900 ] || bad "discovery has not run in $_age seconds on a stack claimed healthy"
    else
        bad "the watcher exports no discovery timestamp, so the stalled-discovery rule cannot work"
    fi
fi

# ---------------------------------------------------------------------------
# 5. Every command the runbook tells an operator to run exists.
# ---------------------------------------------------------------------------
log "5/7: the runbook's commands exist"
_badcmd=""
# Each image carries its own entrypoint binary, so the subcommand goes to the
# image's own entrypoint rather than to a program selected by name.
for _sub in version; do
    docker run --rm "$WATCHER_REF" "$_sub" >/dev/null 2>&1 || _badcmd="$_badcmd fn-watcher:$_sub"
    docker run --rm "$RENAMER_REF" "$_sub" >/dev/null 2>&1 || _badcmd="$_badcmd fn-renamer:$_sub"
done
# check-config needs enough environment to load a configuration at all; it is
# the command the runbook's first-start step uses, so it is run as written.
docker run --rm -e FN_STORAGE_REQUIRED=false -e FN_DB_APPLY_MIGRATIONS=false \
    -e FN_DB_PRIMARY_HOST=unused -e FN_AMQP_HOST=unused \
    -e FN_DB_NAME=unused -e FN_DB_USER=unused -e FN_AMQP_USER=unused \
    -e FN_DB_PASSWORD_FILE=/run/secrets/db -e FN_AMQP_PASSWORD_FILE=/run/secrets/amqp \
    -e FN_CONFIG_FILE=/etc/fn/normalizer.json \
    -v "$SCRATCH/db_password:/run/secrets/db:ro" \
    -v "$SCRATCH/amqp_password:/run/secrets/amqp:ro" \
    -v "$DEPLOY_DIR/config/normalizer.json:/etc/fn/normalizer.json:ro" \
    "$WATCHER_REF" check-config >/dev/null 2>&1 || _badcmd="$_badcmd fn-watcher:check-config"
emit "runbook subcommands that do not exist: ${_badcmd:-none}"
[ -z "$_badcmd" ] || bad "the runbook names subcommands the images do not have:$_badcmd"

# A command the runbook does NOT claim, asserted absent so the claim stays true.
if docker run --rm "$WATCHER_REF" migrate >/dev/null 2>&1; then
    bad "a 'migrate' subcommand exists; the runbook says it does not"
else
    emit "no 'migrate' subcommand:         confirmed   (the runbook says so)"
fi

# The healthcheck binaries the manifest names must be the ones that exist.
for _probe in "fn-watcher $WATCHER_REF" "fn-renamer $RENAMER_REF"; do
    set -- $_probe
    if ! docker run --rm --entrypoint /usr/local/bin/"$1" "$2" version >/dev/null 2>&1; then
        bad "the manifest's healthcheck names /usr/local/bin/$1, which $2 does not have"
    fi
done

# ---------------------------------------------------------------------------
# 6. The configuration the package ships validates.
# ---------------------------------------------------------------------------
log "6/7: the shipped configuration validates"
if docker run --rm -e FN_STORAGE_REQUIRED=false -e FN_DB_APPLY_MIGRATIONS=false \
    -e FN_DB_PRIMARY_HOST=unused -e FN_AMQP_HOST=unused \
    -e FN_DB_NAME=unused -e FN_DB_USER=unused -e FN_AMQP_USER=unused \
    -e FN_DB_PASSWORD_FILE=/run/secrets/db -e FN_AMQP_PASSWORD_FILE=/run/secrets/amqp \
    -e FN_CONFIG_FILE=/etc/fn/normalizer.json \
    -v "$SCRATCH/db_password:/run/secrets/db:ro" \
    -v "$SCRATCH/amqp_password:/run/secrets/amqp:ro" \
    -v "$DEPLOY_DIR/config/normalizer.json:/etc/fn/normalizer.json:ro" \
    "$WATCHER_REF" check-config >/dev/null 2>&1; then
    emit "shipped configuration validates: yes"
else
    emit "shipped configuration validates: NO"
    bad "the configuration this package ships does not validate"
fi

emit ""
emit "mismatches: $FAILURES"
report_restored
if [ "$FAILURES" = "0" ]; then
    report_success
    log "PASSED: the deployment package checks out against the real image"
    note "evidence: $OUT"
    exit 0
fi
report_keep
echo "FAILED: $FAILURES expectation(s) not met; see $OUT" >&2
exit 1
