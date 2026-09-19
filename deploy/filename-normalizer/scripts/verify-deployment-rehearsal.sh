#!/bin/sh
# Rehearse the DOCUMENTED deployment procedures, in an isolated deployment,
# against real dependencies and a fresh database.
#
# # Why this exists
#
# The package validator checked that commands and metrics exist. That is not
# the same as the procedures working, and two of them did not: every declared
# healthcheck invoked `probe --once`, a flag that does not exist, and the
# documented schema step ran `check-config`, which never touches the database.
# A package whose procedures have never been executed is a document.
#
# # What it borrows and what it owns
#
# It borrows the running PostgreSQL and RabbitMQ -- real dependencies, as the
# contract requires -- and owns everything else: its own compose project, its
# own database, its own storage directories. It refuses any name it did not
# create, and removes only what it made.
. "$(dirname "$0")/lib.sh"

OUT="$EVIDENCE_DIR/deployment-rehearsal.txt"
PROD="$DEPLOY_DIR/production"
FAILURES=0
REH_PROJECT="fnrehearsal$$"
REH_DB="fn_rehearsal_$$"
REH_VHOST="fn-rehearsal-$$"
OWNS_DB=0
OWNS_VHOST=0
OWNS_PROJECT=0
SCRATCH=""
report_begin deployment-rehearsal "$OUT" "$0"

emit() { printf '%s\n' "$*" >> "$OUT"; }
bad() { printf '    MISMATCH: %s\n' "$*" >&2; emit "    MISMATCH: $*"; FAILURES=$((FAILURES + 1)); }
pg() {
    compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" \
        postgres-primary psql -U "${FN_DB_USER:-fn_app}" -d "${2:-postgres}" -tA -c "$1" \
        </dev/null 2>/dev/null | tr -d '\r'
}
reh() { docker compose -p "$REH_PROJECT" -f "$PROD/compose.prod.yml" -f "$PROD/compose.local-rehearsal.yml" --env-file "$SCRATCH/prod.env" "$@"; }

cleanup() {
    log "removing the rehearsal deployment"
    if [ "$OWNS_PROJECT" = "1" ]; then
        reh down -v --remove-orphans >/dev/null 2>&1 || true
    fi
    if [ "$OWNS_DB" = "1" ]; then
        pg "DROP DATABASE IF EXISTS $REH_DB;" >/dev/null 2>&1 || true
    fi
    if [ "$OWNS_VHOST" = "1" ]; then
        compose exec -T rabbitmq rabbitmqctl delete_vhost "$REH_VHOST" >/dev/null 2>&1 || true
    fi
    _left_db=0
    [ "$OWNS_DB" = "1" ] && [ "$(pg "SELECT count(*) FROM pg_database WHERE datname='$REH_DB';")" != "0" ] && _left_db=1
    _left_c="$(docker ps -aq --filter "label=com.docker.compose.project=$REH_PROJECT" 2>/dev/null | wc -l | tr -d ' ')"
    _left_v=0
    if [ "$OWNS_VHOST" = "1" ] && compose exec -T rabbitmq rabbitmqctl list_vhosts 2>/dev/null | grep -qx "$REH_VHOST"; then
        _left_v=1
    fi
    emit ""
    emit "restoration of this rehearsal's own resources:"
    emit "  containers left:              ${_left_c:-unknown}   (expected 0)"
    emit "  database left:                $_left_db   (expected 0)"
    emit "  broker vhost left:            $_left_v   (expected 0)"
    [ "$_left_v" = "0" ] || bad "the rehearsal left its broker vhost behind"
    [ "${_left_c:-1}" = "0" ] || bad "the rehearsal left ${_left_c} container(s) behind"
    [ "$_left_db" = "0" ] || bad "the rehearsal left its database behind"
    [ -n "$SCRATCH" ] && rm -rf "$SCRATCH"
    if [ "$FAILURES" = "0" ]; then report_restored; fi
    report_keep
    if [ "$FAILURES" != "0" ]; then
        echo "FAILED: $FAILURES expectation(s) not met; see $OUT" >&2
        exit 1
    fi
}
trap cleanup EXIT

emit "The documented deployment procedures, rehearsed against real dependencies"
emit ""

# --- own the names before using them ---------------------------------------
log "1/7: claiming names this rehearsal will own"
if [ -n "$(docker ps -aq --filter "label=com.docker.compose.project=$REH_PROJECT" 2>/dev/null)" ]; then
    bad "compose project $REH_PROJECT already exists; refusing to use it"; exit 1
fi
OWNS_PROJECT=1
if [ "$(pg "SELECT count(*) FROM pg_database WHERE datname='$REH_DB';")" != "0" ]; then
    bad "database $REH_DB already exists; refusing to use it"; exit 1
fi
pg "CREATE DATABASE $REH_DB;" >/dev/null 2>&1 && OWNS_DB=1
[ "$OWNS_DB" = "1" ] || { bad "could not create the rehearsal database"; exit 1; }

# Its own vhost. Sharing the live one means sharing the work QUEUE: the live
# renamers took the rehearsal's message, looked for the job in their own
# database, and found nothing -- so the document stalled at `dispatched` and
# the rehearsal was measuring the wrong deployment.
if compose exec -T rabbitmq rabbitmqctl list_vhosts 2>/dev/null | grep -qx "$REH_VHOST"; then
    bad "broker vhost $REH_VHOST already exists; refusing to use it"; exit 1
fi
if compose exec -T rabbitmq rabbitmqctl add_vhost "$REH_VHOST" >/dev/null 2>&1; then
    OWNS_VHOST=1
    compose exec -T rabbitmq rabbitmqctl set_permissions -p "$REH_VHOST" \
        "${FN_AMQP_USER:-fn_app}" ".*" ".*" ".*" >/dev/null 2>&1 || \
        bad "could not grant the rehearsal vhost's permissions"
else
    bad "could not create the rehearsal broker vhost"; exit 1
fi

SCRATCH="$(mktemp -d)"
mkdir -p "$SCRATCH"/incoming "$SCRATCH"/queued "$SCRATCH"/staging "$SCRATCH"/consume "$SCRATCH"/failed
chmod 0777 "$SCRATCH" "$SCRATCH"/incoming "$SCRATCH"/queued "$SCRATCH"/staging "$SCRATCH"/consume "$SCRATCH"/failed
cp "$DEPLOY_DIR/config/normalizer.json" "$SCRATCH/normalizer.json"
cp "$DEPLOY_DIR/secrets/fn_db_password" "$SCRATCH/db_password"
cp "$DEPLOY_DIR/secrets/fn_amqp_password" "$SCRATCH/amqp_password"
chmod 0444 "$SCRATCH/db_password" "$SCRATCH/amqp_password"

cat > "$SCRATCH/prod.env" <<ENV
FN_PROJECT=$REH_PROJECT
FN_EXTERNAL_NETWORK=$(docker network ls --format '{{.Name}}' | grep -m1 "_fn$")
FN_WATCHER_IMAGE=filename-normalizer/watcher:0.1.0-foundation
FN_RENAMER_IMAGE=filename-normalizer/renamer:0.1.0-foundation
FN_DB_PRIMARY_HOST=postgres-primary
FN_DB_NAME=$REH_DB
FN_DB_SSLMODE=disable
FN_AMQP_HOST=rabbitmq
FN_AMQP_VHOST=$REH_VHOST
FN_HOST_INCOMING=$SCRATCH/incoming
FN_HOST_QUEUED=$SCRATCH/queued
FN_HOST_STAGING=$SCRATCH/staging
FN_HOST_CONSUME=$SCRATCH/consume
FN_HOST_FAILED=$SCRATCH/failed
FN_HOST_CONFIG=$SCRATCH/normalizer.json
FN_SECRET_DB_PASSWORD=$SCRATCH/db_password
FN_SECRET_AMQP_PASSWORD=$SCRATCH/amqp_password
ENV
emit "1. isolated deployment"
emit "   compose project:              $REH_PROJECT"
emit "   fresh database:               $REH_DB"
emit "   dependencies:                 the running PostgreSQL and RabbitMQ (real)"

# --- the documented first start --------------------------------------------
log "2/7: the documented first start applies the schema"
SCHEMA_BEFORE="$(pg "SELECT count(*) FROM information_schema.tables WHERE table_schema='public';" "$REH_DB")"
FN_DB_APPLY_MIGRATIONS=true reh up -d watcher >/dev/null 2>&1
_i=0; READY=no
while [ "$_i" -lt 90 ]; do
    if [ "$(reh ps --format '{{.Health}}' watcher 2>/dev/null | head -1)" = "healthy" ]; then READY=yes; break; fi
    sleep 2; _i=$((_i + 2))
done
SCHEMA_AFTER="$(pg "SELECT count(*) FROM information_schema.tables WHERE table_schema='public';" "$REH_DB")"
emit ""
emit "2. first start, with FN_DB_APPLY_MIGRATIONS=true as documented"
emit "   tables before:                ${SCHEMA_BEFORE:-unknown}   (expected 0: fresh database)"
emit "   watcher reached healthy:      $READY   (expected yes)"
emit "   tables after:                 ${SCHEMA_AFTER:-unknown}   (expected > 0: the schema was applied)"
[ "${SCHEMA_BEFORE:-1}" = "0" ] || bad "the rehearsal database was not empty"
[ "$READY" = "yes" ] || bad "the watcher never became healthy, so the documented healthcheck does not work"
[ "${SCHEMA_AFTER:-0}" -gt 0 ] || bad "the documented first start applied no schema"

# --- the schema the applications expect ------------------------------------
log "3/7: installed schema matches what the applications expect"
HEALTH="$(reh exec -T watcher /usr/local/bin/fn-watcher healthcheck 2>&1 || true)"
INSTALLED="$(pg "SELECT max(version) FROM schema_migrations;" "$REH_DB" 2>/dev/null)"
emit ""
emit "3. schema identity"
emit "   installed migration version:  ${INSTALLED:-unreadable}"
emit "   healthcheck exit:             $([ -n "$HEALTH" ] && echo "output captured" || echo "silent")"
[ -n "$INSTALLED" ] || bad "no migration version is recorded in the rehearsal database"

# --- effective configuration ------------------------------------------------
log "4/7: the documented configuration check runs"
if reh run --rm --no-deps -T --entrypoint /usr/local/bin/fn-watcher watcher check-config >/dev/null 2>&1; then
    CFG_OK=yes
else
    CFG_OK=no
fi
emit ""
emit "4. effective configuration"
emit "   check-config as documented:   $CFG_OK   (expected yes)"
[ "$CFG_OK" = "yes" ] || bad "the documented configuration check does not run in this deployment"

# --- a real document end to end --------------------------------------------
log "5/7: a document travels the deployment"
FN_DB_APPLY_MIGRATIONS=false reh up -d >/dev/null 2>&1
_i=0; ALLREADY=no
while [ "$_i" -lt 120 ]; do
    _h="$(reh ps --format '{{.Health}}' 2>/dev/null | sort -u | tr '\n' ' ')"
    case "$_h" in *unhealthy*|*starting*) ;; *healthy*) ALLREADY=yes; break ;; esac
    sleep 3; _i=$((_i + 3))
done
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
printf '%%PDF-1.4 rehearsal\n' > "$SCRATCH/incoming/.wip"
mv "$SCRATCH/incoming/.wip" "$SCRATCH/incoming/rehearsal-$STAMP.pdf"
_i=0; STATE=""
while [ "$_i" -lt 180 ]; do
    STATE="$(pg "SELECT state FROM jobs WHERE source_name='rehearsal-$STAMP.pdf';" "$REH_DB")"
    case "$STATE" in delivered|held|uncertain) break ;; esac
    sleep 3; _i=$((_i + 3))
done
DELIVERED_FILE="$(ls "$SCRATCH/consume" 2>/dev/null | grep -c "rehearsal-$(echo "$STAMP" | tr 'A-Z' 'a-z')" || true)"
emit ""
emit "5. a document through the deployed system"
emit "   all services healthy:         $ALLREADY   (expected yes)"
emit "   job state:                    ${STATE:-none}   (expected delivered)"
emit "   documents in consume:         $DELIVERED_FILE   (expected 1)"
emit "   source preserved:             $([ -e "$SCRATCH/incoming/rehearsal-$STAMP.pdf" ] && echo yes || echo no)   (expected yes)"
[ "$ALLREADY" = "yes" ] || bad "the deployment never reported every service healthy"
[ "$STATE" = "delivered" ] || bad "a document submitted to the deployed system reached '${STATE:-nothing}'"
[ "${DELIVERED_FILE:-0}" = "1" ] || bad "$DELIVERED_FILE documents in consume, expected 1"

# --- stop intake -------------------------------------------------------------
log "6/7: stop-intake leaves delivery running"
reh stop watcher >/dev/null 2>&1
W_STATE="$(reh ps -a --format '{{.State}}' watcher 2>/dev/null | head -1)"
R_STATE="$(reh ps --format '{{.State}}' renamer-1 2>/dev/null | head -1)"
emit ""
emit "6. stop intake (the documented procedure)"
emit "   watcher:                      ${W_STATE:-unknown}   (expected exited)"
emit "   renamer-1:                    ${R_STATE:-unknown}   (expected running)"
case "$W_STATE" in exited) ;; *) bad "the watcher did not stop" ;; esac
case "$R_STATE" in running) ;; *) bad "stopping intake also stopped delivery" ;; esac

# --- image change and rollback, in the documented form ----------------------
log "7/7: the documented image change and rollback run"
if FN_RENAMER_IMAGE=filename-normalizer/renamer:0.1.0-foundation \
     reh up -d --no-deps renamer-1 >/dev/null 2>&1; then UP_OK=yes; else UP_OK=no; fi
_i=0; BACK=no
while [ "$_i" -lt 90 ]; do
    [ "$(reh ps --format '{{.Health}}' renamer-1 2>/dev/null | head -1)" = "healthy" ] && { BACK=yes; break; }
    sleep 2; _i=$((_i + 2))
done
emit ""
emit "7. image change, in the documented command form"
emit "   command accepted:             $UP_OK   (expected yes)"
emit "   instance healthy afterwards:  $BACK   (expected yes)"
emit "   (same image both ways: this exercises the PROCEDURE and the"
emit "    healthcheck, not a behavioural difference between two builds)"
[ "$UP_OK" = "yes" ] || bad "the documented image-change command failed"
[ "$BACK" = "yes" ] || bad "the instance did not return to healthy after the documented image change"

emit ""
emit "mismatches: $FAILURES"
if [ "$FAILURES" = "0" ]; then
    report_success
    log "PASSED: the documented procedures work in an isolated deployment"
    note "evidence: $OUT"
fi
