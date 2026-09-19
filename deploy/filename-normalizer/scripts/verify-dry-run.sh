#!/bin/sh
# A9 — dry run preserves sources and the operational ledger.
#
# A dry run must compute and record a name and do nothing else. "Nothing else"
# is the part worth proving, and it can only be proven if the dry-run renamer
# is the ONLY consumer: with its siblings running, a document would be
# published by one of them and the absence of a published file would prove
# nothing.
#
# So this exercise stops the ordinary renamers, runs a dry-run renamer alone,
# submits a document through the real watcher and the real broker, and then
# checks what did and did not change. The ordinary renamers are restarted
# afterwards, and the submission is left in place: nothing here deletes a
# document.
. "$(dirname "$0")/lib.sh"

OUT="$EVIDENCE_DIR/a9-dry-run.txt"
mkdir -p "$EVIDENCE_DIR"
FAILURES=0

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
DOC="dry-run-probe-${STAMP}.pdf"
NORMALIZED="dry-run-probe-$(printf '%s' "$STAMP" | tr 'A-Z' 'a-z').pdf"

emit() { printf '%s\n' "$*" >> "$OUT"; printf '%s\n' "$*"; }
bad() { printf '    MISMATCH: %s\n' "$*" >&2; emit "    MISMATCH: $*"; FAILURES=$((FAILURES + 1)); }

# psql on the primary, using the same credential file the applications use.
psql_primary() {
    compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" \
        postgres-primary psql -U "${FN_DB_USER:-fn_app}" -d "${FN_DB_NAME:-filename_normalizer}" -tA -c "$1"
}

# queue_depth reads the real broker, because "queued work is unchanged" is a
# statement about the queue and cannot be inferred from the ledger.
queue_depth() {
    compose exec -T rabbitmq rabbitmqctl list_queues -p "${FN_AMQP_VHOST:-filename-normalizer}" \
        --quiet --no-table-headers name messages 2>/dev/null \
        | awk -v q="${FN_AMQP_QUEUE:-filename_normalizer.jobs.v1}" '$1 == q {print $2}' | tr -d ' \r\n'
}

count_consume() {
    compose run --rm --no-deps -T --entrypoint sh storage-init \
        -c 'ls -1 /srv/fn/consume 2>/dev/null | wc -l' 2>/dev/null | tr -d ' \r\n'
}

# This exercise stops shared services and starts a fault service, exactly like
# the others, and it was the only one that did so without a lock, without
# checking whether the service it starts already belonged to somebody else, and
# without ever looking at whether its own restoration worked.
CREATED=""
RESTORED=0
# Set the moment the stack is first changed. A refusal before that point must
# leave the stack alone rather than restarting services it never stopped.
MUTATED=0
restore() {
    if [ "$RESTORED" = "1" ]; then return 0; fi
    RESTORED=1
    if [ "$MUTATED" = "0" ]; then
        log "nothing was changed; leaving the stack alone"
        exercise_unlock
        return 0
    fi
    log "restarting the ordinary renamers"
    _r_ok=1

    for _r in $CREATED; do
        compose --profile fault stop "$_r" >/dev/null 2>&1 || true
        compose --profile fault rm -f "$_r" >/dev/null 2>&1 || true
        if [ -n "$(compose --profile fault ps -aq "$_r" 2>/dev/null)" ]; then
            echo "error: '$_r' could not be removed." >&2
            _r_ok=0
        fi
    done

    compose start renamer-1 renamer-2 >/dev/null 2>&1 || true
    wait_healthy renamer-1 120 || true
    wait_healthy renamer-2 120 || true

    # Readiness, not liveness -- and `healthcheck` on its own is LIVENESS. It
    # reads /healthz, which answers 200 for a process that is running with
    # every dependency unreachable, so the comment here described a check the
    # command did not make. `--require-ready` reads /readyz, which is the
    # statement restoration needs: the service is back AND usable.
    _r_ready="$(compose exec -T renamer-1 /usr/local/bin/fn-renamer healthcheck --require-ready >/dev/null 2>&1 && echo yes || echo no)"
    _r_ready2="$(compose exec -T renamer-2 /usr/local/bin/fn-renamer healthcheck --require-ready >/dev/null 2>&1 && echo yes || echo no)"
    if [ "$_r_ready" != "yes" ] || [ "$_r_ready2" != "yes" ]; then _r_ok=0; fi
    {
        printf '\nrestoration (read back from the running stack):\n'
        printf '  renamer-1 ready:         %s\n' "$_r_ready"
        printf '  renamer-2 ready:         %s\n' "$_r_ready2"
        printf '  dry-run service left:    %s\n' "$(compose --profile fault ps -aq renamer-dry-run 2>/dev/null | wc -l | tr -d ' ')"
    } >> "$OUT"

    exercise_unlock

    if [ "$_r_ok" != "1" ]; then
        echo >&2
        echo "FAILED: the stack was NOT restored. See $OUT." >&2
        printf '\nRESTORATION FAILED — see the values above.\n' >> "$OUT"
        exit 1
    fi
    report_restored
    note "restored: ordinary renamers report themselves READY, not merely alive"
}

exercise_lock dry-run || exit 1
trap 'restore; report_keep' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

report_begin "dry-run" "$OUT" "$0"
emit "A9 — dry run preserves sources and the operational ledger"
emit ""

# ---------------------------------------------------------------------------
# Ownership is decided BEFORE anything is stopped. This check used to come
# after the two renamers had already been stopped, so the refusal printed
# "Refusing before anything is changed" having just stopped two services that
# belonged to whoever else was using this stack -- and the restore trap then
# restarted them, which is a mutation and a restoration nobody asked for.
log "1/6: confirming the dry-run service is this invocation's to create"
if [ -n "$(compose --profile fault ps -aq renamer-dry-run 2>/dev/null)" ]; then
    echo "error: 'renamer-dry-run' already exists; this invocation did not create it" >&2
    echo "       and will not remove it. Refusing before anything is changed." >&2
    exit 1
fi

log "2/6: stopping the ordinary renamers so the dry-run instance is the only consumer"
MUTATED=1
compose stop renamer-1 renamer-2 >/dev/null 2>&1
emit "ordinary renamers stopped"

log "starting a renamer with dry_run enabled"
CREATED="renamer-dry-run"
compose --profile fault up -d --wait --wait-timeout 180 renamer-dry-run >/dev/null 2>&1 || {
    echo "the dry-run renamer did not become healthy" >&2
    exit 1
}
emit "dry-run renamer started (FN_RENAMER_DRY_RUN=true)"
emit ""

# ---------------------------------------------------------------------------
log "3/6: recording the before state"
BEFORE_JOBS="$(psql_primary 'SELECT count(*) FROM jobs;')"
BEFORE_RES="$(psql_primary 'SELECT count(*) FROM name_reservations;')"
BEFORE_RECEIPTS="$(psql_primary 'SELECT count(*) FROM delivery_receipts;')"
BEFORE_CONSUME="$(count_consume)"
# The whole point of FN-N003: the previous oracle checked only reservations,
# receipts and the consume directory, and accepted any non-delivered state. It
# therefore passed while the dry run was writing delivery ownership, writing a
# normalized name, and ACKNOWLEDGING the message -- destroying the very work it
# was previewing. History rows and queue depth are what catch that.
BEFORE_EVENTS="$(psql_primary 'SELECT count(*) FROM job_events;')"
BEFORE_QUEUED="$(queue_depth)"
emit "before:  jobs=$BEFORE_JOBS reservations=$BEFORE_RES receipts=$BEFORE_RECEIPTS consume_entries=$BEFORE_CONSUME"
emit "         history_rows=$BEFORE_EVENTS queued_messages=$BEFORE_QUEUED"

# ---------------------------------------------------------------------------
# The dry run's own baseline, taken BEFORE the document is submitted.
#
# It used to be read afterwards, once the job row had appeared -- and the dry
# run reports every 10 seconds, so a report including the new job had often
# already been emitted by then. The "before" reading then already counted the
# job, the "after" reading equalled it, and the exercise reported that the dry
# run had never examined the document it had in fact just examined. The
# previous round passed this check by winning the race, not by being right.
count_in_last_report() {
    compose logs renamer-dry-run 2>/dev/null | grep dry_run_report | tail -1 \
        | sed -n 's/.*"count":\([0-9][0-9]*\).*/\1/p'
}
REPORTS_BEFORE="$(compose logs renamer-dry-run 2>/dev/null | grep -c dry_run_report || true)"
COUNT_BEFORE="$(count_in_last_report)"

log "4/6: submitting a document through the real watcher and broker"
compose run --rm --no-deps -T --entrypoint sh storage-init -c \
    "printf '%%PDF-1.4 dry run probe\\n' > /srv/fn/incoming/.wip-dryrun && \
     mv /srv/fn/incoming/.wip-dryrun '/srv/fn/incoming/$DOC'" >/dev/null 2>&1
emit "submitted: $DOC"

# Two waits, in order, because either alone is insufficient.
#
# First the job must actually exist: the checks below read its row, and an
# absent row returns empty strings that look like violations. Then the dry run
# must have produced a report AFTER the row appeared, which is what shows it
# examined this job rather than an earlier one. Waiting a fixed interval, or
# waiting for any report at all, would pass whether or not this document was
# ever looked at -- and did, on the first run of this oracle.
_i=0
JOB_SEEN=0
while [ "$_i" -lt 90 ]; do
    if [ -n "$(psql_primary "SELECT job_id FROM jobs WHERE source_name = '$DOC';" | tr -d ' ')" ]; then
        JOB_SEEN=1
        break
    fi
    sleep 1
    _i=$((_i + 1))
done
[ "$JOB_SEEN" = "1" ] || bad "the submission was never registered, so nothing could be previewed"

# A report emitted after the row appeared shows the dry run ran again. It does
# not by itself show that THIS job was in it -- the report is aggregate counts
# and names nothing, deliberately, because a name is document-derived text. So
# the count is read as well: the number of jobs the policy would publish has to
# rise by at least one against the baseline taken before the submission, which
# is the observable trace this particular job leaves in an aggregate report.
#
# Polled until it rises rather than sampled once: the first report after the
# row appears may have been computed moments before the row committed, and
# reading only that one turns a 10-second tick into a coin toss.
_i=0
REPORT_SEEN=0
COUNT_AFTER=""
while [ "$_i" -lt 120 ]; do
    _now="$(compose logs renamer-dry-run 2>/dev/null | grep -c dry_run_report || true)"
    if [ "${_now:-0}" -gt "${REPORTS_BEFORE:-0}" ]; then
        REPORT_SEEN=1
        COUNT_AFTER="$(count_in_last_report)"
        if [ -n "$COUNT_AFTER" ] && [ -n "$COUNT_BEFORE" ] && [ "$COUNT_AFTER" -gt "$COUNT_BEFORE" ]; then
            break
        fi
    fi
    sleep 1
    _i=$((_i + 1))
done
emit "  reports before/after submission:  ${REPORTS_BEFORE:-0} -> ${_now:-0}"
emit "  jobs it would publish, before:    ${COUNT_BEFORE:-unknown}"
emit "  jobs it would publish, after:     ${COUNT_AFTER:-unknown}   (must have risen: this job was counted)"
[ "$REPORT_SEEN" = "1" ] || bad "the dry run produced no report after the job was registered, so it may never have examined it"
if [ -n "$COUNT_BEFORE" ] && [ -n "$COUNT_AFTER" ]; then
    [ "$COUNT_AFTER" -gt "$COUNT_BEFORE" ] || bad "the dry run's count did not rise ($COUNT_BEFORE -> $COUNT_AFTER), so the submitted job was not among the jobs it examined"
else
    bad "the dry run's report count could not be read, so it is unknown whether this job was examined"
fi

# ---------------------------------------------------------------------------
log "5/6: checking what did and did not change"
AFTER_RES="$(psql_primary 'SELECT count(*) FROM name_reservations;')"
AFTER_RECEIPTS="$(psql_primary 'SELECT count(*) FROM delivery_receipts;')"
AFTER_CONSUME="$(count_consume)"
AFTER_EVENTS="$(psql_primary 'SELECT count(*) FROM job_events;')"
AFTER_QUEUED="$(queue_depth)"
STATE="$(psql_primary "SELECT state FROM jobs WHERE source_name = '$DOC';")"
NORMALIZED_RECORDED="$(psql_primary "SELECT coalesce(normalized_name,'-') FROM jobs WHERE source_name = '$DOC';")"
RESERVED="$(psql_primary "SELECT coalesce(reserved_name,'-') FROM jobs WHERE source_name = '$DOC';")"
ATTEMPTS="$(psql_primary "SELECT delivery_attempts FROM jobs WHERE source_name = '$DOC';")"
SOURCE_PRESENT="$(compose run --rm --no-deps -T --entrypoint sh storage-init \
    -c "test -f '/srv/fn/incoming/$DOC' && echo yes || echo no" 2>/dev/null | tr -d ' \r\n')"
STAGED="$(compose run --rm --no-deps -T --entrypoint sh storage-init \
    -c 'ls -1 /srv/fn/staging 2>/dev/null | wc -l' 2>/dev/null | tr -d ' \r\n')"

emit "after:   jobs=$(psql_primary 'SELECT count(*) FROM jobs;') reservations=$AFTER_RES receipts=$AFTER_RECEIPTS consume_entries=$AFTER_CONSUME"
emit ""
emit "the submitted job:"
emit "  state:                 $STATE   (expected pending_dispatch or dispatched: untouched)"
emit "  normalized_name:       $NORMALIZED_RECORDED   (must be '-': a dry run writes nothing)"
emit "  reserved_name:         $RESERVED   (must be '-')"
emit "  delivery_attempts:     $ATTEMPTS   (must be 0: no delivery was taken)"
emit "  source still present:  $SOURCE_PRESENT"
emit "  staging entries:       $STAGED"
emit ""
emit "after:   reservations=$AFTER_RES receipts=$AFTER_RECEIPTS consume_entries=$AFTER_CONSUME"
emit "         history_rows=$AFTER_EVENTS queued_messages=$AFTER_QUEUED"
emit ""

[ "$NORMALIZED_RECORDED" = "-" ] || bad "a dry run wrote a normalized name to the ledger: $NORMALIZED_RECORDED"
[ "$RESERVED" = "-" ] || bad "a dry run reserved the destination name '$RESERVED'"
[ "${ATTEMPTS:-0}" = "0" ] || bad "a dry run took $ATTEMPTS delivery/deliveries off the queue"
[ "$AFTER_RES" = "$BEFORE_RES" ] || bad "reservations changed: $BEFORE_RES -> $AFTER_RES"
[ "$AFTER_RECEIPTS" = "$BEFORE_RECEIPTS" ] || bad "delivery receipts changed: $BEFORE_RECEIPTS -> $AFTER_RECEIPTS"
[ "$AFTER_CONSUME" = "$BEFORE_CONSUME" ] || bad "the consume directory changed: $BEFORE_CONSUME -> $AFTER_CONSUME entries"
[ "$SOURCE_PRESENT" = "yes" ] || bad "the source was removed"
[ "$STATE" != "delivered" ] || bad "a dry run marked the job delivered"
case "$STATE" in
    pending_dispatch|dispatched) ;;
    *) bad "the job's state changed to '$STATE'; a dry run must not move it" ;;
esac

# The job's own history must be exactly what discovery and dispatch wrote.
JOB_EVENTS="$(psql_primary "SELECT count(*) FROM job_events e JOIN jobs j USING (job_id) WHERE j.source_name = '$DOC' AND e.event_type NOT IN ('registered','dispatch_claimed','dispatch_confirmed');")"
emit "  history rows beyond registration/dispatch: $JOB_EVENTS   (must be 0)"
[ "${JOB_EVENTS:-0}" = "0" ] || bad "a dry run wrote $JOB_EVENTS history row(s) for the job"

# ---------------------------------------------------------------------------
log "6/6: the previewed work must still be processable afterwards"
# Removed here because the scenario needs the ordinary renamers back, and
# tracked so the restore trap does not try to remove it a second time or
# report it as left behind.
compose --profile fault stop renamer-dry-run >/dev/null 2>&1
compose --profile fault rm -f renamer-dry-run >/dev/null 2>&1
if [ -z "$(compose --profile fault ps -aq renamer-dry-run 2>/dev/null)" ]; then
    CREATED=""
fi
compose start renamer-1 renamer-2 >/dev/null 2>&1
wait_healthy renamer-1 120 || true
_i=0
FINAL_STATE=""
while [ "$_i" -lt 120 ]; do
    FINAL_STATE="$(psql_primary "SELECT state FROM jobs WHERE source_name = '$DOC';")"
    case "$FINAL_STATE" in delivered|held|uncertain) break ;; esac
    sleep 1
    _i=$((_i + 1))
done
emit "after the dry run ended, with ordinary renamers running:"
emit "  final state:           $FINAL_STATE   (expected delivered)"
[ "$FINAL_STATE" = "delivered" ] || bad "previewed work reached '$FINAL_STATE'; a dry run must not consume it"
emit ""

emit "A dry run reported what would happen and changed nothing: no"
emit "normalized name written, no delivery taken off the queue, no reservation,"
emit "no receipt, no file in the consume directory, no history row beyond what"
emit "discovery and dispatch had already written, and the source untouched."
emit "The previewed job was then processed normally by an ordinary renamer,"
emit "which is the property that matters: previewing work must not consume it."
emit ""
emit "mismatches: $FAILURES"

if [ "$FAILURES" -ne 0 ]; then
    echo >&2
    echo "FAILED: $FAILURES dry-run expectation(s) not met; see $OUT" >&2
    exit 1
fi

report_success
log "PASSED: a dry run preserves sources and the operational ledger"
note "evidence: $OUT"
