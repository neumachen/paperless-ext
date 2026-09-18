#!/bin/sh
# FN-N010 — the acceptance work that needs a real interruption, a real stale
# worker, real filesystem boundaries and real write failures.
#
# Each scenario stops the ordinary renamers first, so the instance under test
# is the only consumer. Without that, one of its siblings would quietly do the
# work and the scenario would pass while proving nothing.
#
# Everything here uses the real watcher, the real broker, the real cluster and
# real filesystems. The only synthetic elements are the TIMING of the crash in
# scenario 1 and the length of the publication hold in scenario 3, both of
# which the application provides through injected fault points: the window
# between claiming a publication and committing its receipt is microseconds
# wide and cannot be hit from outside the process.
#
# # What this exercise may not do to the stack
#
# It stops services, creates services, and cuts a container off the network.
# Each of those is a mutation somebody else may be relying on, so:
#
#   * it takes an exclusive lock before touching anything, and a second
#     invocation is refused rather than interleaved;
#   * it refuses to start -- and never removes -- a fault service that already
#     existed, because that service is not its to destroy;
#   * it restores on EVERY exit path including an interruption, and the
#     restoration is verified against the RUNNING stack. A failed restoration
#     fails this target: leaving a renamer off the network while reporting
#     success is worse than the failure it was testing.
. "$(dirname "$0")/lib.sh"

OUT="$EVIDENCE_DIR/a6a7a10a11-recovery.txt"
mkdir -p "$EVIDENCE_DIR"
FAILURES=0
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
LOWER="$(printf '%s' "$STAMP" | tr 'A-Z' 'a-z')"
NET="${PROJECT}_fn"

# Fault services this invocation created, and therefore may remove. Anything
# that was already there belongs to someone else and is left alone.
CREATED=""
# Containers this invocation disconnected from the network, and must reconnect.
DISCONNECTED=""
WATCHER_OVERRIDDEN=0

emit() { printf '%s\n' "$*" >> "$OUT"; printf '%s\n' "$*"; }
bad() { printf '    MISMATCH: %s\n' "$*" >&2; emit "    MISMATCH: $*"; FAILURES=$((FAILURES + 1)); }

psqlq() {
    compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" \
        postgres-primary psql -U "${FN_DB_USER:-fn_app}" -d "${FN_DB_NAME:-filename_normalizer}" -tA -c "$1" \
        2>/dev/null | tr -d ' \r'
}

# submit writes a document to a temporary name and renames it into place.
submit() {
    _s_name="$1"; _s_bytes="${2:-64}"; _s_fill="${3:-x}"
    compose run --rm --no-deps -T --entrypoint sh storage-init -c \
        "head -c $_s_bytes /dev/zero | tr '\\0' '$_s_fill' > /srv/fn/incoming/.wip-$LOWER && \
         mv /srv/fn/incoming/.wip-$LOWER '/srv/fn/incoming/$_s_name'" >/dev/null 2>&1
}

in_storage() {
    compose run --rm --no-deps -T --entrypoint sh storage-init -c "$1" 2>/dev/null | tr -d ' \r\n'
}

# await_state polls until a job reaches one of the given states, or times out.
# It never simply sleeps: a fixed wait would pass whether or not the delivery
# was ever handled.
await_state() {
    _aw_name="$1"; _aw_want="$2"; _aw_secs="${3:-90}"
    _aw_i=0
    while [ "$_aw_i" -lt "$_aw_secs" ]; do
        _aw_got="$(psqlq "SELECT state FROM jobs WHERE source_name = '$_aw_name';")"
        case " $_aw_want " in
            *" $_aw_got "*) printf '%s' "$_aw_got"; return 0 ;;
        esac
        sleep 1
        _aw_i=$((_aw_i + 1))
    done
    printf '%s' "${_aw_got:-absent}"
    return 1
}

await_registered() {
    _ar_i=0
    while [ "$_ar_i" -lt "${2:-60}" ]; do
        if [ -n "$(psqlq "SELECT job_id FROM jobs WHERE source_name = '$1';")" ]; then return 0; fi
        sleep 1; _ar_i=$((_ar_i + 1))
    done
    return 1
}

stop_ordinary() { compose stop renamer-1 renamer-2 >/dev/null 2>&1; }

# start_fault creates a fault-profile service, refusing one that already
# exists. Removing a container this run did not create would destroy another
# exercise's subject mid-assertion.
start_fault() {
    _sf_svc="$1"; shift
    if [ -n "$(compose --profile fault ps -aq "$_sf_svc" 2>/dev/null)" ]; then
        echo "error: service '$_sf_svc' already exists; this invocation did not create it" >&2
        echo "       and will not remove it. Refusing before anything is changed." >&2
        return 1
    fi
    CREATED="$CREATED $_sf_svc"
    # The variables are exported in a SUBSHELL rather than passed through
    # `env`: `compose` is a shell function, and env can only exec a binary, so
    # `env VAR=v compose ...` failed to find a command and took the whole
    # scenario down without saying why.
    (
        for _sf_kv in "$@"; do
            export "$_sf_kv"
        done
        compose --profile fault up -d "$_sf_svc" >/dev/null 2>&1
    ) || {
        echo "error: could not start '$_sf_svc'" >&2
        compose --profile fault logs --tail 20 "$_sf_svc" >&2 2>/dev/null || true
        return 1
    }
    return 0
}

drop_fault() {
    for _df in "$@"; do
        case " $CREATED " in
            *" $_df "*)
                compose --profile fault stop "$_df" >/dev/null 2>&1 || true
                compose --profile fault rm -f "$_df" >/dev/null 2>&1 || true
                # Untracked only once the container is actually gone. CREATED
                # is the leak oracle -- restoration fails if anything is still
                # in it -- so dropping a name whose removal failed scrubbed the
                # evidence from the very variable that was supposed to report
                # it, and restoration then announced "fault services left:
                # none" over a container that was still running.
                if [ -z "$(compose --profile fault ps -aq "$_df" 2>/dev/null)" ]; then
                    CREATED="$(printf '%s' "$CREATED" | tr ' ' '\n' | grep -vx "$_df" | tr '\n' ' ')"
                else
                    echo "error: '$_df' could not be removed; it stays on the list." >&2
                fi
                ;;
        esac
    done
}

# cut and heal are paired. Every cut records the container so the restore trap
# can heal it even if the script dies in between -- which is the whole point:
# an exercise that only reconnects on the success path leaves a worker
# permanently off the network the first time an assertion fails.
cut_network() {
    _cn_cid="$(compose ps -q "$1" | head -1)"
    if [ -z "$_cn_cid" ]; then return 1; fi
    docker network disconnect "$NET" "$_cn_cid" >/dev/null 2>&1 || return 1
    DISCONNECTED="$DISCONNECTED $_cn_cid"
    return 0
}

heal_network() {
    _hn_left=""
    for _hn_cid in $DISCONNECTED; do
        docker network connect "$NET" "$_hn_cid" >/dev/null 2>&1 || true
        # Verified, then untracked. Emptying the list unconditionally meant a
        # container this exercise had disconnected and failed to reconnect was
        # forgotten, and the restoration check only inspects the three
        # application services -- so anything else stayed off the network with
        # nothing recording it.
        if docker inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}' "$_hn_cid" 2>/dev/null \
            | grep -q "$NET"; then
            continue
        fi
        echo "error: could not reconnect $_hn_cid to $NET." >&2
        _hn_left="$_hn_left $_hn_cid"
    done
    DISCONNECTED="$_hn_left"
}

# Placeholder so the restore trap can call this before scenario 8 defines the
# real one. A trap that referenced an undefined function would abort restoration
# at that line and leave everything after it undone.
restore_perm() { :; }

RESTORED=0
restore() {
    if [ "$RESTORED" = "1" ]; then return 0; fi
    RESTORED=1
    log "restoring the stack and verifying the running state"

    # Reconnect first. Everything below needs the network back, and a container
    # left disconnected is the most damaging thing this script can leave behind.
    heal_network
    restore_perm || _r_perm_failed=1
    drop_fault renamer-fault renamer-hold renamer-altfs renamer-tinyfs renamer-permdenied renamer-tmpfsdest

    # Recreate rather than start: a service that was recreated with an
    # environment override keeps that environment across a plain restart. The
    # watcher is included because the separate-filesystem scenario points it at
    # a different destination root, and a restart would keep that.
    compose up -d --force-recreate --wait --wait-timeout 180 renamer-1 renamer-2 watcher >/dev/null 2>&1 || true
    wait_healthy watcher 180 || true
    wait_healthy renamer-1 120 || true
    wait_healthy renamer-2 120 || true

    _r_ok=1
    if [ "${_r_perm_failed:-0}" = "1" ]; then _r_ok=0; fi
    # Anything this run disconnected and could not reconnect. The per-service
    # loop below only inspects the three application services, so a container
    # outside that set would otherwise be left off the network silently.
    if [ -n "$(printf '%s' "$DISCONNECTED" | tr -d ' ')" ]; then
        _r_ok=0
        printf '  containers left disconnected: %s\n' "$DISCONNECTED" >> "$OUT"
    fi
    for _r_svc in watcher renamer-1 renamer-2; do
        _r_cid="$(compose ps -q "$_r_svc" 2>/dev/null | head -1)"
        if [ -z "$_r_cid" ]; then _r_ok=0; _r_nets="absent"; else
            _r_nets="$(docker inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}' "$_r_cid" 2>/dev/null || echo unknown)"
            case " $_r_nets " in *" $NET "*) ;; *) _r_ok=0 ;; esac
        fi
        printf '  %-12s networks: %s\n' "$_r_svc" "$_r_nets" >> "$OUT"
    done

    # The watcher's destination root must be back to the stack's own. Read
    # from the RUNNING container's environment, which is what the process was
    # started with, rather than from a fresh process asked what it would load.
    _r_wconsume="$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' \
        "$(compose ps -q watcher 2>/dev/null | head -1)" 2>/dev/null \
        | sed -n 's/^FN_STORAGE_CONSUME=//p' | head -1)"
    case "${_r_wconsume:-/srv/fn/consume}" in
        /srv/fn/consume) ;;
        *) _r_ok=0 ;;
    esac
    printf '  watcher destination root: %s   (expected /srv/fn/consume)\n' \
        "${_r_wconsume:-/srv/fn/consume}" >> "$OUT"

    # Effective readiness, not just "the container is up": a renamer that is
    # running but cannot reach the broker is not a restored stack.
    _r_ready="$(compose exec -T renamer-1 /usr/local/bin/fn-renamer healthcheck >/dev/null 2>&1 && echo yes || echo no)"
    _r_ready2="$(compose exec -T renamer-2 /usr/local/bin/fn-renamer healthcheck >/dev/null 2>&1 && echo yes || echo no)"
    _r_readyw="$(compose exec -T watcher /usr/local/bin/fn-watcher healthcheck >/dev/null 2>&1 && echo yes || echo no)"
    if [ "$_r_ready" != "yes" ] || [ "$_r_ready2" != "yes" ] || [ "$_r_readyw" != "yes" ]; then _r_ok=0; fi

    # No fault service this run created may be left behind.
    _r_left="$(printf '%s' "$CREATED" | tr -s ' ')"
    if [ -n "$(printf '%s' "$_r_left" | tr -d ' ')" ]; then _r_ok=0; fi

    {
        printf '\nrestoration (read back from the running stack):\n'
        printf '  renamer-1 healthcheck:   %s\n' "$_r_ready"
        printf '  renamer-2 healthcheck:   %s\n' "$_r_ready2"
        printf '  watcher   healthcheck:   %s\n' "$_r_readyw"
        printf '  fault services left:     %s\n' "${_r_left:-none}"
        printf '  containers reconnected:  yes\n'
    } >> "$OUT"

    exercise_unlock

    if [ "$_r_ok" != "1" ]; then
        echo >&2
        echo "FAILED: the stack was NOT restored. See $OUT." >&2
        printf '\nRESTORATION FAILED — see the values above.\n' >> "$OUT"
        exit 1
    fi
    note "restored: all application services on $NET and answering their own healthcheck"
}

exercise_lock recovery || exit 1
trap 'restore' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

: > "$OUT"
emit "FN-N010 — interruption, stale worker, filesystem boundaries, write failures"
emit ""

# ---------------------------------------------------------------------------
# 1. A6: a real interruption between the link and the receipt.
# ---------------------------------------------------------------------------
log "1/9: A6 — interrupting a renamer between the destination link and the receipt"
stop_ordinary
DOC1="a6-after-link-$STAMP.pdf"
NAME1="a6-after-link-$LOWER.pdf"

start_fault renamer-fault FN_FAULT_POINTS=after_link || exit 1
sleep 6
submit "$DOC1"

# The faulting renamer exits when it fires, so wait for the container to stop.
_i=0
while [ "$_i" -lt 90 ]; do
    _running="$(compose --profile fault ps -q renamer-fault 2>/dev/null | head -1)"
    if [ -z "$_running" ]; then break; fi
    _state="$(docker inspect -f '{{.State.Status}}' "$_running" 2>/dev/null || echo gone)"
    if [ "$_state" != "running" ]; then break; fi
    sleep 1
    _i=$((_i + 1))
done
EXIT1="$(docker inspect -f '{{.State.ExitCode}}' "$(compose --profile fault ps -aq renamer-fault | head -1)" 2>/dev/null || echo unknown)"
STATE1="$(psqlq "SELECT state FROM jobs WHERE source_name = '$DOC1';")"
PUBLISHED1="$(in_storage "test -f '/srv/fn/consume/$NAME1' && echo yes || echo no")"
RECEIPTS1="$(psqlq "SELECT count(*) FROM delivery_receipts r JOIN jobs j USING (job_id) WHERE j.source_name = '$DOC1';")"
CLAIM1="$(psqlq "SELECT coalesce(publish_claimed_by,'-') FROM jobs WHERE source_name = '$DOC1';")"

emit "1. A6 — interruption AFTER the link, BEFORE the receipt"
emit "   renamer exit code:            $EXIT1   (90 = injected stop)"
emit "   job state after the crash:    $STATE1   (expected publishing)"
emit "   publication claimed by:       $CLAIM1   (the dead attempt still holds it)"
emit "   destination present:          $PUBLISHED1"
emit "   delivery receipts:            $RECEIPTS1   (expected 0)"
[ "$STATE1" = "publishing" ] || bad "the interrupted job is in state '$STATE1', not publishing"
[ "$PUBLISHED1" = "yes" ] || bad "the document was not linked into place before the crash"
[ "$RECEIPTS1" = "0" ] || bad "a receipt exists even though the crash preceded it"

# Recovery: an ordinary renamer must reconcile it without republishing.
drop_fault renamer-fault
compose start renamer-1 renamer-2 >/dev/null 2>&1 || true
wait_healthy renamer-1 120 || true
RECOVERED1="$(await_state "$DOC1" "delivered held uncertain" 180)"
COUNT1="$(in_storage "ls -1 /srv/fn/consume 2>/dev/null | grep -c '^a6-after-link-$LOWER' || true")"
RECON1="$(psqlq "SELECT count(*) FROM job_events e JOIN jobs j USING (job_id) WHERE j.source_name = '$DOC1' AND e.event_type = 'reconciled';")"

emit "   after recovery:               $RECOVERED1   (expected delivered)"
emit "   reconciled events:            $RECON1   (expected 1: not republished)"
emit "   documents with that name:     $COUNT1   (expected 1)"
[ "$RECOVERED1" = "delivered" ] || bad "recovery left the job in '$RECOVERED1'"
[ "$RECON1" = "1" ] || bad "the recovery did not record a reconciliation"
[ "$COUNT1" = "1" ] || bad "$COUNT1 documents exist for one job after recovery"
emit ""

# ---------------------------------------------------------------------------
# 2. A6: interrupted BEFORE the link -- the destination never appears -- and
#    the uncertain outcome must survive a LATER DELIVERY, not merely a reread.
# ---------------------------------------------------------------------------
log "2/9: A6 — interrupting before the link, so the destination never appears"
stop_ordinary
DOC2="a6-before-link-$STAMP.pdf"
NAME2="a6-before-link-$LOWER.pdf"

start_fault renamer-fault FN_FAULT_POINTS=before_link || exit 1
sleep 6
submit "$DOC2"
_i=0
while [ "$_i" -lt 90 ]; do
    _running="$(compose --profile fault ps -q renamer-fault 2>/dev/null | head -1)"
    if [ -z "$_running" ]; then break; fi
    _state="$(docker inspect -f '{{.State.Status}}' "$_running" 2>/dev/null || echo gone)"
    if [ "$_state" != "running" ]; then break; fi
    sleep 1
    _i=$((_i + 1))
done
STATE2="$(psqlq "SELECT state FROM jobs WHERE source_name = '$DOC2';")"
PUBLISHED2="$(in_storage "test -f '/srv/fn/consume/$NAME2' && echo yes || echo no")"

emit "2. A6 — interruption BEFORE the link"
emit "   job state after the crash:    $STATE2   (expected publishing: the claim was taken)"
emit "   destination present:          $PUBLISHED2   (expected no)"
[ "$STATE2" = "publishing" ] || bad "the interrupted job is in state '$STATE2'"
[ "$PUBLISHED2" = "no" ] || bad "a destination exists although the crash preceded the link"

drop_fault renamer-fault
compose start renamer-1 renamer-2 >/dev/null 2>&1 || true
wait_healthy renamer-1 120 || true
RECOVERED2="$(await_state "$DOC2" "delivered held uncertain" 180)"
JOB2="$(psqlq "SELECT job_id FROM jobs WHERE source_name = '$DOC2';")"
emit "   after recovery:               $RECOVERED2   (expected uncertain: a publication may have happened)"
[ "$RECOVERED2" = "uncertain" ] || bad "recovery reached '$RECOVERED2'; an unconfirmable publication must be uncertain"

# "It stayed uncertain" has to mean a LATER DELIVERY ARRIVED AND WAS SETTLED
# WITHOUT REOPENING IT. Re-reading the same row a moment later shows only that
# nothing happened, which is also what a job nobody ever redelivered looks
# like. So the message is republished onto the real queue and the disposition
# of that delivery is read out of the job's own history.
DELIV_BEFORE2="$(psqlq "SELECT count(*) FROM job_events e WHERE e.job_id = '$JOB2' AND e.event_type = 'delivery_received';")"
EVENTS_BEFORE2="$(psqlq "SELECT count(*) FROM job_events e WHERE e.job_id = '$JOB2';")"
compose exec -T rabbitmq rabbitmqadmin \
    --vhost "${FN_AMQP_VHOST:-filename-normalizer}" \
    --username "${FN_AMQP_USER:-fn_app}" \
    --password "$(cat "$DEPLOY_DIR/secrets/fn_amqp_password")" \
    --non-interactive \
    publish message \
    --exchange "${FN_AMQP_EXCHANGE:-filename_normalizer.jobs}" \
    --routing-key "${FN_AMQP_ROUTING_KEY:-normalize}" \
    --properties '{"delivery_mode":2,"content_type":"application/json"}' \
    --payload "{\"contract_version\":1,\"job_id\":\"$JOB2\",\"attempt\":99,\"enqueued_at\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" >/dev/null 2>&1 \
    && REPUB2=yes || REPUB2=no

_i=0
DELIV_AFTER2="$DELIV_BEFORE2"
while [ "$_i" -lt 60 ]; do
    DELIV_AFTER2="$(psqlq "SELECT count(*) FROM job_events e WHERE e.job_id = '$JOB2' AND e.event_type = 'delivery_received';")"
    if [ "${DELIV_AFTER2:-0}" -gt "${DELIV_BEFORE2:-0}" ]; then break; fi
    sleep 1; _i=$((_i + 1))
done
sleep 5
STATE2B="$(psqlq "SELECT state FROM jobs WHERE job_id = '$JOB2';")"
EVENTS_AFTER2="$(psqlq "SELECT count(*) FROM job_events e WHERE e.job_id = '$JOB2';")"
NEWEVENTS2="$(psqlq "SELECT string_agg(event_type, ',' ORDER BY event_id) FROM (SELECT event_type, event_id FROM job_events WHERE job_id = '$JOB2' ORDER BY event_id DESC LIMIT ($EVENTS_AFTER2 - $EVENTS_BEFORE2)) t;")"
SETTLED2="$(compose logs --since 3m renamer-1 renamer-2 2>/dev/null | grep -c "delivery_settled.*$JOB2" || true)"
PUBLISHED2B="$(in_storage "test -f '/srv/fn/consume/$NAME2' && echo yes || echo no")"

emit "   a later delivery was published to the real queue: $REPUB2"
emit "   delivery_received events:     $DELIV_BEFORE2 -> $DELIV_AFTER2   (the delivery really arrived)"
emit "   what that delivery wrote:     ${NEWEVENTS2:-none}"
# "Settled" has to be shown durably, not by counting log lines: a handler can
# print anything and still leave the delivery unacknowledged, and a redelivery
# that is quietly requeued forever prints exactly the same thing.
#
# The broker's queue-level numbers cannot carry this claim either, and an
# earlier version of this check used them anyway: the work queue is SHARED, so
# `messages_unacknowledged` counts whatever else the stack is doing and says
# nothing about this job. The job's own history does say it -- a requeued
# delivery comes back, so a delivery count that stops climbing is a delivery
# that was acknowledged.
DELIV_SETTLE_A="$(psqlq "SELECT count(*) FROM job_events WHERE job_id = '$JOB2' AND event_type = 'delivery_received';")"
sleep 12
DELIV_SETTLE_B="$(psqlq "SELECT count(*) FROM job_events WHERE job_id = '$JOB2' AND event_type = 'delivery_received';")"
UNACKED2="$(queue_field "${FN_AMQP_QUEUE:-filename_normalizer.jobs.v1}" messages_unacknowledged)"
READY2="$(queue_field "${FN_AMQP_QUEUE:-filename_normalizer.jobs.v1}" messages_ready)"
emit "   its disposition:              $SETTLED2 log line(s), and the delivery did"
emit "                                 not come back: $DELIV_SETTLE_A -> $DELIV_SETTLE_B"
emit "                                 over 12s (a requeued delivery returns)"
emit "   broker queue at that moment:  ${UNACKED2:-unknown} unacknowledged, ${READY2:-unknown} ready"
emit "                                 (context only: the queue is shared, so these"
emit "                                 numbers are about the stack, not this job)"
emit "   state after that delivery:    $STATE2B   (expected uncertain: not reopened)"
emit "   destination after it:         $PUBLISHED2B   (expected no: nothing was published)"
[ "$REPUB2" = "yes" ] || bad "the redelivery could not be published, so the uncertain job was never re-offered"
[ "${DELIV_AFTER2:-0}" -gt "${DELIV_BEFORE2:-0}" ] || bad "no further delivery reached the uncertain job, so nothing was proven about it"
[ "$STATE2B" = "uncertain" ] || bad "an uncertain job was reopened to '$STATE2B' by a later delivery"
[ "$PUBLISHED2B" = "no" ] || bad "a later delivery of an uncertain job published a document"
[ "${DELIV_SETTLE_B:-0}" = "${DELIV_SETTLE_A:-0}" ] || bad "the delivery came back ($DELIV_SETTLE_A -> $DELIV_SETTLE_B): it was requeued, not settled"
case "${NEWEVENTS2:-}" in
    *reserved*|*publish_attempted*|*delivered*) bad "a later delivery of an uncertain job did real work: $NEWEVENTS2" ;;
esac
emit "   => the job is UNRESOLVED BY DESIGN and stays that way. It is not"
emit "      guessed closed here; an operator decides. job_id=$JOB2"
emit ""

# ---------------------------------------------------------------------------
# 3. A7 + competing publication: two attempts on one job, overlapping in time,
#    at the claim/publication boundary.
#
# This is the scenario the previous round did not have. Its evidence for
# "concurrent attempts are safe" was a duplicate message arriving after the
# job was already delivered, which only shows that an existing receipt
# short-circuits a redelivery. It says nothing about two attempts that are
# both live and both intend to publish.
#
# Producing that overlap needs a first attempt that STAYS mid-publication:
#
#   1. renamer-hold takes the publication claim and then sleeps, holding it,
#      with the destination still empty. Its container keeps running and keeps
#      every mount -- this is also the A7 stale-worker condition.
#   2. It is cut off the network while paused, so the broker returns its
#      unacknowledged delivery. That is what hands the SAME job to a sibling.
#   3. renamer-1 picks the job up and reaches the same boundary while the
#      holder is provably still in its pause window.
#
# Both attempts are then observed by name, with timestamps, and the durable
# result must contain exactly one publication.
# ---------------------------------------------------------------------------
log "3/9: A7 — two live attempts on one job at the publication boundary"
stop_ordinary
DOC3="a7-overlap-$STAMP.pdf"
NAME3="a7-overlap-$LOWER.pdf"
# Long enough that the sibling demonstrably arrives inside the window, short
# enough that the deferral does not churn the ledger for minutes: the sibling
# retries about every ReconnectDelay while it waits.
HOLD_SECS=45
TAKEOVER=600

# The sibling's takeover window has to outlast the hold, and it is the
# SIBLING's setting that matters: the window is evaluated by whoever is
# thinking about taking the claim, not by whoever holds it. With the ordinary
# 20s window renamer-1 would decide the holder was dead while it was visibly
# still paused, and the run would measure takeover instead of contention.
#
# renamer-1 is therefore recreated with a long window for this scenario. The
# restore trap recreates it from the plain environment afterwards and checks
# the result, so the override cannot outlive this exercise.
# Exported in a subshell, not as an assignment prefix: `compose` is a shell
# function, and POSIX leaves the effect of an assignment prefix on a function
# unspecified -- it does not reliably reach the process compose starts.
(
    export FN_PUBLISH_TAKEOVER_AFTER="${TAKEOVER}s"
    compose up -d --force-recreate --wait --wait-timeout 180 renamer-1 >/dev/null 2>&1
) || true
compose stop renamer-1 >/dev/null 2>&1 || true
TAKEOVER_CID="$(compose ps -aq renamer-1 2>/dev/null | head -1 || true)"
TAKEOVER_SET=""
if [ -n "$TAKEOVER_CID" ]; then
    TAKEOVER_SET="$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$TAKEOVER_CID" 2>/dev/null \
        | sed -n 's/^FN_PUBLISH_TAKEOVER_AFTER=//p' | head -1 || true)"
fi

start_fault renamer-hold FN_FAULT_HOLD="${HOLD_SECS}s" || exit 1
wait_healthy renamer-hold 120 || true
submit "$DOC3" 2000000
await_registered "$DOC3" 90 || bad "the overlap probe was never registered"
JOB3="$(psqlq "SELECT job_id FROM jobs WHERE source_name = '$DOC3';")"

# Wait for the holder to be demonstrably paused WITH the claim held.
_i=0
HOLDER=""
while [ "$_i" -lt 90 ]; do
    HOLDER="$(psqlq "SELECT coalesce(publish_claimed_by,'') FROM jobs WHERE job_id = '$JOB3';")"
    if [ -n "$HOLDER" ]; then break; fi
    sleep 1; _i=$((_i + 1))
done
PAUSED="$(compose --profile fault logs renamer-hold 2>/dev/null | grep -c fault_point_paused || true)"
HOLD_STARTED="$(psqlq "SELECT to_char(publish_attempted_at,'HH24:MI:SS.MS') FROM jobs WHERE job_id = '$JOB3';")"
STATE3A="$(psqlq "SELECT state FROM jobs WHERE job_id = '$JOB3';")"
DEST_AT_HOLD="$(in_storage "test -f '/srv/fn/consume/$NAME3' && echo yes || echo no")"

emit "3. A7 + competing publication — two live attempts on one job"
emit "   sibling's takeover window:    ${TAKEOVER_SET:-unset} (longer than the ${HOLD_SECS}s hold, so"
emit "                                 the sibling is deciding about a LIVE holder, not a dead one)"
emit "   attempt A (renamer-hold):"
emit "     holds the claim as:         ${HOLDER:-none}"
emit "     claim taken at:             ${HOLD_STARTED:-unknown}"
emit "     paused at the fault point:  $PAUSED log line(s)"
emit "     job state while held:       $STATE3A   (expected publishing)"
emit "     destination present yet:    $DEST_AT_HOLD   (expected no: the pause is BEFORE the link,"
emit "                                 so only the claim can stop a sibling publishing)"
[ "${TAKEOVER_SET:-}" = "${TAKEOVER}s" ] || bad "the sibling's takeover window is '${TAKEOVER_SET:-unset}', so it may treat the live holder as dead"
[ -n "$HOLDER" ] || bad "no attempt ever took the publication claim, so there is nothing to overlap with"
[ "${PAUSED:-0}" -ge 1 ] || bad "the holder never reported pausing, so it may not have been mid-publication"
[ "$STATE3A" = "publishing" ] || bad "the held job is in '$STATE3A', not publishing"
[ "$DEST_AT_HOLD" = "no" ] || bad "the destination already exists; the window under test is before the link"

# Bring the sibling up FIRST, so the requeued delivery reaches a worker that
# is already consuming. Starting it after the cut would spend most of the
# hold window on container startup.
compose start renamer-1 >/dev/null 2>&1 || true
wait_healthy renamer-1 120 || true

# Now cut the holder off the broker. It keeps running, keeps its mounts, and
# keeps the claim; the broker returns its unacknowledged delivery to the queue.
HOLD_CID="$(compose --profile fault ps -q renamer-hold | head -1)"
cut_network renamer-hold && CUT=yes || CUT=no
emit "     cut off the broker:         $CUT (still running, mounts retained)"
[ "$CUT" = "yes" ] || bad "the holder could not be disconnected, so no sibling could be handed the job"

# Attempt B must be observed ARRIVING while attempt A is still paused. The
# deferral event is written by the sibling, names the holder, and can only be
# written while the holder's claim is live -- which is the overlap, recorded
# durably rather than inferred from timing.
_i=0
DEFERRED3=0
while [ "$_i" -lt 90 ]; do
    DEFERRED3="$(psqlq "SELECT count(*) FROM job_events WHERE job_id = '$JOB3' AND event_type = 'delivery_deferred';")"
    if [ "${DEFERRED3:-0}" -ge 1 ]; then break; fi
    sleep 2; _i=$((_i + 2))
done
DEFER_TO="$(psqlq "SELECT detail->>'publication_held_by' FROM job_events WHERE job_id = '$JOB3' AND event_type = 'delivery_deferred' ORDER BY event_id LIMIT 1;")"
DEFER_AT="$(psqlq "SELECT to_char(occurred_at,'HH24:MI:SS.MS') FROM job_events WHERE job_id = '$JOB3' AND event_type = 'delivery_deferred' ORDER BY event_id LIMIT 1;")"
# Is the holder still inside its pause at that moment? Both facts come from the
# ledger's own clock, so this is not an assumption about wall time.
STILL_HELD="$(psqlq "SELECT (occurred_at < (SELECT publish_attempted_at FROM jobs WHERE job_id = '$JOB3') + interval '$HOLD_SECS seconds') FROM job_events WHERE job_id = '$JOB3' AND event_type = 'delivery_deferred' ORDER BY event_id LIMIT 1;")"
HOLDER_ALIVE="$(docker inspect -f '{{.State.Running}}' "$HOLD_CID" 2>/dev/null || echo unknown)"
DUP_AT_OVERLAP="$(in_storage "ls -1 /srv/fn/consume 2>/dev/null | grep -c '^a7-overlap-$LOWER' || true")"

emit "   attempt B (renamer-1), while A still holds the claim:"
emit "     stood down, deferrals:      $DEFERRED3   (expected >= 1)"
emit "     names the holder as:        ${DEFER_TO:-none}   (expected ${HOLDER:-?})"
emit "     stood down at:              ${DEFER_AT:-unknown}"
emit "     inside A's hold window:     $STILL_HELD   (expected t: the two attempts overlapped)"
emit "     A's container still alive:  $HOLDER_ALIVE   (expected true: A is stale, not dead)"
emit "     documents published so far: $DUP_AT_OVERLAP   (expected 0: neither attempt published)"
[ "${DEFERRED3:-0}" -ge 1 ] || bad "the sibling never stood down, so no overlap at the claim boundary was observed"
[ "$DEFER_TO" = "$HOLDER" ] || bad "the deferral names '$DEFER_TO' but the claim is held by '$HOLDER'"
[ "$STILL_HELD" = "t" ] || bad "the sibling's attempt did not fall inside the holder's pause window; the attempts did not overlap"
[ "$HOLDER_ALIVE" = "true" ] || bad "the holder was not alive during the overlap, so this is an interruption case, not a concurrency case"
[ "${DUP_AT_OVERLAP:-0}" = "0" ] || bad "$DUP_AT_OVERLAP documents were published while both attempts were mid-flight"

# A deferral must not spend the job's retry budget: two healthy workers taking
# turns must not be able to hold a document nothing is wrong with.
ATT3="$(psqlq "SELECT delivery_attempts FROM jobs WHERE job_id = '$JOB3';")"
DELIV3="$(psqlq "SELECT count(*) FROM job_events WHERE job_id = '$JOB3' AND event_type = 'delivery_received';")"
emit "     deliveries received:        $DELIV3"
emit "     attempts charged:           $ATT3   (deferrals were returned to the budget)"
[ "${ATT3:-0}" -le "${DELIV3:-0}" ] || bad "more attempts are charged ($ATT3) than deliveries received ($DELIV3)"

# The overlap has been observed. Reconnect the holder so it finishes its own
# publication against a reachable ledger -- the point is that the attempt that
# HELD the claim completes it, and the sibling that deferred does not produce
# a second copy. Both processes are alive for the rest of the scenario.
heal_network
# renamer-2 is deliberately NOT started here. It was never recreated with this
# scenario's long takeover window, so it would arrive with the ordinary 20s
# one, decide that a holder paused for 45s must be dead, take the claim, find
# no destination and record `uncertain` -- for a publication the holder was
# about to complete. That is the exercise handing itself a stale-takeover case
# in the middle of a contention case. The restore trap recreates renamer-2
# from the plain environment, and scenario 4 starts it again.
FINAL3="$(await_state "$DOC3" "delivered held uncertain" 240)"
COUNT3="$(in_storage "ls -1 /srv/fn/consume 2>/dev/null | grep -c '^a7-overlap-$LOWER' || true")"
RECEIPTS3="$(psqlq "SELECT count(*) FROM delivery_receipts r JOIN jobs j USING (job_id) WHERE j.source_name = '$DOC3';")"
RESV3="$(psqlq "SELECT count(*) FROM name_reservations n JOIN jobs j USING (job_id) WHERE j.source_name = '$DOC3' AND n.blocked_at IS NULL;")"
NAMES3="$(in_storage "ls -1 /srv/fn/consume 2>/dev/null | grep '^a7-overlap-$LOWER' | tr '\\n' ' ' || true")"
AGREE3="$(psqlq "SELECT count(*) FROM jobs j JOIN delivery_receipts r USING (job_id) WHERE j.source_name = '$DOC3' AND j.state = 'delivered' AND j.reserved_name = r.delivered_name;")"

emit "   durable result of the overlap:"
emit "     final state:                $FINAL3"
emit "     documents published:        $COUNT3   (expected 1: not one per attempt)"
emit "     names present:              ${NAMES3:-none}   (expected exactly the reserved name, no suffixed twin)"
emit "     delivery receipts:          $RECEIPTS3   (expected 1)"
emit "     active reservations:        $RESV3   (expected 1)"
emit "     state and receipt agree:    $AGREE3   (expected 1: one identity, not two)"
[ "${COUNT3:-0}" = "1" ] || bad "$COUNT3 documents exist for one job after two competing attempts"
[ "${RECEIPTS3:-0}" = "1" ] || bad "$RECEIPTS3 receipts exist for one job"
[ "${RESV3:-0}" -le 1 ] || bad "$RESV3 active reservations exist for one job"
[ "$FINAL3" = "delivered" ] || bad "the contended job reached '$FINAL3'"
[ "${AGREE3:-0}" = "1" ] || bad "the job's state and its receipt do not describe the same delivery"
drop_fault renamer-hold
# The holder is removed while it may still hold a staged temporary it can no
# longer unlink. That is this exercise's doing, so this exercise clears it.
in_storage "rm -f /srv/fn/consume/.fn-$JOB3.* 2>/dev/null; true" >/dev/null 2>&1 || true
emit ""

# ---------------------------------------------------------------------------
# 4. Distinct submissions with IDENTICAL bytes must stay distinct.
#
# The ownership rule above is deliberately identity-based rather than
# content-based. That choice only matters if equal bytes are a real case, so
# it is exercised as one: two different submissions, byte-for-byte identical,
# must produce two documents and two receipts. Anything that deduplicated them
# would be discarding a document nobody asked it to discard.
# ---------------------------------------------------------------------------
log "4/9: two distinct submissions with identical bytes stay distinct"
compose start renamer-1 renamer-2 >/dev/null 2>&1 || true
wait_healthy renamer-1 120 || true
DOC4A="twin-a-$STAMP.pdf"
DOC4B="twin-b-$STAMP.pdf"
NAME4A="twin-a-$LOWER.pdf"
NAME4B="twin-b-$LOWER.pdf"
submit "$DOC4A" 4096 'z'
submit "$DOC4B" 4096 'z'
STATE4A="$(await_state "$DOC4A" "delivered held uncertain" 150)"
STATE4B="$(await_state "$DOC4B" "delivered held uncertain" 150)"
SUMS4="$(in_storage "md5sum '/srv/fn/consume/$NAME4A' '/srv/fn/consume/$NAME4B' 2>/dev/null | awk '{print \$1}' | sort -u | wc -l")"
PRESENT4="$(in_storage "ls -1 /srv/fn/consume 2>/dev/null | grep -c '^twin-[ab]-$LOWER' || true")"
RECEIPTS4="$(psqlq "SELECT count(*) FROM delivery_receipts r JOIN jobs j USING (job_id) WHERE j.source_name IN ('$DOC4A','$DOC4B');")"
INODES4="$(in_storage "ls -i '/srv/fn/consume/$NAME4A' '/srv/fn/consume/$NAME4B' 2>/dev/null | awk '{print \$1}' | sort -u | wc -l")"

emit "4. equal-byte submissions remain distinct"
emit "   states:                       $STATE4A / $STATE4B   (expected delivered / delivered)"
emit "   distinct content digests:     $SUMS4   (1: the bytes really are identical)"
emit "   documents published:          $PRESENT4   (expected 2: not deduplicated)"
emit "   delivery receipts:            $RECEIPTS4   (expected 2)"
emit "   distinct inodes:              $INODES4   (expected 2: two files, not one linked twice)"
[ "$STATE4A" = "delivered" ] || bad "the first twin reached '$STATE4A'"
[ "$STATE4B" = "delivered" ] || bad "the second twin reached '$STATE4B'"
[ "${SUMS4:-0}" = "1" ] || bad "the twins are not byte-identical, so the case was not exercised"
[ "${PRESENT4:-0}" = "2" ] || bad "$PRESENT4 documents exist for two equal-byte submissions"
[ "${RECEIPTS4:-0}" = "2" ] || bad "$RECEIPTS4 receipts exist for two submissions"
[ "${INODES4:-0}" = "2" ] || bad "the twins share an inode; one submission was discarded"
emit ""

# ---------------------------------------------------------------------------
# 5. Recovery must not adopt a FOREIGN file whose bytes happen to match.
#
# A job interrupted before its link leaves a reserved name and a recorded
# claim identity. If something else then occupies that name with identical
# content, recovery must refuse it: adopting it would both take a file this
# job never wrote and merge two submissions the contract keeps distinct.
# ---------------------------------------------------------------------------
log "5/9: recovery refuses a foreign file at the reserved name, even byte-identical"
stop_ordinary
DOC5="a6-foreign-$STAMP.pdf"
NAME5="a6-foreign-$LOWER.pdf"
start_fault renamer-fault FN_FAULT_POINTS=before_link || exit 1
sleep 6
submit "$DOC5" 8192 'q'
_i=0
while [ "$_i" -lt 90 ]; do
    _running="$(compose --profile fault ps -q renamer-fault 2>/dev/null | head -1)"
    if [ -z "$_running" ]; then break; fi
    _state="$(docker inspect -f '{{.State.Status}}' "$_running" 2>/dev/null || echo gone)"
    if [ "$_state" != "running" ]; then break; fi
    sleep 1; _i=$((_i + 1))
done
JOB5="$(psqlq "SELECT job_id FROM jobs WHERE source_name = '$DOC5';")"
RESERVED5="$(psqlq "SELECT coalesce(reserved_name,'-') FROM jobs WHERE job_id = '$JOB5';")"
CLAIMINO5="$(psqlq "SELECT coalesce(publish_inode::text,'-') FROM jobs WHERE job_id = '$JOB5';")"

# Put a FOREIGN file with exactly the job's bytes at the reserved name. It is
# written by a different process, so it is a different inode -- which is the
# only thing that distinguishes it from this job's own work.
in_storage "head -c 8192 /dev/zero | tr '\\0' 'q' > '/srv/fn/consume/$RESERVED5'" >/dev/null 2>&1
FOREIGN_INO5="$(in_storage "ls -i '/srv/fn/consume/$RESERVED5' 2>/dev/null | awk '{print \$1}'")"
SAMEBYTES5="$(in_storage "md5sum '/srv/fn/incoming/$DOC5' '/srv/fn/consume/$RESERVED5' 2>/dev/null | awk '{print \$1}' | sort -u | wc -l")"

drop_fault renamer-fault
compose start renamer-1 renamer-2 >/dev/null 2>&1 || true
wait_healthy renamer-1 120 || true
FINAL5="$(await_state "$DOC5" "delivered held uncertain" 180)"
CAT5F="$(psqlq "SELECT coalesce(failure_category,'-') FROM jobs WHERE job_id = '$JOB5';")"
RECEIPTS5="$(psqlq "SELECT count(*) FROM delivery_receipts WHERE job_id = '$JOB5';")"
STILL_FOREIGN5="$(in_storage "ls -i '/srv/fn/consume/$RESERVED5' 2>/dev/null | awk '{print \$1}'")"

emit "5. recovery and a foreign file with matching bytes"
emit "   reserved name:                $RESERVED5"
emit "   inode this job claimed:       $CLAIMINO5"
emit "   inode actually at that name:  $FOREIGN_INO5   (a different file)"
emit "   byte-identical:               $([ "${SAMEBYTES5:-0}" = "1" ] && echo yes || echo no)   (the case only counts if they match)"
emit "   outcome:                      $FINAL5   (expected held)"
emit "   category:                     $CAT5F   (expected destination_conflict)"
emit "   receipts written:             $RECEIPTS5   (expected 0: nothing was adopted)"
emit "   foreign file still intact:    $([ "$STILL_FOREIGN5" = "$FOREIGN_INO5" ] && echo yes || echo NO)"
[ "${SAMEBYTES5:-0}" = "1" ] || bad "the planted file is not byte-identical, so the interesting case was not exercised"
[ "$FINAL5" = "held" ] || bad "recovery reached '$FINAL5' against a foreign file; it must refuse and hold"
[ "$CAT5F" = "destination_conflict" ] || bad "category is '$CAT5F', not destination_conflict"
[ "${RECEIPTS5:-0}" = "0" ] || bad "recovery claimed a foreign file as its own delivery"
[ "$STILL_FOREIGN5" = "$FOREIGN_INO5" ] || bad "the foreign file was replaced or removed"

# The intruder was planted by this exercise, so this exercise takes it away
# again -- but ONLY while it is still demonstrably the file that was planted.
#
# `bad` records a mismatch and carries on, so the branch that fires when the
# inode at that name is no longer the planted one fell straight through to an
# unconditional rm. The one case where the path may hold somebody else's file
# -- a real delivery, say -- was exactly the case that deleted it. The identity
# is re-read here and the removal is gated on it.
NOW5="$(in_storage "ls -i '/srv/fn/consume/$RESERVED5' 2>/dev/null | awk '{print \$1}'")"
if [ -n "$NOW5" ] && [ "$NOW5" = "$FOREIGN_INO5" ]; then
    in_storage "rm -f '/srv/fn/consume/$RESERVED5' 2>/dev/null; true" >/dev/null 2>&1 || true
    GONE5="$(in_storage "test -e '/srv/fn/consume/$RESERVED5' && echo present || echo removed")"
    emit "   the planted intruder was:     $GONE5 afterwards (inode $FOREIGN_INO5, the one this exercise planted)"
    [ "$GONE5" = "removed" ] || bad "the exercise left its planted file in the destination"
elif [ -z "$NOW5" ]; then
    emit "   the planted intruder was:     already gone before cleanup"
else
    emit "   the planted intruder was:     NOT removed: inode $NOW5 is not the planted $FOREIGN_INO5"
    bad "the file at the reserved name is not the one this exercise planted; leaving it alone"
fi
emit ""

# ---------------------------------------------------------------------------
# 6. A10: the publication actually crosses a filesystem boundary.
#
# The topology matters and is reported in full rather than asserted in one
# direction. What has to be true for the copy fallback to be exercised is that
# STAGING and CONSUME are different filesystems, because the staged temporary
# is written in staging and linked into consume. The incoming root is reported
# alongside it because the source is READ across that boundary, and because
# the two claims are easy to confuse: an incoming/consume split alone would
# not exercise the fallback at all.
# ---------------------------------------------------------------------------
log "6/9: A10 — staging on a tmpfs, so the copy fallback is actually taken"
stop_ordinary
DOC6="a10-altfs-$STAMP.pdf"
NAME6="a10-altfs-$LOWER.pdf"

start_fault renamer-altfs || exit 1
wait_healthy renamer-altfs 180 || true
# The runtime image is FROM scratch and has no shell, and a tmpfs is private to
# its container, so nothing outside can stat it. The application reports the
# device behind each root in its own effective configuration, which is the only
# vantage point that can see all the roots at once.
DEV_JSON="$(compose --profile fault exec -T renamer-altfs /usr/local/bin/fn-renamer check-config 2>/dev/null \
    | run_py "import json,sys
d,_ = json.JSONDecoder().raw_decode(sys.stdin.read().lstrip())
v = d.get('storage_devices', {})
print('%s %s %s %s' % (v.get('incoming','?'), v.get('staging','?'), v.get('consume','?'), v.get('queued','?')))")"
INCOMING_DEV="$(printf '%s' "$DEV_JSON" | awk '{print $1}')"
STAGING_DEV="$(printf '%s' "$DEV_JSON" | awk '{print $2}')"
CONSUME_DEV="$(printf '%s' "$DEV_JSON" | awk '{print $3}')"
QUEUED_DEV="$(printf '%s' "$DEV_JSON" | awk '{print $4}')"

submit "$DOC6" 200000
STATE6="$(await_state "$DOC6" "delivered held uncertain" 180)"
PUB6="$(in_storage "test -f '/srv/fn/consume/$NAME6' && echo yes || echo no")"
SIZE6="$(in_storage "wc -c < '/srv/fn/consume/$NAME6' 2>/dev/null || echo 0")"

emit "6. A10 — publication across a real filesystem boundary"
emit "   device behind each root, as the running process sees it:"
emit "     incoming:                   $INCOMING_DEV"
emit "     queued:                     $QUEUED_DEV"
emit "     staging:                    $STAGING_DEV"
emit "     consume:                    $CONSUME_DEV"
if [ -z "$STAGING_DEV" ] || [ "$STAGING_DEV" = "?" ] || [ -z "$CONSUME_DEV" ] || [ "$CONSUME_DEV" = "?" ]; then
    bad "the storage devices could not be read, so the topology is unverified"
elif [ "$STAGING_DEV" = "$CONSUME_DEV" ]; then
    bad "staging and consume share device $STAGING_DEV; the cross-filesystem path was NOT exercised"
else
    emit "   => staging ($STAGING_DEV) and consume ($CONSUME_DEV) are different filesystems,"
    emit "      so the working copy could not be renamed into place and the copy"
    emit "      fallback was taken. This is the boundary that matters: the staged"
    emit "      temporary is written in staging and linked into consume."
fi
if [ "$INCOMING_DEV" = "$CONSUME_DEV" ]; then
    emit "   note: incoming ($INCOMING_DEV) and consume ($CONSUME_DEV) are the same device"
    emit "      here. The source is only READ, so that split is not what the"
    emit "      fallback depends on -- staging/consume is. Stated so the claim"
    emit "      is not read as broader than what was exercised."
else
    emit "   incoming ($INCOMING_DEV) and consume ($CONSUME_DEV) also differ."
fi
emit "   final state:                  $STATE6   (expected delivered)"
emit "   destination present:          $PUB6"
emit "   published size:               $SIZE6 bytes   (expected 200000: a complete copy)"
[ "$STATE6" = "delivered" ] || bad "publication across a filesystem boundary reached '$STATE6'"
[ "$PUB6" = "yes" ] || bad "no document was published across the boundary"
[ "${SIZE6:-0}" = "200000" ] || bad "the published document is $SIZE6 bytes, not the 200000 submitted"
drop_fault renamer-altfs
emit ""

# ---------------------------------------------------------------------------
# 7. A11: the working copy runs out of space mid-write, accounted in full.
# ---------------------------------------------------------------------------
log "7/9: A11 — a real ENOSPC while writing the working copy"
stop_ordinary
DOC7="a11-nospace-$STAMP.pdf"
start_fault renamer-tinyfs || exit 1
wait_healthy renamer-tinyfs 180 || true
submit "$DOC7" 5000000   # 5 MB into a 1 MB staging tmpfs
STATE7="$(await_state "$DOC7" "delivered held uncertain" 180)"
JOB7="$(psqlq "SELECT job_id FROM jobs WHERE source_name = '$DOC7';")"
CAT7="$(psqlq "SELECT coalesce(failure_category,'-') FROM jobs WHERE job_id = '$JOB7';")"
ATTEMPTS7="$(psqlq "SELECT delivery_attempts FROM jobs WHERE job_id = '$JOB7';")"
EVENTS7="$(psqlq "SELECT count(*) FROM job_events WHERE job_id = '$JOB7';")"
PUB7="$(in_storage "ls -1 /srv/fn/consume 2>/dev/null | grep -c 'a11-nospace-$LOWER' || true")"
RECEIPTS7="$(psqlq "SELECT count(*) FROM delivery_receipts WHERE job_id = '$JOB7';")"
RESV7="$(psqlq "SELECT count(*) FROM name_reservations WHERE job_id = '$JOB7' AND blocked_at IS NULL;")"
SRC7="$(in_storage "test -f '/srv/fn/incoming/$DOC7' && echo yes || echo no")"
SRCSIZE7="$(in_storage "wc -c < '/srv/fn/incoming/$DOC7' 2>/dev/null || echo 0")"
# The full accounting: nothing partial may be left where it can be seen --
# including a dotfile the consumer would ignore but an operator would find
# months later.
CONSUME_TMP7="$(in_storage "ls -1a /srv/fn/consume 2>/dev/null | grep -c \"^\\.fn-$JOB7\\.\" || true")"
SHARED_STAGE7="$(in_storage "ls -1a /srv/fn/staging 2>/dev/null | grep -c \"$JOB7\" || true")"
# Anything left by an EARLIER scenario is reported separately: this run kills
# containers mid-publication on purpose, and a temporary stranded that way is
# that scenario's artifact, not evidence about this failed write.
OTHER_TMP7="$(in_storage "ls -1a /srv/fn/consume 2>/dev/null | grep -c '^\.fn-' || true")"

TINY_LEFT7="$(in_storage "ls -1a /srv/fn/staging-tiny 2>/dev/null | grep -c '\.work$' || true")"
TINY_BYTES7="$(in_storage "du -sb /srv/fn/staging-tiny 2>/dev/null | cut -f1 || echo unknown")"
# Existence and size are not preservation: a file can keep its length and lose
# its contents. The source is hashed and compared with what discovery recorded.
SRCSUM7="$(in_storage "sha256sum '/srv/fn/incoming/$DOC7' 2>/dev/null | cut -c1-64")"
REGSUM7="$(psqlq "SELECT encode(content_fingerprint,'hex') FROM jobs WHERE source_name = '$DOC7';")"

emit "7. A11 — out of space while writing the working copy"
emit "   final state:                  $STATE7   (expected held)"
emit "   category:                     $CAT7"
emit "   delivery attempts:            $ATTEMPTS7   (the retryable failure was retried, budget 2)"
emit "   history rows:                 $EVENTS7   (bounded, not a spin)"
emit "   full accounting of what the failed write left behind:"
emit "     documents published:        $PUB7   (expected 0)"
emit "     delivery receipts:          $RECEIPTS7   (expected 0)"
emit "     active reservations:        $RESV7   (expected 0: the name was not left held)"
emit "     this job's temporaries in consume: $CONSUME_TMP7   (expected 0)"
emit "     this job's temporaries in staging: $SHARED_STAGE7   (expected 0)"
emit "     temporaries from other scenarios: $OTHER_TMP7   (reported, not charged here:"
emit "       this exercise kills containers mid-publication on purpose, and a"
emit "       process stopped between staging and linking cannot unlink what it"
emit "       staged. Scenario 3's holder is removed exactly that way.)"
emit "     source still present:       $SRC7 ($SRCSIZE7 bytes, unchanged and complete)"
emit "   the failing filesystem itself, now that it can be read:"
emit "     partial .work files left in the 1 MB filesystem: $TINY_LEFT7"
emit "     bytes still occupied there:                      $TINY_BYTES7"
emit "   (This replaces the previous round's stated limitation. The filesystem"
emit "    that runs out of space is a tmpfs-backed NAMED volume now, so an"
emit "    inspector can mount the very filesystem the write failed on. It used"
emit "    to be a container-private tmpfs that nothing outside could list --"
emit "    the runtime image is FROM scratch -- so what the failed write left"
emit "    behind was genuinely unobserved and the evidence said so.)"
emit "   source preservation is byte-verified, not inferred from its size:"
emit "     source sha256 now:          ${SRCSUM7:-unreadable}"
emit "     recorded at registration:   ${REGSUM7:-none}   (must be equal)"
[ "$STATE7" = "held" ] || bad "an out-of-space write reached '$STATE7'"
[ "${PUB7:-0}" = "0" ] || bad "$PUB7 documents were published despite the failed copy"
[ "${RECEIPTS7:-0}" = "0" ] || bad "$RECEIPTS7 receipts exist for a job that never published"
[ "${RESV7:-0}" = "0" ] || bad "$RESV7 reservations are still held by a job that failed"
[ "${CONSUME_TMP7:-0}" = "0" ] || bad "$CONSUME_TMP7 attempt temporaries were left in the consume directory"
[ "${SHARED_STAGE7:-0}" = "0" ] || bad "$SHARED_STAGE7 attempt temporaries were left in the shared staging directory"
[ "${ATTEMPTS7:-0}" -ge 2 ] || bad "the failure was not retried before being held (attempts=$ATTEMPTS7)"
[ -n "$SRCSUM7" ] && [ "$SRCSUM7" = "$REGSUM7" ] || bad "the source's bytes are not what discovery recorded; preservation is not established"
[ "${EVENTS7:-0}" -le 60 ] || bad "$EVENTS7 history rows; the retry bound did not hold"
[ "$SRC7" = "yes" ] || bad "the source was removed by a failed publication"
[ "${SRCSIZE7:-0}" = "5000000" ] || bad "the source is $SRCSIZE7 bytes, not the 5000000 submitted"
# The real reason must survive the retry budget. Before the holdOr fix, a
# persistent storage fault always ended as retry_exhausted, because the final
# delivery was requeued and the next one was stopped by the budget check
# before it ever reached storage.
case "$CAT7" in
    storage_error|permission_denied|storage_unavailable) ;;
    retry_exhausted) bad "the real reason was lost: a full disk was recorded as retry_exhausted" ;;
    *) bad "category '$CAT7' does not describe a storage failure" ;;
esac
drop_fault renamer-tinyfs
emit ""

# ---------------------------------------------------------------------------
# 8. A permission denial at the PUBLICATION itself.
#
# The previous round's permission evidence was a fixture the application never
# actually hit as an unprivileged writer, which is not coverage: a process
# running as UID 0 walks straight through a mode-based denial.
#
# It also cannot be arranged by configuration. A destination that is already
# unwritable makes the application not-ready, so it never attempts the write
# that is supposed to be refused. The denial has to arrive while an attempt is
# in flight, which is what the publication hold is for: the attempt claims the
# publication and pauses with the destination still writable, the mode is then
# revoked by a root-owned process, and the link the application goes on to
# make is refused by the kernel -- as UID 65532, which is who it really is.
#
# It has to be the ORDINARY consume root. A job carries the destination it was
# accepted for, so a renamer pointed at some private directory refuses the job
# as a destination mismatch and never attempts the write at all -- which is
# the guard working, not a denial. The revocation is therefore done while this
# is the only consumer, lasts seconds, and is undone on every exit path
# INCLUDING an interruption, because a consume root left root-owned and
# unwritable would stop the whole stack.
# ---------------------------------------------------------------------------
log "8/9: a real permission denial at the publication"
stop_ordinary
DOC8="perm-denied-$STAMP.pdf"
NAME8="perm-denied-$LOWER.pdf"
PERM_ROOT="/srv/fn/consume"
PERM_TOUCHED=0
# Captured BEFORE anything is revoked, and restored to exactly this. Restoring
# to a hardcoded 0770 65532:65532 quietly rewrote whatever the destination's
# owner and mode had actually been, and called that success -- a deployment
# using a shared group, or any mode but 0770, would have been changed by a test
# that claims to leave the stack as it found it.
PERM_BEFORE=""

restore_perm() {
    if [ "$PERM_TOUCHED" = "1" ]; then
        if [ -z "$PERM_BEFORE" ]; then
            echo "error: the consume root's original mode was never captured;" >&2
            echo "       refusing to guess. Inspect $PERM_ROOT by hand." >&2
            printf '\nPERMISSION RESTORATION FAILED: no captured original\n' >> "$OUT"
            return 1
        fi
        _rp_mode_want="${PERM_BEFORE%%:*}"
        _rp_own_want="${PERM_BEFORE#*:}"
        compose run --rm --no-deps -T --user 0 --entrypoint sh storage-init \
            -c "chown $_rp_own_want $PERM_ROOT; chmod $_rp_mode_want $PERM_ROOT" >/dev/null 2>&1 || true
        # in_storage strips whitespace, so a two-field stat comes back run
        # together and could never equal a spaced expectation -- the check
        # reported a restoration failure for a directory it had just restored
        # correctly. A separator that survives the strip fixes it.
        _rp_now="$(in_storage "stat -c '%a:%u:%g' $PERM_ROOT 2>/dev/null || echo unknown")"
        if [ "$_rp_now" != "$PERM_BEFORE" ]; then
            echo "error: the consume root was NOT restored (now: $_rp_now, was: $PERM_BEFORE)." >&2
            echo "       Fix it before running anything else:" >&2
            echo "       docker compose run --rm --user 0 --entrypoint sh storage-init \\" >&2
            echo "         -c 'chown $_rp_own_want $PERM_ROOT && chmod $_rp_mode_want $PERM_ROOT'" >&2
            printf '\nPERMISSION RESTORATION FAILED: consume root is %s, was %s\n' "$_rp_now" "$PERM_BEFORE" >> "$OUT"
            return 1
        fi
        PERM_TOUCHED=0
        note "consume root restored to what it was before this run ($PERM_BEFORE)"
    fi
    return 0
}

start_fault renamer-permdenied FN_PERM_HOLD=60s || exit 1
wait_healthy renamer-permdenied 180 || true
WHO8="$(docker inspect -f '{{.Config.User}}' "$(compose --profile fault ps -q renamer-permdenied | head -1)" 2>/dev/null || echo unknown)"
MODE_BEFORE8="$(in_storage "stat -c %a $PERM_ROOT 2>/dev/null || echo unknown")"
PERM_BEFORE="$(in_storage "stat -c '%a:%u:%g' $PERM_ROOT 2>/dev/null || echo unknown")"
if [ "$PERM_BEFORE" = "unknown" ]; then
    bad "the consume root's mode and owner could not be read; not revoking anything"
    PERM_BEFORE=""
fi

submit "$DOC8" 4096
await_registered "$DOC8" 90 || bad "the permission probe was never registered"
JOB8="$(psqlq "SELECT job_id FROM jobs WHERE source_name = '$DOC8';")"

# Wait until the attempt is demonstrably paused holding its claim, with the
# destination still writable. Revoking before that would deny something else.
_i=0
HOLDER8=""
while [ "$_i" -lt 90 ]; do
    HOLDER8="$(psqlq "SELECT coalesce(publish_claimed_by,'') FROM jobs WHERE job_id = '$JOB8';")"
    if [ -n "$HOLDER8" ]; then break; fi
    sleep 1; _i=$((_i + 1))
done
PAUSED8="$(compose --profile fault logs renamer-permdenied 2>/dev/null | grep -c fault_point_paused || true)"
[ -n "$HOLDER8" ] || bad "the attempt never reached the publication boundary, so nothing could be denied there"
[ "${PAUSED8:-0}" -ge 1 ] || bad "the attempt never paused, so the mode change could not land inside its publication"

# Revoke write, as root, on a directory the renamer does not own.
compose run --rm --no-deps -T --user 0 --entrypoint sh storage-init \
    -c "chown 0:0 $PERM_ROOT && chmod 0555 $PERM_ROOT" >/dev/null 2>&1 && PERM_TOUCHED=1
MODE_AFTER8="$(in_storage "stat -c '%a-uid%u' $PERM_ROOT 2>/dev/null || echo unknown")"

STATE8="$(await_state "$DOC8" "delivered held uncertain" 240)"
CAT8="$(psqlq "SELECT coalesce(failure_category,'-') FROM jobs WHERE job_id = '$JOB8';")"
RECEIPTS8="$(psqlq "SELECT count(*) FROM delivery_receipts WHERE job_id = '$JOB8';")"
PUB8="$(in_storage "ls -1 $PERM_ROOT 2>/dev/null | grep -c 'perm-denied-$LOWER' || true")"
SRC8="$(in_storage "test -f '/srv/fn/incoming/$DOC8' && echo yes || echo no")"
SRCSIZE8="$(in_storage "wc -c < '/srv/fn/incoming/$DOC8' 2>/dev/null || echo 0")"
DENIED8="$(compose --profile fault logs renamer-permdenied 2>/dev/null | grep -c 'permission_denied' || true)"

emit "8. permission denied at the publication"
emit "   NOTE: the hold (60s) deliberately outlives the handler budget (the 20s"
emit "   shutdown timeout), so the refusal is established after the attempt's"
emit "   own deadline has passed. That is the case that used to lose the"
emit "   outcome: every ledger write failed, the job stayed 'publishing', and"
emit "   the next delivery settled it as 'uncertain' -- a publication the kernel"
emit "   had plainly refused became one needing a person. The expectations below"
emit "   therefore test the denial AND that a determined outcome survives the"
emit "   deadline that governed the attempt."
emit "   the renamer runs as:          ${WHO8:-unknown}   (not root: a root writer would not be denied)"
emit "   destination mode before:      $MODE_BEFORE8 (owned by the runtime account)"
emit "   destination mode during:      $MODE_AFTER8   (0555, root-owned, revoked mid-publication)"
emit "   the attempt was paused:       $PAUSED8 log line(s), claim held by ${HOLDER8:-none}"
emit "   final state:                  $STATE8   (expected held)"
emit "   category:                     $CAT8   (expected permission_denied)"
emit "   denial recorded in its log:   $DENIED8 line(s)"
emit "   documents published:          $PUB8   (expected 0)"
emit "   delivery receipts:            $RECEIPTS8   (expected 0)"
emit "   source preserved:             $SRC8 ($SRCSIZE8 bytes)"
if [ "$WHO8" = "0" ] || [ "$WHO8" = "root" ] || [ "$WHO8" = "0:0" ]; then
    bad "the renamer runs as root, so a mode-based denial proves nothing"
fi
[ "$STATE8" = "held" ] || bad "a refused publication reached '$STATE8'"
[ "$CAT8" = "permission_denied" ] || bad "category is '$CAT8', not permission_denied"
[ "${PUB8:-0}" = "0" ] || bad "$PUB8 documents were published despite the denial"
[ "${RECEIPTS8:-0}" = "0" ] || bad "a receipt was written for a publication the kernel refused"
[ "$SRC8" = "yes" ] || bad "the source was removed after a refused publication"
[ "${SRCSIZE8:-0}" = "4096" ] || bad "the source is $SRCSIZE8 bytes, not the 4096 submitted"

restore_perm
# A write refused mid-publication cannot clean up after itself: removing the
# staged temporary needs the same directory permission the kernel just took
# away. What it leaves is reported, and removed here, rather than passed over.
LEFT8="$(in_storage "ls -1a $PERM_ROOT 2>/dev/null | grep -c \"^\\.fn-$JOB8\\.\" || true")"
OTHER8="$(in_storage "ls -1a $PERM_ROOT 2>/dev/null | grep -c '^\.fn-' || true")"
emit "   staged temporaries stranded by the denial (this job): $LEFT8"
emit "   temporaries in the destination from other work:      $OTHER8   (reported, not touched)"
emit "   (cleanup needs the same permission the denial removed, so this is a"
emit "    real consequence of the fault, not a leak in the ordinary path. Only"
emit "    this job's are removed: the earlier glob took every .fn-* in the real"
emit "    consume root, including artifacts this invocation never created.)"
# Scoped to this job's temporaries. The glob removed every .fn-* in the real
# consume root, including ones this invocation never created -- the very
# artifacts the scenario above reports as belonging to other actors.
in_storage "rm -f $PERM_ROOT/.fn-$JOB8.* 2>/dev/null; true" >/dev/null 2>&1 || true
drop_fault renamer-permdenied
emit ""

emit "Retry budget: scenario 7 is the demonstration. An out-of-space write is a"
emit "RETRYABLE storage failure that never succeeds, so the delivery is returned"
emit "and redelivered until the durable budget is spent and the job is held. An"
emit "immediate destination_conflict, which the previous round offered as"
emit "evidence, settles on the first attempt and never touches the budget."
emit "   attempts recorded in scenario 7: $ATTEMPTS7 against a budget of 2"
emit ""

# ---------------------------------------------------------------------------
# 9. A10, completed: the SOURCE and the DESTINATION on different filesystems.
#
# Scenario 6 crosses staging/consume, which is the boundary the copy fallback
# actually depends on. The acceptance criterion names a different one --
# incoming to consume -- and the two are not interchangeable, so this publishes
# into a tmpfs-backed destination and compares the bytes that arrive with the
# bytes that were submitted.
# ---------------------------------------------------------------------------
log "9/9: A10 — incoming and consume on different filesystems, bytes compared"
stop_ordinary
TMPFS_DEST="/srv/fn/consume-tmpfs"
DOC9="a10-crossfs-$STAMP.pdf"
NAME9="a10-crossfs-$LOWER.pdf"

# The watcher records the destination a submission is accepted for, so it has
# to be pointed at the same destination as the renamer that will publish it.
# The restore trap recreates it from the plain environment afterwards.
(
    export FN_STORAGE_CONSUME="$TMPFS_DEST"
    compose up -d --force-recreate --wait --wait-timeout 180 watcher >/dev/null 2>&1
) || true
WATCHER_OVERRIDDEN=1
start_fault renamer-tmpfsdest || exit 1
wait_healthy renamer-tmpfsdest 180 || true

submit "$DOC9" 300000
STATE9="$(await_state "$DOC9" "delivered held uncertain" 240)"
DEV_IN9="$(in_storage "stat -c %d /srv/fn/incoming")"
DEV_OUT9="$(in_storage "stat -c %d $TMPFS_DEST")"
SRC_SUM9="$(in_storage "sha256sum '/srv/fn/incoming/$DOC9' 2>/dev/null | cut -c1-64")"
PUB_SUM9="$(in_storage "sha256sum '$TMPFS_DEST/$NAME9' 2>/dev/null | cut -c1-64")"
PUB_SIZE9="$(in_storage "wc -c < '$TMPFS_DEST/$NAME9' 2>/dev/null || echo 0")"
RECEIPT_SUM9="$(psqlq "SELECT encode(r.content_fingerprint,'hex') FROM delivery_receipts r JOIN jobs j USING (job_id) WHERE j.source_name = '$DOC9';")"
PUB_COUNT9="$(in_storage "ls -1 $TMPFS_DEST 2>/dev/null | grep -c '^a10-crossfs-$LOWER' || true")"
TMP_LEFT9="$(in_storage "ls -1a $TMPFS_DEST 2>/dev/null | grep -c '^\.fn-' || true")"

emit "9. A10 — the source and the destination on different filesystems"
emit "   device behind incoming:       $DEV_IN9"
emit "   device behind the destination:$DEV_OUT9   (a tmpfs: a different filesystem)"
emit "   final state:                  $STATE9   (expected delivered)"
emit "   published copies:             $PUB_COUNT9   (expected 1)"
emit "   published size:               $PUB_SIZE9 bytes   (expected 300000: a complete copy)"
emit "   source sha256:                ${SRC_SUM9:-unreadable}"
emit "   published sha256:             ${PUB_SUM9:-unreadable}   (must equal the source)"
emit "   receipt records:              ${RECEIPT_SUM9:-none}   (must equal both)"
emit "   staged temporaries left:      $TMP_LEFT9   (expected 0: nothing partial is visible)"
[ "$DEV_IN9" != "$DEV_OUT9" ] || bad "incoming and the destination are the same filesystem; the case was not exercised"
[ "$STATE9" = "delivered" ] || bad "publication across the boundary reached '$STATE9'"
[ "${PUB_COUNT9:-0}" = "1" ] || bad "$PUB_COUNT9 copies exist for one submission"
[ "${PUB_SIZE9:-0}" = "300000" ] || bad "the published document is $PUB_SIZE9 bytes, not the 300000 submitted"
[ -n "$SRC_SUM9" ] && [ "$SRC_SUM9" = "$PUB_SUM9" ] || bad "the published bytes differ from the source bytes"
[ "$PUB_SUM9" = "$RECEIPT_SUM9" ] || bad "the receipt records a different fingerprint than the file on disk"
[ "${TMP_LEFT9:-0}" = "0" ] || bad "$TMP_LEFT9 staged temporaries are visible in the destination"
drop_fault renamer-tmpfsdest
emit ""

emit "mismatches: $FAILURES"

if [ "$FAILURES" -ne 0 ]; then
    echo >&2
    echo "FAILED: $FAILURES expectation(s) not met; see $OUT" >&2
    exit 1
fi

log "PASSED: interruption, competing publication, stale worker, filesystem boundary, write failures"
note "evidence: $OUT"
