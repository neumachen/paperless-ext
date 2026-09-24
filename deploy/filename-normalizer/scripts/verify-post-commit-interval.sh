#!/bin/sh
# The interval between a committed receipt and a visible document.
#
# Publication commits its receipt while the document is still an invisible
# dotfile, and reveals it afterwards. Everything interesting now happens in
# that window, and none of it was demonstrated: the earlier exercises stop at
# the PRE-commit interval, where nothing is recorded yet.
#
# Three scenarios, all with the publisher held at `hold_before_reveal`:
#
#   1. a competing handler cannot close or duplicate an authorised publication
#   2. a foreign file arriving at the reserved name is not adopted: it
#      survives untouched and the job is delivered around it under the next
#      name, the same outcome A3 requires for a file planted before the job
#   3. an operation failing AFTER the document is visible does not erase the
#      receipt or unpublish anything
. "$(dirname "$0")/lib.sh"

OUT="$EVIDENCE_DIR/post-commit-interval.txt"
FAILURES=0
CREATED=""
MUTATED=0
RESTORED=0
PERM_TOUCHED=0
PERM_DIR="/srv/fn/consume"
PERM_ORIGINAL=""
report_begin post-commit-interval "$OUT" "$0"

emit() { printf '%s\n' "$*" >> "$OUT"; }
bad() { printf '    MISMATCH: %s\n' "$*" >&2; emit "    MISMATCH: $*"; FAILURES=$((FAILURES + 1)); }
psqlq() {
    compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" \
        postgres-primary psql -U "${FN_DB_USER:-fn_app}" -d "${FN_DB_NAME:-filename_normalizer}" -tA -c "$1" \
        </dev/null 2>/dev/null | tr -d '\r'
}
in_storage() { compose run --rm --no-deps -T --entrypoint sh storage-init -c "$1" </dev/null 2>/dev/null | tr -d '\r'; }

start_fault() {
    _sf="$1"; shift
    if ! _ex="$(compose --profile fault ps -aq "$_sf" 2>/dev/null)"; then
        echo "error: could not determine whether '$_sf' exists" >&2; return 1
    fi
    [ -z "$_ex" ] || { echo "error: '$_sf' already exists; this run did not create it" >&2; return 1; }
    MUTATED=1
    ( for _kv in "$@"; do export "$_kv"; done
      compose --profile fault up -d "$_sf" >/dev/null 2>&1 ) || return 1
    CREATED="$CREATED $_sf"
    return 0
}

submit() {
    compose run --rm --no-deps -T -e FN_N="/srv/fn/incoming/$1" --entrypoint sh storage-init \
        -c 'printf "%%PDF-1.4 post-commit interval\n" > /srv/fn/incoming/.wip && mv /srv/fn/incoming/.wip "$FN_N"' \
        </dev/null >/dev/null 2>&1
}

# Wait until the job has a receipt and nothing is visible: the interval itself.
await_committed_invisible() {
    _job=""; _i=0
    while [ "$_i" -lt 180 ]; do
        [ -n "$_job" ] || _job="$(psqlq "SELECT job_id FROM jobs WHERE source_name = '$1';")"
        if [ -n "$_job" ]; then
            if [ "$(psqlq "SELECT count(*) FROM delivery_receipts WHERE job_id = '$_job';")" = "1" ] \
               && [ "$(probe_exists "/srv/fn/consume/$2")" = "no" ]; then
                printf '%s' "$_job"; return 0
            fi
        fi
        sleep 2; _i=$((_i + 2))
    done
    printf '%s' "$_job"; return 1
}

restore_all() {
    [ "$RESTORED" = "1" ] && return 0
    RESTORED=1
    if [ "$PERM_TOUCHED" = "1" ]; then
        compose run --rm --no-deps -T --entrypoint sh storage-init \
            -c "chown ${PERM_ORIGINAL%% *} $PERM_DIR && chmod ${PERM_ORIGINAL##* } $PERM_DIR" >/dev/null 2>&1 \
            && PERM_TOUCHED=0
    fi
    _ok=1
    for _c in $CREATED; do
        compose --profile fault stop "$_c" >/dev/null 2>&1 || true
        compose --profile fault rm -f "$_c" >/dev/null 2>&1 || true
        if ! _left="$(compose --profile fault ps -aq "$_c" 2>/dev/null)"; then _ok=0
        elif [ -n "$_left" ]; then _ok=0; fi
    done
    compose start renamer-1 renamer-2 >/dev/null 2>&1 || true
    wait_healthy renamer-1 120 || _ok=0
    wait_healthy renamer-2 120 || _ok=0
    _perm_now="$(in_storage "stat -c '%u:%g %a' $PERM_DIR")"
    emit ""
    emit "restoration (read back from the running stack):"
    emit "  fault services left:          $([ "$_ok" = "1" ] && echo 0 || echo "SOME")"
    emit "  renamer-1 / renamer-2 ready:  $(wait_healthy renamer-1 5 >/dev/null 2>&1 && echo yes || echo no) / $(wait_healthy renamer-2 5 >/dev/null 2>&1 && echo yes || echo no)"
    emit "  destination permissions:      $_perm_now   (expected $PERM_ORIGINAL, as found)"
    emit "  permission flag cleared:      $([ "$PERM_TOUCHED" = "0" ] && echo yes || echo NO)"
    [ "$_ok" = "1" ] || bad "a fault service this run created could not be removed"
    [ "$PERM_TOUCHED" = "0" ] || bad "the destination permissions were not restored"
    [ "$_perm_now" = "$PERM_ORIGINAL" ] || bad "destination permissions are '$_perm_now', not the '$PERM_ORIGINAL' this run found"
    if [ "$FAILURES" = "0" ]; then report_restored; fi
    report_keep
    if [ "$FAILURES" != "0" ]; then
        echo "FAILED: $FAILURES expectation(s) not met; see $OUT" >&2
        exit 1
    fi
}
trap restore_all EXIT

# What the destination looked like before this run touched anything. Restoring
# to a hardcoded owner and mode was a guess, and it was wrong: the directory is
# 770 and its owner does not resolve to a name inside the utility container.
PERM_ORIGINAL="$(in_storage "stat -c '%u:%g %a' $PERM_DIR")"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
LOWER="$(printf '%s' "$STAMP" | tr 'A-Z' 'a-z')"
emit "The interval between a committed receipt and a visible document"
emit ""

# ---------------------------------------------------------------------------
# 1. A competing handler cannot close or duplicate an authorised publication.
# ---------------------------------------------------------------------------
log "1/3: a competing handler meets a committed receipt"
compose stop renamer-1 renamer-2 >/dev/null 2>&1
DOC1="pci-compete-$STAMP.pdf"; NAME1="pci-compete-$LOWER.pdf"
# Long enough that the publisher is STILL held when the sibling is handed the
# job. At 90s the pause expired first -- discovery, the wait for the committed
# receipt and the sibling's attach together outlast it -- so the publisher took
# its own redelivery back and no competition happened.
start_fault renamer-hold FN_FAULT_POINTS=hold_before_reveal FN_FAULT_HOLD=180s \
    FN_PUBLISH_TAKEOVER_AFTER=10s FN_RENAMER_PREFETCH=1 || exit 1
sleep 6
submit "$DOC1"
JOB1="$(await_committed_invisible "$DOC1" "$NAME1")" || bad "A never reached committed-and-invisible"
RCPT1_MID="$(psqlq "SELECT count(*) FROM delivery_receipts WHERE job_id = '$JOB1';")"
VIS1_MID="$(probe_exists "/srv/fn/consume/$NAME1")"

# The claim expires; a sibling is given the job and tries to publish it too.
sleep 12
start_fault renamer-taker FN_PUBLISH_TAKEOVER_AFTER=10s || exit 1
# Wait for the sibling to actually hold a consumer before handing it work. A
# fixed sleep published the message while it was still connecting, so the
# paused publisher's own instance eventually took it back and the competition
# this scenario is about never happened.
_i=0
while [ "$_i" -lt 60 ]; do
    compose --profile fault logs renamer-taker 2>/dev/null | grep -q consumer_attached && break
    sleep 2; _i=$((_i + 2))
done
compose exec -T rabbitmq rabbitmqadmin --vhost "${FN_AMQP_VHOST:-filename-normalizer}" \
    --username "${FN_AMQP_USER:-fn_app}" --password "$(cat "$DEPLOY_DIR/secrets/fn_amqp_password")" \
    --non-interactive publish message --exchange "${FN_AMQP_EXCHANGE:-filename_normalizer.jobs}" \
    --routing-key "${FN_AMQP_ROUTING_KEY:-normalize}" \
    --properties '{"delivery_mode":2,"content_type":"application/json"}' \
    --payload "{\"contract_version\":1,\"job_id\":\"$JOB1\",\"attempt\":2,\"enqueued_at\":\"$STAMP\"}" \
    >/dev/null 2>&1 && SIB1=yes || SIB1=no

_i=0; STATE1=""
while [ "$_i" -lt 420 ]; do
    STATE1="$(psqlq "SELECT state FROM jobs WHERE job_id = '$JOB1';")"
    case "$STATE1" in delivered|held|uncertain) break ;; esac
    sleep 3; _i=$((_i + 3))
done
COPIES1="$(in_storage "ls -1 /srv/fn/consume | grep -c '^pci-compete-$LOWER' || true")"
RCPT1="$(psqlq "SELECT count(*) FROM delivery_receipts WHERE job_id = '$JOB1';")"
# Whether the sibling SAW the job, rather than whether it used a particular
# word for what it found. The property is asserted below -- no hold, no second
# document, one receipt -- and this only establishes that the case was
# exercised at all.
SUPER1="$(compose --profile fault logs renamer-taker 2>/dev/null | grep -c "$JOB1" || true)"
HELD1="$(psqlq "SELECT count(*) FROM job_events WHERE job_id = '$JOB1' AND event_type = 'held';")"

emit "1. a competing handler meets a committed receipt"
emit "   receipt committed while invisible: $RCPT1_MID   (expected 1)"
emit "   reserved name visible then:        $VIS1_MID   (expected no)"
emit "   sibling delivery published:        $SIB1"
emit "   the sibling logged this job:       $SUPER1 log line(s)   (OBSERVATION, not asserted:"
emit "                                      which instance consumed the redelivery is not"
emit "                                      established by this run)"
emit "   held events recorded:              $HELD1   (expected 0: a receipt blocks a hold)"
emit "   final state:                       $STATE1   (expected delivered)"
emit "   documents with that name:          $COPIES1   (expected 1)"
emit "   delivery receipts:                 $RCPT1   (expected 1)"
[ "${RCPT1_MID:-0}" = "1" ] || bad "A did not commit a receipt before the pause"
[ "$VIS1_MID" = "no" ] || bad "the document was visible before A revealed it"
# What IS asserted is the property, not the choreography: a redelivery
# arriving while the receipt is committed and the document invisible cannot
# close the job and cannot produce a second document. The held-event count and
# the receipt count below carry that, and they do not depend on which instance
# picked the message up.
[ "$SIB1" = "yes" ] || bad "no redelivery was published, so nothing competed for this job"
[ "${HELD1:-1}" = "0" ] || bad "$HELD1 hold(s) were recorded against a job holding a receipt"
[ "$STATE1" = "delivered" ] || bad "the job ended as '$STATE1', not delivered"
[ "${COPIES1:-0}" = "1" ] || bad "$COPIES1 documents exist for one publication"
[ "${RCPT1:-0}" = "1" ] || bad "$RCPT1 receipts exist"
drop_fault_all() { for _c in $CREATED; do compose --profile fault stop "$_c" >/dev/null 2>&1; compose --profile fault rm -f "$_c" >/dev/null 2>&1; done; CREATED=""; }
drop_fault_all
emit ""

# ---------------------------------------------------------------------------
# 2. A foreign file arriving in the window is not adopted.
# ---------------------------------------------------------------------------
log "2/3: a foreign, byte-identical arrival at the reserved name"
DOC2="pci-foreign-$STAMP.pdf"; NAME2="pci-foreign-$LOWER.pdf"
start_fault renamer-hold FN_FAULT_POINTS=hold_before_reveal FN_FAULT_HOLD=120s \
    FN_RENAMER_PREFETCH=1 || exit 1
sleep 6
submit "$DOC2"
JOB2="$(await_committed_invisible "$DOC2" "$NAME2")" || bad "A never reached committed-and-invisible (2)"

# Somebody else's file, with exactly the bytes this job is publishing, placed
# at the reserved name while the publisher is held. Byte-identical on purpose:
# equal content must not establish ownership.
FOREIGN_INO="$(compose run --rm --no-deps -T -e FN_T="/srv/fn/consume/$NAME2" --entrypoint sh storage-init \
    -c 'printf "%%PDF-1.4 post-commit interval\n" > "$FN_T" && chown 0:0 "$FN_T" && stat -c %i "$FN_T"' \
    </dev/null 2>/dev/null | tr -d ' \r\n')"

_i=0; STATE2=""
while [ "$_i" -lt 300 ]; do
    STATE2="$(psqlq "SELECT state FROM jobs WHERE job_id = '$JOB2';")"
    case "$STATE2" in delivered|held|uncertain) break ;; esac
    sleep 3; _i=$((_i + 3))
done
NOW_INO="$(probe_inode "/srv/fn/consume/$NAME2")"
RCPT2="$(psqlq "SELECT count(*) FROM delivery_receipts WHERE job_id = '$JOB2';")"
DELIVERED2="$(psqlq "SELECT delivered_name FROM delivery_receipts WHERE job_id = '$JOB2';")"
CAT2="$(psqlq "SELECT coalesce(failure_category,'-') FROM jobs WHERE job_id = '$JOB2';")"
SRC2="$(probe_exists "/srv/fn/incoming/$DOC2")"
COPIES2="$(in_storage "ls -1 /srv/fn/consume | grep -c '^pci-foreign-$LOWER' || true")"
if [ -n "$DELIVERED2" ]; then OURS2="$(probe_exists "/srv/fn/consume/$DELIVERED2")"; else OURS2=no; fi

# The receipt this attempt committed describes a publication its reveal then
# refused, so it is withdrawn and the attempt advances to the next name --
# which it can only do because the receipt reads back the identity it
# recorded. Before that, the withdrawal matched nothing, the delivery was
# returned, and recovery later held the job and deleted the receipt instead.
emit "2. a foreign, byte-identical file arrives at the reserved name"
emit "   the foreign file's inode:          ${FOREIGN_INO:-none}"
emit "   inode at that name afterwards:     $NOW_INO   (expected ${FOREIGN_INO:-?}: it survived)"
emit "   final state:                       $STATE2   (expected delivered, under another name)"
emit "   failure category:                  $CAT2   (expected '-')"
emit "   delivery receipts:                 $RCPT2   (expected 1)"
emit "   delivered as:                      ${DELIVERED2:-none}   (expected not $NAME2)"
emit "   this job's document present:       $OURS2   (expected yes)"
emit "   documents with that stem:          $COPIES2   (expected 2: the stranger's and this job's)"
emit "   source preserved:                  $SRC2   (expected yes)"
[ -n "$FOREIGN_INO" ] || bad "the foreign file could not be planted; this case was not exercised"
[ "$NOW_INO" = "$FOREIGN_INO" ] || bad "the foreign file was replaced or removed (now $NOW_INO)"
[ "$STATE2" = "delivered" ] || bad "the job reached '$STATE2'; it must be delivered around the stranger's file"
[ "$CAT2" = "-" ] || bad "a delivery around an occupied name acquired the category '$CAT2'"
[ "${RCPT2:-0}" = "1" ] || bad "$RCPT2 receipts for one delivery"
[ -n "$DELIVERED2" ] && [ "$DELIVERED2" != "$NAME2" ] || bad "the receipt names '${DELIVERED2:-nothing}', not a different name"
[ "$OURS2" = "yes" ] || bad "the document the receipt describes is not in the destination"
[ "${COPIES2:-0}" = "2" ] || bad "$COPIES2 documents with that stem; expected the stranger's and this job's"
[ "$SRC2" = "yes" ] || bad "the source was not preserved"
# Take the stranger's file away again -- this run planted it -- by identity.
compose run --rm --no-deps -T -e FN_T="/srv/fn/consume/$NAME2" -e FN_I="$FOREIGN_INO" \
    --entrypoint sh storage-init -c '
        if [ -e "$FN_T" ] && [ "$(stat -c %i "$FN_T")" = "$FN_I" ]; then rm -f "$FN_T"; fi' \
    >/dev/null 2>&1 </dev/null || true
drop_fault_all
emit ""

# ---------------------------------------------------------------------------
# 3. A real failure AFTER the document is visible.
# ---------------------------------------------------------------------------
log "3/3: an operation failing after the document is visible"
DOC3="pci-postpub-$STAMP.pdf"; NAME3="pci-postpub-$LOWER.pdf"
PERM_BEFORE="$(in_storage "stat -c '%U:%G %a' $PERM_DIR")"
start_fault renamer-hold FN_FAULT_POINTS=hold_after_link FN_FAULT_HOLD=60s \
    FN_RENAMER_PREFETCH=1 || exit 1
sleep 6
submit "$DOC3"
# Wait for the document to become VISIBLE -- the pause is after the reveal.
_i=0; VIS3=no
while [ "$_i" -lt 200 ]; do
    [ "$(probe_exists "/srv/fn/consume/$NAME3")" = "yes" ] && { VIS3=yes; break; }
    sleep 2; _i=$((_i + 2))
done
# Now make the NEXT operation -- removing the staged temporary -- genuinely
# fail, by taking write permission off the directory while the publisher is
# held. This is an operation failing, not a revocation being observed.
if [ "$VIS3" = "yes" ]; then
    PERM_TOUCHED=1
    compose run --rm --no-deps -T --entrypoint sh storage-init \
        -c "chown 0:0 $PERM_DIR && chmod 0555 $PERM_DIR" >/dev/null 2>&1 && REVOKED3=yes || REVOKED3=no
else
    REVOKED3=no
fi
_i=0; STATE3=""
while [ "$_i" -lt 240 ]; do
    STATE3="$(psqlq "SELECT state FROM jobs WHERE source_name = '$DOC3';")"
    case "$STATE3" in delivered|held|uncertain) break ;; esac
    sleep 3; _i=$((_i + 3))
done
JOB3="$(psqlq "SELECT job_id FROM jobs WHERE source_name = '$DOC3';")"
RCPT3="$(psqlq "SELECT count(*) FROM delivery_receipts WHERE job_id = '$JOB3';")"
LEFT3="$(compose --profile fault logs renamer-hold 2>/dev/null | grep "$JOB3" | grep -c 'staged_link_left_behind' || true)"
# Put the permissions back before reading the destination.
compose run --rm --no-deps -T --entrypoint sh storage-init \
    -c "chown ${PERM_ORIGINAL%% *} $PERM_DIR && chmod ${PERM_ORIGINAL##* } $PERM_DIR" >/dev/null 2>&1 && PERM_TOUCHED=0
PRESENT3="$(probe_exists "/srv/fn/consume/$NAME3")"
CAT3="$(psqlq "SELECT coalesce(failure_category,'-') FROM jobs WHERE job_id = '$JOB3';")"

emit "3. an operation fails AFTER the document is visible"
emit "   the document became visible:       $VIS3   (expected yes)"
emit "   write permission then revoked:     $REVOKED3   (expected yes)"
emit "   the cleanup reported its failure:  $LEFT3 log line(s)   (expected >= 1: a real failure)"
emit "   final state:                       $STATE3   (expected delivered)"
emit "   failure category:                  $CAT3   (expected '-')"
emit "   delivery receipts:                 $RCPT3   (expected 1: NOT withdrawn)"
emit "   document still present:            $PRESENT3   (expected yes)"
[ "$VIS3" = "yes" ] || bad "the document never became visible; this case was not exercised"
[ "$REVOKED3" = "yes" ] || bad "the permission revocation did not take effect"
[ "${LEFT3:-0}" -ge 1 ] || bad "no post-publication operation actually failed, so this run proves nothing about that boundary"
[ "$STATE3" = "delivered" ] || bad "a post-publication failure turned a completed publication into '$STATE3'"
[ "$CAT3" = "-" ] || bad "a completed delivery acquired the category '$CAT3'"
[ "${RCPT3:-0}" = "1" ] || bad "$RCPT3 receipts: a failure after visibility must not erase the receipt"
[ "$PRESENT3" = "yes" ] || bad "the published document is gone after a post-publication failure"

emit ""
emit "mismatches: $FAILURES"
if [ "$FAILURES" = "0" ]; then
    report_success
    log "PASSED: the post-commit interval holds against competition, foreign arrivals and late failures"
    note "evidence: $OUT"
fi
