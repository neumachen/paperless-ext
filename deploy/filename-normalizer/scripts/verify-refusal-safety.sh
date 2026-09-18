#!/bin/sh
# A refusal must change nothing, and cleanup must touch only what it created.
#
# # Why this exists
#
# Every exercise in this directory refuses to run in situations it cannot make
# safe: another exercise holds the lock, a service it would later destroy
# already exists, a file it would later delete is already there. Those refusals
# were printed correctly and then followed by mutation anyway:
#
#   * the consumer exercise deleted three fixed volume names unconditionally,
#     so a pre-existing Paperless instance -- exactly what the refusal is
#     about -- lost its database and media;
#   * the configuration exercise installed its restore trap BEFORE its
#     preflight, so a refusal was followed by force-recreating all three
#     application services onto hardcoded defaults, and by deleting the file it
#     had just refused to overwrite;
#   * the dry-run exercise took no lock at all and adopted, then destroyed, a
#     service it had not created.
#
# Those are fixed. This exercise is the evidence, and it is deliberately
# adversarial: it puts each exercise in the situation it must refuse, and then
# checks that the stack is byte-for-byte the way it was found.
. "$(dirname "$0")/lib.sh"

OUT="$EVIDENCE_DIR/refusal-safety.txt"
mkdir -p "$EVIDENCE_DIR"
FAILURES=0
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
DECOY_VOL="${PROJECT}_paperless-data"
DECOY_CREATED=0
LOCK_HELD=0

emit() { printf '%s\n' "$*" >> "$OUT"; printf '%s\n' "$*"; }
bad() { printf '    MISMATCH: %s\n' "$*" >&2; emit "    MISMATCH: $*"; FAILURES=$((FAILURES + 1)); }

in_storage() {
    compose run --rm --no-deps -T --entrypoint sh storage-init -c "$1" 2>/dev/null | tr -d ' \r\n'
}

# The state that must survive a refusal, read from the running processes and
# from the filesystem rather than from what the scripts say they did.
snapshot() {
    printf 'renamer1_env=%s\n' "$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' \
        "$(compose ps -q renamer-1 2>/dev/null | head -1)" 2>/dev/null \
        | sed -n 's/^FN_CONFIG_FILE=//p' | head -1)"
    printf 'renamer1_consume=%s\n' "$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' \
        "$(compose ps -q renamer-1 2>/dev/null | head -1)" 2>/dev/null \
        | sed -n 's/^FN_STORAGE_CONSUME=//p' | head -1)"
    printf 'watcher_env=%s\n' "$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' \
        "$(compose ps -q watcher 2>/dev/null | head -1)" 2>/dev/null \
        | sed -n 's/^FN_CONFIG_FILE=//p' | head -1)"
    printf 'renamer1_id=%s\n' "$(compose ps -q renamer-1 2>/dev/null | head -1)"
    printf 'renamer2_id=%s\n' "$(compose ps -q renamer-2 2>/dev/null | head -1)"
    printf 'watcher_id=%s\n' "$(compose ps -q watcher 2>/dev/null | head -1)"
    printf 'volumes=%s\n' "$(docker volume ls -q --filter "name=^${PROJECT}_" 2>/dev/null | sort | tr '\n' ',')"
    printf 'consume_perm=%s\n' "$(in_storage "stat -c '%a:%u:%g' /srv/fn/consume")"
    printf 'consume_entries=%s\n' "$(in_storage "ls -1 /srv/fn/consume 2>/dev/null | wc -l")"
    printf 'live_config=%s\n' "$(cksum "$DEPLOY_DIR/config/normalizer.json" 2>/dev/null | awk '{print $1}')"
}

cleanup() {
    if [ "$LOCK_HELD" = "1" ]; then
        rm -f "$EVIDENCE_DIR/.exercise.lock/owner" 2>/dev/null || true
        rmdir "$EVIDENCE_DIR/.exercise.lock" 2>/dev/null || true
        LOCK_HELD=0
    fi
    if [ "$DECOY_CREATED" = "1" ]; then
        docker volume rm "$DECOY_VOL" >/dev/null 2>&1 || true
        DECOY_CREATED=0
    fi
}
trap 'cleanup' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

: > "$OUT"
emit "A refusal changes nothing"
emit ""

BEFORE="$(snapshot)"

# ---------------------------------------------------------------------------
# 1. Every exercise refuses while another holds the lock, and mutates nothing.
# ---------------------------------------------------------------------------
log "1/2: each exercise must refuse while the lock is held, without touching anything"
mkdir -p "$EVIDENCE_DIR/.exercise.lock" 2>/dev/null
printf 'refusal-safety pid=%s at=%s\n' "$$" "$STAMP" > "$EVIDENCE_DIR/.exercise.lock/owner"
LOCK_HELD=1
emit "1. the lock is held by this exercise; each of the four is invoked"

for _s in verify-recovery verify-consumer verify-config-restart verify-dry-run; do
    _log="$EVIDENCE_DIR/refusal-$_s.log"
    if sh "$SCRIPT_DIR/$_s.sh" > "$_log" 2>&1; then
        _rc=0
    else
        _rc=$?
    fi
    _refused="$(grep -c 'holds the lock' "$_log" 2>/dev/null || true)"
    emit "   $_s: exit $_rc, lock refusal messages: ${_refused:-0}"
    [ "$_rc" != "0" ] || bad "$_s ran to completion while another exercise held the lock"
    [ "${_refused:-0}" -ge 1 ] || bad "$_s did not say it was refused because of the lock"
done

rm -f "$EVIDENCE_DIR/.exercise.lock/owner"
rmdir "$EVIDENCE_DIR/.exercise.lock"
LOCK_HELD=0

AFTER_LOCK="$(snapshot)"
if [ "$BEFORE" = "$AFTER_LOCK" ]; then
    emit "   the stack is unchanged: same containers, same environments, same"
    emit "   volumes, same destination permissions, same live configuration"
else
    bad "the stack changed while four exercises were refusing to run"
    emit "   BEFORE:"; printf '%s\n' "$BEFORE" | sed 's/^/     /' >> "$OUT"
    emit "   AFTER:";  printf '%s\n' "$AFTER_LOCK" | sed 's/^/     /' >> "$OUT"
fi
emit ""

# ---------------------------------------------------------------------------
# 2. The consumer exercise refuses a pre-existing instance AND leaves its state
#    alone. This is the one that used to destroy a database on the way past.
# ---------------------------------------------------------------------------
log "2/2: a pre-existing consumer volume must survive the refusal"
if [ -n "$(docker volume ls -q --filter "name=^${DECOY_VOL}$" 2>/dev/null)" ]; then
    emit "2. SKIPPED: $DECOY_VOL already exists and is not this exercise's to use"
else
    docker volume create "$DECOY_VOL" >/dev/null 2>&1 && DECOY_CREATED=1
    docker run --rm -v "$DECOY_VOL:/v" "$UTIL_IMAGE" \
        sh -c 'printf "a pre-existing library\n" > /v/precious.txt' >/dev/null 2>&1 || true
    MARKER_BEFORE="$(docker run --rm -v "$DECOY_VOL:/v" "$UTIL_IMAGE" \
        sh -c 'cat /v/precious.txt 2>/dev/null' 2>/dev/null | tr -d '\r\n')"

    # A service container that exists is what the consumer exercise refuses on.
    compose --profile consumer create paperless-redis >/dev/null 2>&1 || true

    _log="$EVIDENCE_DIR/refusal-consumer-preexisting.log"
    if sh "$SCRIPT_DIR/verify-consumer.sh" > "$_log" 2>&1; then _rc=0; else _rc=$?; fi
    _refused="$(grep -c 'did not create it' "$_log" 2>/dev/null || true)"

    MARKER_AFTER="$(docker run --rm -v "$DECOY_VOL:/v" "$UTIL_IMAGE" \
        sh -c 'cat /v/precious.txt 2>/dev/null' 2>/dev/null | tr -d '\r\n')"
    VOL_AFTER="$(docker volume ls -q --filter "name=^${DECOY_VOL}$" 2>/dev/null)"

    emit "2. a pre-existing consumer service and volume"
    emit "   verify-consumer exit:         $_rc   (expected non-zero: refused)"
    emit "   refusal messages:             ${_refused:-0}   (expected >= 1)"
    emit "   the volume still exists:      $([ -n "$VOL_AFTER" ] && echo yes || echo NO)"
    emit "   its contents survived:        $([ "$MARKER_AFTER" = "$MARKER_BEFORE" ] && [ -n "$MARKER_BEFORE" ] && echo yes || echo NO)"
    [ "$_rc" != "0" ] || bad "the consumer exercise adopted a service it did not create"
    [ "${_refused:-0}" -ge 1 ] || bad "the consumer exercise did not report a refusal"
    [ -n "$VOL_AFTER" ] || bad "the pre-existing volume was deleted by an exercise that refused to run"
    [ -n "$MARKER_BEFORE" ] && [ "$MARKER_AFTER" = "$MARKER_BEFORE" ] || bad "the pre-existing volume's contents were destroyed"

    compose --profile consumer rm -f paperless-redis >/dev/null 2>&1 || true
    docker volume rm "$DECOY_VOL" >/dev/null 2>&1 && DECOY_CREATED=0
fi
emit ""

AFTER="$(snapshot)"
if [ "$BEFORE" = "$AFTER" ]; then
    emit "The stack is exactly as it was found."
else
    bad "the stack did not end as it began"
    emit "   BEFORE:"; printf '%s\n' "$BEFORE" | sed 's/^/     /' >> "$OUT"
    emit "   AFTER:";  printf '%s\n' "$AFTER" | sed 's/^/     /' >> "$OUT"
fi
emit ""
emit "mismatches: $FAILURES"

if [ "$FAILURES" -ne 0 ]; then
    echo >&2
    echo "FAILED: $FAILURES expectation(s) not met; see $OUT" >&2
    exit 1
fi

log "PASSED: refusals are non-mutating and cleanup respects ownership"
note "evidence: $OUT"
