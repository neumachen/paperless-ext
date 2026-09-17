#!/bin/sh
# FN-N006 — accepted configuration meaning must survive a restart.
#
# The previous round's evidence for this registered a job with an invented
# foreign policy identity and observed that a renamer refused it. That shows
# the comparison works; it does not show that a REAL configuration change,
# applied by a REAL restart, preserves what was already accepted. This script
# changes the configuration on disk, restarts the services, and looks at what
# happens to work that was accepted under the old one.
#
# Two changes are exercised:
#
#   1. A naming change (a new transform rule). The policy identity moves, so
#      queued work accepted under the old identity must be held rather than
#      renamed under rules it was not accepted with.
#   2. A destination change. The consume root moves, so work already accepted
#      for the old destination must be held rather than redirected -- its
#      reservation covers a directory the new configuration does not use.
#
# The original configuration is restored at the end, whatever happens.
. "$(dirname "$0")/lib.sh"

OUT="$EVIDENCE_DIR/config-restart.txt"
CONFIG="$DEPLOY_DIR/config/normalizer.json"
BACKUP="$DEPLOY_DIR/config/.normalizer.json.before-restart-test"
mkdir -p "$EVIDENCE_DIR"
FAILURES=0
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
LOWER="$(printf '%s' "$STAMP" | tr 'A-Z' 'a-z')"

emit() { printf '%s\n' "$*" >> "$OUT"; printf '%s\n' "$*"; }
bad() { printf '    MISMATCH: %s\n' "$*" >&2; emit "    MISMATCH: $*"; FAILURES=$((FAILURES + 1)); }

psqlq() {
    compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" \
        postgres-primary psql -U "${FN_DB_USER:-fn_app}" -d "${FN_DB_NAME:-filename_normalizer}" -tA -c "$1" \
        2>/dev/null | tr -d ' \r'
}

submit() {
    compose run --rm --no-deps -T --entrypoint sh storage-init -c \
        "printf '%%PDF-1.4 config restart probe\\n' > /srv/fn/incoming/.wip-cfg-$LOWER && \
         mv /srv/fn/incoming/.wip-cfg-$LOWER '/srv/fn/incoming/$1'" >/dev/null 2>&1
}

await_state() {
    _aw_name="$1"; _aw_want="$2"; _aw_secs="${3:-90}"; _aw_i=0
    while [ "$_aw_i" -lt "$_aw_secs" ]; do
        _aw_got="$(psqlq "SELECT state FROM jobs WHERE source_name = '$_aw_name';")"
        case " $_aw_want " in *" $_aw_got "*) printf '%s' "$_aw_got"; return 0 ;; esac
        sleep 1; _aw_i=$((_aw_i + 1))
    done
    printf '%s' "${_aw_got:-absent}"; return 1
}

restart_apps() {
    compose restart watcher renamer-1 renamer-2 >/dev/null 2>&1
    wait_healthy watcher 180 || true
    wait_healthy renamer-1 180 || true
    wait_healthy renamer-2 180 || true
}

restore() {
    log "restoring the original configuration and restarting"
    if [ -f "$BACKUP" ]; then
        mv "$BACKUP" "$CONFIG"
    fi
    restart_apps
}
trap restore EXIT INT TERM

cp "$CONFIG" "$BACKUP"
: > "$OUT"
emit "FN-N006 — accepted configuration meaning across a real restart"
emit ""

BEFORE_ID="$(compose --profile tools run --rm fnctl status 2>/dev/null \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['policyIdentity'])" 2>/dev/null || echo unknown)"
emit "policy identity before: $BEFORE_ID"
emit ""

# ---------------------------------------------------------------------------
# 1. A naming change. Work accepted under the old policy must not be renamed
#    under the new one.
# ---------------------------------------------------------------------------
log "1/2: a naming-policy change across a restart"

# Stop the renamers so a job can be accepted and left queued across the change.
compose stop renamer-1 renamer-2 >/dev/null 2>&1
DOC1="cfg-policy-$STAMP.pdf"
submit "$DOC1"
_i=0
while [ "$_i" -lt 60 ]; do
    [ -n "$(psqlq "SELECT job_id FROM jobs WHERE source_name = '$DOC1';")" ] && break
    sleep 1; _i=$((_i + 1))
done
ACCEPTED_ID="$(psqlq "SELECT policy_version FROM jobs WHERE source_name = '$DOC1';")"
emit "1. naming change"
emit "   job accepted under:           $ACCEPTED_ID"
[ -n "$ACCEPTED_ID" ] || bad "the probe document was never registered"

# Apply a real naming change and restart.
python3 - "$CONFIG" <<'PY'
import json, sys
p = sys.argv[1]
d = json.load(open(p))
d.setdefault("normalization", {}).setdefault("rules", []).append({
    "name": "cfg-restart-probe",
    "pattern": "^cfg-",
    "replacement": "configured-",
})
json.dump(d, open(p, "w"), indent=2)
PY
restart_apps

AFTER_ID="$(compose --profile tools run --rm fnctl status 2>/dev/null \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['policyIdentity'])" 2>/dev/null || echo unknown)"
emit "   policy identity after:        $AFTER_ID"
[ "$AFTER_ID" != "$BEFORE_ID" ] || bad "a naming change did not move the policy identity"

STATE1="$(await_state "$DOC1" "delivered held uncertain" 120)"
CAT1="$(psqlq "SELECT coalesce(failure_category,'-') FROM jobs WHERE source_name = '$DOC1';")"
NAME1="$(psqlq "SELECT coalesce(normalized_name,'-') FROM jobs WHERE source_name = '$DOC1';")"
emit "   state after the restart:      $STATE1   (expected held)"
emit "   category:                     $CAT1   (expected policy_version_mismatch)"
emit "   normalized name assigned:     $NAME1   (expected '-': not renamed under the new rules)"
[ "$STATE1" = "held" ] || bad "queued work reached '$STATE1' after a naming change"
[ "$CAT1" = "policy_version_mismatch" ] || bad "category is '$CAT1'"
[ "$NAME1" = "-" ] || bad "the job was renamed under rules it was not accepted with: $NAME1"

# A document submitted AFTER the change must process normally under the new
# policy, so the refusal is specific to already-accepted work.
DOC1B="cfg-after-$STAMP.pdf"
submit "$DOC1B"
STATE1B="$(await_state "$DOC1B" "delivered held uncertain" 120)"
emit "   new work under the new policy: $STATE1B   (expected delivered)"
[ "$STATE1B" = "delivered" ] || bad "work submitted after the change reached '$STATE1B'"
emit ""

# Restore before the second scenario so it starts from a known policy.
mv "$BACKUP" "$CONFIG"
cp "$CONFIG" "$BACKUP"
restart_apps

# ---------------------------------------------------------------------------
# 2. A destination change. Work accepted for the old consume root must not be
#    redirected to a new one.
# ---------------------------------------------------------------------------
log "2/2: a destination-root change across a restart"
compose stop renamer-1 renamer-2 >/dev/null 2>&1
DOC2="cfg-dest-$STAMP.pdf"
submit "$DOC2"
_i=0
while [ "$_i" -lt 60 ]; do
    [ -n "$(psqlq "SELECT job_id FROM jobs WHERE source_name = '$DOC2';")" ] && break
    sleep 1; _i=$((_i + 1))
done
ACCEPTED_ROOT="$(psqlq "SELECT coalesce(destination_root,'-') FROM jobs WHERE source_name = '$DOC2';")"
emit "2. destination change"
emit "   job accepted for:             $ACCEPTED_ROOT"
[ "$ACCEPTED_ROOT" != "-" ] || bad "no accepted destination was recorded on the job"

# Point the renamers at a different consume root. The environment overrides the
# file, which is the documented precedence, so this is a real configuration
# change applied the supported way.
compose stop renamer-1 renamer-2 >/dev/null 2>&1
FN_STORAGE_CONSUME=/srv/fn/consume-alt compose up -d --wait --wait-timeout 180 renamer-1 >/dev/null 2>&1 || true
sleep 6
RUNNING_ROOT="$(compose exec -T renamer-1 /usr/local/bin/fn-renamer check-config 2>/dev/null \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['storage']['consume'])" 2>/dev/null || echo unknown)"
emit "   renamer now configured for:   $RUNNING_ROOT"

STATE2="$(await_state "$DOC2" "delivered held uncertain" 120)"
CAT2="$(psqlq "SELECT coalesce(failure_category,'-') FROM jobs WHERE source_name = '$DOC2';")"
emit "   state after the change:       $STATE2   (expected held)"
emit "   category:                     $CAT2   (expected destination_mismatch)"
if [ "$RUNNING_ROOT" = "$ACCEPTED_ROOT" ]; then
    emit "   NOTE: the destination did not actually change in this run, so this"
    emit "         scenario did not exercise the mismatch. Reported, not claimed."
else
    [ "$STATE2" = "held" ] || bad "work accepted for another destination reached '$STATE2'"
    [ "$CAT2" = "destination_mismatch" ] || bad "category is '$CAT2'"
fi
emit ""

emit "Activation is a restart; nothing here reloads configuration in place."
emit "Work accepted under one configuration is held for an operator rather than"
emit "reinterpreted, and work submitted afterwards proceeds normally."
emit ""
emit "mismatches: $FAILURES"

if [ "$FAILURES" -ne 0 ]; then
    echo >&2
    echo "FAILED: $FAILURES expectation(s) not met; see $OUT" >&2
    exit 1
fi

log "PASSED: accepted configuration meaning survives a real restart"
note "evidence: $OUT"
