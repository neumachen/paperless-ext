#!/bin/sh
# FN-N006 — accepted configuration meaning must survive a restart.
#
# The previous round's evidence for this registered a job with an invented
# foreign policy identity and observed that a renamer refused it. That shows
# the comparison works; it does not show that a REAL configuration change,
# applied by a REAL restart, preserves what was already accepted. This script
# changes the configuration the applications actually run with, recreates the
# services, and looks at what happens to work that was accepted under the old
# one.
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
# # What this exercise is not allowed to do
#
# The stack's configuration file is bind-mounted read-only into every service
# and is shared: it is the deployment's own configuration, not this script's
# scratch space. Earlier versions of this exercise edited it in place and kept
# a single fixed-name backup, which meant a second run -- or a rerun after a
# crash -- would copy the ALREADY-MODIFIED file over the only copy of the
# original. So nothing here writes to that file at all. The changed
# configuration is a new file this invocation creates, and the applications
# are pointed at it through FN_CONFIG_FILE. Restoring is then a matter of
# recreating the services without the override, which is checked against the
# running processes rather than against the filesystem.
. "$(dirname "$0")/lib.sh"

OUT="$EVIDENCE_DIR/config-restart.txt"
LIVE_CONFIG="$DEPLOY_DIR/config/normalizer.json"
mkdir -p "$EVIDENCE_DIR"
FAILURES=0
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
LOWER="$(printf '%s' "$STAMP" | tr 'A-Z' 'a-z')"

# Per-invocation names. Nothing this script touches is shared with another run.
EX_TAG="$$-$STAMP"
EX_NAME=".exercise-$EX_TAG.json"
EX_HOST="$DEPLOY_DIR/config/$EX_NAME"
EX_IN_CONTAINER="/etc/fn/$EX_NAME"
DEFAULT_CONFIG="/etc/fn/normalizer.json"
DEFAULT_CONSUME="/srv/fn/consume"
ALT_CONSUME="/srv/fn/consume-alt"

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

# apply_env recreates the application services with the given environment.
#
# `compose restart` is wrong here and was the defect this replaces: it restarts
# the EXISTING container, which keeps the environment it was created with. An
# exercise that recreated a renamer with an override could not undo the
# override by restarting it -- the stack would be left pointing at the
# exercise's configuration, and the script would report success. Recreation is
# what makes the current environment take effect, in both directions.
# cleanup_exercise_file removes the configuration file THIS invocation wrote.
#
# Gated on having written it. The preflight refuses when a file with this
# invocation's name already exists -- it belongs to somebody else -- and the
# cleanup then deleted it anyway, which contradicted the refusal it had just
# printed.
CREATED_EX=0
cleanup_exercise_file() {
    if [ "$CREATED_EX" = "1" ] && [ -f "$EX_HOST" ]; then
        rm -f "$EX_HOST"
    fi
}

apply_env() {
    _ae_cfg="$1"; _ae_consume="$2"
    MUTATED=1
    FN_CONFIG_FILE="$_ae_cfg" FN_STORAGE_CONSUME="$_ae_consume" \
        compose up -d --force-recreate --wait --wait-timeout 180 \
        watcher renamer-1 renamer-2 >/dev/null 2>&1 || true
    wait_healthy watcher 180 || true
    wait_healthy renamer-1 180 || true
    wait_healthy renamer-2 180 || true
}

# restore puts the stack back and PROVES it, on every exit path.
#
# It runs from a trap, so it runs after a failed assertion, after an
# interruption, and after an unexpected error -- not only after a clean
# finish. Restoration is judged by asking the running processes what
# configuration they are using, because restoring a file proves nothing about
# a process that read it before the change. A restoration that did not take
# effect fails this target rather than being mentioned.
RESTORED=0
# Set immediately before the first change to the running stack. Until then,
# there is nothing to restore -- and restoring anyway is not harmless: the
# preflight refuses when the stack is already running a configuration that is
# not the default, and "restoring" then force-recreated all three services onto
# the hardcoded default, overwriting exactly the state the refusal existed to
# protect, while reporting success.
MUTATED=0
restore() {
    if [ "$RESTORED" = "1" ]; then return 0; fi
    RESTORED=1
    if [ "$MUTATED" = "0" ]; then
        log "nothing was changed; leaving the stack alone"
        cleanup_exercise_file
        exercise_unlock
        return 0
    fi
    log "restoring the original configuration and verifying the running state"

    # What was captured, not what the script assumes the default to be.
    #
    # The preflight accepts a stack whose consume root is not the compose
    # default -- it checks the configuration FILE -- and restoration then put
    # all three services onto the hardcoded default anyway, quietly moving the
    # destination of an installation that had deliberately been pointed
    # somewhere else. The baseline read from the running processes is the only
    # thing that describes the state this run is obliged to give back.
    _r_want_cfg="${BASELINE_CFG:-$DEFAULT_CONFIG}"
    _r_want_consume="${BASELINE_CONSUME:-$DEFAULT_CONSUME}"
    case "$_r_want_cfg" in unreadable|"") _r_want_cfg="$DEFAULT_CONFIG" ;; esac
    case "$_r_want_consume" in unreadable|"") _r_want_consume="$DEFAULT_CONSUME" ;; esac
    apply_env "$_r_want_cfg" "$_r_want_consume"

    _r_cfg="$(effective_value renamer-1 "d['config_file']")"
    _r_consume="$(effective_value renamer-1 "d['storage']['consume']")"
    _r_id="$(effective_value renamer-1 "d['policy']['identity']")"
    _r_wcfg="$(watcher_effective_value "d['config_file']")"
    # renamer-2 is recreated by apply_env exactly like the other two, and it
    # was never read back. A worker left on the exercise's configuration would
    # have kept normalizing documents under it while this script reported the
    # stack restored.
    _r_cfg2="$(effective_value renamer-2 "d['config_file']")"
    _r_consume2="$(effective_value renamer-2 "d['storage']['consume']")"
    _r_ok=1
    if [ "$_r_cfg" != "$_r_want_cfg" ]; then _r_ok=0; fi
    if [ "$_r_wcfg" != "$_r_want_cfg" ]; then _r_ok=0; fi
    if [ "$_r_cfg2" != "$_r_want_cfg" ]; then _r_ok=0; fi
    if [ "$_r_consume" != "$_r_want_consume" ]; then _r_ok=0; fi
    if [ "$_r_consume2" != "$_r_want_consume" ]; then _r_ok=0; fi
    if [ -n "$BASELINE_ID" ] && [ "$_r_id" != "$BASELINE_ID" ]; then _r_ok=0; fi

    # Configuration alone is not a restored stack: a service can hold the right
    # configuration and be unable to reach its database. /readyz is asked for,
    # not /healthz, which answers 200 for a process with every dependency down.
    _r_rdy1="$(compose exec -T renamer-1 /usr/local/bin/fn-renamer healthcheck --require-ready >/dev/null 2>&1 && echo yes || echo no)"
    _r_rdy2="$(compose exec -T renamer-2 /usr/local/bin/fn-renamer healthcheck --require-ready >/dev/null 2>&1 && echo yes || echo no)"
    _r_rdyw="$(compose exec -T watcher /usr/local/bin/fn-watcher healthcheck --require-ready >/dev/null 2>&1 && echo yes || echo no)"
    if [ "$_r_rdy1" != "yes" ] || [ "$_r_rdy2" != "yes" ] || [ "$_r_rdyw" != "yes" ]; then _r_ok=0; fi

    cleanup_exercise_file

    {
        printf '\nrestoration (read back from the running processes, not from disk):\n'
        printf '  restoring to the CAPTURED baseline, not a default\n'
        printf '  renamer-1 config_file:   %s   (expected %s)\n' "$_r_cfg" "$_r_want_cfg"
        printf '  watcher   config_file:   %s   (expected %s)\n' "$_r_wcfg" "$_r_want_cfg"
        printf '  renamer-1 consume root:  %s   (expected %s)\n' "$_r_consume" "$_r_want_consume"
        printf '  renamer-2 config_file:   %s   (expected %s)\n' "$_r_cfg2" "$_r_want_cfg"
        printf '  renamer-2 consume root:  %s   (expected %s)\n' "$_r_consume2" "$_r_want_consume"
        printf '  renamer-1 policy:        %s   (expected %s)\n' "$_r_id" "${BASELINE_ID:-unknown}"
        printf '  renamer-1/2, watcher ready: %s / %s / %s   (expected yes/yes/yes)\n' \
            "$_r_rdy1" "$_r_rdy2" "$_r_rdyw"
        if [ -f "$EX_HOST" ]; then
            printf '  exercise config removed: no\n'
        else
            printf '  exercise config removed: yes\n'
        fi
    } >> "$OUT"

    exercise_unlock

    if [ "$_r_ok" != "1" ]; then
        echo >&2
        echo "FAILED: the stack was NOT restored to its prior effective configuration." >&2
        echo "        config_file=$_r_cfg watcher=$_r_wcfg renamer-2=$_r_cfg2 consume=$_r_consume policy=$_r_id" >&2
        echo "        ready: renamer-1=$_r_rdy1 renamer-2=$_r_rdy2 watcher=$_r_rdyw" >&2
        echo "        See $OUT. Restore by hand before running anything else." >&2
        printf '\nRESTORATION FAILED — see the values above.\n' >> "$OUT"
        exit 1
    fi
    note "restored: config_file=$_r_cfg consume=$_r_consume policy=$_r_id"
}

# Lock before anything is mutated. Two of these running at once would fight
# over one stack's services and one configuration directory, and the loser
# would restore over the winner's change mid-assertion.
exercise_lock config-restart || exit 1
BASELINE_ID=""
trap 'restore; report_keep' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [ -e "$EX_HOST" ]; then
    echo "error: $EX_HOST already exists; refusing to overwrite it." >&2
    exit 1
fi

report_begin "config-restart" "$OUT" "$0"
emit "FN-N006 — accepted configuration meaning across a real restart"
emit ""

# The baseline is read from the RUNNING renamer, which is what restoration is
# later compared against.
BASELINE_ID="$(effective_value renamer-1 "d['policy']['identity']")"
BASELINE_CFG="$(effective_value renamer-1 "d['config_file']")"
BASELINE_CONSUME="$(effective_value renamer-1 "d['storage']['consume']")"
emit "baseline, read from the running renamer:"
emit "  config_file:      $BASELINE_CFG"
emit "  policy identity:  $BASELINE_ID"
emit "  consume root:     $BASELINE_CONSUME"
emit ""
case "$BASELINE_ID" in unreadable|"") echo "error: cannot read the running configuration; is the stack up?" >&2; exit 1 ;; esac
[ "$BASELINE_CFG" = "$DEFAULT_CONFIG" ] || {
    echo "error: the stack is already running a non-default configuration ($BASELINE_CFG)." >&2
    echo "       This exercise will not run on top of another one's changes." >&2
    exit 1
}

# ---------------------------------------------------------------------------
# 1. A naming change. Work accepted under the old policy must not be renamed
#    under the new one.
# ---------------------------------------------------------------------------
log "1/2: a naming-policy change across a restart"

# Stop the renamers so a job can be accepted and left queued across the change.
MUTATED=1
compose stop renamer-1 renamer-2 >/dev/null 2>&1
DOC1="cfg-policy-$STAMP.pdf"
submit "$DOC1"
_i=0
while [ "$_i" -lt 60 ]; do
    if [ -n "$(psqlq "SELECT job_id FROM jobs WHERE source_name = '$DOC1';")" ]; then break; fi
    sleep 1; _i=$((_i + 1))
done
ACCEPTED_ID="$(psqlq "SELECT policy_version FROM jobs WHERE source_name = '$DOC1';")"
emit "1. naming change"
emit "   job accepted under:           $ACCEPTED_ID"
[ -n "$ACCEPTED_ID" ] || bad "the probe document was never registered"

# Build the changed configuration as a NEW file, derived from the live one and
# written beside it. The live file is read, never written.
docker run --rm -i \
    -v "$DEPLOY_DIR/config:/cfg" \
    "$UTIL_PY_IMAGE" python3 -c '
import json, sys
src, dst = sys.argv[1], sys.argv[2]
d = json.load(open(src))
d.setdefault("normalization", {}).setdefault("rules", []).append({
    "name": "cfg-restart-probe",
    "pattern": "^cfg-",
    "replacement": "configured-",
})
json.dump(d, open(dst, "w"), indent=2)
' /cfg/normalizer.json "/cfg/$EX_NAME" || { echo "could not write the exercise configuration" >&2; exit 1; }
# Written by this invocation, so this invocation may remove it. Until this
# point the cleanup must leave the path alone: a file already there belongs to
# somebody else, which is what the preflight refusal is about.
CREATED_EX=1
emit "   exercise configuration:       $EX_IN_CONTAINER (new file; the live one is untouched)"

apply_env "$EX_IN_CONTAINER" "$DEFAULT_CONSUME"

AFTER_CFG="$(effective_value renamer-1 "d['config_file']")"
AFTER_ID="$(effective_value renamer-1 "d['policy']['identity']")"
emit "   renamer now reading:          $AFTER_CFG"
emit "   policy identity after:        $AFTER_ID"
[ "$AFTER_CFG" = "$EX_IN_CONTAINER" ] || bad "the change did not take effect: the renamer is still reading $AFTER_CFG"
[ "$AFTER_ID" != "$BASELINE_ID" ] || bad "a naming change did not move the policy identity"

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

# Back to the baseline configuration before the second scenario, and checked,
# so scenario 2 is not measuring scenario 1's leftovers.
apply_env "$DEFAULT_CONFIG" "$DEFAULT_CONSUME"
MID_ID="$(effective_value renamer-1 "d['policy']['identity']")"
emit "   between scenarios, policy back to: $MID_ID"
[ "$MID_ID" = "$BASELINE_ID" ] || bad "the baseline policy was not restored between scenarios: $MID_ID"

# ---------------------------------------------------------------------------
# 2. A destination change. Work accepted for the old consume root must not be
#    redirected to a new one.
# ---------------------------------------------------------------------------
log "2/2: a destination-root change across a restart"
MUTATED=1
compose stop renamer-1 renamer-2 >/dev/null 2>&1
DOC2="cfg-dest-$STAMP.pdf"
submit "$DOC2"
_i=0
while [ "$_i" -lt 60 ]; do
    if [ -n "$(psqlq "SELECT job_id FROM jobs WHERE source_name = '$DOC2';")" ]; then break; fi
    sleep 1; _i=$((_i + 1))
done
ACCEPTED_ROOT="$(psqlq "SELECT coalesce(destination_root,'-') FROM jobs WHERE source_name = '$DOC2';")"
emit "2. destination change"
emit "   job accepted for:             $ACCEPTED_ROOT"
[ "$ACCEPTED_ROOT" != "-" ] || bad "no accepted destination was recorded on the job"

# Point the applications at a different consume root, recreating them so the
# change actually takes.
apply_env "$DEFAULT_CONFIG" "$ALT_CONSUME"
RUNNING_ROOT="$(effective_value renamer-1 "d['storage']['consume']")"
emit "   renamer now configured for:   $RUNNING_ROOT   (expected $ALT_CONSUME)"

# The scenario must OCCUR. Reporting that it did not and passing anyway is how
# a change that silently failed to apply gets recorded as evidence for the
# behaviour it never exercised.
if [ "$RUNNING_ROOT" != "$ALT_CONSUME" ]; then
    bad "the destination change did not take effect (renamer still on '$RUNNING_ROOT'), so this scenario did not run"
else
    STATE2="$(await_state "$DOC2" "delivered held uncertain" 120)"
    CAT2="$(psqlq "SELECT coalesce(failure_category,'-') FROM jobs WHERE source_name = '$DOC2';")"
    DEST2="$(compose run --rm --no-deps -T --entrypoint sh storage-init \
        -c 'ls -1 /srv/fn/consume-alt 2>/dev/null | wc -l' 2>/dev/null | tr -d ' \r\n')"
    emit "   state after the change:       $STATE2   (expected held)"
    emit "   category:                     $CAT2   (expected destination_mismatch)"
    emit "   entries in the new root:      $DEST2   (expected 0: not redirected)"
    [ "$STATE2" = "held" ] || bad "work accepted for another destination reached '$STATE2'"
    [ "$CAT2" = "destination_mismatch" ] || bad "category is '$CAT2'"
    [ "${DEST2:-0}" = "0" ] || bad "the job was redirected into the new destination root"
fi
emit ""

emit "Activation is a restart; nothing here reloads configuration in place."
emit "Work accepted under one configuration is held for an operator rather than"
emit "reinterpreted, and work submitted afterwards proceeds normally. The live"
emit "configuration file was never written: the changed configuration was a"
emit "separate file this run created and removed, and restoration is verified"
emit "by reading it back out of the running processes."
emit ""
emit "mismatches: $FAILURES"

if [ "$FAILURES" -ne 0 ]; then
    echo >&2
    echo "FAILED: $FAILURES expectation(s) not met; see $OUT" >&2
    exit 1
fi

report_success
log "PASSED: accepted configuration meaning survives a real restart"
note "evidence: $OUT"
