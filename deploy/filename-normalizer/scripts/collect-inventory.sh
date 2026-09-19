#!/bin/sh
# A retained inventory of the durable and filesystem state, taken after
# qualification and written into the evidence directory.
#
# # Why this is a script and not a paste
#
# A previous pass reported counts that no retained artifact contained: the
# survey was run twice, the second result went to a scratch file, and the report
# quoted the second while `.evidence/final-state.txt` still held the first. A
# reader checking the cited artifact found different numbers. An inventory that
# is produced by a committed script, writes into the evidence directory, and
# stamps itself with the source it was taken against cannot drift from the
# report that quotes it.
#
# # Why it states its own completeness
#
# The first version of this script hit a malformed query, and `set -e` ended it
# mid-write. What was left on disk was a *truncated* inventory that did not say
# so: it opened with a correct header and correct job-state counts, then simply
# stopped, and the sections the report relied on were absent rather than wrong.
# An artifact that can end early has to declare whether it did. Every query is
# therefore run without aborting the run, failures are counted and named, and
# the completeness verdict is the first thing in the file — not the last, where
# a reader who has already found their number would never reach it.
#
# It reads. It changes nothing, resolves nothing, and deletes nothing.
. "$(dirname "$0")/lib.sh"

SELF="$SCRIPT_DIR/$(basename "$0")"
OUT="$EVIDENCE_DIR/final-state.txt"
BASELINE="$EVIDENCE_DIR/uncertain-baseline.txt"
BODY="$EVIDENCE_DIR/.inventory-body.$$"
FAILURES="$EVIDENCE_DIR/.inventory-failures.$$"
: > "$BODY"
: > "$FAILURES"
trap 'rm -f "$BODY" "$FAILURES" "$EVIDENCE_DIR"/.uncertain-now.txt "$EVIDENCE_DIR"/.uncertain-now-sorted.txt "$EVIDENCE_DIR"/.uncertain-rows.txt "$EVIDENCE_DIR"/.delivered.txt' EXIT

# A query that fails records the failure and returns empty, rather than ending
# the run under `set -e`. The alternative is the truncated artifact described
# above: a reader cannot distinguish "this section is empty" from "this script
# stopped here".
psqlq() {
    if _q_out="$(compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" \
        postgres-primary psql -U "${FN_DB_USER:-fn_app}" -d "${FN_DB_NAME:-filename_normalizer}" \
        -tA -v ON_ERROR_STOP=1 -c "$1" < /dev/null 2>&1)"; then
        printf '%s\n' "$_q_out"
    else
        printf '%s\n' "$1" >> "$FAILURES"
        printf '  QUERY FAILED (see collection status above): %s\n' "$_q_out" | head -3
        return 0
    fi
}

# An existence probe that cannot report absence it did not observe. FN-R5-05's
# rule applies to this script too: a check that could not run is `unknown`, and
# `unknown` is not `no`.
in_storage() {
    _is_out="$(compose run --rm --no-deps -T --entrypoint sh storage-init -c "$1" 2>/dev/null < /dev/null | tr -d ' \r\n' || true)"
    case "$_is_out" in
        yes|no) printf '%s' "$_is_out" ;;
        *) printf 'unknown' ;;
    esac
}

{
    printf '=== jobs by state ===\n'
    psqlq "SELECT state || '|' || count(*) FROM jobs GROUP BY state ORDER BY 1;"

    printf '\n=== holds by category ===\n'
    # Ordering by the count requires the count as its own column: the previous
    # form selected one concatenated string and then ordered by a second column
    # that did not exist, which is the query that ended the run.
    psqlq "SELECT coalesce(failure_category,'-') || '|' || c FROM (SELECT failure_category, count(*) AS c FROM jobs WHERE state = 'held' GROUP BY failure_category) s ORDER BY c DESC, 1;"

    printf '\n=== receipts and reservations ===\n'
    psqlq "SELECT 'delivery_receipts=' || count(*) FROM delivery_receipts;"
    psqlq "SELECT 'reservations_total=' || count(*) FROM name_reservations;"
    psqlq "SELECT 'reservations_blocked=' || count(*) FROM name_reservations WHERE blocked_at IS NOT NULL;"
} >> "$BODY"

# ---------------------------------------------------------------------------
# Uncertain publications: preserved versus newly created.
#
# The contract is that nothing here resolves an uncertain job or deletes one.
# Showing a total does not demonstrate that: a total can stay the same while one
# job is resolved and another created. The baseline file records every uncertain
# job id this stack has ever carried, so a later run can show that each earlier
# one is STILL uncertain, and name the ones its own scenarios added.
# ---------------------------------------------------------------------------
psqlq "SELECT job_id FROM jobs WHERE state = 'uncertain' ORDER BY created_at;" | sed '/^$/d' > "$EVIDENCE_DIR/.uncertain-now.txt"

if [ ! -s "$EVIDENCE_DIR/.uncertain-now.txt" ] && [ -s "$FAILURES" ]; then
    printf '\nuncertain-job comparison SKIPPED: the listing query failed, and an\n' >> "$BODY"
    printf 'empty result must not be read as "no uncertain jobs remain".\n' >> "$BODY"
else
    # ------------------------------------------------------------------
    # Where the baseline comes from, and why not from right now.
    #
    # Seeding it from the current state makes the comparison below compare
    # this run to itself: 30 known, 30 still uncertain, 0 lost — a line that
    # reads like preservation and demonstrates nothing. The baseline has to be
    # an observation made EARLIER, so it is recovered from the retained
    # inventories under .evidence-retained/, which were written before this
    # pass and are not rewritten by it. Only if none of them lists an id does
    # this fall back to the current state, and then it says so and makes no
    # preservation claim.
    # ------------------------------------------------------------------
    if [ ! -f "$BASELINE" ]; then
        _seed_src=""
        for _ret in "$EXT_DIR"/.evidence-retained/*/final-state.txt; do
            [ -f "$_ret" ] || continue
            grep -oE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' "$_ret" \
                >> "$BASELINE.seed" 2>/dev/null || true
            _seed_src="$_seed_src $(basename "$(dirname "$_ret")")"
        done
        if [ -s "$BASELINE.seed" ]; then
            sort -u "$BASELINE.seed" > "$BASELINE"
            printf '\nuncertain baseline recovered from retained inventories:%s\n' \
                "$_seed_src" >> "$BODY"
            printf '  ids recovered: %s\n' "$(wc -l < "$BASELINE" | tr -d ' ')" >> "$BODY"
        else
            cp "$EVIDENCE_DIR/.uncertain-now.txt" "$BASELINE"
            printf '\nuncertain baseline CREATED FROM THE CURRENT STATE on this run: %s ids.\n' \
                "$(wc -l < "$BASELINE" | tr -d ' ')" >> "$BODY"
            printf '  No preservation claim follows from this run: the comparison below\n' >> "$BODY"
            printf '  is against itself. It becomes meaningful from the next run on.\n' >> "$BODY"
        fi
        rm -f "$BASELINE.seed"
    fi
    # `comm` compares sorted input and silently produces nonsense otherwise.
    # The listing above is ordered by registration time on purpose — that is
    # what the per-job section reads — so the comparison gets its own sorted
    # copy. The previous form compared the time-ordered listing directly and
    # sent comm's complaint to /dev/null, which means the preservation counts
    # it printed were not trustworthy.
    sort -u "$EVIDENCE_DIR/.uncertain-now.txt" > "$EVIDENCE_DIR/.uncertain-now-sorted.txt"
    {
        printf '\n=== uncertain publications: preserved vs newly created ===\n'
        _known="$(wc -l < "$BASELINE" | tr -d ' ')"
        _now="$(wc -l < "$EVIDENCE_DIR/.uncertain-now-sorted.txt" | tr -d ' ')"
        _gone="$(comm -23 "$BASELINE" "$EVIDENCE_DIR/.uncertain-now-sorted.txt" | wc -l | tr -d ' ')"
        _new="$(comm -13 "$BASELINE" "$EVIDENCE_DIR/.uncertain-now-sorted.txt" | wc -l | tr -d ' ')"
        printf 'previously recorded uncertain jobs: %s\n' "$_known"
        printf '  of those, still uncertain now:    %s\n' "$((_known - _gone))"
        printf '  of those, no longer uncertain:    %s   (expected 0: nothing resolves them)\n' "$_gone"
        printf 'uncertain now:                      %s\n' "$_now"
        printf '  created since the baseline:       %s   (the scenarios that leave one create it by design)\n' "$_new"
        if [ "${_gone:-0}" != "0" ]; then
            printf 'THESE WERE RESOLVED OR DELETED AND MUST NOT HAVE BEEN:\n'
            comm -23 "$BASELINE" "$EVIDENCE_DIR/.uncertain-now-sorted.txt"
        fi
        printf '\nids created since the baseline:\n'
        comm -13 "$BASELINE" "$EVIDENCE_DIR/.uncertain-now-sorted.txt" | sed 's/^/  /'
        # The count above and the list just printed come from one comparison,
        # so they must agree. They did not once: the count used the sorted
        # listing and the list still used the time-ordered one, and `comm` on
        # unsorted input printed 26 ids under a heading that said 13. A number
        # and a list that contradict each other are worse than either alone,
        # so the artifact checks them against each other.
        _listed="$(comm -13 "$BASELINE" "$EVIDENCE_DIR/.uncertain-now-sorted.txt" | wc -l | tr -d ' ')"
        if [ "$_listed" != "$_new" ]; then
            printf 'INTERNAL INCONSISTENCY: count says %s, list holds %s\n' "$_new" "$_listed"
            printf 'inconsistent created-since count/list\n' >> "$FAILURES"
        fi
    } >> "$BODY"

    # Per-job filesystem evidence, for every uncertain job.
    printf '\n=== each uncertain job, with its filesystem evidence ===\n' >> "$BODY"
    psqlq "SELECT job_id || '|' || source_name || '|' || coalesce(reserved_name,'-') || '|' || to_char(created_at,'YYYY-MM-DD HH24:MI:SSZ') FROM jobs WHERE state = 'uncertain' ORDER BY created_at;" \
        | sed '/^$/d' > "$EVIDENCE_DIR/.uncertain-rows.txt"
    while IFS='|' read -r jid src res when; do
        [ -n "$jid" ] || continue
        _src="$(in_storage "test -e '/srv/fn/incoming/$src' && echo yes || echo no")"
        _dst="n/a"
        [ "$res" != "-" ] && _dst="$(in_storage "test -e '/srv/fn/consume/$res' && echo yes || echo no")"
        _rec="$(psqlq "SELECT count(*) FROM delivery_receipts WHERE job_id = '$jid';" | tr -d ' ')"
        {
            printf '%s\n' "$jid"
            printf '  registered:        %s\n' "$when"
            printf '  source:            %s  (present in incoming: %s)\n' "$src" "$_src"
            printf '  reserved name:     %s  (present in consume: %s)\n' "$res" "$_dst"
            printf '  delivery receipts: %s\n' "${_rec:-unknown}"
        } >> "$BODY"
    done < "$EVIDENCE_DIR/.uncertain-rows.txt"
fi

printf '\n=== delivered jobs whose SOURCE is still in incoming ===\n' >> "$BODY"
psqlq "SELECT source_name FROM jobs WHERE state = 'delivered';" | sed '/^$/d' > "$EVIDENCE_DIR/.delivered.txt"
if docker run --rm -i -v "${PROJECT}_fn-incoming:/incoming:ro" "$UTIL_PY_IMAGE" python3 -c "
import os, sys
have = set(os.listdir('/incoming'))
names = [l.strip() for l in sys.stdin if l.strip()]
present = [n for n in names if n in have]
print('delivered jobs         :', len(names))
print('  source still present :', len(present))
print('  source absent        :', len(names) - len(present), '(removed by the harness that created it; the application never removes a source)')
" < "$EVIDENCE_DIR/.delivered.txt" >> "$BODY" 2>/dev/null; then
    :
else
    printf 'delivered-source survey FAILED to run\n' >> "$BODY"
    printf 'delivered-source survey\n' >> "$FAILURES"
fi

printf '\n=== filesystem ===\n' >> "$BODY"
if docker run --rm -v "${PROJECT}_fn-consume:/consume:ro" -v "${PROJECT}_fn-incoming:/incoming:ro" \
    "$UTIL_PY_IMAGE" python3 -c "
import os
c = os.listdir('/consume'); i = os.listdir('/incoming')
print('consume entries      :', len(c))
print('  visible documents  :', len([x for x in c if not x.startswith('.')]))
print('  .fn-* intermediates:', len([x for x in c if x.startswith('.fn-')]))
print('incoming entries     :', len(i))
" >> "$BODY" 2>/dev/null; then
    :
else
    printf 'filesystem survey FAILED to run\n' >> "$BODY"
    printf 'filesystem survey\n' >> "$FAILURES"
fi

# ---------------------------------------------------------------------------
# Assemble: the completeness verdict FIRST, then the body.
# ---------------------------------------------------------------------------
_failed="$(wc -l < "$FAILURES" | tr -d ' ')"
{
    printf 'Durable and filesystem inventory\n'
    printf 'taken:           %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf -- '-- collection status -----------------------------------------\n'
    if [ "$_failed" -eq 0 ]; then
        printf 'COMPLETE: every query and survey in this inventory ran.\n'
    else
        printf 'INCOMPLETE: %s query/survey(s) failed. This artifact is PARTIAL\n' "$_failed"
        printf 'and must not be read as a full inventory. Failed:\n'
        sed 's/^/  - /' "$FAILURES"
    fi
    printf -- '-- provenance: two independent facts -------------------------\n'
    printf 'inventory script: %s\n' "$(basename "$0")"
    # By absolute path: lib.sh cd's to the deploy directory, so a relative
    # "$0" no longer resolves and `cksum_sha256` reported "absent" — a
    # provenance line that silently said nothing.
    printf '  sha256:         %s\n' "$(cksum_sha256 "$SELF")"
    printf 'application under test, asked of the running process:\n'
    printf '  %s\n' "$(running_application_identity)"
    printf 'An exercise or inventory may be corrected without the application\n'
    printf 'changing, and the application may be rebuilt without them changing.\n'
    printf 'Neither digest implies the other.\n'
    printf -- '--------------------------------------------------------------\n\n'
    cat "$BODY"
} > "$OUT"

cat "$OUT"
note "inventory written to $OUT"
[ "$_failed" -eq 0 ] || { echo "inventory INCOMPLETE: $_failed failed query/survey(s)" >&2; exit 1; }
