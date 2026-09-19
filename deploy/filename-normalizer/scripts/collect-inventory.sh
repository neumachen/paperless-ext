#!/bin/sh
# A retained inventory of the durable and filesystem state, taken after
# qualification and written into the evidence directory.
#
# # Why this is a script and not a paste
#
# The previous pass reported counts that no retained artifact contained: the
# survey was run twice, the second result went to a scratch file, and the report
# quoted the second while `.evidence/final-state.txt` still held the first. A
# reader checking the cited artifact found different numbers. An inventory that
# is produced by a committed script, writes into the evidence directory, and
# stamps itself with the source it was taken against cannot drift from the
# report that quotes it.
#
# It reads. It changes nothing, resolves nothing, and deletes nothing.
. "$(dirname "$0")/lib.sh"

OUT="$EVIDENCE_DIR/final-state.txt"
BASELINE="$EVIDENCE_DIR/uncertain-baseline.txt"

psqlq() {
    compose exec -T -e PGPASSWORD="$(cat "$DEPLOY_DIR/secrets/fn_db_password")" \
        postgres-primary psql -U "${FN_DB_USER:-fn_app}" -d "${FN_DB_NAME:-filename_normalizer}" \
        -tA -c "$1" < /dev/null
}
in_storage() {
    compose run --rm --no-deps -T --entrypoint sh storage-init -c "$1" 2>/dev/null < /dev/null | tr -d ' \r\n'
}

{
    printf 'Durable and filesystem inventory\n'
    printf 'taken:           %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf -- '-- provenance: two independent facts -------------------------\n'
    printf 'inventory script: %s\n' "$(basename "$0")"
    printf '  sha256:         %s\n' "$(cksum_sha256 "$0")"
    printf 'application under test, asked of the running process:\n'
    printf '  %s\n' "$(running_application_identity)"
    printf -- '--------------------------------------------------------------\n\n'

    printf '=== jobs by state ===\n'
    psqlq "SELECT state || '|' || count(*) FROM jobs GROUP BY state ORDER BY 1;"

    printf '\n=== holds by category ===\n'
    psqlq "SELECT coalesce(failure_category,'-') || '|' || count(*) FROM jobs WHERE state = 'held' GROUP BY 1 ORDER BY 2 DESC;"

    printf '\n=== receipts and reservations ===\n'
    psqlq "SELECT 'delivery_receipts=' || count(*) FROM delivery_receipts;"
    psqlq "SELECT 'reservations_total=' || count(*) FROM name_reservations;"
    psqlq "SELECT 'reservations_blocked=' || count(*) FROM name_reservations WHERE blocked_at IS NOT NULL;"
} > "$OUT"

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

if [ ! -f "$BASELINE" ]; then
    cp "$EVIDENCE_DIR/.uncertain-now.txt" "$BASELINE"
    printf 'uncertain baseline created on this run: %s ids\n' "$(wc -l < "$BASELINE" | tr -d ' ')" >> "$OUT"
fi

{
    printf '\n=== uncertain publications: preserved vs newly created ===\n'
    _known="$(wc -l < "$BASELINE" | tr -d ' ')"
    _now="$(wc -l < "$EVIDENCE_DIR/.uncertain-now.txt" | tr -d ' ')"
    _gone="$(comm -23 "$BASELINE" "$EVIDENCE_DIR/.uncertain-now.txt" 2>/dev/null | wc -l | tr -d ' ')"
    _new="$(comm -13 "$BASELINE" "$EVIDENCE_DIR/.uncertain-now.txt" 2>/dev/null | wc -l | tr -d ' ')"
    printf 'previously recorded uncertain jobs: %s\n' "$_known"
    printf '  of those, still uncertain now:    %s\n' "$((_known - _gone))"
    printf '  of those, no longer uncertain:    %s   (expected 0: nothing resolves them)\n' "$_gone"
    printf 'uncertain now:                      %s\n' "$_now"
    printf '  created since the baseline:       %s   (the A6 before-link scenario creates one per run)\n' "$_new"
    if [ "${_gone:-0}" != "0" ]; then
        printf 'THESE WERE RESOLVED OR DELETED AND MUST NOT HAVE BEEN:\n'
        comm -23 "$BASELINE" "$EVIDENCE_DIR/.uncertain-now.txt"
    fi
    printf '\nids created since the baseline:\n'
    comm -13 "$BASELINE" "$EVIDENCE_DIR/.uncertain-now.txt" | sed 's/^/  /'
} >> "$OUT"

# Per-job filesystem evidence, for every uncertain job.
printf '\n=== each uncertain job, with its filesystem evidence ===\n' >> "$OUT"
psqlq "SELECT job_id || '|' || source_name || '|' || coalesce(reserved_name,'-') || '|' || to_char(created_at,'YYYY-MM-DD HH24:MI:SSZ') FROM jobs WHERE state = 'uncertain' ORDER BY created_at;" > "$EVIDENCE_DIR/.uncertain-rows.txt"
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
        printf '  delivery receipts: %s\n' "$_rec"
    } >> "$OUT"
done < "$EVIDENCE_DIR/.uncertain-rows.txt"
rm -f "$EVIDENCE_DIR/.uncertain-rows.txt" "$EVIDENCE_DIR/.uncertain-now.txt"

{
    printf '\n=== delivered jobs whose SOURCE is still in incoming ===\n'
} >> "$OUT"
psqlq "SELECT source_name FROM jobs WHERE state = 'delivered';" > "$EVIDENCE_DIR/.delivered.txt"
docker run --rm -i -v "${PROJECT}_fn-incoming:/incoming:ro" "$UTIL_PY_IMAGE" python3 -c "
import os, sys
have = set(os.listdir('/incoming'))
names = [l.strip() for l in sys.stdin if l.strip()]
present = [n for n in names if n in have]
print('delivered jobs         :', len(names))
print('  source still present :', len(present))
print('  source absent        :', len(names) - len(present), '(removed by the harness that created it; the application never removes a source)')
" < "$EVIDENCE_DIR/.delivered.txt" >> "$OUT" 2>/dev/null
rm -f "$EVIDENCE_DIR/.delivered.txt"

{
    printf '\n=== filesystem ===\n'
} >> "$OUT"
docker run --rm -v "${PROJECT}_fn-consume:/consume:ro" -v "${PROJECT}_fn-incoming:/incoming:ro" \
    "$UTIL_PY_IMAGE" python3 -c "
import os
c = os.listdir('/consume'); i = os.listdir('/incoming')
print('consume entries      :', len(c))
print('  visible documents  :', len([x for x in c if not x.startswith('.')]))
print('  .fn-* intermediates:', len([x for x in c if x.startswith('.fn-')]))
print('incoming entries     :', len(i))
" >> "$OUT" 2>/dev/null

cat "$OUT"
note "inventory written to $OUT"
