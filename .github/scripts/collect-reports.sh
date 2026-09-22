#!/bin/sh
# Copy an explicit allow-list of reports out of the evidence directory, then
# refuse to hand any of them over if a generated credential appears inside.
#
# The evidence directory is NOT archived wholesale. It holds raw service logs,
# per-run state files and snapshots of document storage, and an artifact is a
# durable, downloadable copy of whatever went into it. Only files named below
# are copied, and every copied file is scanned for this run's three generated
# passwords before the directory is offered for upload.
#
# Inputs (environment):
#   FN_CI_REPORTS  optional. Output directory. Default <repo>/.ci-reports.
set -eu

die() { echo "error: $*" >&2; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DEPLOY_DIR="$REPO_ROOT/deploy/filename-normalizer"
EVIDENCE="$REPO_ROOT/extensions/filename-normalizer/.evidence"
OUT="${FN_CI_REPORTS:-$REPO_ROOT/.ci-reports}"

[ -d "$EVIDENCE" ] || die "$EVIDENCE does not exist; nothing has run yet."

rm -rf "$OUT"
mkdir -p "$OUT/phases" "$OUT/public-commands" "$OUT/stack"

copied=0
take() {
    _t_src="$1"; _t_dest="$2"
    [ -f "$_t_src" ] || return 0
    cp "$_t_src" "$_t_dest"
    copied=$((copied + 1))
}

# --- build and verification logs -----------------------------------------
take "$EVIDENCE/f1-verify.log" "$OUT/f1-verify.log"
take "$EVIDENCE/f1-build.log"  "$OUT/f1-build.log"

# --- the suite's own result files ----------------------------------------
take "$EVIDENCE/run-metadata.txt"  "$OUT/run-metadata.txt"
take "$EVIDENCE/phase-results.txt" "$OUT/phase-results.txt"
for f in "$EVIDENCE"/phase-*.log; do
    take "$f" "$OUT/phases/$(basename "$f")"
done

# --- the public entry points' recorded exit statuses ---------------------
for f in "$EVIDENCE"/public-commands/*.txt; do
    take "$f" "$OUT/public-commands/$(basename "$f")"
done

# --- one snapshot of real stack state ------------------------------------
# The snapshot directory also contains raw per-service logs; they are not in
# this list and are not copied.
SNAPSHOT=""
if [ -f "$EVIDENCE/latest-snapshot.txt" ]; then
    SNAPSHOT="$(cat "$EVIDENCE/latest-snapshot.txt")"
fi
if [ -z "$SNAPSHOT" ] || [ ! -d "$SNAPSHOT" ]; then
    SNAPSHOT="$(find "$EVIDENCE" -maxdepth 1 -type d -name 'snapshot-*' | sort | tail -1)"
fi
if [ -n "$SNAPSHOT" ] && [ -d "$SNAPSHOT" ]; then
    for name in compose-ps.txt images.txt image-digests.txt \
                container-hardening.txt postgres-cluster.txt \
                postgres-standby.txt rabbitmq.txt storage-roles.txt; do
        take "$SNAPSHOT/$name" "$OUT/stack/$name"
    done
    for f in "$SNAPSHOT"/*-healthz.txt "$SNAPSHOT"/*-readyz.txt "$SNAPSHOT"/*-metrics.txt; do
        take "$f" "$OUT/stack/$(basename "$f")"
    done
    printf '%s\n' "$(basename "$SNAPSHOT")" > "$OUT/stack/snapshot-source.txt"
fi

# --- credential scan ------------------------------------------------------
# The backstop. Any collected file that contains one of this run's generated
# passwords is deleted and the whole collection is refused, because the next
# step would upload it.
PATTERNS="$OUT/.secret-patterns"
: > "$PATTERNS"
chmod 0600 "$PATTERNS"
if [ -f "$DEPLOY_DIR/.env" ]; then
    for key in FN_DB_PASSWORD FN_REPLICATION_PASSWORD FN_AMQP_PASSWORD; do
        sed -n "s/^$key=//p" "$DEPLOY_DIR/.env" | head -1 | sed '/^$/d' >> "$PATTERNS"
    done
fi

leaked=0
if [ -s "$PATTERNS" ]; then
    find "$OUT" -type f ! -name '.secret-patterns' > "$OUT/.scan-list"
    while IFS= read -r f; do
        if grep -F -q -f "$PATTERNS" "$f" 2>/dev/null; then
            echo "error: a generated credential appears in $f; deleting it." >&2
            rm -f "$f"
            leaked=$((leaked + 1))
        fi
    done < "$OUT/.scan-list"
    rm -f "$OUT/.scan-list"
else
    echo "warning: no generated credentials were readable, so the scan proved nothing." >&2
fi
rm -f "$PATTERNS"

# Never collected, stated here so the omission is deliberate and reviewable:
#   .env and secrets/            -- the credentials themselves
#   .evidence/logs/              -- raw service logs
#   .evidence/state/             -- per-run scalars the suite passes to itself
#   snapshot-*/logs/             -- the same raw logs inside a snapshot
#   .evidence-retained/          -- earlier runs kept on a developer machine
index="$(find "$OUT" -type f | sort | sed "s|^$OUT/|  |")"
printf '%s\n' "$index" > "$OUT/INDEX.txt"
echo "collected $copied file(s) into $OUT"
printf '%s\n' "$index"

[ "$leaked" -eq 0 ] || die "$leaked collected file(s) contained a generated credential."
