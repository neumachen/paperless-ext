#!/bin/sh
# Prepare the storage roles and generate synthetic document fixtures.
#
# Two jobs:
#
#  1. Hand each named volume to the unprivileged runtime account. Docker
#     creates a volume owned by root; without this the applications would
#     report the roles as unwritable, which is correct behaviour but not a
#     usable stack.
#
#  2. Generate synthetic documents. Every fixture is produced here from
#     deterministic pseudo-random bytes. No real document is ever used, and
#     nothing outside this project's volumes is touched.
set -eu

ROOT="${FN_STORAGE_ROOT:-/srv/fn}"
UID_RT="${FN_RUNTIME_UID:-65532}"
GID_RT="${FN_RUNTIME_GID:-65532}"
COUNT="${FN_FIXTURE_COUNT:-12}"

for role in incoming queued staging consume failed; do
    dir="$ROOT/$role"
    if [ ! -d "$dir" ]; then
        echo "storage-init: $dir is not mounted; refusing to create it" >&2
        exit 1
    fi
    chown "$UID_RT:$GID_RT" "$dir"
    chmod 0770 "$dir"
    echo "storage-init: prepared $dir"
done

# An alternate destination used only by the configuration-change exercise. It
# is prepared the same way as the real roots and is optional, so the ordinary
# stack does not depend on it being mounted here.
for role in consume-alt consume-tmpfs staging-tiny; do
    dir="$ROOT/$role"
    if [ -d "$dir" ]; then
        chown "$UID_RT:$GID_RT" "$dir"
        chmod 0770 "$dir"
        echo "storage-init: prepared $dir"
    fi
done

# Synthetic fixtures live in a per-run subdirectory of incoming so a rerun
# never disturbs files another run owns.
FIX="$ROOT/incoming/synthetic"
mkdir -p "$FIX"

# The names below are synthetic and exist to cover the naming policy's shape:
# spaces, mixed case, punctuation, Unicode letters and CJK. They are inputs
# for a later milestone; nothing in this build normalizes them.
i=0
while [ "$i" -lt "$COUNT" ]; do
    i=$((i + 1))
    case $((i % 6)) in
        0) name="Bank Statement - August (Final) 2026 ${i}.PDF" ;;
        1) name="Synthetic Invoice #${i}.pdf" ;;
        2) name="Medical  --  Statement ${i}.pdf" ;;
        3) name="Überweisung Straße ${i}.PDF" ;;
        4) name="請求書 2026 ${i}.PDF" ;;
        5) name="  TAX___RETURN 2025 ${i}!!.PDF" ;;
    esac
    # Deterministic pseudo-random content of a plausible document size.
    dd if=/dev/urandom of="$FIX/$name" bs=1024 count=$((8 + i)) status=none
done

# A temporary-suffix file and a hidden file: discovery must skip both. They are
# present so a later increment's behaviour can be observed, not because
# anything in this build reads them.
dd if=/dev/urandom of="$FIX/incomplete-upload.pdf.part" bs=1024 count=4 status=none
dd if=/dev/urandom of="$FIX/.hidden-draft.pdf" bs=1024 count=2 status=none

chown -R "$UID_RT:$GID_RT" "$FIX"
chmod 0750 "$FIX"

echo "storage-init: generated $(find "$FIX" -type f | wc -l | tr -d ' ') synthetic files in $FIX"
