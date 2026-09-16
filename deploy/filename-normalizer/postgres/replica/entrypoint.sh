#!/bin/sh
# Standby bootstrap.
#
# On an empty data directory this takes a real base backup from the primary and
# starts in hot standby. On a populated one it just starts, so a container
# restart resumes streaming from the slot instead of re-cloning.
set -eu

: "${FN_PRIMARY_HOST:?FN_PRIMARY_HOST is required}"
: "${FN_REPLICATION_USER:?FN_REPLICATION_USER is required}"
: "${FN_REPLICATION_PASSWORD:?FN_REPLICATION_PASSWORD is required}"
: "${FN_REPLICATION_SLOT:?FN_REPLICATION_SLOT is required}"

PGDATA="${PGDATA:-/var/lib/postgresql/data}"
export PGPASSWORD="$FN_REPLICATION_PASSWORD"

if [ ! -s "$PGDATA/PG_VERSION" ]; then
    echo "standby: waiting for the primary to accept replication connections"
    attempt=0
    until pg_isready -h "$FN_PRIMARY_HOST" -U "$FN_REPLICATION_USER" -q; do
        attempt=$((attempt + 1))
        if [ "$attempt" -gt 120 ]; then
            echo "standby: primary did not become ready; refusing to start" >&2
            exit 1
        fi
        sleep 1
    done

    echo "standby: taking a base backup from ${FN_PRIMARY_HOST}"
    rm -rf "${PGDATA:?}/"* 2>/dev/null || true
    # -R writes standby.signal and primary_conninfo; -Xs streams WAL during the
    # backup so the clone is consistent without relying on WAL archiving.
    pg_basebackup \
        --host="$FN_PRIMARY_HOST" \
        --username="$FN_REPLICATION_USER" \
        --pgdata="$PGDATA" \
        --wal-method=stream \
        --slot="$FN_REPLICATION_SLOT" \
        --write-recovery-conf \
        --checkpoint=fast \
        --progress --verbose
    chmod 0700 "$PGDATA"
    echo "standby: base backup complete"
fi

# application_name identifies this standby in pg_stat_replication, which is
# what the integration suite reads as replication evidence.
#
# The logging flags match the primary's: bind-parameter logging is disabled so
# replayed or replica-side statements cannot publish document identity into
# ordinary database logs.
exec postgres \
    -c hot_standby=on \
    -c hot_standby_feedback=on \
    -c listen_addresses='*' \
    -c primary_slot_name="$FN_REPLICATION_SLOT" \
    -c "primary_conninfo=host=${FN_PRIMARY_HOST} port=5432 user=${FN_REPLICATION_USER} password=${FN_REPLICATION_PASSWORD} application_name=fn_standby_1" \
    -c log_line_prefix='%m [%p] %q%u@%d ' \
    -c log_destination=stderr \
    -c log_statement=none \
    -c log_parameter_max_length=0 \
    -c log_parameter_max_length_on_error=0
