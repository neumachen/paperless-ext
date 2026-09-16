#!/bin/sh
# Primary initialisation: create the replication role and a physical
# replication slot for the standby.
#
# The slot makes the primary retain WAL the standby has not yet consumed, so a
# standby that is down for a while can catch up instead of needing a rebuild.
# It also means a permanently dead standby would accumulate WAL on the primary;
# that trade-off is stated in the deployment notes.
set -eu

: "${FN_REPLICATION_USER:?FN_REPLICATION_USER is required}"
: "${FN_REPLICATION_PASSWORD:?FN_REPLICATION_PASSWORD is required}"
: "${FN_REPLICATION_SLOT:?FN_REPLICATION_SLOT is required}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<SQL
DO \$\$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${FN_REPLICATION_USER}') THEN
        CREATE ROLE ${FN_REPLICATION_USER} WITH REPLICATION LOGIN PASSWORD '${FN_REPLICATION_PASSWORD}';
    END IF;
END
\$\$;

SELECT pg_create_physical_replication_slot('${FN_REPLICATION_SLOT}')
 WHERE NOT EXISTS (
     SELECT 1 FROM pg_replication_slots WHERE slot_name = '${FN_REPLICATION_SLOT}'
 );
SQL

# Allow the replication role in from the compose network only. The container
# network is private to this project; no host port is published for the
# database, and credentials come from the project's .env file.
cat >> "$PGDATA/pg_hba.conf" <<HBA
# File Normalizer: streaming replication from the project network.
host    replication     ${FN_REPLICATION_USER}      all                     scram-sha-256
HBA
