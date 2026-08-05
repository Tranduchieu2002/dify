#!/bin/bash
set -e

PGDATA="${PGDATA:-/var/lib/postgresql/data/pgdata}"
PRIMARY_HOST="${PRIMARY_HOST:-db_postgres}"
PRIMARY_PORT="${PRIMARY_PORT:-5432}"
REPLICATION_USER="${REPLICATION_USER:-replicator}"
REPLICATION_PASSWORD="${REPLICATION_PASSWORD:-replicator_pass}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"

log() { echo "[replica] $*"; }

# Wait for primary to accept connections
log "Waiting for primary at $PRIMARY_HOST:$PRIMARY_PORT ..."
until pg_isready -h "$PRIMARY_HOST" -p "$PRIMARY_PORT" -U "$POSTGRES_USER" 2>/dev/null; do
  sleep 2
done
log "Primary is ready."

if [ ! -f "$PGDATA/PG_VERSION" ]; then
  log "Data directory empty — starting pg_basebackup."
  log "This may take a long time for large databases. Primary stays live during backup."

  mkdir -p "$PGDATA"
  chmod 700 "$PGDATA"

  # Avoid password prompt via .pgpass
  echo "$PRIMARY_HOST:$PRIMARY_PORT:replication:$REPLICATION_USER:$REPLICATION_PASSWORD" > ~/.pgpass
  chmod 600 ~/.pgpass

  pg_basebackup \
    -h "$PRIMARY_HOST" \
    -p "$PRIMARY_PORT" \
    -U "$REPLICATION_USER" \
    -D "$PGDATA" \
    -Fp \
    -Xs \
    -P \
    -R \
    --checkpoint=fast

  log "pg_basebackup complete. Replica will stream WAL from primary."
else
  log "Data directory exists — resuming standby."
fi

log "Starting standby server..."
exec postgres \
  -c 'max_connections=50' \
  -c 'hot_standby=on' \
  -c 'hot_standby_feedback=on'
