#!/bin/bash
set -e

PGDATA="${PGDATA:-/var/lib/postgresql/data/pgdata}"
PRIMARY_HOST="${PRIMARY_HOST:-db_postgres}"
PRIMARY_PORT="${PRIMARY_PORT:-5432}"
REPLICATION_USER="${REPLICATION_USER:-replicator}"
REPLICATION_PASSWORD="${REPLICATION_PASSWORD:-replicator_pass}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"

log() { echo "[replica] $*"; }

log "Waiting for primary at $PRIMARY_HOST:$PRIMARY_PORT ..."
until pg_isready -h "$PRIMARY_HOST" -p "$PRIMARY_PORT" -U "$POSTGRES_USER" 2>/dev/null; do
  sleep 2
done
log "Primary is ready."

if [ ! -f "$PGDATA/PG_VERSION" ]; then
  log "Data directory empty — starting pg_basebackup."
  log "Primary stays live during backup."

  mkdir -p "$PGDATA"

  PGPASS_FILE="$(mktemp)"
  echo "$PRIMARY_HOST:$PRIMARY_PORT:replication:$REPLICATION_USER:$REPLICATION_PASSWORD" > "$PGPASS_FILE"
  chmod 600 "$PGPASS_FILE"

  PGPASSFILE="$PGPASS_FILE" pg_basebackup \
    -h "$PRIMARY_HOST" \
    -p "$PRIMARY_PORT" \
    -U "$REPLICATION_USER" \
    -D "$PGDATA" \
    -Fp -Xs -P -R \
    --checkpoint=fast

  rm -f "$PGPASS_FILE"
  log "pg_basebackup complete."
else
  log "Data directory exists — resuming standby."
fi

# pg_basebackup -R writes primary_conninfo but omits the password.
# Ensure it is always present so WAL streaming works after restarts.
CONNINFO="host=$PRIMARY_HOST port=$PRIMARY_PORT user=$REPLICATION_USER password=$REPLICATION_PASSWORD"
AUTO_CONF="$PGDATA/postgresql.auto.conf"
if grep -q "^primary_conninfo" "$AUTO_CONF" 2>/dev/null; then
  sed -i "s|^primary_conninfo.*|primary_conninfo = '$CONNINFO'|" "$AUTO_CONF"
else
  echo "primary_conninfo = '$CONNINFO'" >> "$AUTO_CONF"
fi
log "primary_conninfo updated."

chown -R postgres:postgres "$PGDATA"
chmod 700 "$PGDATA"

log "Starting standby server..."
exec su-exec postgres postgres \
  -c "max_connections=${POSTGRES_MAX_CONNECTIONS:-100}" \
  -c 'hot_standby=on' \
  -c 'hot_standby_feedback=on'
