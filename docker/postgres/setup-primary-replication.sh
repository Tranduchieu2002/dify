#!/bin/bash
# Run ONCE from the docker/ directory before starting the replica.
# Usage: bash postgres/setup-primary-replication.sh
set -e

COMPOSE_FILES="-f docker-compose.yaml -f docker-compose.replica.yaml"
REPLICATION_PASSWORD="${REPLICATION_PASSWORD:-replicator_pass}"

log() { echo "==> $*"; }

log "Step 1: Creating replication user on primary..."
docker compose $COMPOSE_FILES --profile postgresql exec -T db_postgres \
  psql -U "${DB_USERNAME:-postgres}" -c "
    DO \$\$
    BEGIN
      IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'replicator') THEN
        CREATE USER replicator WITH REPLICATION ENCRYPTED PASSWORD '$REPLICATION_PASSWORD';
        RAISE NOTICE 'User replicator created.';
      ELSE
        ALTER USER replicator WITH ENCRYPTED PASSWORD '$REPLICATION_PASSWORD';
        RAISE NOTICE 'User replicator already exists — password updated.';
      END IF;
    END
    \$\$;
  "

log "Step 2: Adding pg_hba.conf entry for replication..."
docker compose $COMPOSE_FILES --profile postgresql exec -T db_postgres bash -c "
  if ! grep -q 'replication.*replicator' \$PGDATA/pg_hba.conf; then
    echo 'host replication replicator all md5' >> \$PGDATA/pg_hba.conf
    echo '  Added replication entry.'
  else
    echo '  Entry already exists — skipping.'
  fi
"

log "Step 3: Reloading pg_hba.conf (no downtime)..."
docker compose $COMPOSE_FILES --profile postgresql exec -T db_postgres \
  psql -U "${DB_USERNAME:-postgres}" -c "SELECT pg_reload_conf();"

echo ""
echo "✓ Primary configured. Next steps:"
echo ""
echo "  # Restart primary with wal_level=replica (~5s downtime):"
echo "  docker compose $COMPOSE_FILES --profile postgresql up -d db_postgres"
echo ""
echo "  # Then start the replica (pg_basebackup runs automatically on first start):"
echo "  docker compose $COMPOSE_FILES --profile postgresql up -d db_postgres_replica"
echo ""
echo "  # Watch replica logs:"
echo "  docker logs -f dify-db-replica"
echo ""
echo "  # Verify replication is active:"
echo "  docker compose $COMPOSE_FILES --profile postgresql exec db_postgres \\"
echo "    psql -U ${DB_USERNAME:-postgres} -c \"SELECT * FROM pg_stat_replication;\""
