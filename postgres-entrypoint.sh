#!/bin/bash
#postgres-entrypoint.sh
set -e

docker-entrypoint.sh postgres &

until psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c 'SELECT 1' 2>/dev/null; do
    echo "Waiting for PostgreSQL to be ready..."
    sleep 1
done

psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/init.sql

wait