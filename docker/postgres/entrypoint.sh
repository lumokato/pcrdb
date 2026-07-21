#!/bin/sh
set -eu

: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"
: "${POSTGRES_DB:?POSTGRES_DB is required}"
: "${PCRDB_WEB_PASSWORD:?PCRDB_WEB_PASSWORD is required}"
: "${PCRDB_WORKER_PASSWORD:?PCRDB_WORKER_PASSWORD is required}"

ready_file="${PGDATA}/.pcrdb-migrations-ready"
rm -f "$ready_file"

docker-entrypoint.sh postgres &
postgres_pid=$!

shutdown() {
    kill -TERM "$postgres_pid" 2>/dev/null || true
    wait "$postgres_pid" || true
}
trap shutdown INT TERM

until PGPASSWORD="$POSTGRES_PASSWORD" pg_isready \
    --host 127.0.0.1 \
    --port 5432 \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" >/dev/null 2>&1; do
    if ! kill -0 "$postgres_pid" 2>/dev/null; then
        wait "$postgres_pid"
        exit $?
    fi
    sleep 1
done

export PGPASSWORD="$POSTGRES_PASSWORD"

for migration in /opt/pcrdb/migrations/*.sql; do
    [ -f "$migration" ] || continue
    psql \
        --host 127.0.0.1 \
        --port 5432 \
        --username "$POSTGRES_USER" \
        --dbname "$POSTGRES_DB" \
        --file "$migration"
done

psql \
    --host 127.0.0.1 \
    --port 5432 \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --file /opt/pcrdb/grant_roles.sql

touch "$ready_file"
wait "$postgres_pid"
