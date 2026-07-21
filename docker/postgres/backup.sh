#!/bin/sh
set -eu

: "${PGHOST:?PGHOST is required}"
: "${PGPORT:?PGPORT is required}"
: "${PGDATABASE:?PGDATABASE is required}"
: "${PGUSER:?PGUSER is required}"
: "${PGPASSWORD:?PGPASSWORD is required}"

backup_dir=${PCRDB_BACKUP_DIR:-/backups}
interval_seconds=${PCRDB_BACKUP_INTERVAL_SECONDS:-86400}
retry_seconds=${PCRDB_BACKUP_RETRY_SECONDS:-900}
retention_count=${PCRDB_BACKUP_RETENTION_COUNT:-1}

case "$interval_seconds:$retry_seconds:$retention_count" in
    *[!0-9:]*|0:*|*:0:*|*:*:0)
        echo "Backup timing and retention values must be positive integers" >&2
        exit 1
        ;;
esac

mkdir -p "$backup_dir"

prune_old_backups() {
    kept=0
    for dump_file in $(ls -1t "$backup_dir"/pcrdb-*.dump 2>/dev/null || true); do
        kept=$((kept + 1))
        if [ "$kept" -le "$retention_count" ]; then
            continue
        fi
        rm -f "$dump_file" "$dump_file.sha256"
    done
}

create_backup() {
    timestamp=$(date -u +%Y%m%dT%H%M%SZ)
    filename="pcrdb-${timestamp}.dump"
    temporary="$backup_dir/.${filename}.tmp"
    final="$backup_dir/$filename"

    rm -f "$temporary"
    echo "Starting PostgreSQL logical backup: $filename"

    if ! pg_dump \
        --format=custom \
        --compress=3 \
        --no-owner \
        --no-privileges \
        --file "$temporary"; then
        rm -f "$temporary"
        return 1
    fi

    if ! pg_restore --list "$temporary" >/dev/null; then
        rm -f "$temporary"
        return 1
    fi

    mv "$temporary" "$final"
    (
        cd "$backup_dir"
        sha256sum "$filename" > "$filename.sha256"
    )
    prune_old_backups
    echo "Completed PostgreSQL logical backup: $filename"
}

while true; do
    if create_backup; then
        sleep "$interval_seconds"
    else
        echo "PostgreSQL logical backup failed; retrying later" >&2
        sleep "$retry_seconds"
    fi
done
