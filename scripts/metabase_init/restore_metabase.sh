#!/bin/bash
# Restore Metabase dashboards/config from backup into the metabase database.
# This runs automatically on first PostgreSQL startup (empty volume only)
# via docker-entrypoint-initdb.d.

BACKUP_FILE="/docker-entrypoint-initdb.d/metabase/metabase_backup.sql"

if [ -f "$BACKUP_FILE" ]; then
    echo "Restoring Metabase backup from $BACKUP_FILE ..."
    psql -U "$POSTGRES_USER" -d metabase -f "$BACKUP_FILE"
    echo "Metabase backup restored successfully."
else
    echo "No Metabase backup found at $BACKUP_FILE, skipping restore."
fi
