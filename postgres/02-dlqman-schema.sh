#!/bin/bash
# Run after DLQman creates its tables via Hibernate auto-DDL.
# This script pre-creates the DLQman tables with TEXT columns to avoid
# the varchar(255) limitation. It runs during PostgreSQL initialization
# against the 'dlqman' database.

set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname dlqman <<-EOSQL
    CREATE TABLE IF NOT EXISTS message (
        id               UUID PRIMARY KEY,
        destinationtopic TEXT,
        fingerprint      TEXT,
        matchedrule      TEXT,
        processat        TIMESTAMP,
        sourceid         TEXT,
        sourceoffset     BIGINT,
        sourcepartition  INTEGER,
        sourcetopic      TEXT,
        status           TEXT,
        waittime         BIGINT
    );

    CREATE TABLE IF NOT EXISTS metadata (
        id         UUID PRIMARY KEY,
        key        TEXT,
        type       INTEGER,
        value      TEXT,
        message_id UUID REFERENCES message(id)
    );
EOSQL
