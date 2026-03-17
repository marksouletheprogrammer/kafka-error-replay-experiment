-- ============================================
-- CDC DLQ/Replay Prototype
-- PostgreSQL initialization script
-- ============================================

-- Datastore schema (target data store)
CREATE SCHEMA IF NOT EXISTS datastore;

-- Replay schema (metadata store)
CREATE SCHEMA IF NOT EXISTS replay;

-- DLQman database (created separately since DLQman uses Hibernate auto-DDL)
-- PostgreSQL entrypoint runs init scripts against the default DB, so we
-- create the dlqman DB here for DLQman's reactive driver to connect to.
CREATE DATABASE dlqman;

-- ============================================
-- Target Datastore Tables (simulated CDC targets)
-- ============================================

-- Eligibility domain
CREATE TABLE datastore.members (
    id          BIGINT PRIMARY KEY,
    first_name  VARCHAR(100),
    last_name   VARCHAR(100),
    email       VARCHAR(255),
    date_of_birth VARCHAR(20),
    source_pos      BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE datastore.coverage (
    id              BIGINT PRIMARY KEY,
    member_id       BIGINT,
    plan_code       VARCHAR(50),
    effective_date  VARCHAR(20),
    termination_date VARCHAR(20),
    source_pos          BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE datastore.enrollment (
    id              BIGINT PRIMARY KEY,
    member_id       BIGINT,
    program         VARCHAR(100),
    status          VARCHAR(20),
    enrollment_date VARCHAR(20),
    source_pos          BIGINT NOT NULL DEFAULT 0
);

-- Claims domain
CREATE TABLE datastore.claims (
    id              BIGINT PRIMARY KEY,
    member_id       BIGINT,
    provider_id     BIGINT,
    claim_date      VARCHAR(20),
    total_amount    DOUBLE PRECISION,
    status          VARCHAR(20),
    source_pos          BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE datastore.claim_lines (
    id              BIGINT PRIMARY KEY,
    claim_id        BIGINT,
    line_number     INT,
    procedure_code  VARCHAR(20),
    amount          DOUBLE PRECISION,
    source_pos          BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE datastore.adjustments (
    id              BIGINT PRIMARY KEY,
    claim_id        BIGINT,
    adjustment_type VARCHAR(50),
    amount          DOUBLE PRECISION,
    reason_code     VARCHAR(20),
    source_pos          BIGINT NOT NULL DEFAULT 0
);

-- Providers domain
CREATE TABLE datastore.providers (
    id              BIGINT PRIMARY KEY,
    npi             VARCHAR(20),
    name            VARCHAR(200),
    specialty       VARCHAR(100),
    tax_id          VARCHAR(20),
    source_pos          BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE datastore.facilities (
    id              BIGINT PRIMARY KEY,
    name            VARCHAR(200),
    facility_type   VARCHAR(50),
    address         VARCHAR(300),
    state           VARCHAR(2),
    source_pos          BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE datastore.networks (
    id              BIGINT PRIMARY KEY,
    network_name    VARCHAR(200),
    network_type    VARCHAR(50),
    region          VARCHAR(100),
    effective_date  VARCHAR(20),
    source_pos          BIGINT NOT NULL DEFAULT 0
);

-- ============================================
-- Replay Metadata Store
-- ============================================

CREATE TABLE replay.error_records (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    connector_name      VARCHAR(100) NOT NULL,
    original_topic      VARCHAR(255) NOT NULL,
    original_partition  INT,
    original_offset     BIGINT,
    error_code          VARCHAR(100),
    error_message       TEXT,
    message_key         BYTEA,
    message_payload     BYTEA,
    source_position         BIGINT,
    replay_count        INT NOT NULL DEFAULT 0,
    status              VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    created_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    last_replayed_at    TIMESTAMP WITH TIME ZONE,
    resolved_at         TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_error_records_status ON replay.error_records(status);
CREATE INDEX idx_error_records_connector ON replay.error_records(connector_name);
CREATE INDEX idx_error_records_topic ON replay.error_records(original_topic);
CREATE INDEX idx_error_records_created ON replay.error_records(created_at);
