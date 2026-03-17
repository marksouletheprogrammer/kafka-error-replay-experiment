# CDC DLQ/Replay Prototype

A fully local Docker-based prototype of a CDC streaming error handling, dead letter queue (DLQ), and message replay architecture. This prototype demonstrates the B2 (microservice) approach with a metadata store, REST API, and the DLQman OOB DLQ management UI.

## Architecture

```
┌──────────────────┐
│  Error Simulator │ ── produces valid Avro + malformed bytes ──┐
└──────────────────┘                                            │
                                                                ▼
                                                     ┌──────────────────┐
                                                     │   Kafka Broker   │
                                                     │    (KRaft)       │
                                                     └────────┬─────────┘
                                                              │
                              ┌────────────────────────────────┼────────────────────────────────┐
                              │                                │                                │
                    9 source topics                   3 DLQ topics                     Schema Registry
                    (cdc.members, etc.)               (dlq-jdbc-sink-*)
                              │                                │
                              ▼                                ▼
                   ┌─────────────────────┐          ┌──────────────────┐
                   │   Kafka Connect     │          │  Replay Service  │──── REST API (port 8085)
                   │  (3 JDBC Sink       │──DLQ──►  │  (Spring Boot)   │
                   │   Connectors)       │          │  + Metadata Store│──┐
                   └─────────┬───────────┘          └──────────────────┘  │
                             │                                            │
                             │                      ┌──────────────────┐  │
                             │                      │     DLQman       │  │
                             │                      │  (Quarkus UI)    │──┘── both read DLQ topics
                             │                      │  port 8080       │
                             ▼                      └──────────────────┘
                   ┌─────────────────────┐
                   │    PostgreSQL 18    │
                   │  ┌───────────────┐  │
                   │  │datastore schema│  │ ◄── 9 datastore tables
                   │  ├───────────────┤  │
                   │  │ replay schema │  │ ◄── error_records metadata store
                   │  └───────────────┘  │
                   └─────────────────────┘
```

## Components

| Container | Port | Description |
|---|---|---|
| **kafka** | 9092 | Confluent Kafka broker in KRaft mode (no ZooKeeper). Single broker. |
| **schema-registry** | 8081 | Confluent Schema Registry for Avro schema management. |
| **kafka-connect** | 8083 | Confluent Kafka Connect worker running 3 JDBC Sink Connector instances. |
| **connector-init** | — | One-shot container that registers the 3 sink connectors via the Connect REST API, then exits. |
| **postgres** | 5432 | PostgreSQL 18 with `datastore` schema (target data) and `replay` schema (metadata store). |
| **error-simulator** | — | Java app that continuously produces valid Avro records and intentionally malformed bytes to all 9 source topics. |
| **replay-service** | 8085 | Spring Boot microservice: DLQ consumer, metadata store, REST API, replay engine. |
| **dlqman** | 8080 | [DLQman](https://github.com/irori-ab/dlqman) — OOB Quarkus-based DLQ processor with rule-based matching, resend strategies, and lifecycle tracking. Consumes all 3 DLQ topics as a background processor. Health check at `/q/health`. **Note:** DLQman's web UI (Quarkus Dev UI) is only available in dev mode; in this Docker Compose setup it runs in production mode as a headless processor. |

## Connectors & Topics

| Connector | Source Topics | DLQ Topic | Datastore Tables |
|---|---|---|---|
| `jdbc-sink-eligibility` | `cdc.members`, `cdc.coverage`, `cdc.enrollment` | `dlq-jdbc-sink-eligibility` | `datastore.members`, `datastore.coverage`, `datastore.enrollment` |
| `jdbc-sink-claims` | `cdc.claims`, `cdc.claim_lines`, `cdc.adjustments` | `dlq-jdbc-sink-claims` | `datastore.claims`, `datastore.claim_lines`, `datastore.adjustments` |
| `jdbc-sink-providers` | `cdc.providers`, `cdc.facilities`, `cdc.networks` | `dlq-jdbc-sink-providers` | `datastore.providers`, `datastore.facilities`, `datastore.networks` |

Each connector is configured with:
- `errors.tolerance = all`
- `errors.deadletterqueue.context.headers.enable = true`
- `insert.mode = upsert` with primary key and `source_pos` for idempotent writes

## Fake Data

The error simulator generates records for 9 CDC entities across 3 domains:

**Eligibility domain:**
- **members** — id, first_name, last_name, email, date_of_birth, source_pos
- **coverage** — id, member_id, plan_code, effective_date, termination_date, source_pos
- **enrollment** — id, member_id, program, status, enrollment_date, source_pos

**Claims domain:**
- **claims** — id, member_id, provider_id, claim_date, total_amount, status, source_pos
- **claim_lines** — id, claim_id, line_number, procedure_code, amount, source_pos
- **adjustments** — id, claim_id, adjustment_type, amount, reason_code, source_pos

**Providers domain:**
- **providers** — id, npi, name, specialty, tax_id, source_pos
- **facilities** — id, name, facility_type, address, state, source_pos
- **networks** — id, network_name, network_type, region, effective_date, source_pos

### How Errors Are Generated

The simulator sends two types of messages:
- **Valid records** (~70%) — well-formed Avro records that the JDBC Sink Connector successfully writes to PostgreSQL.
- **Bad records** (~30%) — raw garbage bytes that cannot be deserialized as Avro. The connector's Avro deserializer rejects them, and they are routed to the DLQ topic with `__connect.errors.*` headers.

The error rate is configurable via the `ERROR_RATE` environment variable (default: 0.3).

## Assumptions & Simplifications

- **No real CDC source** — there is no upstream CDC system. The error simulator produces fake CDC-like messages directly to Kafka.
- **Single Kafka broker** — no replication, no high availability. Sufficient for demonstrating the DLQ/replay pattern.
- **No compliance controls** — no ACLs, no encryption, no data protection. This is a local prototype.
- **No Avro for bad records** — errors are generated by sending raw bytes (not malformed Avro). This triggers deserialization failures, which is the most common DLQ-eligible error type.
- **Direct-to-DB replay is a placeholder** — the replay service logs the action but does not actually deserialize the Avro payload and write to PostgreSQL. Re-produce mode is fully functional.
- **`source_pos` is a monotonic counter** — simulates a CDC source position for idempotent upserts.

## Prerequisites

- **Docker** (with Docker Compose v2)
- **~4 GB RAM** available for Docker
- The following ports must be free: 5432, 8080, 8081, 8083, 8085, 9092

## Quick Start

```bash
# Clone and start everything
docker compose up --build -d

# Watch logs
docker compose logs -f

# Wait ~60-90 seconds for all services to be healthy
docker compose ps
```

The first build takes several minutes (Maven dependency downloads for the Java apps). Subsequent starts are faster due to Docker layer caching.

## Walkthrough

### 1. Verify services are running

```bash
docker compose ps
```

All services should show `healthy` or `running` (connector-init will show `exited` — that's expected).

### 2. Check connector status

```bash
# List connectors
curl -s http://localhost:8083/connectors | jq .

# Check a specific connector
curl -s http://localhost:8083/connectors/jdbc-sink-eligibility/status | jq .
```

### 3. Verify valid records in PostgreSQL

```bash
docker exec -it postgres psql -U postgres -d appdb -c "SELECT COUNT(*) FROM datastore.members;"
docker exec -it postgres psql -U postgres -d appdb -c "SELECT * FROM datastore.members LIMIT 5;"
```

### 4. View DLQ messages via Replay Service API

```bash
# List all captured errors
curl -s http://localhost:8085/errors | jq .

# Filter by connector
curl -s "http://localhost:8085/errors?connector=jdbc-sink-eligibility" | jq .

# Filter by status
curl -s "http://localhost:8085/errors?status=PENDING" | jq .

# Get statistics
curl -s http://localhost:8085/errors/stats | jq .
```

### 5. View DLQ messages via DLQman REST API

DLQman runs as a headless DLQ processor in production mode (no web UI). Use the REST API:

```bash
# List all DLQ messages captured by DLQman
curl -s http://localhost:8080/v1/messages | jq .

# List active ingester processes (one per DLQ source)
curl -s http://localhost:8080/v1/processes | jq .

# Health check
curl -s http://localhost:8080/q/health | jq .
```

DLQman provides:
- Automatic DLQ message ingestion from all 3 DLQ topics
- Rule-based matching (deserialization errors, converter errors, catch-all)
- Configurable resend strategies with backoff

### 6. Trigger replay via API

```bash
# Replay a single record (re-produce to original topic)
curl -s -X POST "http://localhost:8085/errors/{id}/replay?mode=re-produce" | jq .

# Replay a single record (direct-to-db, placeholder)
curl -s -X POST "http://localhost:8085/errors/{id}/replay?mode=direct-to-db" | jq .

# Bulk replay all PENDING errors for a connector
curl -s -X POST "http://localhost:8085/errors/replay?connector=jdbc-sink-claims&mode=re-produce" | jq .

# Abandon a record
curl -s -X POST "http://localhost:8085/errors/{id}/abandon" | jq .
```

### 7. Observe replay counting and status lifecycle

```bash
# After replaying, check the record status
curl -s http://localhost:8085/errors/{id} | jq '{status, replayCount, lastReplayedAt, resolvedAt}'

# View stats to see status distribution
curl -s http://localhost:8085/errors/stats | jq .
```

Status lifecycle: `PENDING` → `REPLAYING` → `RESOLVED` or back to `PENDING` (on failure). After exceeding max replay count (default 3): `ABANDONED`.

## API Reference

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/errors` | List errors. Query params: `status`, `connector`, `topic` |
| `GET` | `/errors/{id}` | Get single error record by UUID |
| `POST` | `/errors/{id}/replay` | Replay one record. Query param: `mode` (`re-produce` or `direct-to-db`) |
| `POST` | `/errors/replay` | Bulk replay. Query params: `status`, `connector`, `topic`, `mode` |
| `POST` | `/errors/{id}/abandon` | Mark record as ABANDONED |
| `GET` | `/errors/stats` | Summary counts by status, error code, and connector |

## Ports & URLs

| Service | URL | Description |
|---|---|---|
| Kafka broker | `localhost:9092` | External listener for local Kafka tools |
| Schema Registry | http://localhost:8081 | Schema Registry REST API |
| Kafka Connect | http://localhost:8083 | Connect REST API (connector management) |
| PostgreSQL | `localhost:5432` | Database (user: `postgres`, pass: `postgres`, db: `appdb`) |
| Replay Service | http://localhost:8085 | Replay REST API |
| DLQman | http://localhost:8080 | DLQ management UI |

## Teardown

```bash
# Stop and remove all containers and volumes
docker compose down -v
```

## Related Documentation

- [DLQman](https://github.com/irori-ab/dlqman) — OOB DLQ management tool
