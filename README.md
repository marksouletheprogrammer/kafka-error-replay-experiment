# CDC DLQ/Replay Prototype

A fully local Docker-based prototype of a CDC streaming error handling, dead letter queue (DLQ), and message replay architecture. This prototype demonstrates the B2 (microservice) approach with a metadata store, REST API, and the DLQman OOB DLQ management UI.

## Architecture

```
┌──────────────────┐
│  Error Simulator │ ── valid Avro + malformed bytes ──┐
└──────────────────┘   (envelope: flat|debezium|       │
                        goldengate)                    ▼
                                              ┌──────────────────┐         ┌──────────────┐
                                              │   Kafka Broker   │ ◄────── │   kafka-ui   │
                                              │     (KRaft)      │         │  (port 8084) │
                                              └────────┬─────────┘         └──────────────┘
                                                       │
                ┌──────────────────────────────────────┼───────────────────────┬─────────────────┐
                │                                      │                       │                 │
         9 source topics                       3 DLQ topics            dlq-replay-terminal   Schema Registry
         (cdc.members,…)                       (dlq-jdbc-sink-*)       (terminal failures)   (port 8081)
                │                                      │                       ▲
                ▼                                      ▼                       │
     ┌─────────────────────────┐          ┌────────────────────────┐           │
     │     Kafka Connect       │          │   jdbc-sink-replay     │───────────┘
     │  3 Debezium JDBC sinks  │── DLQ ──►│   (4th Debezium sink   │── upserts ──┐
     │  (eligibility, claims,  │          │    consuming DLQs)     │             │
     │   providers)            │          └────────────────────────┘             │
     └────────────┬────────────┘                                                  │
                  │                ┌──────────────────┐  ┌──────────────────┐    │
                  │                │     DLQman       │  │  Replay Service  │    │
                  │                │  (Quarkus,       │  │  (Spring Boot,   │    │
                  │                │   port 8080)     │  │   port 8085,     │    │
                  │                │   reads DLQs     │  │   opt-in profile)│    │
                  │                └──────────────────┘  └──────────────────┘    │
                  ▼                                                              │
     ┌─────────────────────────┐                                                 │
     │     PostgreSQL 18       │ ◄───────────────────────────────────────────────┘
     │  ┌───────────────────┐  │
     │  │ datastore schema  │  │ ◄── 9 datastore tables (written by both sink chains)
     │  ├───────────────────┤  │
     │  │  replay schema    │  │ ◄── error_records metadata (replay-service only)
     │  └───────────────────┘  │
     └─────────────────────────┘
```

## Components

| Container | Port | Description |
|---|---|---|
| **kafka** | 9092 | Confluent Kafka broker in KRaft mode (no ZooKeeper). Single broker. |
| **schema-registry** | 8081 | Confluent Schema Registry for Avro schema management. |
| **kafka-connect** | 8083 | Confluent Kafka Connect worker running 4 sink connector instances (3 primary + 1 replay), all using the Debezium JDBC sink (`io.debezium.connector.jdbc.JdbcSinkConnector`). |
| **connector-init** | — | One-shot container that registers all connectors in `kafka-connect/connectors/*.json` via the Connect REST API, then exits. |
| **postgres** | 5432 | PostgreSQL 18 with `datastore` schema (target data) and `replay` schema (metadata store). |
| **error-simulator** | — | Java app that continuously produces valid Avro records and intentionally malformed bytes to all 9 source topics. Envelope format and error rate are configurable — see [Configuration & Modes](#configuration--modes). |
| **kafka-ui** | 8084 | Provectus kafka-ui topic browser. Configured with Schema Registry as the default key/value serde so Avro keys (e.g. Debezium structured `{id: long}` keys) render correctly. |
| **replay-service** | 8085 | Spring Boot microservice: DLQ consumer, metadata store, REST API, replay engine. **Opt-in via `--profile replay-service`** — not started by default. |
| **dlqman** | 8080 | [DLQman](https://github.com/irori-ab/dlqman) — OOB Quarkus-based DLQ processor with rule-based matching, resend strategies, and lifecycle tracking. Consumes all 3 DLQ topics as a background processor. Health check at `/q/health`. **Note:** DLQman's web UI (Quarkus Dev UI) is only available in dev mode; in this Docker Compose setup it runs in production mode as a headless processor. |

## Connectors & Topics

All four sink connectors use the Debezium JDBC sink (`io.debezium.connector.jdbc.JdbcSinkConnector`), which natively understands the Debezium envelope (`{before, after, source, op, ts_ms}`) emitted by the simulator.

| Connector | Source Topics | DLQ Topic | Datastore Tables |
|---|---|---|---|
| `jdbc-sink-eligibility` | `cdc.members`, `cdc.coverage`, `cdc.enrollment` | `dlq-jdbc-sink-eligibility` | `datastore.members`, `datastore.coverage`, `datastore.enrollment` |
| `jdbc-sink-claims` | `cdc.claims`, `cdc.claim_lines`, `cdc.adjustments` | `dlq-jdbc-sink-claims` | `datastore.claims`, `datastore.claim_lines`, `datastore.adjustments` |
| `jdbc-sink-providers` | `cdc.providers`, `cdc.facilities`, `cdc.networks` | `dlq-jdbc-sink-providers` | `datastore.providers`, `datastore.facilities`, `datastore.networks` |
| `jdbc-sink-replay` | all 3 `dlq-jdbc-sink-*` topics | `dlq-replay-terminal` | same 9 `datastore.*` tables |

`jdbc-sink-replay` is a 4th sink that automatically reprocesses messages landing in the primary DLQs. Records that still fail after this second pass go to `dlq-replay-terminal` for manual inspection.

The simulator emits **three** record classes (see [Configuration & Modes](#configuration--modes) for rates):

| Class | Goes to | Outcome |
|---|---|---|
| Good | `cdc.*` | Primary sink upserts into `datastore.*`. |
| Unrecoverable bad | `cdc.*` (raw garbage bytes) | Primary sink fails → `dlq-jdbc-sink-*` → replay sink also fails → `dlq-replay-terminal`. |
| Recoverable DLQ | `dlq-jdbc-sink-*` directly, with `__connect.errors.*` headers | Replay sink + `ReplayHeaderToTopic` SMT route it back to `datastore.<table>` and the upsert succeeds. |

Each connector is configured with:
- `errors.tolerance = all`
- `errors.deadletterqueue.context.headers.enable = true`
- `insert.mode = upsert`, `primary.key.mode = record_value`, `primary.key.fields = id`
- `schema.evolution = none` (the replay connector uses `basic` so it can create the same tables on first run)

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
- The following ports must be free: 5432, 8080, 8081, 8083, 8084, 8085, 9092

## Quick Start

```bash
# Build images and start the default stack (no replay-service)
docker compose up --build -d

# Watch logs
docker compose logs -f

# Wait ~60-90 seconds for all services to be healthy
docker compose ps
```

The first build takes several minutes (Maven dependency downloads for `error-simulator`, `dlqman`, and the custom SMT bundled into the `kafka-connect` image). Subsequent starts are faster due to Docker layer caching.

The `replay-service` is **not** started by default — it is gated behind a Compose profile. See [Optional: Replay Service](#optional-replay-service) below if you want the REST API + metadata store described in the Walkthrough.

## Configuration & Modes

Most knobs live on `error-simulator` in `docker-compose.yml`:

| Variable | Default | Notes |
|---|---|---|
| `ENVELOPE_FORMAT` | `debezium` | One of `flat`, `debezium`, `goldengate`. Controls the value envelope and key shape. **Only `debezium` round-trips through the JDBC sinks today** — the sinks expect a Debezium envelope. The other two modes are useful for inspecting key/value formats in kafka-ui. |
| `ERROR_RATE` | `0.3` | Fraction of messages emitted as failures (DLQ-bound). |
| `RECOVERABLE_RATE` | `0.1` | Of the failing records (per `ERROR_RATE`), the fraction sent as **recoverable DLQ records** — well-formed Avro published directly to a `dlq-jdbc-sink-*` topic with synthetic Connect error headers. The rest are unrecoverable garbage bytes sent to the source `cdc.*` topics. |
| `BATCH_SIZE` | `20` | Messages per loop iteration. |
| `INTERVAL_MS` | `5000` | Milliseconds between batches. |

Key shape per envelope mode (all serialized via `KafkaAvroSerializer`):

| Mode | Key | Value envelope |
|---|---|---|
| `flat` | Avro primitive `long` | bare data record |
| `debezium` | Avro record `{id: long}` | `{before, after, source, op, ts_ms}` |
| `goldengate` | Avro primitive `string` | `{table, op_type, op_ts, current_ts, pos, primary_keys, before, after}` |

To change a setting:

```bash
# Edit docker-compose.yml -> error-simulator.environment, then either:
docker compose up -d --force-recreate error-simulator
# ...or, if changing ENVELOPE_FORMAT (key/value schemas change shape):
docker compose down -v && docker compose up -d --build
```

## Optional: Replay Service

```bash
docker compose --profile replay-service up -d
```

All the `http://localhost:8085/...` examples in the Walkthrough require this profile. Without it, only the auto-replay path (`jdbc-sink-replay` connector) and DLQman are active.

To stop just the replay-service:

```bash
docker compose --profile replay-service stop replay-service
```

## Observing the Replay Flow

With the default rates, you should see roughly 70% good records, 27% unrecoverable failures, and 3% recoverable replays. To make replays easy to spot, crank the rates:

```bash
# Edit docker-compose.yml -> error-simulator.environment:
#   ERROR_RATE: "1.0"        # every record fails
#   RECOVERABLE_RATE: "1.0"  # ...and every failure is recoverable
docker compose down -v && docker compose up -d --build
```

Then:

```bash
# 1. Watch the simulator log the recoverable sends
docker compose logs -f error-simulator | grep RECOVERABLE

# 2. Tail kafka-connect for successful replay-sink upserts (no "Unknown magic byte")
docker compose logs -f kafka-connect | grep -E "jdbc-sink-replay|ERROR"

# 3. Inspect a recoverable DLQ record in kafka-ui (http://localhost:8084)
#    Browse topic dlq-jdbc-sink-claims and confirm:
#      - keySerde / valueSerde resolve to SchemaRegistry (key/value are real Avro)
#      - headers include __connect.errors.topic and __connect.errors.connector.name

# 4. Watch row counts grow in datastore.* as the replay sink writes recovered rows
watch -n 2 'docker exec postgres psql -U postgres -d appdb -c \
  "SELECT '"'"'members'"'"' t, count(*) FROM datastore.members
   UNION ALL SELECT '"'"'claims'"'"', count(*) FROM datastore.claims
   UNION ALL SELECT '"'"'providers'"'"', count(*) FROM datastore.providers;"'

# 4b. Find ONLY the replayed rows. The simulator stamps recoverable-DLQ records with
#     an id offset of 9_000_000_000_000, so they're trivially queryable in any table:
docker exec postgres psql -U postgres -d appdb -c "
  SELECT 'members'   t, count(*) FROM datastore.members   WHERE id >= 9000000000000
  UNION ALL SELECT 'claims',    count(*) FROM datastore.claims    WHERE id >= 9000000000000
  UNION ALL SELECT 'providers', count(*) FROM datastore.providers WHERE id >= 9000000000000;"

# Or look at a few of them directly:
docker exec postgres psql -U postgres -d appdb -c \
  "SELECT * FROM datastore.claims WHERE id >= 9000000000000 LIMIT 5;"

# 5. Confirm only truly malformed records reach the terminal DLQ
docker exec kafka kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic dlq-replay-terminal --from-beginning --max-messages 3 --timeout-ms 5000
```

Return to defaults by reverting `ERROR_RATE`/`RECOVERABLE_RATE` in `docker-compose.yml` and recreating the simulator:

```bash
docker compose up -d --force-recreate error-simulator
```

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
| kafka-ui | http://localhost:8084 | Topic browser, message inspector, connector status |
| PostgreSQL | `localhost:5432` | Database (user: `postgres`, pass: `postgres`, db: `appdb`) |
| Replay Service | http://localhost:8085 | Replay REST API (only when `--profile replay-service` is active) |
| DLQman | http://localhost:8080 | Headless DLQ processor REST API |

## Reset & Teardown

```bash
# Stop containers, keep images & volumes (fast restart)
docker compose down

# Full reset — also drops Postgres data, Kafka topics, and registered schemas
docker compose down -v

# Rebuild after code changes (error-simulator, dlqman, kafka-connect SMT)
docker compose up -d --build

# Recreate just one service (e.g. after editing docker-compose env vars)
docker compose up -d --force-recreate error-simulator

# Re-register connectors without bouncing the broker / DB
docker compose up -d connector-init
```

If you change `ENVELOPE_FORMAT` (or anything else that changes Kafka key/value schemas), do a full reset — the existing topics will have records in the old format and the JDBC sinks will fail until they're cleared:

```bash
docker compose down -v && docker compose up -d --build
```

## Related Documentation

- [DLQman](https://github.com/irori-ab/dlqman) — OOB DLQ management tool
