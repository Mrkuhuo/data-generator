# Multisource Data Generator

This repository is being rewritten into a platform that generates synthetic data for multiple delivery targets instead of a MySQL/Kafka-specific demo.

## Current status

- Legacy implementation is archived under `legacy/backend-legacy` and `legacy/frontend-legacy`
- New backend foundation is in `backend/`
- New frontend shell is in `frontend/`
- Real runtime delivery is implemented for file, HTTP, MySQL, PostgreSQL, and Kafka
- Quartz-backed scheduling, pause/resume/disable controls, and execution snapshots are now wired into the rebuilt platform
- The new schema now models:
  - connector instances
  - dataset definitions
  - job definitions
  - job executions
  - job execution logs

## Target product shape

The rebuilt platform is aimed at these capability layers:

1. Connector center for MySQL, PostgreSQL, Kafka, HTTP, and file outputs
2. Dataset studio for rule-driven synthetic schema definitions
3. Job control for delivery, scheduling, retries, and runtime configuration
4. Execution ledger for logs, outcomes, and observability

## Tech stack

- Backend: Spring Boot 3, Java 21, JPA, Flyway, Quartz
- Frontend: Vue 3, TypeScript, Vite
- Database: MySQL 8

## Local development

### Backend

```bash
cd backend
mvn spring-boot:run
```

### Frontend

```bash
cd frontend
npm install
npm run dev
```

### Infrastructure

```bash
docker compose up -d mysql postgres redpanda redpanda-init http-echo
```

The backend expects these environment variables when not using the defaults:

- `APP_DATASOURCE_URL`
- `APP_DATASOURCE_USERNAME`
- `APP_DATASOURCE_PASSWORD`

Optional quickstart target overrides:

- `MDG_QUICKSTART_HTTP_BASE_URL`
- `MDG_QUICKSTART_MYSQL_JDBC_URL`
- `MDG_QUICKSTART_POSTGRESQL_JDBC_URL`
- `MDG_QUICKSTART_KAFKA_BOOTSTRAP_SERVERS`

When the backend runs inside `docker compose`, these values are already injected so quickstart connectors point to the compose services.

### Local target services

The compose file now provisions:

- `mysql`: application database plus `demo_sink.synthetic_user_activity`
- `postgres`: `demo_sink.synthetic_user_activity`
- `redpanda`: Kafka-compatible broker with topic `synthetic.user.activity`
- `http-echo`: simple HTTP request echo target on port `9000`

If your MySQL or PostgreSQL volumes were created before the new init scripts were added, recreate those volumes once so the demo sink tables are initialized.

## API entry points

- Overview: `GET /api/overview`
- Connectors: `GET/POST/PUT/DELETE /api/connectors`
- Connector quickstart: `POST /api/connectors/quickstart/file`
- Connector quickstart: `POST /api/connectors/quickstart/http`
- Connector quickstart: `POST /api/connectors/quickstart/mysql`
- Connector quickstart: `POST /api/connectors/quickstart/postgresql`
- Connector quickstart: `POST /api/connectors/quickstart/kafka`
- Connector test: `POST /api/connectors/{id}/test`
- Datasets: `GET/POST/PUT/DELETE /api/datasets`
- Dataset quickstart: `POST /api/datasets/quickstart`
- Dataset preview: `POST /api/datasets/{id}/preview`
- Jobs: `GET/POST/PUT/DELETE /api/jobs`
- Job quickstart: `POST /api/jobs/quickstart`
- Job run: `POST /api/jobs/{id}/run`
- Job pause: `POST /api/jobs/{id}/pause`
- Job resume: `POST /api/jobs/{id}/resume`
- Job disable: `POST /api/jobs/{id}/disable`
- Executions: `GET /api/executions`

## Preview schema DSL

The new preview engine accepts a JSON object schema rooted at:

```json
{
  "type": "object",
  "fields": {
    "userId": { "rule": "sequence", "start": 10001, "step": 1 },
    "city": {
      "rule": "weighted_enum",
      "options": [
        { "value": "Shanghai", "weight": 4 },
        { "value": "Beijing", "weight": 3 }
      ]
    },
    "score": { "rule": "random_decimal", "min": 60, "max": 99.99, "scale": 2 },
    "active": { "rule": "boolean", "trueRate": 0.8 },
    "createdAt": { "rule": "datetime", "from": "2025-01-01T00:00:00Z", "to": "2025-12-31T23:59:59Z" },
    "profile": {
      "rule": "object",
      "fields": {
        "deviceId": { "rule": "string", "prefix": "dev-", "length": 10 },
        "channel": { "rule": "enum", "values": ["app", "web"] }
      }
    },
    "tags": {
      "rule": "array",
      "sizeMin": 1,
      "sizeMax": 3,
      "item": { "rule": "enum", "values": ["new", "vip", "trial"] }
    },
    "email": { "rule": "template", "template": "user-${userId}@demo.local" }
  }
}
```

Currently implemented rules:

- `fixed`
- `sequence`
- `random_int`
- `random_decimal`
- `string`
- `enum`
- `weighted_enum`
- `boolean`
- `datetime`
- `object`
- `array`
- `reference`
- `template`

## Current runtime support

- `FILE` connectors support real delivery in `jsonl`, `json`, and `csv`
- `HTTP` connectors support real delivery with `POST`, `PUT`, or `PATCH`
- `MYSQL` connectors support real batch insert delivery with `APPEND` and `OVERWRITE`
- `POSTGRESQL` connectors support real batch insert delivery with `APPEND` and `OVERWRITE`
- `KAFKA` connectors support real message delivery with `APPEND` and `STREAM`

## Runtime config examples

MySQL / PostgreSQL jobs use `target.table` and can optionally pin column order:

```json
{
  "count": 1000,
  "seed": 20260412,
  "batchSize": 500,
  "target": {
    "table": "synthetic_user_activity",
    "columns": ["userId", "city", "score", "active", "createdAt", "profile", "tags", "email"]
  }
}
```

Kafka jobs use `target.topic`, with optional partition, message key, and headers:

```json
{
  "count": 1000,
  "seed": 20260412,
  "target": {
    "topic": "synthetic.user.activity",
    "keyField": "userId",
    "headers": {
      "source": "multisource-data-generator"
    }
  }
}
```

One-time jobs use `schedule.triggerAt` in ISO-8601 format:

```json
{
  "count": 200,
  "seed": 20260412,
  "schedule": {
    "triggerAt": "2026-04-12T14:00:00Z"
  },
  "target": {
    "table": "synthetic_user_activity"
  }
}
```

Cron jobs use `scheduleType=CRON` with `cronExpression`, for example:

```text
0 */5 * * * ?
```

The front-end job workspace now exposes:

- create / edit / delete
- run now
- pause / resume / disable
- scheduler state, next fire time, and previous fire time

## Roadmap

### Phase 1

- Foundation rewrite
- New schema
- New backend APIs
- New front-end shell

### Phase 2

- Connector SPI
- MySQL / PostgreSQL / Kafka / HTTP / file connectors
- Dataset rule engine
- Preview pipeline
- Real runtime delivery for file, HTTP, MySQL, PostgreSQL, and Kafka connectors

### Phase 3

- Execution engine
- Scheduling
- Retry and stop controls
- Log streaming and monitoring

## Verification

Validated in the current workspace:

- `backend`: `mvn -DskipTests compile`
- `frontend`: `npm run build`

Not validated in the current workspace:

- `docker compose config` could not be executed here because Docker CLI is not installed in this environment
