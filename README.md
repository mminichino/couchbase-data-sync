# Couchbase Data Sync

Focused Debezium-embedded CDC tool that syncs **Oracle**, **PostgreSQL**, and **Microsoft SQL Server** into **Couchbase**.

## Modules

| Module | Purpose |
|--------|---------|
| `core` | Config loader, connector builders, transform, sink, supervisor |
| `cli` | `syncmgr` CLI |

## Quick start

Requires **Java 17+**. Prefer the **JVM** distribution.

```bash
./gradlew :cli:installDist

# 1) Start idle supervisor
./cli/build/install/syncmgr/bin/syncmgr run --port 9405

# 2) Deploy a pipeline (separate terminal)
./cli/build/install/syncmgr/bin/syncmgr deploy \
  -c examples/postgres-to-couchbase/connection.yaml \
  -m examples/postgres-to-couchbase/mapping.yaml

# Status / stop pipeline (supervisor stays up)
./cli/build/install/syncmgr/bin/syncmgr status
./cli/build/install/syncmgr/bin/syncmgr stop
```

Gradle:

```bash
./gradlew :cli:runSupervisor
./gradlew :cli:deployPipeline -Pconnection=examples/.../connection.yaml -Pmapping=examples/.../mapping.yaml
```

Docker Compose (supervisor only — point configs at existing DBs):

```bash
docker compose up -d --build
docker compose exec syncmgr deploy \
  -c /config/postgres-to-couchbase/connection.yaml \
  -m /config/postgres-to-couchbase/mapping.yaml
```

## Supervisor API

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/health` | Liveness |
| GET | `/status` | Supervisor + pipeline state |
| POST | `/deploy` | Body: `{ "connectionYaml", "mappingYaml" }` — validate, start or replace |
| POST | `/pipeline/stop` | Stop active pipeline |
| GET | `/metrics` | Prometheus (pipeline metrics when deployed) |

## Configuration

Two YAML files per pipeline (submitted via `deploy`):

- **`connection.yaml`** — source/target, offsets, metrics, DLQ
- **`mapping.yaml`** — table → collection mapping, keys, field rules

Secrets: `${env:VAR}` / `${vault:path#field}`.

### Offset backends

| Backend | Use when |
|---------|----------|
| `file` | Single replica |
| `kafka` | Shared Kafka topics |
| `couchbase` | Shared docs + optional HA lease |

### Durability & auto keyspaces

```yaml
target:
  durability: majority
  autoCreateKeyspaces: true
  keyspaceReadyTimeoutSeconds: 60
```

### Oracle XStream

```yaml
source:
  connectionAdapter: xstream
  outServerName: dbzxout
```

## CLI

```text
syncmgr run [--bind 0.0.0.0] [--port 9405] [--config-dir ./data/config]
syncmgr deploy -c connection.yaml -m mapping.yaml [--url http://127.0.0.1:9405]
syncmgr status [--url ...]
syncmgr stop   [--url ...]
syncmgr validate -c ... -m ... [--syntax-only]
syncmgr dry-run  -c ... -m ...
```

See `docs/` for architecture and operations.
