# Couchbase Data Sync

Focused Debezium-embedded CDC tool that syncs **Oracle**, **PostgreSQL**, and **Microsoft SQL Server** into **Couchbase**. Additional sources can be added via `SourceConnectorBuilder` adapters.

## Modules

| Module | Purpose |
|--------|---------|
| `core` | Config loader, connector builders, transform engine, Couchbase sink, offsets, metrics |
| `cli` | `syncmgr` CLI (Picocli): validate, dry-run, run, status, stop |

## Quick start

Requires **Java 17+**. Prefer the **JVM** distribution / Gradle run tasks over GraalVM native-image for `run`.

```bash
# Build
./gradlew :cli:installDist

# Validate (Gradle)
./gradlew :cli:validateConfig \
  -Pconnection=examples/postgres-to-couchbase/connection.yaml \
  -Pmapping=examples/postgres-to-couchbase/mapping.yaml \
  -PsyntaxOnly

# Run pipeline (Gradle / JVM)
./gradlew :cli:runPipeline \
  -Pconnection=examples/postgres-to-couchbase/connection.yaml \
  -Pmapping=examples/postgres-to-couchbase/mapping.yaml

# Or installDist binary
./cli/build/install/syncmgr/bin/syncmgr validate \
  -c examples/postgres-to-couchbase/connection.yaml \
  -m examples/postgres-to-couchbase/mapping.yaml \
  --syntax-only
```

Docker Compose (Postgres + Couchbase skeleton):

```bash
docker compose up -d postgres couchbase
```

Helm:

```bash
helm upgrade --install cdc ./deploy/helm/cdc-couchbase-sync \
  --set secrets.existingSecret=cdc-secrets
```

## Configuration

Two YAML files per pipeline instance:

- **`connection.yaml`** — source/target connectivity, offsets, metrics, DLQ
- **`mapping.yaml`** — table → collection mapping, key templates, field renames/coercions

Secrets must use `${env:VAR}` or `${vault:path#field}` — never literal passwords in YAML.

### Offset backends

| Backend | Use when |
|---------|----------|
| `file` | Single replica / local PVC |
| `kafka` | Shared offsets via Kafka topics |
| `couchbase` | Shared offsets in Couchbase (`cdc_state` collection) |

For multi-replica, set `offsets.ha.enabled: true` (active/standby leader election). See `examples/postgres-to-couchbase/connection-ha-couchbase.yaml`.

### Durability

```yaml
target:
  durability: majority   # none | majority | majorityAndPersistActive | persistToMajority
  kvTimeoutSeconds: 10
  autoCreateKeyspaces: true          # create missing scopes/collections from mapping
  keyspaceReadyTimeoutSeconds: 60
```

### Oracle XStream

```yaml
source:
  type: oracle
  connectionAdapter: xstream
  outServerName: dbzxout
```

### Key templates

| Token | Meaning |
|-------|---------|
| `{COLUMN}` | Source column value |
| `{pk}` | Debezium record primary key |
| `{uuid4}` | Random UUID |

### Soft delete

When `softDelete: true`, deletes become upserts with `_deleted: true` and `deletedAt` (ISO-8601).

## Metrics

Prometheus scrape endpoint (default `:9404/metrics`) plus `/health`.

## CLI

```text
syncmgr validate  -c connection.yaml -m mapping.yaml [--syntax-only]
syncmgr dry-run   -c connection.yaml -m mapping.yaml
syncmgr run       -c connection.yaml -m mapping.yaml
syncmgr status    -c connection.yaml
syncmgr stop      -c connection.yaml
```

## Native image (optional)

```bash
./gradlew :cli:nativeCompile
```

Use JVM (`runPipeline` / `installDist`) for production CDC. Native-image is mainly for lightweight `validate` / `status`.

## Source prerequisites

| Source | Requirements |
|--------|----------------|
| Oracle | Supplemental logging + LogMiner **or** XStream Out server |
| Postgres | `wal_level=logical`, replication slot, `pgoutput` publication |
| SQL Server | CDC enabled on database and each captured table |

See `docs/` for more detail.
