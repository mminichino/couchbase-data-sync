# Architecture

```text
source DB ──► Debezium Embedded Engine ──► TransformEngine ──► CouchbaseSink
                      │                         │                    │
              OffsetBackingStore          mapping.yaml          durability + DLQ
              SchemaHistory                                     Prometheus /health
                      ▲
         file | kafka | couchbase
         (+ optional HA lease)
```

## Running (JVM preferred)

```bash
./gradlew :cli:runPipeline -Pconnection=... -Pmapping=...
# or
./gradlew :cli:run --args='run -c ... -m ...'
```

Native-image (`:cli:nativeCompile`) is optional and best suited to `validate` / `status`.

## Offset backends

| Backend | Multi-replica | Notes |
|---------|---------------|-------|
| `file` | No | PVC per pod; simplest for single replica |
| `kafka` | Yes (with HA) | `KafkaOffsetBackingStore` + `KafkaSchemaHistory` |
| `couchbase` | Yes (with HA) | Custom stores in target (or dedicated) bucket |

### HA leader election

Set `offsets.ha.enabled: true` with `backend: kafka|couchbase`. Standbys wait on a Couchbase lease document (`cdc::lease::<pipeline>`); only the leader runs Debezium. Losing the lease stops the engine so a standby can take over from shared offsets.

**Important:** Even with shared offsets, only one active engine may consume a given Postgres slot / Oracle LogMiner session / SQL Server CDC capture — HA is active/standby, not sharded parallelism.

## Soft-delete semantics

| `softDelete` | Debezium `op=d` |
|--------------|-----------------|
| `true` | Upsert with `_deleted=true`, `deletedAt=<iso8601>` |
| `false` | Couchbase KV `remove` |

## Couchbase durability

`target.durability`: `none` | `majority` | `majorityAndPersistActive` | `persistToMajority`

## Oracle adapters

`source.connectionAdapter`: `logminer` (default) or `xstream` (requires `source.outServerName`).

## Extending sources

1. Implement `SourceConnectorBuilder`
2. Register in `SourceConnectorFactory.findBuilder`
3. Add `source.type` to `connection.schema.json`
