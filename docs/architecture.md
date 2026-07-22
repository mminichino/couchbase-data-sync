# Architecture

```text
syncmgr run  ──► PipelineSupervisor (HTTP :9405)
                      │
                      │  POST /deploy  (connection + mapping YAML)
                      ▼
                 SyncPipeline
                      │
 source DB ──► Debezium Engine ──► Transform ──► CouchbaseSink
                      │
              file | kafka | couchbase offsets
```

## Supervisor lifecycle

1. `syncmgr run` starts idle (health/status/metrics only).
2. `syncmgr deploy` validates YAML, POSTs to `/deploy`, which stops any active pipeline and starts a new `SyncPipeline`.
3. `syncmgr stop` stops the pipeline; the supervisor keeps running.
4. Changing source connectivity requires another `deploy` (Debezium engine is rebuilt).

## Running (JVM)

```bash
./gradlew :cli:runSupervisor
./gradlew :cli:deployPipeline -Pconnection=... -Pmapping=...
```

## Offset backends

| Backend | Multi-replica | Notes |
|---------|---------------|-------|
| `file` | No | Local/PVC |
| `kafka` | Yes (with HA) | Shared topics |
| `couchbase` | Yes (with HA) | Shared KV + lease |

## Soft-delete / durability / XStream

See README. Soft-delete uses `_deleted` + `deletedAt`. Durability via `target.durability`. Oracle `connectionAdapter: logminer|xstream`.
