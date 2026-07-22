# Operations

## Deploy (Kubernetes)

1. Create a Secret with source/target passwords (`PG_CDC_PASSWORD`, `CB_PASSWORD`, etc.).
2. Set `connection` / `mapping` in Helm values (or replace ConfigMap).
3. **Single replica + file offsets:** enable a PVC for `/data` (`persistence.enabled=true`).
4. **Multi-replica HA:** set `offsets.backend: couchbase` (or `kafka`), `offsets.ha.enabled: true`, create collection `cdc_state`, and scale replicas. Only the lease holder runs Debezium.
5. Optionally enable `serviceMonitor.enabled` for Prometheus Operator.

## Stopping

```bash
syncmgr stop -c connection.yaml   # local STOP file + shared Couchbase stop when HA/couchbase
# or SIGTERM / kubectl delete pod (shutdown hook calls pipeline.stop())
```

## Lag / status

`syncmgr status` reports pipeline state and offset backend metadata.

## DLQ

Failed Couchbase writes append JSON lines under `deadLetter.path`. Replay is manual (re-upsert from the file).

## Durability

Raise `target.durability` for stronger persistence guarantees at the cost of write latency. Requires Couchbase bucket replicas for `majority` / `persistToMajority`.

## Auto-create scopes / collections

When `target.autoCreateKeyspaces` is true (default), `syncmgr run` creates any missing scopes/collections referenced by `mapping.yaml` (and the Couchbase offset/HA collection when used), then waits until each keyspace accepts KV ops. The bucket itself must already exist (Compose bootstrap / ops creates it).
