# Operations

## Local / Docker

```bash
docker compose up -d --build
# Deploy against existing Oracle/Postgres/MSSQL + Couchbase:
docker compose exec syncmgr deploy -c /config/postgres-to-couchbase/connection.yaml \
  -m /config/postgres-to-couchbase/mapping.yaml
```

Ensure secret env vars (`PG_CDC_PASSWORD`, `CB_PASSWORD`, …) are set on the supervisor container before deploy.

## Kubernetes

The chart starts the **idle supervisor**. Sample configs are mounted at `/config` for convenience.

```bash
kubectl exec -it <pod> -- syncmgr deploy -c /config/connection.yaml -m /config/mapping.yaml
kubectl exec -it <pod> -- syncmgr status
kubectl exec -it <pod> -- syncmgr stop
```

## Stopping

- `syncmgr stop` — stops the CDC pipeline; supervisor remains.
- SIGTERM / `docker compose stop` — shuts down supervisor and any active pipeline.

## Auto-create scopes / collections

When `target.autoCreateKeyspaces` is true (default), deploy creates missing scopes/collections from `mapping.yaml` and waits for KV readiness. The **bucket** must already exist.
