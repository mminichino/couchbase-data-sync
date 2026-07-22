-- Demo schema for postgres-to-couchbase quick start
CREATE TABLE IF NOT EXISTS orders (
    id          BIGSERIAL PRIMARY KEY,
    status      TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    internal_notes TEXT
);

CREATE TABLE IF NOT EXISTS order_items (
    id          BIGSERIAL PRIMARY KEY,
    order_id    BIGINT NOT NULL REFERENCES orders(id),
    sku         TEXT NOT NULL,
    qty         INT NOT NULL
);

ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;

-- Publication is also auto-created by Debezium (publication.autocreate.mode=filtered),
-- but creating it here makes dry-run / first-start smoother.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'syncmgr_orders_pub') THEN
    CREATE PUBLICATION syncmgr_orders_pub FOR TABLE orders, order_items;
  END IF;
END $$;

INSERT INTO orders (status) VALUES ('NEW') ON CONFLICT DO NOTHING;
