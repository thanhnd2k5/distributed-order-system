-- v1.5 Order lifecycle, outbox attempt tracking, Flow C menu cache, replication heartbeat, and display alerts.

DO $$
DECLARE
    constraint_name text;
BEGIN
    SELECT c.conname
    INTO constraint_name
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname = current_schema()
      AND t.relname = 'orders'
      AND c.contype = 'c'
      AND pg_get_constraintdef(c.oid) LIKE '%PENDING_INVENTORY%';

    IF constraint_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE orders DROP CONSTRAINT %I', constraint_name);
    END IF;
END $$;

ALTER TABLE orders
    ADD CONSTRAINT chk_orders_status
        CHECK (status IN ('PENDING_INVENTORY', 'RESERVED', 'FAILED', 'DONE', 'CANCELLED')),
    ADD COLUMN status_rank SMALLINT NOT NULL DEFAULT 0,
    ADD COLUMN close_reason TEXT,
    ADD COLUMN cancel_flag VARCHAR(32) NOT NULL DEFAULT 'NONE',
    ADD COLUMN cancel_requested_at TIMESTAMPTZ,
    ADD COLUMN cancel_message TEXT;

UPDATE orders
SET status_rank = CASE
    WHEN status = 'PENDING_INVENTORY' THEN 0
    WHEN status = 'RESERVED' THEN 1
    ELSE 2
END;

ALTER TABLE orders
    ADD CONSTRAINT chk_orders_status_rank CHECK (status_rank IN (0, 1, 2)),
    ADD CONSTRAINT chk_orders_cancel_flag CHECK (cancel_flag IN ('NONE', 'REQUESTED', 'KITCHEN_WILL_NOT_COOK'));

ALTER TABLE outbox_events
    ADD COLUMN id BIGSERIAL NOT NULL,
    ADD COLUMN order_id UUID REFERENCES orders (order_id),
    ADD COLUMN kind VARCHAR(32) NOT NULL DEFAULT 'CREATE_ORDER',
    ADD COLUMN attempted BOOLEAN NOT NULL DEFAULT FALSE;

UPDATE outbox_events
SET order_id = aggregate_id
WHERE order_id IS NULL;

ALTER TABLE outbox_events
    ALTER COLUMN order_id SET NOT NULL,
    ADD CONSTRAINT uq_outbox_events_id UNIQUE (id),
    ADD CONSTRAINT chk_outbox_events_kind CHECK (kind IN ('CREATE_ORDER', 'CANCEL_REQUEST'));

CREATE INDEX outbox_order_pending_id_idx
    ON outbox_events (order_id, id)
    WHERE sent_at IS NULL;

ALTER TABLE processed_events DROP CONSTRAINT IF EXISTS chk_processed_outcome;

ALTER TABLE processed_events
    ADD CONSTRAINT chk_processed_outcome CHECK (outcome IN (
        'SUCCESS',
        'ALREADY_APPLIED',
        'SKIPPED_DUPLICATE',
        'SKIPPED_IGNORED_EVENT_TYPE',
        'ORDER_NOT_FOUND',
        'TERMINAL_STATE_IGNORED',
        'ILLEGAL_TRANSITION_IGNORED',
        'STALE_VERSION_IGNORED',
        'CONDITION_NOT_MET'
    ));

INSERT INTO menu_items (dish_id, name, active)
VALUES ('PHO_BO', 'Phở bò', TRUE)
ON CONFLICT (dish_id) DO NOTHING;

CREATE TABLE menu_cache (
    dish_id VARCHAR(50) PRIMARY KEY,
    dish_name VARCHAR(200),
    status VARCHAR(32) NOT NULL CHECK (status IN ('AVAILABLE', 'SOLD_OUT', 'HIDDEN')),
    epoch INTEGER NOT NULL,
    seq BIGINT NOT NULL
);

CREATE TABLE menu_cache_meta (
    id BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
    epoch INTEGER NOT NULL,
    last_heard_at TIMESTAMPTZ
);

INSERT INTO menu_cache_meta (id, epoch, last_heard_at)
VALUES (TRUE, 1, NULL)
ON CONFLICT (id) DO NOTHING;

INSERT INTO menu_cache (dish_id, dish_name, status, epoch, seq)
SELECT dish_id, name, CASE WHEN active THEN 'AVAILABLE' ELSE 'HIDDEN' END, 1, 0
FROM menu_items
ON CONFLICT (dish_id) DO NOTHING;

CREATE TABLE replication_heartbeat (
    branch_id VARCHAR(50) PRIMARY KEY,
    beat_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE alerts (
    id BIGSERIAL PRIMARY KEY,
    kind VARCHAR(64) NOT NULL,
    order_id UUID,
    event_id UUID,
    source_topic VARCHAR(150) NOT NULL,
    source_partition INTEGER NOT NULL,
    source_offset BIGINT NOT NULL,
    detail JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (kind, source_topic, source_partition, source_offset)
);
