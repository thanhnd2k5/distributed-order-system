-- v1.5 Inventory schema migration (preserves V1/V2 untouched)

CREATE TABLE stock (
    ingredient_id VARCHAR(50) PRIMARY KEY,
    name          VARCHAR(200) NOT NULL,
    unit          VARCHAR(20) NOT NULL,
    available     NUMERIC(12,3) NOT NULL CHECK (available >= 0),
    reserved      NUMERIC(12,3) NOT NULL DEFAULT 0 CHECK (reserved >= 0),
    consumed      NUMERIC(12,3) NOT NULL DEFAULT 0 CHECK (consumed >= 0),
    wasted        NUMERIC(12,3) NOT NULL DEFAULT 0 CHECK (wasted >= 0),
    needs_check   BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE recipes (
    dish_id       VARCHAR(50) NOT NULL,
    ingredient_id VARCHAR(50) NOT NULL REFERENCES stock (ingredient_id),
    quantity      NUMERIC(12,3) NOT NULL CHECK (quantity > 0),
    PRIMARY KEY (dish_id, ingredient_id)
);

CREATE TABLE inventory_orders (
    order_id         UUID PRIMARY KEY,
    origin_branch    VARCHAR(50) NOT NULL,
    status           VARCHAR(20) NOT NULL
        CHECK (status IN ('HELD', 'FAILED', 'RELEASED', 'CONSUMED')),
    reason           TEXT,
    order_created_at TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX inventory_orders_status_updated_idx
    ON inventory_orders (status, updated_at DESC);

CREATE TABLE inventory_reservations (
    order_id      UUID NOT NULL REFERENCES inventory_orders (order_id),
    ingredient_id VARCHAR(50) NOT NULL REFERENCES stock (ingredient_id),
    quantity      NUMERIC(12,3) NOT NULL CHECK (quantity > 0),
    outcome       VARCHAR(20)
        CHECK (outcome IN ('RELEASED', 'CONSUMED', 'WASTED')),
    PRIMARY KEY (order_id, ingredient_id)
);

CREATE INDEX inventory_reservations_held_idx
    ON inventory_reservations (ingredient_id)
    WHERE outcome IS NULL;

CREATE TABLE dish_status (
    dish_id   VARCHAR(50) PRIMARY KEY,
    dish_name VARCHAR(200) NOT NULL DEFAULT '',
    status    VARCHAR(20) NOT NULL
        CHECK (status IN ('AVAILABLE', 'SOLD_OUT', 'HIDDEN')),
    epoch     INTEGER NOT NULL,
    seq       BIGINT NOT NULL
);

CREATE TABLE menu_version (
    id    BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
    epoch INTEGER NOT NULL,
    seq   BIGINT NOT NULL
);

CREATE TABLE alerts (
    id               BIGSERIAL PRIMARY KEY,
    kind             VARCHAR(40) NOT NULL
        CHECK (kind IN ('NEEDS_REVIEW', 'UNMATCHED_COMMAND')),
    order_id         UUID,
    event_id         UUID,
    source_topic     TEXT NOT NULL,
    source_partition INTEGER NOT NULL,
    source_offset    BIGINT NOT NULL,
    detail           JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (kind, source_topic, source_partition, source_offset)
);

CREATE INDEX alerts_kind_created_idx
    ON alerts (kind, created_at DESC, id DESC);

-- Adapt V1 processed_events for generic event_id deduplication across all v1.5 event types.
DO $$
DECLARE
    chk_name TEXT;
BEGIN
    FOR chk_name IN
        SELECT c.conname
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = current_schema()
          AND t.relname = 'processed_events'
          AND c.contype = 'c'
    LOOP
        EXECUTE format('ALTER TABLE %I.processed_events DROP CONSTRAINT %I', current_schema(), chk_name);
    END LOOP;
END $$;

ALTER TABLE processed_events
    ALTER COLUMN order_id DROP NOT NULL,
    ALTER COLUMN event_type DROP NOT NULL,
    ALTER COLUMN request_fingerprint DROP NOT NULL,
    ALTER COLUMN outcome DROP NOT NULL,
    ALTER COLUMN processed_at SET DEFAULT now();

-- Adapt V1 outbox_events: drop FK to V1 inventory_order_decisions, allow NULL aggregate_id (menu-events),
-- and promote id BIGSERIAL as primary key for ordered FOR UPDATE SKIP LOCKED relay.
DO $$
DECLARE
    fk_name TEXT;
BEGIN
    FOR fk_name IN
        SELECT c.conname
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = current_schema()
          AND t.relname = 'outbox_events'
          AND c.contype = 'f'
    LOOP
        EXECUTE format('ALTER TABLE %I.outbox_events DROP CONSTRAINT %I', current_schema(), fk_name);
    END LOOP;
END $$;

ALTER TABLE outbox_events
    ALTER COLUMN aggregate_id DROP NOT NULL,
    ALTER COLUMN event_type DROP NOT NULL,
    ALTER COLUMN created_at SET DEFAULT now(),
    ALTER COLUMN next_attempt_at SET DEFAULT now();

ALTER TABLE outbox_events
    ADD COLUMN IF NOT EXISTS id BIGSERIAL;

ALTER TABLE outbox_events
    DROP CONSTRAINT IF EXISTS outbox_events_pkey;

ALTER TABLE outbox_events
    ADD CONSTRAINT outbox_events_pkey PRIMARY KEY (id);

ALTER TABLE outbox_events
    ADD CONSTRAINT outbox_events_event_id_unique UNIQUE (event_id);

DROP INDEX IF EXISTS outbox_pending_idx;
CREATE INDEX outbox_pending_id_idx
    ON outbox_events (next_attempt_at, id)
    WHERE sent_at IS NULL;

-- Seed initial stock from V2 plus multi-ingredient dish PHO_BO for Flow A/C/scenario testing.
INSERT INTO stock (ingredient_id, name, unit, available, reserved, consumed, wasted, needs_check, updated_at)
SELECT ingredient_id, name, unit, available_quantity, 0, 0, 0, FALSE, now()
FROM ingredients
ON CONFLICT (ingredient_id) DO NOTHING;

INSERT INTO stock (ingredient_id, name, unit, available, reserved, consumed, wasted, needs_check, updated_at) VALUES
('BANH_PHO', 'Bánh phở', 'gram', 1000.000, 0, 0, 0, FALSE, now()),
('THIT_BO', 'Thịt bò', 'gram', 1000.000, 0, 0, 0, FALSE, now()),
('HANH_LA', 'Hành lá', 'gram', 500.000, 0, 0, 0, FALSE, now())
ON CONFLICT (ingredient_id) DO NOTHING;

INSERT INTO recipes (dish_id, ingredient_id, quantity)
SELECT dish_id, ingredient_id, quantity_per_dish
FROM recipe_items
ON CONFLICT (dish_id, ingredient_id) DO NOTHING;

INSERT INTO recipes (dish_id, ingredient_id, quantity) VALUES
('PHO_BO', 'BANH_PHO', 150.000),
('PHO_BO', 'THIT_BO', 150.000),
('PHO_BO', 'HANH_LA', 20.000)
ON CONFLICT (dish_id, ingredient_id) DO NOTHING;

INSERT INTO menu_version (id, epoch, seq)
VALUES (TRUE, 1, 1)
ON CONFLICT (id) DO NOTHING;

INSERT INTO dish_status (dish_id, dish_name, status, epoch, seq) VALUES
('CHAO_GA', 'Cháo gà', 'AVAILABLE', 1, 1),
('COM_GA', 'Cơm gà', 'AVAILABLE', 1, 1),
('PHO_BO', 'Phở bò', 'AVAILABLE', 1, 1)
ON CONFLICT (dish_id) DO NOTHING;
