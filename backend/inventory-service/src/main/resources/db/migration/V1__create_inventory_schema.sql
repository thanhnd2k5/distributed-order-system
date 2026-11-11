CREATE TABLE ingredients (
    ingredient_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    unit VARCHAR(20) NOT NULL,
    available_quantity BIGINT NOT NULL CHECK (available_quantity >= 0),
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE recipe_items (
    dish_id VARCHAR(50) NOT NULL,
    ingredient_id VARCHAR(50) NOT NULL REFERENCES ingredients (ingredient_id),
    quantity_per_dish BIGINT NOT NULL CHECK (quantity_per_dish > 0),
    PRIMARY KEY (dish_id, ingredient_id)
);

CREATE TABLE inventory_order_decisions (
    order_id UUID PRIMARY KEY,
    source_type VARCHAR(10) NOT NULL CHECK (source_type IN ('POS', 'ONLINE')),
    source_site_id VARCHAR(50) NOT NULL,
    request_fingerprint CHAR(64) NOT NULL,
    decision VARCHAR(20) NOT NULL CHECK (decision IN ('PROCESSING', 'RESERVED', 'FAILED')),
    reason_code VARCHAR(50),
    reason_message TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CHECK (
        (decision = 'FAILED' AND reason_code IS NOT NULL AND reason_message IS NOT NULL)
        OR (decision <> 'FAILED' AND reason_code IS NULL AND reason_message IS NULL)
    )
);

CREATE INDEX inventory_decisions_created_idx
    ON inventory_order_decisions (created_at DESC, order_id DESC);

CREATE TABLE reservation_items (
    order_id UUID NOT NULL REFERENCES inventory_order_decisions (order_id),
    ingredient_id VARCHAR(50) NOT NULL REFERENCES ingredients (ingredient_id),
    reserved_quantity BIGINT NOT NULL CHECK (reserved_quantity > 0),
    PRIMARY KEY (order_id, ingredient_id)
);

CREATE TABLE processed_events (
    event_id UUID PRIMARY KEY,
    order_id UUID NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    request_fingerprint CHAR(64) NOT NULL,
    outcome VARCHAR(30) NOT NULL CHECK (outcome IN ('APPLIED', 'DUPLICATE_ORDER', 'PAYLOAD_CONFLICT')),
    processed_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX processed_events_order_idx
    ON processed_events (order_id, processed_at);

CREATE TABLE outbox_events (
    event_id UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL REFERENCES inventory_order_decisions (order_id),
    event_type VARCHAR(50) NOT NULL,
    topic VARCHAR(100) NOT NULL,
    message_key VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    sent_at TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX outbox_pending_idx
    ON outbox_events (next_attempt_at, created_at)
    WHERE sent_at IS NULL;
