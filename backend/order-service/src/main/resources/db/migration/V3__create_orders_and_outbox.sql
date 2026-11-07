CREATE TABLE orders (
    order_id UUID PRIMARY KEY,
    client_request_id UUID NOT NULL UNIQUE,
    site_id VARCHAR(50) NOT NULL,
    source_type VARCHAR(10) NOT NULL CHECK (source_type IN ('POS', 'ONLINE')),
    status VARCHAR(32) NOT NULL CHECK (status IN ('PENDING_INVENTORY', 'RESERVED', 'FAILED', 'DONE')),
    failure_reason TEXT,
    version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX orders_recent_idx ON orders (created_at DESC, order_id DESC);

CREATE TABLE order_items (
    order_id UUID NOT NULL REFERENCES orders (order_id),
    line_no INTEGER NOT NULL CHECK (line_no > 0),
    dish_id VARCHAR(50) NOT NULL,
    dish_name_snapshot VARCHAR(200) NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    PRIMARY KEY (order_id, line_no),
    UNIQUE (order_id, dish_id)
);

CREATE TABLE outbox_events (
    event_id UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL REFERENCES orders (order_id),
    event_type VARCHAR(50) NOT NULL,
    topic VARCHAR(100) NOT NULL,
    message_key VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    sent_at TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX outbox_pending_idx ON outbox_events (next_attempt_at, created_at) WHERE sent_at IS NULL;
