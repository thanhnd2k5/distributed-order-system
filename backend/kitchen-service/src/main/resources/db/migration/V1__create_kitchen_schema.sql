CREATE TABLE kitchen_tickets (
    ticket_id UUID PRIMARY KEY,
    order_id UUID NOT NULL UNIQUE,
    source_type VARCHAR(10) NOT NULL CHECK (source_type IN ('POS', 'ONLINE')),
    source_site_id VARCHAR(50) NOT NULL,
    request_fingerprint CHAR(64) NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('WAITING', 'DONE')),
    created_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL,
    CHECK (
        (status = 'WAITING' AND completed_at IS NULL)
        OR (status = 'DONE' AND completed_at IS NOT NULL)
    )
);

CREATE INDEX kitchen_tickets_status_created_idx
    ON kitchen_tickets (status, created_at ASC, ticket_id ASC);

CREATE INDEX kitchen_tickets_completed_idx
    ON kitchen_tickets (completed_at DESC, ticket_id DESC);

CREATE TABLE kitchen_ticket_items (
    ticket_id UUID NOT NULL REFERENCES kitchen_tickets (ticket_id),
    line_no INTEGER NOT NULL CHECK (line_no > 0),
    dish_id VARCHAR(50) NOT NULL,
    dish_name_snapshot VARCHAR(200) NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    PRIMARY KEY (ticket_id, line_no),
    UNIQUE (ticket_id, dish_id)
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
    aggregate_id UUID NOT NULL REFERENCES kitchen_tickets (ticket_id),
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
