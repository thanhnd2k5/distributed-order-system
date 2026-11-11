CREATE TABLE IF NOT EXISTS processed_events (
    event_id UUID PRIMARY KEY,
    order_id UUID,
    event_type VARCHAR(64) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    outcome VARCHAR(32) NOT NULL DEFAULT 'SUCCESS',
    CONSTRAINT chk_processed_outcome CHECK (outcome IN ('SUCCESS', 'SKIPPED_DUPLICATE', 'SKIPPED_IGNORED_EVENT_TYPE', 'ORDER_NOT_FOUND', 'TERMINAL_STATE_IGNORED'))
);

CREATE INDEX IF NOT EXISTS idx_processed_events_order_id ON processed_events(order_id);
