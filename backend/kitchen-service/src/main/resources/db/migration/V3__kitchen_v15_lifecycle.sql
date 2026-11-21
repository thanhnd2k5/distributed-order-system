CREATE TABLE kitchen_orders (
    order_id         UUID PRIMARY KEY,
    status           TEXT NOT NULL CHECK (status IN (
        'NOT_EXISTS', 'CANCEL_PENDING', 'QUEUED', 'COOKING', 'DONE', 'CANCELLED', 'FAILED'
    )),
    origin_branch    TEXT,
    cancel_branches  TEXT[] NOT NULL DEFAULT '{}',
    cancel_reasons   JSONB NOT NULL DEFAULT '{}'::jsonb,
    cancelled_by     TEXT CHECK (cancelled_by IN ('CUSTOMER', 'KITCHEN')),
    close_reason     TEXT,
    items            JSONB,
    reserved         JSONB,
    spoiled          JSONB,
    order_created_at TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX kitchen_orders_status_created_idx
    ON kitchen_orders (status, order_created_at ASC NULLS LAST, order_id ASC);

CREATE TABLE flagged_ingredients (
    ingredient_id TEXT PRIMARY KEY,
    needs_check   BOOLEAN NOT NULL,
    epoch         INT NOT NULL,
    seq           BIGINT NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE alerts (
    id               BIGSERIAL PRIMARY KEY,
    kind             TEXT NOT NULL CHECK (kind IN ('NEEDS_REVIEW', 'WRONG_BRANCH_CANCEL')),
    order_id         UUID,
    event_id         UUID,
    source_topic     TEXT NOT NULL,
    source_partition INT NOT NULL,
    source_offset    BIGINT NOT NULL,
    detail           JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (kind, source_topic, source_partition, source_offset)
);

CREATE INDEX alerts_kind_created_idx
    ON alerts (kind, created_at DESC, id DESC);

ALTER TABLE processed_events
    ALTER COLUMN order_id DROP NOT NULL,
    ALTER COLUMN request_fingerprint DROP NOT NULL,
    ALTER COLUMN outcome SET DEFAULT 'APPLIED',
    ALTER COLUMN processed_at SET DEFAULT now();

DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = (quote_ident(current_schema()) || '.processed_events')::regclass
          AND contype = 'c'
    LOOP
        EXECUTE format('ALTER TABLE processed_events DROP CONSTRAINT %I', r.conname);
    END LOOP;
END $$;

DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = (quote_ident(current_schema()) || '.outbox_events')::regclass
          AND contype = 'f'
    LOOP
        EXECUTE format('ALTER TABLE outbox_events DROP CONSTRAINT %I', r.conname);
    END LOOP;
END $$;

ALTER TABLE outbox_events
    ADD COLUMN IF NOT EXISTS seq_id BIGSERIAL;

CREATE INDEX IF NOT EXISTS outbox_pending_seq_idx
    ON outbox_events (next_attempt_at, seq_id)
    WHERE sent_at IS NULL;
