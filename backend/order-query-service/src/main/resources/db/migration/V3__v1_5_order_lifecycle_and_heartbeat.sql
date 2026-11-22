-- V3: Extend central replica orders tables for v1.5 order lifecycle and add per-branch replication_heartbeat tables & reporting views.

DO $$
DECLARE
    target_schema TEXT;
    con_name TEXT;
BEGIN
    FOREACH target_schema IN ARRAY ARRAY['${pos01_schema}', '${pos02_schema}', '${online_schema}'] LOOP
        FOR con_name IN
            SELECT c.conname
            FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = target_schema
              AND t.relname = 'orders'
              AND c.contype = 'c'
              AND pg_get_constraintdef(c.oid) LIKE '%PENDING_INVENTORY%'
        LOOP
            EXECUTE format('ALTER TABLE %I.orders DROP CONSTRAINT %I', target_schema, con_name);
        END LOOP;
    END LOOP;
END $$;

ALTER TABLE ${pos01_schema}.orders
    ADD CONSTRAINT chk_pos01_orders_status CHECK (status IN ('PENDING_INVENTORY', 'RESERVED', 'FAILED', 'DONE', 'CANCELLED')),
    ADD COLUMN status_rank SMALLINT NOT NULL DEFAULT 0,
    ADD CONSTRAINT chk_pos01_orders_status_rank CHECK (status_rank IN (0, 1, 2)),
    ADD COLUMN close_reason TEXT,
    ADD COLUMN cancel_flag VARCHAR(32) NOT NULL DEFAULT 'NONE',
    ADD CONSTRAINT chk_pos01_orders_cancel_flag CHECK (cancel_flag IN ('NONE', 'REQUESTED', 'KITCHEN_WILL_NOT_COOK')),
    ADD COLUMN cancel_requested_at TIMESTAMPTZ,
    ADD COLUMN cancel_message TEXT;

ALTER TABLE ${pos02_schema}.orders
    ADD CONSTRAINT chk_pos02_orders_status CHECK (status IN ('PENDING_INVENTORY', 'RESERVED', 'FAILED', 'DONE', 'CANCELLED')),
    ADD COLUMN status_rank SMALLINT NOT NULL DEFAULT 0,
    ADD CONSTRAINT chk_pos02_orders_status_rank CHECK (status_rank IN (0, 1, 2)),
    ADD COLUMN close_reason TEXT,
    ADD COLUMN cancel_flag VARCHAR(32) NOT NULL DEFAULT 'NONE',
    ADD CONSTRAINT chk_pos02_orders_cancel_flag CHECK (cancel_flag IN ('NONE', 'REQUESTED', 'KITCHEN_WILL_NOT_COOK')),
    ADD COLUMN cancel_requested_at TIMESTAMPTZ,
    ADD COLUMN cancel_message TEXT;

ALTER TABLE ${online_schema}.orders
    ADD CONSTRAINT chk_online_orders_status CHECK (status IN ('PENDING_INVENTORY', 'RESERVED', 'FAILED', 'DONE', 'CANCELLED')),
    ADD COLUMN status_rank SMALLINT NOT NULL DEFAULT 0,
    ADD CONSTRAINT chk_online_orders_status_rank CHECK (status_rank IN (0, 1, 2)),
    ADD COLUMN close_reason TEXT,
    ADD COLUMN cancel_flag VARCHAR(32) NOT NULL DEFAULT 'NONE',
    ADD CONSTRAINT chk_online_orders_cancel_flag CHECK (cancel_flag IN ('NONE', 'REQUESTED', 'KITCHEN_WILL_NOT_COOK')),
    ADD COLUMN cancel_requested_at TIMESTAMPTZ,
    ADD COLUMN cancel_message TEXT;

UPDATE ${pos01_schema}.orders
SET status_rank = CASE status
        WHEN 'RESERVED' THEN 1
        WHEN 'DONE' THEN 2
        WHEN 'FAILED' THEN 2
        WHEN 'CANCELLED' THEN 2
        ELSE 0
    END,
    close_reason = COALESCE(close_reason, failure_reason);

UPDATE ${pos02_schema}.orders
SET status_rank = CASE status
        WHEN 'RESERVED' THEN 1
        WHEN 'DONE' THEN 2
        WHEN 'FAILED' THEN 2
        WHEN 'CANCELLED' THEN 2
        ELSE 0
    END,
    close_reason = COALESCE(close_reason, failure_reason);

UPDATE ${online_schema}.orders
SET status_rank = CASE status
        WHEN 'RESERVED' THEN 1
        WHEN 'DONE' THEN 2
        WHEN 'FAILED' THEN 2
        WHEN 'CANCELLED' THEN 2
        ELSE 0
    END,
    close_reason = COALESCE(close_reason, failure_reason);

CREATE TABLE ${pos01_schema}.replication_heartbeat (
    branch_id VARCHAR(50) PRIMARY KEY,
    beat_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT chk_pos01_heartbeat_branch CHECK (branch_id = 'POS01')
);

CREATE TABLE ${pos02_schema}.replication_heartbeat (
    branch_id VARCHAR(50) PRIMARY KEY,
    beat_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT chk_pos02_heartbeat_branch CHECK (branch_id = 'POS02')
);

CREATE TABLE ${online_schema}.replication_heartbeat (
    branch_id VARCHAR(50) PRIMARY KEY,
    beat_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT chk_online_heartbeat_branch CHECK (branch_id = 'ONLINE')
);

CREATE OR REPLACE VIEW ${reporting_schema}.all_orders AS
SELECT order_id, client_request_id, site_id, source_type, status, failure_reason, version, created_at, updated_at,
       status_rank, close_reason, cancel_flag, cancel_requested_at, cancel_message
FROM ${pos01_schema}.orders
UNION ALL
SELECT order_id, client_request_id, site_id, source_type, status, failure_reason, version, created_at, updated_at,
       status_rank, close_reason, cancel_flag, cancel_requested_at, cancel_message
FROM ${pos02_schema}.orders
UNION ALL
SELECT order_id, client_request_id, site_id, source_type, status, failure_reason, version, created_at, updated_at,
       status_rank, close_reason, cancel_flag, cancel_requested_at, cancel_message
FROM ${online_schema}.orders;

CREATE OR REPLACE VIEW ${reporting_schema}.all_heartbeats AS
SELECT branch_id, beat_at
FROM ${pos01_schema}.replication_heartbeat
UNION ALL
SELECT branch_id, beat_at
FROM ${pos02_schema}.replication_heartbeat
UNION ALL
SELECT branch_id, beat_at
FROM ${online_schema}.replication_heartbeat;

GRANT SELECT ON ${pos01_schema}.replication_heartbeat,
                ${pos02_schema}.replication_heartbeat,
                ${online_schema}.replication_heartbeat,
                ${reporting_schema}.all_orders,
                ${reporting_schema}.all_heartbeats TO ${reader_user};
