CREATE SCHEMA IF NOT EXISTS ${pos01_schema};
CREATE SCHEMA IF NOT EXISTS ${pos02_schema};
CREATE SCHEMA IF NOT EXISTS ${online_schema};
CREATE SCHEMA IF NOT EXISTS ${reporting_schema};

-- POS01 fragment replica tables (no FK on order_items to avoid parallel initial-sync race)
CREATE TABLE ${pos01_schema}.orders (
    order_id UUID PRIMARY KEY,
    client_request_id UUID NOT NULL UNIQUE,
    site_id VARCHAR(50) NOT NULL,
    source_type VARCHAR(10) NOT NULL CHECK (source_type IN ('POS', 'ONLINE')),
    status VARCHAR(32) NOT NULL CHECK (status IN ('PENDING_INVENTORY', 'RESERVED', 'FAILED', 'DONE')),
    failure_reason TEXT,
    version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT chk_pos01_fragment_site CHECK (site_id = 'POS01' AND source_type = 'POS')
);
CREATE INDEX pos01_orders_recent_idx ON ${pos01_schema}.orders (created_at DESC, order_id DESC);

CREATE TABLE ${pos01_schema}.order_items (
    order_id UUID NOT NULL,
    line_no INTEGER NOT NULL CHECK (line_no > 0),
    dish_id VARCHAR(50) NOT NULL,
    dish_name_snapshot VARCHAR(200) NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    PRIMARY KEY (order_id, line_no),
    UNIQUE (order_id, dish_id)
);

-- POS02 fragment replica tables
CREATE TABLE ${pos02_schema}.orders (
    order_id UUID PRIMARY KEY,
    client_request_id UUID NOT NULL UNIQUE,
    site_id VARCHAR(50) NOT NULL,
    source_type VARCHAR(10) NOT NULL CHECK (source_type IN ('POS', 'ONLINE')),
    status VARCHAR(32) NOT NULL CHECK (status IN ('PENDING_INVENTORY', 'RESERVED', 'FAILED', 'DONE')),
    failure_reason TEXT,
    version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT chk_pos02_fragment_site CHECK (site_id = 'POS02' AND source_type = 'POS')
);
CREATE INDEX pos02_orders_recent_idx ON ${pos02_schema}.orders (created_at DESC, order_id DESC);

CREATE TABLE ${pos02_schema}.order_items (
    order_id UUID NOT NULL,
    line_no INTEGER NOT NULL CHECK (line_no > 0),
    dish_id VARCHAR(50) NOT NULL,
    dish_name_snapshot VARCHAR(200) NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    PRIMARY KEY (order_id, line_no),
    UNIQUE (order_id, dish_id)
);

-- ONLINE fragment replica tables
CREATE TABLE ${online_schema}.orders (
    order_id UUID PRIMARY KEY,
    client_request_id UUID NOT NULL UNIQUE,
    site_id VARCHAR(50) NOT NULL,
    source_type VARCHAR(10) NOT NULL CHECK (source_type IN ('POS', 'ONLINE')),
    status VARCHAR(32) NOT NULL CHECK (status IN ('PENDING_INVENTORY', 'RESERVED', 'FAILED', 'DONE')),
    failure_reason TEXT,
    version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT chk_online_fragment_site CHECK (site_id = 'ONLINE' AND source_type = 'ONLINE')
);
CREATE INDEX online_orders_recent_idx ON ${online_schema}.orders (created_at DESC, order_id DESC);

CREATE TABLE ${online_schema}.order_items (
    order_id UUID NOT NULL,
    line_no INTEGER NOT NULL CHECK (line_no > 0),
    dish_id VARCHAR(50) NOT NULL,
    dish_name_snapshot VARCHAR(200) NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    PRIMARY KEY (order_id, line_no),
    UNIQUE (order_id, dish_id)
);

-- Explicit-column UNION ALL views reconstructing the logical global order relation
CREATE OR REPLACE VIEW ${reporting_schema}.all_orders AS
SELECT order_id, client_request_id, site_id, source_type, status, failure_reason, version, created_at, updated_at
FROM ${pos01_schema}.orders
UNION ALL
SELECT order_id, client_request_id, site_id, source_type, status, failure_reason, version, created_at, updated_at
FROM ${pos02_schema}.orders
UNION ALL
SELECT order_id, client_request_id, site_id, source_type, status, failure_reason, version, created_at, updated_at
FROM ${online_schema}.orders;

CREATE OR REPLACE VIEW ${reporting_schema}.all_order_items AS
SELECT order_id, line_no, dish_id, dish_name_snapshot, quantity
FROM ${pos01_schema}.order_items
UNION ALL
SELECT order_id, line_no, dish_id, dish_name_snapshot, quantity
FROM ${pos02_schema}.order_items
UNION ALL
SELECT order_id, line_no, dish_id, dish_name_snapshot, quantity
FROM ${online_schema}.order_items;

-- Provision dedicated read-only role for Order Query Service runtime
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${reader_user}') THEN
        EXECUTE format('CREATE ROLE %I WITH LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE', '${reader_user}', '${reader_password}');
    END IF;
END $$;

GRANT USAGE ON SCHEMA ${pos01_schema}, ${pos02_schema}, ${online_schema}, ${reporting_schema} TO ${reader_user};
GRANT SELECT ON ALL TABLES IN SCHEMA ${pos01_schema}, ${pos02_schema}, ${online_schema}, ${reporting_schema} TO ${reader_user};
GRANT pg_read_all_stats TO ${reader_user};
