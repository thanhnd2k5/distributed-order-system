DO $$
BEGIN
    IF current_schema() = 'pos01' THEN
        ALTER TABLE orders
            ADD CONSTRAINT chk_orders_fragment_site
            CHECK (site_id = 'POS01' AND source_type = 'POS');
    ELSIF current_schema() = 'pos02' THEN
        ALTER TABLE orders
            ADD CONSTRAINT chk_orders_fragment_site
            CHECK (site_id = 'POS02' AND source_type = 'POS');
    ELSIF current_schema() = 'online' THEN
        ALTER TABLE orders
            ADD CONSTRAINT chk_orders_fragment_site
            CHECK (site_id = 'ONLINE' AND source_type = 'ONLINE');
    END IF;
END $$;
