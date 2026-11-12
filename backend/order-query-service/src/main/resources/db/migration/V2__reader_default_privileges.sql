-- Future tables/views created by the migration owner stay readable by query_reader.
ALTER DEFAULT PRIVILEGES IN SCHEMA ${pos01_schema} GRANT SELECT ON TABLES TO ${reader_user};
ALTER DEFAULT PRIVILEGES IN SCHEMA ${pos02_schema} GRANT SELECT ON TABLES TO ${reader_user};
ALTER DEFAULT PRIVILEGES IN SCHEMA ${online_schema} GRANT SELECT ON TABLES TO ${reader_user};
ALTER DEFAULT PRIVILEGES IN SCHEMA ${reporting_schema} GRANT SELECT ON TABLES TO ${reader_user};
