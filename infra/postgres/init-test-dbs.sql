CREATE ROLE order_app WITH LOGIN PASSWORD 'order_demo' SUPERUSER;
CREATE ROLE inventory_app WITH LOGIN PASSWORD 'inventory_demo' SUPERUSER;
CREATE ROLE kitchen_app WITH LOGIN PASSWORD 'kitchen_demo' SUPERUSER;
CREATE ROLE central_app WITH LOGIN PASSWORD 'central_demo' SUPERUSER;
CREATE ROLE query_reader WITH LOGIN PASSWORD 'query_demo' NOSUPERUSER NOCREATEDB NOCREATEROLE;

CREATE DATABASE pos01_test OWNER order_app;
CREATE DATABASE inventory_test OWNER inventory_app;
CREATE DATABASE kitchen_test OWNER kitchen_app;
CREATE DATABASE query_test OWNER central_app;
