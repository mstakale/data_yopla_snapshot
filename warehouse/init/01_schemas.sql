-- Runs once, automatically, the first time the warehouse container starts
-- (official postgres image behavior: any *.sql file in /docker-entrypoint-initdb.d
-- is executed against the database named by POSTGRES_DB).
--
-- The `warehouse` database itself is already created by the POSTGRES_DB
-- environment variable in docker-compose.override.yml, so this script only
-- adds the schemas for the medallion layers.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;
