-- ============================================================================
--  Least-privilege database roles (principle of least privilege)
--
--    cdcuser     owner of the schema; used only for setup / administration
--    app_writer  the Write API: DML on the tables, no DDL, cannot drop or alter
--    debezium    the CDC connector: read-only + REPLICATION to stream the WAL
-- ============================================================================

CREATE ROLE app_writer LOGIN PASSWORD 'writerpass';
GRANT CONNECT ON DATABASE shopdb TO app_writer;
GRANT USAGE ON SCHEMA public TO app_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app_writer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO app_writer;

CREATE ROLE debezium LOGIN REPLICATION PASSWORD 'dbzpass';
GRANT CONNECT ON DATABASE shopdb TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
