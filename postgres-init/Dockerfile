# PostgreSQL 15 with the schema, seed data and roles baked in. The scripts in
# /docker-entrypoint-initdb.d run once, the first time the data volume is empty.
FROM postgres:15
COPY 01_schema.sql 02_seed.sql 03_roles.sql /docker-entrypoint-initdb.d/
