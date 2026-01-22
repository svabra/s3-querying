# AI Coding Agent Instructions

## Project Overview
This is a **data querying platform** that combines DuckDB, PostgreSQL, and MinIO (S3-compatible storage) in a Docker-based development environment. The goal is enabling DuckDB to query both S3 data lakes and PostgreSQL databases seamlessly.

## Architecture

### Core Services (docker-compose.yaml)
- **DuckDB**: Analytics query engine with S3 and PostgreSQL connectivity
- **PostgreSQL** (evo1-postgres:5432): Source operational database
- **MinIO** (evo1-minio:9000): S3-compatible object storage (mock AWS S3)
- **pgAdmin** (localhost:5050): PostgreSQL management UI
- **minio_init**: One-time service that bootstraps MinIO with a `lake` bucket

### Data Flow
1. Raw data stored in MinIO S3 bucket `lake://` (accessed via HTTP)
2. DuckDB queries S3 data directly via httpfs extension
3. DuckDB also scans PostgreSQL tables via postgres_scan() function
4. Queries can join/combine data from both sources

## Key Development Tasks

### Startup
```bash
docker compose up -d
```
Brings up all services with health checks. MinIO and Postgres must be healthy before DuckDB starts.

### Access DuckDB CLI
```bash
docker exec -it evo1-duckdb duckdb /data/duckdb.db
```

### Bootstrap DuckDB
```sql
-- Inside DuckDB CLI:
.read /data/bootstrap.sql
```
This loads:
- `postgres` extension (enables PostgreSQL connectivity)
- `httpfs` extension (enables S3/HTTP access)
- MinIO credentials (endpoint, keys, SSL settings for path-style URLs)

### Verify Setup
```sql
SELECT * FROM duckdb_extensions();  -- Check extensions loaded
SELECT * FROM s3_scan('s3://lake/*');  -- Test S3 access
SELECT * FROM postgres_scan('postgresql://demo:demo@postgres:5432/demo', 'public', 'table_name');  -- Test PG access
```

## Critical Files & Patterns

### [duckdb/bootstrap.sql](duckdb/bootstrap.sql)
- **Purpose**: DuckDB initialization script loaded once per database session
- **Pattern**: Extension installation + system configuration (never query data here)
- **Modifications**: Add new extensions or S3 connection settings here, not in SQL queries
- **Key settings**: `s3_endpoint`, `s3_access_key_id`, `s3_secret_access_key`, `s3_use_ssl=false`, `s3_url_style='path'`

### [pg/schema.sql](pg/schema.sql)
- **Purpose**: Postgres table definitions used during ingest
- **Pattern**: Tables are UNLOGGED for faster bulk loads during experiments
- **Country code**: `country_of_registration` is `char(1)` and stores ISO-1 codes
- **Note**: Indexes are intentionally excluded here to keep ingest fast

### [pg/indexes.sql](pg/indexes.sql)
- **Purpose**: Post-ingest index creation
- **Pattern**: Run after data load to avoid per-row index maintenance
- **Safety**: Uses `IF NOT EXISTS` so reruns are safe

### [generate_vehicles.py](generate_vehicles.py)
- **Purpose**: Synthetic data generator that writes to Postgres and S3
- **Ingest order**: Timestamps are mostly sequential to simulate ingestion order
- **Anomalies**: A small number of rows are intentionally misplaced to model ingest anomalies
- **Country code**: Writes ISO-1 codes and aligns license plate prefix

### [pgadmin/servers.json](pgadmin/servers.json) & [pgadmin/pgpass](pgadmin/pgpass)
- Preconfigured PostgreSQL connection for pgAdmin UI
- **Pattern**: Use these for manual verification, not production workflows
- **Note**: pgpass mounted read-only, copied in entrypoint, permissions set to 600

### [docker-compose.yaml](docker-compose.yaml)
- **Service dependencies**: `depends_on` with `condition: service_healthy` ensures proper startup order
- **Credentials**: Hard-coded for development (minio/minio12345, demo/demo for postgres)
- **Volumes**: Persist data (pg_data, minio_data) and mount local config files read-only
- **Port mappings**: Postgres 55432→5432, pgAdmin 5050→80, MinIO S3 9000→9000, MinIO Console 9001→9001

## Project Conventions

### Extension Loading
Always load extensions in bootstrap.sql, not in individual queries. Extensions are:
- `postgres`: For PostgreSQL table access via `postgres_scan(connection_string, schema, table)`
- `httpfs`: For S3/MinIO access via `s3_scan()` or direct HTTP URLs

### S3 Paths in Queries
MinIO uses **path-style URLs** (not virtual-hosted-style):
```sql
-- Correct (path-style, as configured):
SELECT * FROM s3_scan('s3://lake/path/to/file.parquet');

-- Not: s3://lake.minio:9000/...
```

### Connection Strings
- **PostgreSQL**: `postgresql://user:password@host:port/db`
- **MinIO Console**: `localhost:9001` (browser), credentials: minio/minio12345
- **S3 Endpoint**: Internal (container-to-container): `http://minio:9000`

### Debugging Failures
1. Check service health: `docker compose ps` (all should show "healthy" or "running")
2. View service logs: `docker logs evo1-duckdb` (or other service name)
3. Verify extensions loaded in DuckDB: `.mode table` then `SELECT * FROM duckdb_extensions();`
4. Test S3 access separately: `SELECT * FROM s3_scan('s3://lake') LIMIT 1;`
5. For CLI bootstrap failures (Exit Code 1): Ensure DuckDB CLI is interactive - use `docker exec -it` (not just `docker exec`)
6. Wait for minio_init to complete - health check with `docker compose ps` shows `Up` not `Exited`

## Common Workflows for Agents

### Creating a new data query
1. Verify extensions loaded: `.read /data/bootstrap.sql`
2. Test data source availability (S3 or Postgres)
3. Write query using `s3_scan()` or `postgres_scan()`
4. Join/transform as needed

### Generating fresh data
1. Create tables: `Get-Content pg\schema.sql | docker exec -i evo1-postgres psql -U demo -d demo`
2. Generate events: `python .\generate_vehicles.py --days 10`
3. Build indexes after ingest: `Get-Content pg\indexes.sql | docker exec -i evo1-postgres psql -U demo -d demo`

### Investigating connectivity issues
1. Confirm service started: `docker compose logs <service_name>`
2. Check DuckDB extension status: `SELECT * FROM duckdb_extensions();`
3. Verify MinIO bucket exists: `docker exec evo1-minio-init mc ls local/lake`
4. Test direct connectivity: `docker exec -it evo1-duckdb curl -v http://minio:9000/minio/health/live`

### Updating configuration
- Add extensions/credentials → modify [duckdb/bootstrap.sql](duckdb/bootstrap.sql)
- Add Postgres tables to pgAdmin → modify [pgadmin/servers.json](pgadmin/servers.json)
- Add new service → modify [docker-compose.yaml](docker-compose.yaml)
- After changes, restart: `docker compose down && docker compose up -d`

## Gotchas & Important Notes

- **DuckDB persistence**: Database file `/data/duckdb.db` persists; extensions load on each CLI session
- **MinIO URL style**: Path-style URLs required (`http://minio:9000/lake/...` not virtual-hosted)
- **Health checks**: Services won't start until dependencies report healthy (can take 30s for MinIO)
- **.gitignore**: Excludes DuckDB WAL files and temp files; commits never include database state
