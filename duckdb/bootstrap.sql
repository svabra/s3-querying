-- duckdb/bootstrap.sql
-- Enable extensions
INSTALL postgres;
LOAD postgres;

INSTALL httpfs;
LOAD httpfs;

-- Configure MinIO (S3-compatible) settings for this DuckDB database
SET s3_endpoint='minio:9000';
SET s3_access_key_id='minio';
SET s3_secret_access_key='minio12345';
SET s3_use_ssl=false;
SET s3_url_style='path';

-- Optional: create convenient views for Postgres tables (example)
-- CREATE OR REPLACE VIEW pg_events AS
-- SELECT * FROM postgres_scan('postgresql://demo:demo@postgres:5432/demo', 'public', 'events');
