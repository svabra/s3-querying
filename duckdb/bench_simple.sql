.read /data/bootstrap.sql
.timer on

CREATE OR REPLACE VIEW pg_in AS
SELECT * FROM postgres_scan(
  'postgresql://demo:demo@postgres:5432/demo',
  'public',
  'vehicles_incoming'
);

CREATE OR REPLACE VIEW s3_in AS
SELECT *
FROM read_parquet(
  's3://lake/vehicles/direction=incoming/date=*/part-*.parquet',
  hive_partitioning=1
);

SELECT 'PG' AS src, count(*) AS rows FROM pg_in;
SELECT 'S3' AS src, count(*) AS rows FROM s3_in;
