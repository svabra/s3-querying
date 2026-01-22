.read /data/bootstrap.sql
.timer on

-- Count incoming records in PG vs S3
-- Host (Windows): Get-Content duckdb\bench_count_records.sql | docker exec -i evo1-duckdb duckdb /data/duckdb.db

SELECT
  'count.PG' AS case_src,
  count(*) AS rows
FROM postgres_scan(
  'postgresql://demo:demo@postgres:5432/demo',
  'public',
  'vehicles_incoming'
);

SELECT
  'count.S3' AS case_src,
  count(*) AS rows
FROM read_parquet(
  's3://lake/vehicles/direction=incoming/date=*/part-*.parquet',
  hive_partitioning=1
);
