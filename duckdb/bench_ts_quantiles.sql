.read /data/bootstrap.sql
.timer on

-- Timestamp quantiles on incoming events (PG vs S3)
-- Execution focus: columnar scan of a single column with aggregates.
-- Showcases: minimal column projection; S3 reads only ts from Parquet.
-- Expected winner: S3 (single-column Parquet scan).
-- Host (Windows): Get-Content duckdb\bench_ts_quantiles.sql | docker exec -i evo1-duckdb duckdb /data/duckdb.db

SELECT 'Question: Timestamp quantiles for incoming events (PG vs S3)' AS info;
SELECT 'Args: none' AS info;

SELECT
  'quant.PG' AS case_src,
  approx_quantile(extract(epoch from ts)/3600.0, [0.50, 0.90, 0.99]) AS ts_hours_p50_p90_p99
FROM postgres_scan(
  'postgresql://demo:demo@postgres:5432/demo',
  'public',
  'vehicles_incoming'
);

SELECT
  'quant.S3' AS case_src,
  approx_quantile(extract(epoch from ts)/3600.0, [0.50, 0.90, 0.99]) AS ts_hours_p50_p90_p99
FROM read_parquet(
  's3://lake/vehicles/direction=incoming/date=*/part-*.parquet',
  hive_partitioning=1
);
