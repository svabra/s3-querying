.read /data/bootstrap.sql
.timer on

-- Narrow time-window count (PG vs S3)
-- Execution focus: selective time-range filter on ts.
-- Showcases: time-window filtering and potential pushdown differences.
-- Expected winner: S3 (DuckDB postgres_scan overhead dominates here).
-- Host (Windows): Get-Content duckdb\bench_time_window_count.sql | docker exec -i evo1-duckdb duckdb /data/duckdb.db

CREATE OR REPLACE TEMP VIEW bench_params AS
SELECT
  TIMESTAMPTZ '2025-01-03 00:00:00+00' AS my_start,
  TIMESTAMPTZ '2025-01-03 01:00:00+00' AS my_end;

SELECT 'Question: Count incoming rows in a narrow time window (PG vs S3)' AS info;
SELECT
  'Args: start=' || CAST(my_start AS VARCHAR) ||
  ', end=' || CAST(my_end AS VARCHAR) AS info
FROM bench_params;

-- PG: filter by ts
SELECT
  'time.PG' AS case_src,
  count(*) AS rows
FROM postgres_scan(
  'postgresql://demo:demo@postgres:5432/demo',
  'public',
  'vehicles_incoming'
), bench_params
WHERE ts >= my_start
  AND ts < my_end;

-- S3: filter by ts
SELECT
  'time.S3' AS case_src,
  count(*) AS rows
FROM read_parquet(
  's3://lake/vehicles/direction=incoming/date=*/part-*.parquet',
  hive_partitioning=1
), bench_params
WHERE ts >= my_start
  AND ts < my_end;
