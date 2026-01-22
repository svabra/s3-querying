.read /data/bootstrap.sql
.timer on

-- B) "Show all vehicles crossing more than N times
--     in a window of D days starting from Date T"
-- Edit bench_params below to change the window.
-- Execution focus: time-window filter + group-by + HAVING + sort.
-- Showcases: partition pruning on S3 date partitions vs row-streamed PG scan.
-- Expected winner: S3 (partition pruning on date).
-- Host (Windows): Get-Content duckdb\bench_frequent_crossers.sql | docker exec -i evo1-duckdb duckdb /data/duckdb.db

-- Parameters for B) (edit these per benchmark run)
CREATE OR REPLACE TEMP VIEW bench_params AS
SELECT
  DATE '2025-01-01' AS my_T,
  30 AS my_D,
  20 AS my_N;

SELECT 'Question: Vehicles crossing more than N times in D days from T (PG vs S3)' AS info;
SELECT
  'Args: T=' || CAST(my_T AS VARCHAR) ||
  ', D=' || CAST(my_D AS VARCHAR) ||
  ', N=' || CAST(my_N AS VARCHAR) AS info
FROM bench_params;

-- Define sources: Postgres
CREATE OR REPLACE VIEW pg_in AS
SELECT * FROM postgres_scan(
  'postgresql://demo:demo@postgres:5432/demo',
  'public',
  'vehicles_incoming'
);

CREATE OR REPLACE VIEW pg_out AS
SELECT * FROM postgres_scan(
  'postgresql://demo:demo@postgres:5432/demo',
  'public',
  'vehicles_outgoing'
);

-- Define sources: S3 (MinIO)
CREATE OR REPLACE VIEW s3_in AS
SELECT *
FROM read_parquet(
  's3://lake/vehicles/direction=incoming/date=*/part-*.parquet',
  hive_partitioning=1
);

CREATE OR REPLACE VIEW s3_out AS
SELECT *
FROM read_parquet(
  's3://lake/vehicles/direction=outgoing/date=*/part-*.parquet',
  hive_partitioning=1
);

-- Helper: unify events
CREATE OR REPLACE VIEW pg_events AS
SELECT ts, country_of_registration AS country, license_plate AS plate, 'incoming' AS dir
FROM pg_in
UNION ALL
SELECT ts, country_of_registration AS country, license_plate AS plate, 'outgoing' AS dir
FROM pg_out;

CREATE OR REPLACE VIEW s3_events AS
SELECT ts, country_of_registration AS country, license_plate AS plate, 'incoming' AS dir
FROM s3_in
UNION ALL
SELECT ts, country_of_registration AS country, license_plate AS plate, 'outgoing' AS dir
FROM s3_out;

-- B1) PG: filter window then count
SELECT
  'B.PG' AS case_src,
  country,
  plate,
  count(*) AS crossings_in_window
FROM pg_events, bench_params
WHERE ts >= (my_T::DATE)::TIMESTAMPTZ
  AND ts <  ((my_T::DATE) + (my_D::INT) * INTERVAL '1 day')::TIMESTAMPTZ
GROUP BY 2,3
HAVING count(*) > max(my_N)::INT
ORDER BY crossings_in_window DESC
LIMIT 200;

-- B2) S3: filter window then count
SELECT
  'B.S3' AS case_src,
  country,
  plate,
  count(*) AS crossings_in_window
FROM s3_events, bench_params
WHERE ts >= (my_T::DATE)::TIMESTAMPTZ
  AND ts <  ((my_T::DATE) + (my_D::INT) * INTERVAL '1 day')::TIMESTAMPTZ
GROUP BY 2,3
HAVING count(*) > max(my_N)::INT
ORDER BY crossings_in_window DESC
LIMIT 200;
