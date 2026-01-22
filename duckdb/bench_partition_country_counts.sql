.read /data/bootstrap.sql
.timer on

-- Partition-filtered country counts (PG vs S3)
-- Execution focus: partition pruning on S3 date partitions.
-- Showcases: narrow date window + group-by; S3 should read fewer files.
-- Host (Windows): Get-Content duckdb\bench_partition_country_counts.sql | docker exec -i evo1-duckdb duckdb /data/duckdb.db

CREATE OR REPLACE TEMP VIEW bench_params AS
SELECT
  DATE '2025-01-01' AS my_T,
  3 AS my_D;

SELECT 'Question: Country counts for date window (PG vs S3)' AS info;
SELECT
  'Args: T=' || CAST(my_T AS VARCHAR) ||
  ', D=' || CAST(my_D AS VARCHAR) || ' days' AS info
FROM bench_params;

-- Define sources
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

-- PG: filter by ts, then group
SELECT
  'part.PG' AS case_src,
  country_of_registration AS country,
  count(*) AS crossings
FROM pg_in, bench_params
WHERE ts >= (my_T::DATE)::TIMESTAMPTZ
  AND ts < ((my_T::DATE) + (my_D::INT) * INTERVAL '1 day')::TIMESTAMPTZ
GROUP BY 2
ORDER BY crossings DESC
LIMIT 50;

-- S3: filter by partition date, then group
SELECT
  'part.S3' AS case_src,
  country_of_registration AS country,
  count(*) AS crossings
FROM s3_in, bench_params
WHERE CAST(date AS DATE) >= my_T
  AND CAST(date AS DATE) < (my_T + (my_D::INT) * INTERVAL '1 day')
GROUP BY 2
ORDER BY crossings DESC
LIMIT 50;
