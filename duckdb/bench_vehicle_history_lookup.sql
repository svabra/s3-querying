.read /data/bootstrap.sql
.timer on

-- Vehicle history lookup (PG vs S3)
-- Execution focus: selective lookup by (country, plate).
-- Showcases: indexed-style point lookups in PG vs full scan filtering in S3.
-- Host (Windows): Get-Content duckdb\bench_vehicle_history_lookup.sql | docker exec -i evo1-duckdb duckdb /data/duckdb.db

CREATE OR REPLACE TEMP VIEW bench_params AS
SELECT
  'D'::varchar AS my_country,
  'D-TE9973'::varchar AS my_plate,
  100::int AS my_limit;

SELECT 'Question: Recent history for a single vehicle (PG vs S3)' AS info;
SELECT
  'Args: country=' || my_country ||
  ', plate=' || my_plate ||
  ', limit=' || my_limit AS info
FROM bench_params;

-- PG: lookup in incoming + outgoing
SELECT
  'hist.PG' AS case_src,
  ts,
  country_of_registration,
  license_plate,
  vehicle_type,
  colour,
  brand,
  location_of_crossing,
  dir
FROM (
  SELECT
    ts,
    country_of_registration,
    license_plate,
    vehicle_type,
    colour,
    brand,
    location_of_crossing,
    'incoming' AS dir
  FROM postgres_scan(
    'postgresql://demo:demo@postgres:5432/demo',
    'public',
    'vehicles_incoming'
  ), bench_params
  WHERE country_of_registration = my_country
    AND license_plate = my_plate

  UNION ALL

  SELECT
    ts,
    country_of_registration,
    license_plate,
    vehicle_type,
    colour,
    brand,
    location_of_crossing,
    'outgoing' AS dir
  FROM postgres_scan(
    'postgresql://demo:demo@postgres:5432/demo',
    'public',
    'vehicles_outgoing'
  ), bench_params
  WHERE country_of_registration = my_country
    AND license_plate = my_plate
)
ORDER BY ts DESC
LIMIT (SELECT my_limit FROM bench_params);

-- S3: lookup in incoming + outgoing
SELECT
  'hist.S3' AS case_src,
  ts,
  country_of_registration,
  license_plate,
  vehicle_type,
  colour,
  brand,
  location_of_crossing,
  dir
FROM (
  SELECT
    ts,
    country_of_registration,
    license_plate,
    vehicle_type,
    colour,
    brand,
    location_of_crossing,
    'incoming' AS dir
  FROM read_parquet(
    's3://lake/vehicles/direction=incoming/date=*/part-*.parquet',
    hive_partitioning=1
  ), bench_params
  WHERE country_of_registration = my_country
    AND license_plate = my_plate

  UNION ALL

  SELECT
    ts,
    country_of_registration,
    license_plate,
    vehicle_type,
    colour,
    brand,
    location_of_crossing,
    'outgoing' AS dir
  FROM read_parquet(
    's3://lake/vehicles/direction=outgoing/date=*/part-*.parquet',
    hive_partitioning=1
  ), bench_params
  WHERE country_of_registration = my_country
    AND license_plate = my_plate
)
ORDER BY ts DESC
LIMIT (SELECT my_limit FROM bench_params);
