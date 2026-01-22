.read /data/bootstrap.sql
.timer on

-- ----------------------------
-- Define sources: Postgres
-- ----------------------------
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

-- ----------------------------
-- Define sources: S3 (MinIO)
-- ----------------------------
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

-- =========================================================
-- (a) "How long are vehicles staying in Switzerland?"
-- With missing events:
--   For each incoming event, find the next outgoing event for that vehicle
--   using a windowed "min outgoing ts from current row forward".
-- =========================================================

-- Helper: unify events for PG
CREATE OR REPLACE VIEW pg_events AS
SELECT ts, country_of_registration AS country, license_plate AS plate, 'incoming' AS dir
FROM pg_in
UNION ALL
SELECT ts, country_of_registration AS country, license_plate AS plate, 'outgoing' AS dir
FROM pg_out;

-- Compute next outgoing time for each row, then take only incoming rows
CREATE OR REPLACE VIEW pg_stays AS
SELECT
  country,
  plate,
  ts AS in_ts,
  next_out_ts,
  (next_out_ts - ts) AS stay_duration
FROM (
  SELECT
    *,
    min(ts) FILTER (WHERE dir='outgoing')
      OVER (
        PARTITION BY country, plate
        ORDER BY ts
        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
      ) AS next_out_ts
  FROM pg_events
)
WHERE dir='incoming' AND next_out_ts IS NOT NULL;

-- Same for S3
CREATE OR REPLACE VIEW s3_events AS
SELECT ts, country_of_registration AS country, license_plate AS plate, 'incoming' AS dir
FROM s3_in
UNION ALL
SELECT ts, country_of_registration AS country, license_plate AS plate, 'outgoing' AS dir
FROM s3_out;

CREATE OR REPLACE VIEW s3_stays AS
SELECT
  country,
  plate,
  ts AS in_ts,
  next_out_ts,
  (next_out_ts - ts) AS stay_duration
FROM (
  SELECT
    *,
    min(ts) FILTER (WHERE dir='outgoing')
      OVER (
        PARTITION BY country, plate
        ORDER BY ts
        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
      ) AS next_out_ts
  FROM s3_events
)
WHERE dir='incoming' AND next_out_ts IS NOT NULL;

-- Aggregation for (a) (hours quantiles)
SELECT 'PG' AS src,
       approx_quantile(extract(epoch from stay_duration)/3600.0, [0.50, 0.90, 0.99]) AS stay_hours_p50_p90_p99
FROM pg_stays;

SELECT 'S3' AS src,
       approx_quantile(extract(epoch from stay_duration)/3600.0, [0.50, 0.90, 0.99]) AS stay_hours_p50_p90_p99
FROM s3_stays;


-- =========================================================
-- (b) "Show all vehicles crossing more than N times
--      in a window of D days starting from Date T"
-- You will set these parameters before each run.
-- =========================================================

-- Set parameters (edit these per benchmark run)
-- Example:
--   T = 2024-06-01
--   D = 30 days
--   N = 20 crossings
SET my_T = '2024-06-01'::DATE;
SET my_D = 30;
SET my_N = 20;

-- PG: filter window then count
SELECT
  country,
  plate,
  count(*) AS crossings_in_window
FROM pg_events
WHERE ts >= (getvariable('my_T')::DATE)::TIMESTAMPTZ
  AND ts <  ((getvariable('my_T')::DATE) + (getvariable('my_D')::INT) * INTERVAL '1 day')::TIMESTAMPTZ
GROUP BY 1,2
HAVING count(*) > getvariable('my_N')::INT
ORDER BY crossings_in_window DESC
LIMIT 200;

-- S3: filter window then count
SELECT
  country,
  plate,
  count(*) AS crossings_in_window
FROM s3_events
WHERE ts >= (getvariable('my_T')::DATE)::TIMESTAMPTZ
  AND ts <  ((getvariable('my_T')::DATE) + (getvariable('my_D')::INT) * INTERVAL '1 day')::TIMESTAMPTZ
GROUP BY 1,2
HAVING count(*) > getvariable('my_N')::INT
ORDER BY crossings_in_window DESC
LIMIT 200;
