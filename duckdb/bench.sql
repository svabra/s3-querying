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
-- A) "How long are vehicles staying in Switzerland?"
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

-- A1) PG: stay duration quantiles
SELECT 'A.PG' AS case_src,
       approx_quantile(extract(epoch from stay_duration)/3600.0, [0.50, 0.90, 0.99]) AS stay_hours_p50_p90_p99
FROM pg_stays;

-- A2) S3: stay duration quantiles
SELECT 'A.S3' AS case_src,
       approx_quantile(extract(epoch from stay_duration)/3600.0, [0.50, 0.90, 0.99]) AS stay_hours_p50_p90_p99
FROM s3_stays;


-- =========================================================
-- B) "Show all vehicles crossing more than N times
--     in a window of D days starting from Date T"
-- Edit bench_params below to change the window.
-- =========================================================

-- Parameters for B) (edit these per benchmark run)
CREATE OR REPLACE TEMP VIEW bench_params AS
SELECT
  DATE '2025-01-01' AS my_T,
  30 AS my_D,
  20 AS my_N;

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
