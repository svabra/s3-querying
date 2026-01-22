# Query S3 and Postgres with DuckDB

To run the setup of Minio/S3, DuckDB and Postgres, run the docker compose file.

This will bring up a bootstrapped duckdb that has access to minio/s3.

```
docker compose up -d
```

Then verify that duckdb CLI can be accessed.

```
docker exec -it evo1-duckdb duckdb /data/duckdb.db
```

When CLI is up, run the prepare bootstrap, that loads the connection to our minio

```
.read /data/bootstrap.sql
```
This did run the sql statements that loaded httpfs and postgres etensions. Test it:

```
SELECT * FROM duckdb_extensions();
```

## Postgres schema (fast ingest)
Create the tables in Postgres before generating data:

```
Get-Content pg\schema.sql | docker exec -i evo1-postgres psql -U demo -d demo
```

Why this matters:
- The tables are UNLOGGED to speed up bulk ingest during experiments.
- `country_of_registration` uses ISO-1 codes stored as `char(1)` to shrink row size and index keys.

## Generate data
Generate synthetic vehicle events into Postgres and S3:

```
python .\generate_vehicles.py --days 10
```

Why this matters:
- Timestamps are mostly sequential to simulate ingestion order, with a few misplaced rows to model anomalies.
- Country codes are written as ISO-1, and license plates mirror that prefix.

## Benchmarks (run individually)
Run each benchmark from the Windows host with a focused SQL file:

```
Get-Content duckdb\bench_count_records.sql | docker exec -i evo1-duckdb duckdb /data/duckdb.db
```

```
Get-Content duckdb\bench_stay_duration.sql | docker exec -i evo1-duckdb duckdb /data/duckdb.db
```

```
Get-Content duckdb\bench_frequent_crossers.sql | docker exec -i evo1-duckdb duckdb /data/duckdb.db
```

```
Get-Content duckdb\bench_partition_country_counts.sql | docker exec -i evo1-duckdb duckdb /data/duckdb.db
```

```
Get-Content duckdb\bench_ts_quantiles.sql | docker exec -i evo1-duckdb duckdb /data/duckdb.db
```

```
Get-Content duckdb\bench_vehicle_history_lookup.sql | docker exec -i evo1-duckdb duckdb /data/duckdb.db
```

```
Get-Content duckdb\bench_time_window_count.sql | docker exec -i evo1-duckdb duckdb /data/duckdb.db
```

## Create indexes after ingest
Build indexes only after the data load is complete:

```
Get-Content pg\indexes.sql | docker exec -i evo1-postgres psql -U demo -d demo
```

Why this matters:
- Postgres bulk loads are faster without index maintenance on each row.
- You can rerun this safely if needed.





