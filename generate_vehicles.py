#!/usr/bin/env python3
import argparse
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.config import Config
import psycopg


# -----------------------------
# "Swiss-ish" vehicle categories
# -----------------------------
VEHICLE_TYPES = [
    ("passenger_car", 0.62),
    ("delivery_van", 0.13),
    ("truck", 0.12),
    ("motorcycle", 0.08),
    ("bus", 0.03),
    ("other", 0.02),
]

COLOURS = ["white", "black", "silver", "blue", "red", "grey", "green", "brown", "yellow"]
BRANDS_BY_TYPE = {
    "passenger_car": [
        "VW", "BMW", "Mercedes", "Audi", "Skoda", "Toyota", "Renault", "Peugeot",
        "Fiat", "Ford", "Tesla", "Opel", "Seat", "Hyundai",
    ],
    "delivery_van": ["VW", "Mercedes", "Ford", "Renault", "Fiat", "Peugeot", "Iveco"],
    "truck": ["Volvo", "Scania", "MAN", "DAF", "Mercedes", "Iveco"],
    "motorcycle": ["Yamaha", "Honda", "Kawasaki", "BMW", "Suzuki", "Ducati", "KTM"],
    "bus": ["MAN", "Mercedes", "Volvo", "Iveco", "Scania"],
    "other": ["Generic"],
}

# -----------------------------
# Border crossing points (subset)
# and which neighbor corridor they represent.
# -----------------------------
# (location, corridor_country)
CROSSINGS = [
    # Germany corridor
    ("Basel", "DE"),
    ("Rheinfelden", "DE"),
    ("Kreuzlingen", "DE"),
    ("Schaffhausen", "DE"),
    ("Bargen", "DE"),
    # France corridor
    ("Geneve", "FR"),
    ("Bardonnex", "FR"),
    ("Vallorbe", "FR"),
    ("Boncourt", "FR"),
    # Italy corridor
    ("Chiasso", "IT"),
    ("Brusata (Mendrisio)", "IT"),
    ("Brissago", "IT"),
    # Austria corridor
    ("St. Margrethen", "AT"),
    ("Au (SG)", "AT"),
    # Liechtenstein corridor
    ("Schaanwald", "LI"),
    ("Bendern", "LI"),
]

CORRIDOR_TO_CROSSINGS: Dict[str, List[str]] = {}
for loc, cc in CROSSINGS:
    CORRIDOR_TO_CROSSINGS.setdefault(cc, []).append(loc)

ALL_CROSSINGS = [loc for loc, _ in CROSSINGS]


# -----------------------------
# European ISO alpha-2 list (broad)
# (EU + EEA + UK + Balkan + microstates)
# We give big weights to near countries, smaller weights to far ones.
# -----------------------------
NEAR = ["DE", "FR", "IT", "AT", "LI"]
MID = ["NL", "BE", "LU", "DK", "CZ", "PL", "SK", "HU", "SI", "HR"]
FAR = ["ES", "PT", "IE", "SE", "FI", "EE", "LV", "LT", "RO", "BG", "GR", "CY", "MT"]
BALKAN = ["AL", "BA", "RS", "ME", "MK"]
OTHER_EUROPE = ["NO", "IS", "GB", "UA", "MD", "BY", "TR", "SM", "VA", "MC", "AD"]

# Combine, de-dup while keeping order
EURO_COUNTRIES = []
for grp in [NEAR, MID, FAR, BALKAN, OTHER_EUROPE]:
    for c in grp:
        if c not in EURO_COUNTRIES:
            EURO_COUNTRIES.append(c)

ISO1_MAP = {
    "DE": "D",
    "FR": "F",
    "IT": "I",
    "AT": "A",
    "LI": "L",
    "NL": "N",
    "BE": "B",
    "LU": "L",
    "DK": "D",
    "CZ": "C",
    "PL": "P",
    "SK": "S",
    "HU": "H",
    "SI": "S",
    "HR": "H",
    "ES": "E",
    "PT": "P",
    "IE": "I",
    "SE": "S",
    "FI": "F",
    "EE": "E",
    "LV": "L",
    "LT": "L",
    "RO": "R",
    "BG": "B",
    "GR": "G",
    "CY": "C",
    "MT": "M",
    "AL": "A",
    "BA": "B",
    "RS": "R",
    "ME": "M",
    "MK": "M",
    "NO": "N",
    "IS": "I",
    "GB": "G",
    "UA": "U",
    "MD": "M",
    "BY": "B",
    "TR": "T",
    "SM": "S",
    "VA": "V",
    "MC": "M",
    "AD": "A",
}


def to_iso1(country: str) -> str:
    if not country:
        return country
    if len(country) == 1:
        return country
    return ISO1_MAP.get(country, country[0])


def apply_iso1_codes(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    iso1 = np.array([to_iso1(c) for c in df["country_of_registration"].to_numpy()], dtype=object)
    df["country_of_registration"] = iso1
    suffix = df["license_plate"].str.split("-", n=1).str[1]
    df["license_plate"] = pd.Series(iso1, index=df.index) + "-" + suffix
    return df


def build_country_weights(rng: np.random.Generator) -> Tuple[np.ndarray, np.ndarray]:
    """
    Returns (countries, weights) normalized.
    Near countries dominate; far countries small but non-zero.
    """
    w = {}
    # near corridor heavy
    w.update({"DE": 0.42, "FR": 0.18, "IT": 0.20, "AT": 0.08, "LI": 0.02})

    # mid range moderate
    for c in MID:
        w[c] = rng.uniform(0.005, 0.02)

    # far range small
    for c in FAR:
        if c == "GR":
            w[c] = rng.uniform(0.005, 0.02)
        elif c == "SE":
            w[c] = rng.uniform(0.01, 0.02)
        else:
            w[c] = rng.uniform(0.003, 0.015)

    # balkan: small but a bit higher than far in some cases
    for c in BALKAN:
        if c == "AL":
            w[c] = rng.uniform(0.01, 0.03)
        else:
            w[c] = rng.uniform(0.005, 0.02)

    # other europe: generally small
    for c in OTHER_EUROPE:
        w[c] = rng.uniform(0.002, 0.012)

    countries = np.array(list(w.keys()))
    weights = np.array([w[c] for c in countries], dtype=float)
    weights = weights / weights.sum()
    return countries, weights


def choose_vehicle_type(rng: np.random.Generator, n: int) -> np.ndarray:
    types = np.array([t for t, _ in VEHICLE_TYPES])
    probs = np.array([p for _, p in VEHICLE_TYPES], dtype=float)
    probs = probs / probs.sum()
    return rng.choice(types, size=n, p=probs)


def choose_brand(rng: np.random.Generator, vtype: str) -> str:
    return rng.choice(BRANDS_BY_TYPE.get(vtype, ["Generic"]))


def random_plate(rng: np.random.Generator, country: str) -> str:
    # Simplified but unique-ish by country:
    # CC-LLDDDD (e.g., D-AB1234)
    letters = "".join(rng.choice(list("ABCDEFGHJKLMNPQRSTUVWXYZ"), size=2))
    digits = int(rng.integers(0, 10000))
    return f"{country}-{letters}{digits:04d}"


def choose_crossing(rng: np.random.Generator, country: str) -> str:
    # Prefer corridor crossings for near countries; otherwise choose any.
    if country in CORRIDOR_TO_CROSSINGS:
        return rng.choice(CORRIDOR_TO_CROSSINGS[country])
    return rng.choice(ALL_CROSSINGS)


def sequential_seconds(rng: np.random.Generator, n: int) -> np.ndarray:
    if n <= 0:
        return np.array([], dtype=int)
    gaps = rng.exponential(scale=1.0, size=n)
    cum = np.cumsum(gaps)
    if cum[-1] == 0:
        return np.zeros(n, dtype=int)
    secs = (cum / cum[-1]) * 86399
    return secs.astype(int)


def apply_ingest_misplacements(
    rng: np.random.Generator,
    df: pd.DataFrame,
    per_day: int,
    max_offset: int,
) -> pd.DataFrame:
    if per_day <= 0 or max_offset <= 0 or df.empty:
        return df
    n = min(per_day, len(df))
    idx = rng.choice(df.index.to_numpy(), size=n, replace=False)
    offsets = rng.integers(-max_offset, max_offset + 1, size=n)
    new_idx = np.clip(idx + offsets, 0, len(df) - 1)
    for i, j in zip(idx, new_idx):
        if i == j:
            continue
        df.iloc[[i, j]] = df.iloc[[j, i]].to_numpy()
    return df


@dataclass
class S3Config:
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str
    prefix: str


def s3_client(cfg: S3Config):
    return boto3.client(
        "s3",
        endpoint_url=cfg.endpoint,
        aws_access_key_id=cfg.access_key,
        aws_secret_access_key=cfg.secret_key,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def write_parquet_to_s3(s3, cfg: S3Config, df: pd.DataFrame, direction: str, day: date, part: int):
    day_str = day.isoformat()
    key = f"{cfg.prefix}/direction={direction}/date={day_str}/part-{part:05d}.parquet"

    table = pa.Table.from_pandas(df, preserve_index=False)
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd", row_group_size=250_000)
    buf = sink.getvalue().to_pybytes()

    s3.put_object(Bucket=cfg.bucket, Key=key, Body=buf)


def insert_postgres_copy(conn, table: str, df: pd.DataFrame):
    cols = [
        "ts",
        "country_of_registration",
        "license_plate",
        "vehicle_type",
        "colour",
        "brand",
        "location_of_crossing",
    ]
    data = df[cols]

    with conn.cursor() as cur:
        with cur.copy(f"COPY {table} ({', '.join(cols)}) FROM STDIN") as copy:
            for row in data.itertuples(index=False, name=None):
                ts = row[0].isoformat()
                copy.write_row((ts, *row[1:]))


# -----------------------------
# Standout vehicle pools
# -----------------------------
def make_standout_pool(
    rng: np.random.Generator,
    countries: np.ndarray,
    weights: np.ndarray,
    n_commuters: int,
    n_chilled: int,
    n_smugglers: int,
):
    """
    Create "identity pools" for standout vehicles:
    - commuters: mostly near countries
    - chilled trucks: mostly IT/FR/DE/AT
    - smugglers: mixed, but skew near + balkans
    """
    near_probs = np.array([0.55, 0.18, 0.17, 0.07, 0.03])  # DE,FR,IT,AT,LI
    near_countries = np.array(["DE", "FR", "IT", "AT", "LI"])
    commuter_country = rng.choice(near_countries, size=n_commuters, p=near_probs)

    chilled_countries = np.array(["IT", "FR", "DE", "AT"])
    chilled_probs = np.array([0.35, 0.25, 0.25, 0.15])
    chilled_country = rng.choice(chilled_countries, size=n_chilled, p=chilled_probs)

    smuggle_pool = np.array(["DE", "FR", "IT", "AT", "AL", "RS", "BA", "RO", "BG"])
    smuggle_probs = np.array([0.25, 0.12, 0.20, 0.08, 0.10, 0.08, 0.07, 0.05, 0.05])
    smuggle_country = rng.choice(smuggle_pool, size=n_smugglers, p=smuggle_probs)

    def mk_vehicles(cc_arr, vtype):
        plates = [random_plate(rng, cc) for cc in cc_arr]
        colours = rng.choice(COLOURS, size=len(cc_arr))
        brands = [choose_brand(rng, vtype) for _ in range(len(cc_arr))]
        return pd.DataFrame(
            {
                "country_of_registration": cc_arr,
                "license_plate": plates,
                "vehicle_type": np.array([vtype] * len(cc_arr)),
                "colour": colours,
                "brand": brands,
            }
        )

    commuters = mk_vehicles(commuter_country, "passenger_car")
    chilled = mk_vehicles(chilled_country, "truck")
    smugglers = mk_vehicles(smuggle_country, "delivery_van")
    return commuters, chilled, smugglers


def generate_day_events(
    rng: np.random.Generator,
    day_start: datetime,
    incoming_target: int,
    outgoing_target: int,
    countries: np.ndarray,
    weights: np.ndarray,
    commuters: pd.DataFrame,
    chilled: pd.DataFrame,
    smugglers: pd.DataFrame,
    missing_prob: float,
    misplace_per_day: int,
    misplace_max_offset: int,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Generate IN and OUT events for the day.
    - baseline: random independent samples for IN and OUT
    - standout injections: commuters + chilled + smugglers add extra crossings
    - missing_prob: some vehicles may have only IN or only OUT within the day (naturally)
    """

    def baseline_events(n: int) -> pd.DataFrame:
        cc = rng.choice(countries, size=n, p=weights)
        plates = np.array([random_plate(rng, c) for c in cc])
        vtypes = choose_vehicle_type(rng, n)
        colours = rng.choice(COLOURS, size=n)
        brands = np.array([choose_brand(rng, vt) for vt in vtypes], dtype=object)
        loc = np.array([choose_crossing(rng, c) for c in cc], dtype=object)

        # sequential arrivals with small random gaps
        secs = sequential_seconds(rng, n)
        ts = np.array([day_start + timedelta(seconds=int(s)) for s in secs], dtype=object)

        return pd.DataFrame(
            {
                "ts": ts,
                "country_of_registration": cc,
                "license_plate": plates,
                "vehicle_type": vtypes,
                "colour": colours,
                "brand": brands,
                "location_of_crossing": loc,
            }
        )

    incoming = baseline_events(incoming_target)
    outgoing = baseline_events(outgoing_target)

    extra_in_rows: List[Dict[str, object]] = []
    extra_out_rows: List[Dict[str, object]] = []

    # --- inject commuters: weekday pattern (Mon-Fri mostly) ---
    weekday = day_start.weekday()  # 0=Mon
    if weekday <= 4:
        # each commuter: typically 1 in + 1 out, sometimes extra
        n = len(commuters)
        # In morning 05:00-10:00, out 15:00-20:00
        in_secs = rng.integers(5 * 3600, 10 * 3600, size=n)
        out_secs = rng.integers(15 * 3600, 20 * 3600, size=n)

        # some missing (e.g. drove through, sensor miss)
        keep_in = rng.random(size=n) > missing_prob
        keep_out = rng.random(size=n) > missing_prob

        in_df = commuters.loc[keep_in].copy()
        out_df = commuters.loc[keep_out].copy()

        in_df["ts"] = [day_start + timedelta(seconds=int(s)) for s in in_secs[keep_in]]
        out_df["ts"] = [day_start + timedelta(seconds=int(s)) for s in out_secs[keep_out]]

        in_df["location_of_crossing"] = [
            choose_crossing(rng, c) for c in in_df["country_of_registration"].to_numpy()
        ]
        out_df["location_of_crossing"] = [
            choose_crossing(rng, c) for c in out_df["country_of_registration"].to_numpy()
        ]

        incoming = pd.concat([incoming, in_df[incoming.columns]], ignore_index=True)
        outgoing = pd.concat([outgoing, out_df[outgoing.columns]], ignore_index=True)

    # --- inject chilled logistics trucks: multiple border hops per week ---
    if len(chilled) > 0:
        active = rng.random(size=len(chilled)) < 0.35  # 35% active each day
        active_df = chilled.loc[active].copy()
        for _, v in active_df.iterrows():
            k = int(rng.integers(2, 7))  # 2..6 crossings
            secs = rng.integers(0, 86400, size=k)
            dirs = ["incoming" if i % 2 == 0 else "outgoing" for i in range(k)]
            for s, ddir in zip(secs, dirs):
                if rng.random() < missing_prob:
                    continue
                row = {
                    "ts": day_start + timedelta(seconds=int(s)),
                    "country_of_registration": v["country_of_registration"],
                    "license_plate": v["license_plate"],
                    "vehicle_type": v["vehicle_type"],
                    "colour": v["colour"],
                    "brand": v["brand"],
                    "location_of_crossing": choose_crossing(rng, v["country_of_registration"]),
                }
                if ddir == "incoming":
                    extra_in_rows.append(row)
                else:
                    extra_out_rows.append(row)

    # --- inject smugglers: very high + irregular crossings ---
    if len(smugglers) > 0:
        active = rng.random(size=len(smugglers)) < 0.25  # 25% active each day
        active_df = smugglers.loc[active].copy()
        for _, v in active_df.iterrows():
            k = int(rng.integers(6, 20))  # 6..19 crossings/day
            secs = rng.integers(0, 86400, size=k)
            dirs = rng.choice(["incoming", "outgoing"], size=k, p=[0.5, 0.5])
            for s, ddir in zip(secs, dirs):
                if rng.random() < (missing_prob * 1.2):
                    continue
                row = {
                    "ts": day_start + timedelta(seconds=int(s)),
                    "country_of_registration": v["country_of_registration"],
                    "license_plate": v["license_plate"],
                    "vehicle_type": v["vehicle_type"],
                    "colour": v["colour"],
                    "brand": v["brand"],
                    "location_of_crossing": choose_crossing(rng, v["country_of_registration"]),
                }
                if ddir == "incoming":
                    extra_in_rows.append(row)
                else:
                    extra_out_rows.append(row)

    if extra_in_rows:
        incoming = pd.concat([incoming, pd.DataFrame(extra_in_rows)], ignore_index=True)
    if extra_out_rows:
        outgoing = pd.concat([outgoing, pd.DataFrame(extra_out_rows)], ignore_index=True)

    incoming = incoming.sort_values("ts").reset_index(drop=True)
    outgoing = outgoing.sort_values("ts").reset_index(drop=True)

    incoming = apply_ingest_misplacements(rng, incoming, misplace_per_day, misplace_max_offset)
    outgoing = apply_ingest_misplacements(rng, outgoing, misplace_per_day, misplace_max_offset)

    return incoming, outgoing


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", type=int, default=1)
    ap.add_argument("--days", type=int, default=None, help="override total days (ignores --years)")
    ap.add_argument("--start-date", type=str, default="2025-01-01", help="YYYY-MM-DD (UTC)")

    ap.add_argument("--avg-in-per-day", type=int, default=1_100_000)
    ap.add_argument("--avg-out-per-day", type=int, default=1_100_000)
    ap.add_argument("--day-jitter", type=float, default=0.12, help="± jitter factor around avg (0.12 = ±12%)")

    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--chunk-rows", type=int, default=250_000)

    # Allow "few missing"
    ap.add_argument("--missing-prob", type=float, default=0.015, help="probability an injected event is missing")

    # standout scale
    ap.add_argument("--commuters", type=int, default=25_000)
    ap.add_argument("--chilled", type=int, default=2_500)
    ap.add_argument("--smugglers", type=int, default=350)
    ap.add_argument("--misplace-per-day", type=int, default=50)
    ap.add_argument("--misplace-max-offset", type=int, default=250)
    ap.add_argument("--anomaly-per-day", type=int, default=None, help=argparse.SUPPRESS)
    ap.add_argument("--anomaly-max-day-shift", type=int, default=None, help=argparse.SUPPRESS)

    # Postgres
    ap.add_argument("--pg-host", type=str, default="localhost")
    ap.add_argument("--pg-port", type=int, default=55432)
    ap.add_argument("--pg-db", type=str, default="demo")
    ap.add_argument("--pg-user", type=str, default="demo")
    ap.add_argument("--pg-pass", type=str, default="demo")
    ap.add_argument(
        "--pg-target",
        action="append",
        default=[],
        help="additional Postgres DSN to mirror writes to (repeatable)",
    )

    # S3/MinIO
    ap.add_argument("--s3-endpoint", type=str, default="http://localhost:9000")
    ap.add_argument("--s3-access", type=str, default="minio")
    ap.add_argument("--s3-secret", type=str, default="minio12345")
    ap.add_argument("--s3-bucket", type=str, default="lake")
    ap.add_argument("--s3-prefix", type=str, default="vehicles")

    args = ap.parse_args()
    if args.anomaly_per_day is not None:
        args.misplace_per_day = args.anomaly_per_day
    if args.anomaly_max_day_shift is not None:
        args.misplace_max_offset = args.anomaly_max_day_shift
    rng = np.random.default_rng(args.seed)

    start_day = datetime.fromisoformat(args.start_date).replace(tzinfo=timezone.utc)
    if args.days is not None:
        if args.days <= 0:
            raise SystemExit("--days must be > 0")
        days = args.days
    else:
        days = args.years * 365  # PoC simplification

    countries, weights = build_country_weights(rng)
    commuters, chilled, smugglers = make_standout_pool(
        rng,
        countries,
        weights,
        n_commuters=args.commuters,
        n_chilled=args.chilled,
        n_smugglers=args.smugglers,
    )

    primary_dsn = (
        f"postgresql://{args.pg_user}:{args.pg_pass}"
        f"@{args.pg_host}:{args.pg_port}/{args.pg_db}"
    )
    pg_targets = [primary_dsn]
    for target in args.pg_target:
        if target not in pg_targets:
            pg_targets.append(target)

    pg_conns = [psycopg.connect(dsn, autocommit=True) for dsn in pg_targets]

    s3cfg = S3Config(
        endpoint=args.s3_endpoint,
        access_key=args.s3_access,
        secret_key=args.s3_secret,
        bucket=args.s3_bucket,
        prefix=args.s3_prefix.rstrip("/"),
    )
    s3 = s3_client(s3cfg)

    pg_total = 0.0
    s3_total = 0.0
    run_start = time.perf_counter()

    for d in range(days):
        day_start = start_day + timedelta(days=d)
        day = day_start.date()

        j = args.day_jitter
        in_target = int(args.avg_in_per_day * (1 + rng.uniform(-j, j)))
        out_target = int(args.avg_out_per_day * (1 + rng.uniform(-j, j)))

        incoming, outgoing = generate_day_events(
            rng=rng,
            day_start=day_start,
            incoming_target=in_target,
            outgoing_target=out_target,
            countries=countries,
            weights=weights,
            commuters=commuters,
            chilled=chilled,
            smugglers=smugglers,
            missing_prob=args.missing_prob,
            misplace_per_day=args.misplace_per_day,
            misplace_max_offset=args.misplace_max_offset,
        )
        incoming = apply_iso1_codes(incoming)
        outgoing = apply_iso1_codes(outgoing)

        t_pg = time.perf_counter()
        for off in range(0, len(incoming), args.chunk_rows):
            chunk = incoming.iloc[off:off + args.chunk_rows]
            for conn in pg_conns:
                insert_postgres_copy(conn, "vehicles_incoming", chunk)
        for off in range(0, len(outgoing), args.chunk_rows):
            chunk = outgoing.iloc[off:off + args.chunk_rows]
            for conn in pg_conns:
                insert_postgres_copy(conn, "vehicles_outgoing", chunk)
        pg_elapsed = time.perf_counter() - t_pg
        pg_total += pg_elapsed

        t_s3 = time.perf_counter()
        part = 0
        for off in range(0, max(len(incoming), len(outgoing)), args.chunk_rows):
            if off < len(incoming):
                write_parquet_to_s3(s3, s3cfg, incoming.iloc[off:off + args.chunk_rows], "incoming", day, part)
            if off < len(outgoing):
                write_parquet_to_s3(s3, s3cfg, outgoing.iloc[off:off + args.chunk_rows], "outgoing", day, part)
            part += 1
        s3_elapsed = time.perf_counter() - t_s3
        s3_total += s3_elapsed

        print(
            f"{day} incoming={len(incoming):,} outgoing={len(outgoing):,} "
            f"pg={pg_elapsed:.2f}s s3={s3_elapsed:.2f}s",
            flush=True,
        )

    for conn in pg_conns:
        conn.close()

    print("\n--- ingest timings (informational only) ---", flush=True)
    print(f"Total wall time: {time.perf_counter() - run_start:.2f}s", flush=True)
    print(f"Postgres insert total: {pg_total:.2f}s", flush=True)
    print(f"S3 parquet write total: {s3_total:.2f}s", flush=True)


if __name__ == "__main__":
    main()
