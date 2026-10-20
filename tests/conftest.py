"""Shared fixtures: a throwaway warehouse database, mocked S3, and synthetic TLC Parquet.

The test database is created on the warehouse Postgres from .env (localhost:5432),
built from sql/ DDL, and dropped at session end -- the real warehouse is never
touched. Tests that need it skip cleanly when Postgres isn't reachable.
"""

import io
import os
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from dotenv import find_dotenv, load_dotenv
from moto import mock_aws
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import OperationalError

from ingestion.tlc import s3_raw_key

load_dotenv(find_dotenv())

REPO_ROOT = Path(__file__).resolve().parents[1]
DDL_FILES = (
    REPO_ROOT / "sql" / "raw" / "trips.sql",
    REPO_ROOT / "sql" / "control" / "manifest.sql",
)
TEST_BUCKET = "test-raw"
TAXI_TYPE = "yellow"

_MONEY_COLUMNS = {
    "fare_amount": 12.10,
    "extra": 1.00,
    "mta_tax": 0.50,
    "tip_amount": 3.00,
    "tolls_amount": 0.00,
    "improvement_surcharge": 1.00,
    "total_amount": 20.10,
    "congestion_surcharge": 2.50,
    "Airport_fee": 0.00,
}


def _pg_url(database: str) -> URL:
    return URL.create(
        "postgresql+psycopg2",
        username=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", "changeme"),
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        database=database,
    )


@pytest.fixture(scope="session")
def _test_db_engine():
    admin = create_engine(
        _pg_url(os.environ.get("POSTGRES_DB", "nyc_taxi")),
        isolation_level="AUTOCOMMIT",
        connect_args={"connect_timeout": 2},
    )
    try:
        with admin.connect():
            pass
    except OperationalError as exc:
        admin.dispose()
        pytest.skip(f"warehouse Postgres not reachable: {exc.orig}")

    db_name = f"nyc_taxi_test_{uuid.uuid4().hex[:8]}"
    with admin.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{db_name}"'))

    engine = create_engine(_pg_url(db_name))
    try:
        with engine.begin() as conn:
            for ddl in DDL_FILES:
                conn.exec_driver_sql(ddl.read_text())
        yield engine
    finally:
        engine.dispose()
        with admin.connect() as conn:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE)'))
        admin.dispose()


@pytest.fixture
def engine(_test_db_engine):
    """Engine on the throwaway test database, with raw.trips and control.manifest emptied."""
    with _test_db_engine.begin() as conn:
        conn.execute(text("TRUNCATE raw.trips, control.manifest"))
    return _test_db_engine


@pytest.fixture
def s3(monkeypatch):
    """moto-mocked S3 client with TEST_BUCKET created."""
    for key, value in {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_SESSION_TOKEN": "testing",
        "AWS_DEFAULT_REGION": "us-east-1",
    }.items():
        monkeypatch.setenv(key, value)
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=TEST_BUCKET)
        yield client


def make_tlc_table(
    source_month: str,
    rows: int = 3,
    *,
    overrides: dict | None = None,
    extra_columns: dict | None = None,
) -> pa.Table:
    """Build a synthetic yellow-taxi month with TLC's original column names and types.

    Types follow the real files for that year:
      - 2023: passenger_count / RatecodeID are double, location/vendor ids int64
      - 2024+: passenger_count / RatecodeID int64, location/vendor ids int32
      - 2025-01+: cbd_congestion_fee present (absent before -- schema drift)

    Args:
        source_month: ``"YYYY-MM"``; picks the era's schema and the pickup dates.
        rows: number of trips.
        overrides: column -> list of values (may include None), cast to that column's type.
        extra_columns: column -> list of values, appended as columns TLC doesn't ship.
    """
    year, month = (int(part) for part in source_month.split("-"))
    is_2023 = year == 2023
    id_type = pa.int64() if is_2023 else pa.int32()
    count_type = pa.float64() if is_2023 else pa.int64()
    start = datetime(year, month, 1, 8, 0)  # noqa: DTZ001 -- TLC timestamps are naive

    columns = {
        "VendorID": (id_type, [2] * rows),
        "tpep_pickup_datetime": (
            pa.timestamp("us"),
            [start + timedelta(hours=i) for i in range(rows)],
        ),
        "tpep_dropoff_datetime": (
            pa.timestamp("us"),
            [start + timedelta(hours=i, minutes=15) for i in range(rows)],
        ),
        "passenger_count": (count_type, [1] * rows),
        "trip_distance": (pa.float64(), [2.5] * rows),
        "RatecodeID": (count_type, [1] * rows),
        "store_and_fwd_flag": (pa.large_string(), ["N"] * rows),
        "PULocationID": (id_type, [161] * rows),
        "DOLocationID": (id_type, [237] * rows),
        "payment_type": (pa.int64(), [1] * rows),
    }
    columns.update(
        {name: (pa.float64(), [value] * rows) for name, value in _MONEY_COLUMNS.items()}
    )
    if source_month >= "2025-01":
        columns["cbd_congestion_fee"] = (pa.float64(), [0.75] * rows)

    for name, values in (overrides or {}).items():
        columns[name] = (columns[name][0], values)

    table = pa.table(
        {name: pa.array(values, type_) for name, (type_, values) in columns.items()}
    )
    for name, values in (extra_columns or {}).items():
        table = table.append_column(name, pa.array(values))
    return table


@pytest.fixture
def put_month(s3):
    """Upload a synthetic month to its raw S3 key (overwriting any earlier one); return the key."""

    def _put(source_month: str, rows: int = 3, **kwargs) -> str:
        year, month = source_month.split("-")
        key = s3_raw_key(year=year, month=month, taxi_type=TAXI_TYPE)
        buf = io.BytesIO()
        pq.write_table(make_tlc_table(source_month, rows, **kwargs), buf)
        s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=buf.getvalue())
        return key

    return _put
