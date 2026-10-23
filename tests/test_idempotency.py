"""
Validates pipeline *behavior*, Ingestion layer only:
  - re-running an already-loaded month does not duplicate rows
  - out-of-order backfills (month N+2 before month N) do not corrupt state
"""

import psycopg.errors
import pyarrow as pa
import pytest
from sqlalchemy import text

from ingestion import load_raw
from ingestion.load_raw import load_month
from ingestion.manifest import has_been_loaded, mark_loaded
from ingestion.tlc import s3_raw_key
from tests.conftest import TAXI_TYPE, TEST_BUCKET, make_tlc_table


@pytest.fixture
def ingest(engine, s3):
    """Load one month the way DAG 1's load_raw_postgres task does: load_month, then mark_loaded."""

    def _ingest(source_month: str) -> int:
        year, month = source_month.split("-")
        key = s3_raw_key(year=year, month=month, taxi_type=TAXI_TYPE)
        row_count = load_month(
            s3_key=key,
            source_month=source_month,
            engine=engine,
            bucket=TEST_BUCKET,
            s3_client=s3,
        )
        mark_loaded(
            source_month=source_month, row_count=row_count, s3_key=key, engine=engine
        )
        return row_count

    return _ingest


def _month_counts(engine) -> dict[str, int]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT source_month, count(*) FROM raw.trips GROUP BY source_month")
        )
        return dict(rows.all())


def _row_ids(engine, source_month: str) -> list[int]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT id FROM raw.trips WHERE source_month = :m ORDER BY id"),
            {"m": source_month},
        )
        return list(rows.scalars())


def _manifest(engine) -> dict[str, dict]:
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT * FROM control.manifest")).mappings()
        return {row["source_month"]: dict(row) for row in rows}


def test_rerun_same_month_does_not_duplicate(engine, put_month, ingest):
    put_month("2024-03", rows=3)
    ingest("2024-03")
    first = _manifest(engine)["2024-03"]

    ingest("2024-03")

    assert _month_counts(engine) == {"2024-03": 3}
    manifest = _manifest(engine)
    assert list(manifest) == ["2024-03"]
    assert manifest["2024-03"]["row_count"] == 3
    assert manifest["2024-03"]["loaded_at"] > first["loaded_at"]


def test_out_of_order_backfill_keeps_both_months(engine, put_month, ingest):
    put_month("2024-03", rows=3)
    put_month("2024-05", rows=4)

    ingest("2024-05")
    later_month_ids = _row_ids(engine, "2024-05")
    ingest("2024-03")

    assert _month_counts(engine) == {"2024-03": 3, "2024-05": 4}
    # Loading the earlier month must not touch the later month's rows.
    assert _row_ids(engine, "2024-05") == later_month_ids
    manifest = _manifest(engine)
    assert {m: row["row_count"] for m, row in manifest.items()} == {
        "2024-03": 3,
        "2024-05": 4,
    }


def test_reload_replaces_month_instead_of_appending(engine, put_month, ingest):
    put_month("2024-03", rows=3)
    ingest("2024-03")
    old_ids = _row_ids(engine, "2024-03")

    put_month("2024-03", rows=5)
    ingest("2024-03")

    assert _month_counts(engine) == {"2024-03": 5}
    assert set(_row_ids(engine, "2024-03")).isdisjoint(old_ids)
    assert _manifest(engine)["2024-03"]["row_count"] == 5


def test_cbd_congestion_fee_absent_before_2025_loads_as_null(engine, put_month, ingest):
    assert "cbd_congestion_fee" not in make_tlc_table("2024-12").column_names
    put_month("2024-12", rows=3)
    put_month("2025-01", rows=2)

    ingest("2024-12")
    ingest("2025-01")

    with engine.connect() as conn:
        fees = dict(
            conn.execute(
                text(
                    "SELECT source_month, array_agg(cbd_congestion_fee) "
                    "FROM raw.trips GROUP BY source_month"
                )
            ).all()
        )
    assert fees["2024-12"] == [None, None, None]
    assert [float(fee) for fee in fees["2025-01"]] == [0.75, 0.75]


def test_failure_mid_copy_rolls_back_delete(engine, put_month, ingest, monkeypatch):
    put_month("2023-06", rows=3)
    ingest("2023-06")
    ids_before = _row_ids(engine, "2023-06")
    manifest_before = _manifest(engine)

    # Batches of 2: batch 1 (rows 0-1) COPYs fine, then batch 2 hits a fractional
    # passenger_count and the safe int cast raises -- after DELETE and a partial COPY.
    monkeypatch.setattr(load_raw, "_BATCH_ROWS", 2)
    put_month("2023-06", rows=4, overrides={"passenger_count": [1, 1, 1.5, 1]})
    with pytest.raises(pa.ArrowInvalid):
        ingest("2023-06")

    assert _row_ids(engine, "2023-06") == ids_before
    assert _manifest(engine) == manifest_before


def test_unexpected_new_column_fails_loudly(engine, put_month, ingest):
    put_month("2024-03", rows=3)
    ingest("2024-03")
    ids_before = _row_ids(engine, "2024-03")
    manifest_before = _manifest(engine)

    put_month("2024-03", rows=3, extra_columns={"new_tlc_fee": [1.0, 1.0, 1.0]})
    with pytest.raises(psycopg.errors.UndefinedColumn):
        ingest("2024-03")

    assert _row_ids(engine, "2024-03") == ids_before
    assert _manifest(engine) == manifest_before


@pytest.mark.parametrize(
    ("source_month", "source_type"),
    [("2023-01", pa.float64()), ("2025-01", pa.int64())],
)
def test_int_code_columns_load_as_smallint(
    engine, put_month, ingest, source_month, source_type
):
    overrides = {"passenger_count": [1, None, 2], "RatecodeID": [1, None, 99]}
    schema = make_tlc_table(source_month, overrides=overrides).schema
    assert schema.field("passenger_count").type == source_type
    assert schema.field("RatecodeID").type == source_type

    put_month(source_month, rows=3, overrides=overrides)
    ingest(source_month)

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT passenger_count, ratecode_id, vendor_id, pu_location_id, payment_type "
                "FROM raw.trips ORDER BY tpep_pickup_datetime"
            )
        ).all()
    assert [tuple(row) for row in rows] == [
        (1, 1, 2, 161, 1),
        (None, None, 2, 161, 1),
        (2, 99, 2, 161, 1),
    ]


def test_has_been_loaded_tracks_manifest(engine, put_month, ingest):
    assert not has_been_loaded("2024-03", engine=engine)

    put_month("2024-03")
    ingest("2024-03")

    assert has_been_loaded("2024-03", engine=engine)
    assert not has_been_loaded("2024-04", engine=engine)
    assert not has_been_loaded("2024-03", engine=engine, taxi_type="green")
