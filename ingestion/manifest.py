"""Manifest: record which (taxi_type, source_month) partitions are loaded in raw.trips.

Backed by control.manifest (sql/control/manifest.sql). Row presence = loaded.
mark_loaded() upserts, so a rerun updates the existing row instead of
duplicating it -- the control-plane mirror of load_raw's DELETE+COPY.

Takes a SQLAlchemy engine, like load_raw.load_month, so it runs without Airflow.
"""

from sqlalchemy import text

_SELECT = text(
    "SELECT 1 FROM control.manifest WHERE taxi_type = :taxi_type AND source_month = :source_month"
)

_UPSERT = text("""
    INSERT INTO control.manifest (taxi_type, source_month, s3_key, row_count)
    VALUES (:taxi_type, :source_month, :s3_key, :row_count)
    ON CONFLICT (taxi_type, source_month) DO UPDATE SET
        s3_key    = EXCLUDED.s3_key,
        row_count = EXCLUDED.row_count,
        loaded_at = now()
""")


def has_been_loaded(source_month: str, *, engine, taxi_type: str = "yellow") -> bool:
    """Return True if (taxi_type, source_month) is recorded in control.manifest.

    Args:
        source_month: partition tag, ``"YYYY-MM"``.
        engine: SQLAlchemy engine for the warehouse.
        taxi_type: TLC taxi type.
    """
    with engine.connect() as conn:
        row = conn.execute(
            _SELECT, {"taxi_type": taxi_type, "source_month": source_month}
        ).first()
    return row is not None


def mark_loaded(
        source_month: str,
        row_count: int,
        s3_key: str,
        *,
        engine,
        taxi_type: str = "yellow",
) -> None:
    """Upsert (taxi_type, source_month) into control.manifest.

    Args:
        source_month: partition tag, ``"YYYY-MM"``.
        row_count: rows loaded into raw.trips for this partition.
        s3_key: S3 key of the Parquet object loaded for this partition.
        engine: SQLAlchemy engine for the warehouse.
        taxi_type: TLC taxi type.
    """
    with engine.begin() as conn:
        conn.execute(
            _UPSERT,
            {
                "taxi_type": taxi_type,
                "source_month": source_month,
                "s3_key": s3_key,
                "row_count": row_count,
            },
        )
