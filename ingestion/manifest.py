"""Manifest: record which (taxi_type, source_month) partitions are loaded in raw.trips.

Backed by control.manifest (sql/control/manifest.sql). Row presence = loaded.
mark_loaded() upserts, so a rerun updates the existing row instead of
duplicating it -- the control-plane mirror of load_raw's DELETE+COPY.

Takes a SQLAlchemy connection, like load_raw.load_month, so it runs without Airflow.
Neither function commits: the caller runs both in one transaction (DAG 1's
load_raw_postgres uses engine.begin()), so raw.trips and the manifest change together.
"""

from sqlalchemy import text

# statement_timestamp(), not now(): this runs in the same transaction as load_month's
# DELETE+COPY, and now() would return the time that transaction started. The column
# default (sql/control/manifest.sql) covers the INSERT branch the same way.
_UPSERT = text("""
    INSERT INTO control.manifest (taxi_type, source_month, s3_key, row_count)
    VALUES (:taxi_type, :source_month, :s3_key, :row_count)
    ON CONFLICT (taxi_type, source_month) DO UPDATE SET
        s3_key    = EXCLUDED.s3_key,
        row_count = EXCLUDED.row_count,
        loaded_at = statement_timestamp()
""")


def mark_loaded(
        source_month: str,
        row_count: int,
        s3_key: str,
        *,
        conn,
        taxi_type: str = "yellow",
) -> None:
    """Upsert (taxi_type, source_month) into control.manifest. Does not commit.

    Args:
        source_month: partition tag, ``"YYYY-MM"``.
        row_count: rows loaded into raw.trips for this partition.
        s3_key: S3 key of the Parquet object loaded for this partition.
        conn: SQLAlchemy connection with the load's transaction open.
        taxi_type: TLC taxi type.
    """
    conn.execute(
        _UPSERT,
        {
            "taxi_type": taxi_type,
            "source_month": source_month,
            "s3_key": s3_key,
            "row_count": row_count,
        },
    )
