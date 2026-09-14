"""Load: read raw TLC Parquet back from S3 and bulk-load it into the Postgres
raw schema, partitioned by source_month.

Normalized-on-load only: column names are snake_cased and a source_month
column is appended. No filtering, dedup, or business logic.
"""

import io

import boto3
import pandas as pd
from psycopg2 import sql


_COLUMN_RENAMES = {
    "VendorID": "vendor_id",
    "RatecodeID": "ratecode_id",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "Airport_fee": "airport_fee",
}


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """snake_case the TLC column names so every month lands on a stable schema."""
    return df.rename(columns=lambda c: _COLUMN_RENAMES.get(c, c.lower()))


def load_month(
        s3_key: str,
        source_month: str,
        engine,
        *,
        bucket: str,
        schema: str = "raw",
        table: str = "trips",
        s3_client=None,
) -> int:
    """Load one month of raw TLC Parquet from S3 into ``raw.trips``; return the row count.

    Args:
        s3_key: key of the raw Parquet object (e.g. from ``extract.stream_tlc_to_s3``).
        source_month: partition tag, ``"YYYY-MM"``.
        engine: engine for the warehouse.
        bucket: S3 bucket holding ``s3_key``.
        schema, table: destination raw table.
        s3_client: optional boto3 S3 client (defaults to ``boto3.client("s3")``).
    """
    s3 = s3_client or boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=s3_key)
    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))

    df = _normalize_columns(df)
    df["source_month"] = source_month

    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)

    qualified = sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table))
    delete_stmt = sql.SQL("DELETE FROM {} WHERE source_month = %s").format(qualified)
    copy_stmt = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT CSV)").format(
        qualified,
        sql.SQL(", ").join(sql.Identifier(c) for c in df.columns),
    )

    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur:
            cur.execute(delete_stmt, (source_month,))
            cur.copy_expert(copy_stmt.as_string(cur), buf)
        raw_conn.commit()
    except Exception:
        raw_conn.rollback()
        raise
    finally:
        raw_conn.close()

    return len(df)
