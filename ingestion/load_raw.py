"""Load: read raw TLC Parquet back from S3 and bulk-load it into the Postgres
raw schema, partitioned by source_month.

Normalized-on-load only: column names are snake_cased, integer code columns are
cast to int (TLC stores some as double), and a source_month column is appended.
No filtering, dedup, or business logic.
"""

import io

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from psycopg2 import sql

_COLUMN_RENAMES = {
    "VendorID": "vendor_id",
    "RatecodeID": "ratecode_id",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "Airport_fee": "airport_fee",
}

# smallint in raw.trips. Some months ship these as double, and COPY rejects "1.0"
# for smallint. A safe cast raises on fractional or out-of-range values instead of
# silently truncating them.
_INT_COLUMNS = (
    "vendor_id",
    "passenger_count",
    "ratecode_id",
    "pu_location_id",
    "do_location_id",
    "payment_type",
)

_BATCH_ROWS = 500_000


def _normalize_batch(batch: pa.RecordBatch, source_month: str) -> pa.Table:
    """snake_case names, cast integer columns, append source_month."""
    table = pa.Table.from_batches([batch])
    table = table.rename_columns([_COLUMN_RENAMES.get(c, c.lower()) for c in table.column_names])
    for name in _INT_COLUMNS:
        if name in table.column_names:
            idx = table.column_names.index(name)
            table = table.set_column(idx, name, pc.cast(table[name], pa.int16()))
    return table.append_column("source_month", pa.array([source_month] * table.num_rows, pa.string()))


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

    Rows are COPYed in batches inside a single transaction, so a failure part way
    through rolls back the DELETE as well.

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
    parquet = pq.ParquetFile(io.BytesIO(obj["Body"].read()))

    qualified = sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table))
    delete_stmt = sql.SQL("DELETE FROM {} WHERE source_month = %s").format(qualified)

    row_count = 0
    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur:
            cur.execute(delete_stmt, (source_month,))
            for batch in parquet.iter_batches(batch_size=_BATCH_ROWS):
                batch_table = _normalize_batch(batch, source_month)
                copy_stmt = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT CSV)").format(
                    qualified,
                    sql.SQL(", ").join(sql.Identifier(c) for c in batch_table.column_names),
                )
                buf = io.BytesIO()
                pacsv.write_csv(batch_table, buf, pacsv.WriteOptions(include_header=False))
                buf.seek(0)
                cur.copy_expert(copy_stmt.as_string(cur), buf)
                row_count += batch_table.num_rows
        raw_conn.commit()
    except Exception:
        raw_conn.rollback()
        raise
    finally:
        raw_conn.close()

    return row_count
