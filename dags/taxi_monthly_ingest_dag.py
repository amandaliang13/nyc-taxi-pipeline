"""
DAG 1 - taxi_monthly_ingest

Raw ingest of one TLC yellow-taxi month per run:

    resolve_month -> check_source_available -> extract_to_s3 -> load_raw_postgres -> trigger_dbt

Design notes:

- Granularity: one month per run, matching TLC's own publish cadence. This DAG
  is also the backfill target - scripts/backfill.sh triggers it once per month
  over a range, NOT Airflow catchup (which stays off).

- Parameter `download_month` ("yyyyMM"), optional:
    * provided  -> ingest exactly that month (manual trigger / backfill).
    * omitted   -> scheduled run derives (data_interval_start - MONTH_OFFSET
      months). TLC publishes ~6-8 weeks late, so a fixed 6-month lag reliably
      lands on a file that already exists.

- Idempotent:
    * S3 key is deterministic          -> a re-put overwrites.
    * raw load is delete-by-source_month + insert in one transaction
                                       -> a re-run never duplicates rows.
"""

from __future__ import annotations

from datetime import timedelta

from ingestion.tlc import tlc_source_url
import pendulum
from airflow.sdk import Param, Variable, dag, task

# --- config ---------------------------------------------------------------------
TAXI_TYPE = "yellow"
MONTH_OFFSET = 6  # scheduled runs target (data_interval_start - 6 months)

DBT_DAG_ID = "taxi_transform_and_test"

AWS_CONN_ID = "AWS_DEFAULT"  # AIRFLOW_CONN_AWS_DEFAULT in compose.yaml
WAREHOUSE_CONN_ID = "WAREHOUSE_POSTGRES"  # AIRFLOW_CONN_WAREHOUSE_POSTGRES in compose.yaml

RAW_SCHEMA = "raw"
RAW_TABLE = "trips"

# yyyyMM, month 01-12
MONTH_PARAM_PATTERN = r"^\d{4}(0[1-9]|1[0-2])$"


@dag(
    dag_id="taxi_monthly_ingest",
    description="Extract one TLC yellow-taxi month -> S3 raw zone -> Postgres raw schema.",
    schedule="@monthly",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # serialize overlapping backfill triggers on the warehouse
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    params={
        "download_month": Param(
            default=None,
            type=["null", "string"],
            pattern=MONTH_PARAM_PATTERN,
            title="Download month (yyyyMM)",
            description=(
                "Explicit TLC month to ingest, e.g. 202301. Leave empty on "
                f"scheduled runs to use (interval start - {MONTH_OFFSET} months)."
            ),
        ),
    },
    tags=["ingest", "raw", "tlc"],
)
def taxi_monthly_ingest():
    @task
    def resolve_month(**context) -> dict[str, str]:
        """Resolve the effective month once, for every downstream task.

        Returns {"source_month": "YYYY-MM", "year": "YYYY", "month": "MM"}.
        Uses params.download_month when set, else data_interval_start minus
        MONTH_OFFSET months.
        """
        raw = (context["params"] or {}).get("download_month")
        if raw:
            dt = pendulum.from_format(raw, "YYYYMM")
        else:
            dt = context["data_interval_start"].subtract(months=MONTH_OFFSET)
        return {
            "source_month": dt.format("YYYY-MM"),
            "year": dt.format("YYYY"),
            "month": dt.format("MM"),
        }

    @task(retries=4, retry_delay=timedelta(hours=6))
    def check_source_available(month: dict[str, str]):
        """HEAD the TLC URL; raise (-> retry) until the month is published.

        Long retry_delay so a scheduled run that fires a little early self-heals
        without a noisy failure.
        """
        import logging
        from ingestion.tlc import tlc_file_available

        is_available = tlc_file_available(month["year"], month["month"], taxi_type=TAXI_TYPE)
        if not is_available:
            raise RuntimeError("TLC file not available yet")
        logging.info("TLC file is available: %s", 
                     tlc_source_url(month["year"], month["month"], taxi_type=TAXI_TYPE))

    @task
    def extract_to_s3(month: dict[str, str]) -> str:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        from ingestion.extract import stream_tlc_to_s3

        bucket = Variable.get("S3_BUCKET")
        s3 = S3Hook(aws_conn_id=AWS_CONN_ID).get_conn()
        s3_key = stream_tlc_to_s3(
            year=month["year"],
            month=month["month"],
            bucket=bucket,
            taxi_type=TAXI_TYPE,
            s3_client=s3,
        )
        return s3_key


    @task
    def load_raw_postgres(month: dict[str, str], s3_key: str) -> dict[str, object]:
        """Load one month into raw.trips, then record it in the manifest.

        - delete-by-source_month + COPY insert, single transaction (idempotent).
        - normalized-on-load: snake_case columns, nullable superset schema,
          minimal coercion. No filtering / dedup / business logic (that's dbt).
        - manifest upsert drives backfill visibility and "which months need dbt".

        Returns {"source_month": ..., "row_count": ...}.
        """
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from ingestion.load_raw import load_month
        from ingestion.manifest import mark_loaded

        engine = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID).get_sqlalchemy_engine()
        row_count = load_month(
            s3_key=s3_key,
            source_month=month["source_month"],
            engine=engine,
            bucket=Variable.get("S3_BUCKET"),
            s3_client=S3Hook(aws_conn_id=AWS_CONN_ID).get_conn(),
            schema=RAW_SCHEMA,
            table=RAW_TABLE,
        )
        mark_loaded(
            source_month=month["source_month"],
            row_count=row_count,
            s3_key=s3_key,
            conn_id=WAREHOUSE_CONN_ID,
        )
        return {"source_month": month["source_month"], "row_count": row_count}

    month = resolve_month()
    source_ready = check_source_available(month)
    s3_key = extract_to_s3(month)
    loaded = load_raw_postgres(month, s3_key)

    source_ready >> s3_key
    loaded  # TODO: Use asset-based DAGs to trigger dbt DAG


taxi_monthly_ingest()
