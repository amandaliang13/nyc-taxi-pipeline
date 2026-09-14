"""Extract: pull TLC Parquet from source and land it in S3 raw/ unmodified."""

from pathlib import Path

import boto3
import requests

from ingestion.tlc import s3_raw_key, tlc_source_url


def stream_tlc_to_s3(
        year: str,
        month: str,
        bucket: str,
        *,
        taxi_type: str = "yellow",
        s3_client=None,
) -> str:
    """Stream one month TLC Parquet file straight to S3 raw zone; return the S3 key."""

    url = tlc_source_url(year=year, month=month, taxi_type=taxi_type)
    s3_key = s3_raw_key(year=year, month=month, taxi_type=taxi_type)
    s3 = s3_client or boto3.client("s3")

    with requests.get(url, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        s3.upload_fileobj(resp.raw, bucket, s3_key)

    return s3_key


def stream_tlc_to_local(
        year: str,
        month: str,
        taxi_type: str = "yellow"
) -> Path:
    """Download the TLCParquet for a given month to the local data dir; return the local path."""

    url = tlc_source_url(year=year, month=month, taxi_type=taxi_type)

    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    dest_dir = PROJECT_ROOT / "data"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / s3_raw_key(year=year, month=month, taxi_type=taxi_type).split("/")[-1]

    with requests.get(url, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)

    return dest_path
