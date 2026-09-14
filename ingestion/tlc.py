"""Addressing for the TLC data source and S3 raw zone."""

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
_FILENAME = "{taxi_type}_tripdata_{year}-{month}.parquet"

def tlc_source_url(year: str, month: str, taxi_type: str) -> str:
    return f"{TLC_BASE_URL}/{_FILENAME.format(year=year, month=month, taxi_type=taxi_type)}"

def tlc_file_available(year: str, month: str, taxi_type: str) -> bool:
    """HEAD the TLC URL; return True if the month is published."""
    import requests

    url = tlc_source_url(year, month, taxi_type=taxi_type)
    resp = requests.head(url, timeout=30, allow_redirects=True)
    return resp.status_code == 200

def s3_raw_key(year: str, month: str, taxi_type: str) -> str:
    return f"raw/{taxi_type}/year={year}/month={month}/{_FILENAME.format(year=year, month=month, taxi_type=taxi_type)}"