import json
import logging
import os
import time
from datetime import datetime, timezone

import pandas as pd
import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def _to_ndjson(records: list) -> str:
    ingested_at = datetime.now(timezone.utc).isoformat()

    lines = [
        json.dumps({
            "date": row["date"],
            "base": row["base"],
            "currency": row["quote"],
            "rate": row["rate"],
            "ingested_at": ingested_at,
        })
        for row in records
    ]
    return "\n".join(lines)

def daily_forex(ds: str | None = None):
    if not ds:
        ds = datetime.now(tz=timezone.utc).strftime('%Y-%m-%d')

    url = "https://api.frankfurter.dev/v2/rates"
    params = {"base": "USD", "date": ds}

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    dt = datetime.strptime(ds, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    year, month, day = dt.strftime('%Y'), dt.strftime('%m'), dt.strftime('%d')

    s3_bucket = os.getenv("BUCKET_NAME")
    s3_key = f"raw/forex/year={year}/month={month}/day={day}/forex_{dt.strftime('%Y%m%d')}.ndjson"

    ndjson_body = _to_ndjson(data)

    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_hook.load_string(
        string_data=ndjson_body,
        bucket_name=s3_bucket,
        key=s3_key,
        replace=True
    )

    logger.info(f"[SUCCESS] Ingested {len(data)} rates for {ds}")

def backfill_forex(start_date: str, end_date: str):
    dates = pd.date_range(start=start_date, end=end_date).strftime('%Y-%m-%d')
    for date in dates:
        daily_forex(ds=date)
        time.sleep(1)