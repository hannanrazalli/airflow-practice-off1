import time
import json
import os
import boto3
import requests
import logging
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

def daily_forex_ingest(date_str: str = None):
    if not date_str:
        date_str = datetime.now().strftime('%Y-%m-%d')

    url = f"https://api.frankfurter.dev/v2/rates?date={date_str}&base=USD"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    dt = datetime.strptime(date_str, '%Y-%m-%d')
    year, month, day = dt.strftime('%Y'), dt.strftime('%m'), dt.strftime('%d')

    s3_client = boto3.client('s3')
    s3_bucket = os.getenv('BUCKET_NAME')
    s3_key = f"raw/forex/year={year}/month={month}/day={day}/forex_{dt.strftime('%Y%m%d')}.json"

    s3_client.put_object(
        Bucket=s3_bucket,
        Key=s3_key,
        Body=json.dumps(data, indent=4)
    )

    logger.info(f"[SUCCESS] Successfully ingest Forex API date: {date_str}")

def backfill_forex_ingest(start_date: str, end_date: str):
    dates = pd.date_range(start=start_date, end=end_date).strftime('%Y-%m-%d')

    for date in dates:
        daily_forex_ingest(date)

        time.sleep(1)

    logger.info(f"[COMPLETED] Done backfilling for {len(dates)} dates")