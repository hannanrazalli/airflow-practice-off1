import logging
import os
from datetime import datetime, timezone

import awswrangler as wr
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def daily_database(ds: str | None = None):
    if not ds:
        ds = datetime.now(tz=timezone.utc).strftime('%Y-%m-%d')

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    s3_hook = S3Hook(aws_conn_id='aws_default')

    engine = pg_hook.get_sqlalchemy_engine()
    session = s3_hook.get_session()

    TABLES = {
        "accounts" : "updated_at",
        "transactions" : "transaction_date"
    }

    for table in TABLES:
        query = f"SELECT * FROM {table}"
        chunk_size = 100_000
        df_stream = pd.read_sql_query(query, con=engine, chunksize=chunk_size)

        dt = datetime.strptime(ds, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        year, month, day = dt.strftime('%Y'), dt.strftime('%m'), dt.strftime('%d')

        s3_bucket = os.getenv("BUCKET_NAME")
        s3_key = f"s3://{s3_bucket}/raw/database/{table}/year={year}/month={month}/day={day}"
        wr.engine.set('python')

        for i, df_chunk in enumerate(df_stream):
            chunk_key = f"{s3_key}/part_{i}_{dt.strftime('%Y%m%d')}.parquet"

            wr.s3.to_parquet(
                df=df_chunk,
                path=chunk_key,
                boto3_session=session
            )

            logger.info(f"[SUCCESS] Successfully ingest {table} part: {i}")