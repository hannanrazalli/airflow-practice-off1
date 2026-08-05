import boto3
import os
import awswrangler as wr
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def database_ingest(engine, session, s3_bucket, date_str: str = None):
    if not date_str:
        date_str = datetime.now().strftime('%Y-%m-%d')

    TABLES = {
        "accounts" : "updated_at",
        "transactions" : "transaction_date"
    }

    for table, date_col in TABLES.items():
        query = f"SELECT * FROM {table}"
        chunk_size = 100_000
        df_stream = pd.read_sql_query(query, con=engine, chunksize=chunk_size)

        dt = datetime.strptime(date_str, '%Y-%m-%d')
        year, month, day = dt.strftime('%Y'), dt.strftime('%m'), dt.strftime('%d')

        s3_bucket = s3_bucket
        s3_key = f"s3://{s3_bucket}/raw/{table}/year={year}/month={month}/day={day}"

        for i, df_chunk in enumerate(df_stream):
            chunk_key = f"{s3_key}/part_{i}.parquet"

            wr.s3.to_parquet(
                df=df_chunk,
                path=chunk_key,
                boto3_session=session
            )

            print(f"[SUCCESS] Date: {table} part_{i}")

def run_database_ingest(**context):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    s3_hook = S3Hook(aws_conn_id='aws_default')

    engine = pg_hook.get_sqlalchemy_engine()
    session = s3_hook.get_session()

    database_ingest(
        engine=engine,
        session=session,
        s3_bucket=os.getenv("BUCKET_NAME"),
        date_str=context['data_interval_start'].strftime('%Y-%m-%d')
    )