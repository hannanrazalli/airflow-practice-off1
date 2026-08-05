from pendulum import datetime
from airflow.decorators import dag, task

from include.forex_ingest import daily_forex_ingest
from include.database_ingest import run_database_ingest

@dag(
    dag_id='01_Daily_data_to_S3',
    schedule='@daily',
    start_date=datetime(2026, 8, 1),
    catchup=False
)
def pipeline_to_s3():
    run_forex = task(
        daily_forex_ingest,
        task_id = 'daily_forex_ingest'
    )

    run_database = task(
        run_database_ingest,
        task_id = 'daily_db_ingest'
    )

    run_forex()
    run_database()

pipeline_to_s3()