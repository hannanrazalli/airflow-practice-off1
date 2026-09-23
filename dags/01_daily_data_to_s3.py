from pendulum import datetime
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator

default_args = {
    "owner" : "Hannan",
    "aws_conn_id" : "aws_default"
}

@dag(
    dag_id="01_daily_fx_to_s3",
    default_args=default_args,
    schedule='@daily',
    start_date=datetime(2026, 9, 9),
    catchup=False
)
def pipeline_to_s3():
    # Task fx & db
    @task(task_id="daily_forex")
    def run_forex(ds=None):
        from include.Data_ingestion.practice_forex import daily_forex
        daily_forex(ds=ds)

    @task(task_id="daily_database")
    def run_db(ds=None):
        from include.Data_ingestion.practice_db import daily_database
        daily_database(ds=ds)

    # Crawlers
    crawler_fx = GlueCrawlerOperator(
        task_id="run_forex_crawler",
        config={"Name" : "api-data-crawler"}
    )

    crawler_acc = GlueCrawlerOperator(
            task_id="run_acc_crawler",
            config={"Name" : "crawler_db_accounts"}
        )

    crawler_txn = GlueCrawlerOperator(
                task_id="run_txn_crawler",
                config={"Name" : "crawler_db_txn"}
            )

    task_fx = run_forex()
    task_db = run_db()

    task_fx >> crawler_fx
    task_db >> [crawler_acc, crawler_txn]

pipeline_to_s3()