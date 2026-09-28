from datetime import timedelta

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerRunOperator
from cosmos import DbtTaskGroup
from pendulum import datetime

from include.utils.cosmos_config import (
    execution_config,
    profile_config,
    project_config,
    render_config,
)
from include.utils.slack_alerts import slack_failure_alert

default_args = {
    "owner": "Hannan",
    "aws_conn_id": "aws_default",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": slack_failure_alert
}

@dag(
    dag_id="01_daily_fx_to_s3_practice",
    default_args=default_args,
    schedule='@daily',
    start_date=datetime(2026, 9, 9),
    catchup=False
)
def pipeline_to_s3():
    @task(task_id="daily_forex")
    def run_forex(ds=None):
        from include.Data_ingestion.practice_forex import daily_forex
        daily_forex(ds=ds)

    @task(task_id="daily_database")
    def run_db(ds=None):
        from include.Data_ingestion.practice_db import daily_database
        daily_database(ds=ds)

    crawler_fx = GlueCrawlerRunOperator(
        task_id="run_forex_crawler",
        crawler_name="crawler-fx-off",
        wait_for_completion=True
    )
    
    crawler_acc = GlueCrawlerRunOperator(
        task_id="run_acc_crawler",
        crawler_name="crawler-acc-off",
        wait_for_completion=True
    )
    
    crawler_txn = GlueCrawlerRunOperator(
        task_id="run_txn_crawler",
        crawler_name="crawler-db-txn",
        wait_for_completion=True
    )

    dbt_build = DbtTaskGroup(
        group_id="dbt_build_all",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
        operator_args={"install_deps": True},
    )

    task_fx = run_forex()
    task_db = run_db()

    task_fx >> crawler_fx
    task_db >> [crawler_acc, crawler_txn]

    [crawler_acc, crawler_txn, crawler_fx] >> dbt_build

pipeline_to_s3()