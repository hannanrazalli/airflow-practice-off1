from pendulum import datetime
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerRunOperator

from cosmos import DbtTaskGroup
from include.utils.pratice_cosmos import project_config, profile_config, execution_config, render_config
from include.utils.practice_slack import slack_failure_alert

default_args = {
    "owner" : "Hannan_Razalli",
    "retries" : "1",
    "retry_delay" : timedelta(minutes=1),
    "on_failure_callback": slack_failure_alert
}

@dag(
    dag_id="01_practice_1",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 9, 25),
    catchup=False
)
def practice_pipeline():
    @task(task_id="daily_forex")
    def run_forex(ds=None):
        from include.Data_ingestion.practice_forex import daily_forex
        daily_forex(ds=ds)

    @task(task_id="daily_db")
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

    forex = run_forex()
    database = run_db()

    forex >> crawler_fx
    database >> [crawler_acc, crawler_txn]

    [crawler_fx, crawler_acc, crawler_txn] >> dbt_build

practice_pipeline()