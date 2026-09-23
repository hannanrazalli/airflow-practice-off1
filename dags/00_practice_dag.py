import os
import shutil
from pendulum import datetime
from airflow.decorators import dag
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerRunOperator

from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig, DbtTaskGroup
from cosmos.constants import ExecutionMode, InvocationMode, LoadMode
from cosmos.profiles import AthenaAccessKeyProfileMapping

DBT_PROJECT_DIR = "/usr/local/airflow/include/02_dbt/practice_athena"
DBT_EXECUTABLE = "/usr/local/airflow/dbt_venv/bin/dbt"

default_args = {
    "owner": "Hannan",
    "aws_conn_id": "aws_default",
    "retries": 1
}

profile_config = ProfileConfig(
    profile_name="practice_athena",
    target_name="dev",
    profile_mapping=AthenaAccessKeyProfileMapping(
        conn_id="aws_default",
        profile_args={
            "schema": "practice_off_database",
            "database": "awsdatacatalog",
            "s3_staging_dir": "s3://practice1-212105053682-ap-southeast-1-an/athena-results/",
            "region_name": "ap-southeast-1",
        },
    ),
)

execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    dbt_executable_path=DBT_EXECUTABLE,
    invocation_mode=InvocationMode.SUBPROCESS,
)

render_config = RenderConfig(
    load_method=LoadMode.DBT_LS,
    dbt_executable_path=DBT_EXECUTABLE,
    invocation_mode=InvocationMode.SUBPROCESS,
)

project_config = ProjectConfig(DBT_PROJECT_DIR)

@dag(
    dag_id="01_daily_fx_to_s3_practice",
    default_args=default_args,
    schedule='@daily',
    start_date=datetime(2026, 9, 9),
    catchup=False
)
def pipeline_to_s3():
    from airflow.decorators import task

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