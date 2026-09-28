from cosmos import ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, InvocationMode, LoadMode
from cosmos.profiles import AthenaAccessKeyProfileMapping

DBT_PROJECT_DIR = "/usr/local/airflow/include/02_dbt/practice_athena"
project_config = ProjectConfig(dbt_project_path=DBT_PROJECT_DIR)

profile_config = ProfileConfig(
    profile_name="practice_athena",
    target_name="dev",
    profile_mapping=AthenaAccessKeyProfileMapping(
        conn_id="aws_default",
        profile_args={
            "database" : "awsdatacatalog",
            "region_name" : "ap-southeast-1",
            "s3_data_dir" : "s3://practice1-212105053682-ap-southeast-1-an/dbt-output/",
            "s3_staging_dir" : "s3://practice1-212105053682-ap-southeast-1-an/athena-results/",
            "schema" : "practice_off_database"
        },
    ),
)

DBT_EXECUTABLE = "/usr/local/airflow/dbt_venv/bin/dbt"
execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    dbt_executable_path=DBT_EXECUTABLE,
    invocation_mode=InvocationMode.SUBPROCESS
)

render_config = RenderConfig(
    load_method=LoadMode.DBT_LS,
    dbt_executable_path=DBT_EXECUTABLE,
    invocation_mode=InvocationMode.SUBPROCESS
)