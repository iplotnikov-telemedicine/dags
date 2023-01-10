from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow_dbt_python.operators.dbt import DbtRunOperator, DbtTestOperator

args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='example_dbt_operator',
    default_args=args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    # dagrun_timeout=timedelta(minutes=60),
    # tags=['example', 'example2'],
) as dag:
    dbt_test = DbtTestOperator(
        task_id="dbt_test",
        project_dir="/home/ubuntu/dbt/indica",
        profiles_dir="/home/ubuntu/.dbt",
    )
    dbt_run = DbtRunOperator(
        task_id="dbt_run",
        project_dir="/home/ubuntu/dbt/indica",
        profiles_dir="/home/ubuntu/.dbt",
        # fail_fast=True,
        # task_id="dbt_run_hourly",
        # project_dir="s3://my-bucket/dbt/project/key/prefix/",
        # profiles_dir="s3://my-bucket/dbt/profiles/key/prefix/",
        # select=["+tag:hourly"],
        # exclude=["tag:deprecated"],
        # target="production",
        # profile="my-project",
        # full_refresh=False,
    )

    dbt_run >> dbt_test