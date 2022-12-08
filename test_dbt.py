from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import datetime


with DAG(
    dag_id='update_staging',
    schedule_interval='0 9 * * *', # UTC time
    start_date=datetime(year=2022, month=12, day=8),
    catchup=False,
) as dag:

    activate = BashOperator(
        task_id='activate',
        bash_command='source ~/dbt/venv/bin/activate; cd ~/dbt/indica;'
    )

    # dbt_run = BashOperator(
    #     task_id='dbt_run',
    #     bash_command='dbt run'
    # )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='dbt test'
    )

    deactivate = BashOperator(
        task_id='activate_venv',
        bash_command='deactivate'
    )

    activate >> dbt_test >> deactivate