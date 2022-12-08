from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta


with DAG(
    dag_id='dbt_dag',
    start_date=datetime(year=2022, month=12, day=8),
    schedule_interval='0 9 * * *', # UTC time
    description='An Airflow DAG to invoke simple dbt commands',
    schedule_interval=timedelta(days=1),
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