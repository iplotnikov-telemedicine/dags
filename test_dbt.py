from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import datetime


with DAG(
    dag_id='test_dbt',
    schedule_interval='0 9 * * *', # UTC time
    start_date=datetime(year=2022, month=12, day=8),
    catchup=False,
) as dag:

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='source /home/ubuntu/dbt/venv/bin/activate; cd /home/ubuntu/dbt/indica; dbt test; deactivate;'
    )
