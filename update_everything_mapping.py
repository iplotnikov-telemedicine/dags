import sys
import os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
import pendulum
import logging
# import json
from airflow.models import DAG
# from airflow.models import Variable
# from airflow.utils.task_group import TaskGroup
# from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
# from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
# from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow_dbt_python.operators.dbt import DbtRunOperator, DbtTestOperator, DbtSnapshotOperator
from airflow.decorators import task, task_group
from airflow.hooks.base import BaseHook
from python.to_stage_task_mapping import stg_load, get_customers, check_table
from python.core.connections import redshift_conn_dev
from python.core.configs import get_all_job_names


# set up environment: 'staging' - production, 'mock' - development
schema = 'staging'


# Get connection to Redshift DB
redshift_conn = redshift_conn_dev()
cursor = redshift_conn.cursor()


def start_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection('slack').password
    slack_msg = f"""
        :rocket: Start
        *Dag*: {context.get('task_instance').dag_id}
        *Run ID*: {context.get('task_instance').run_id}
        *Execution Time*: {context.get('ts')}
        *Log Url*: {context.get('task_instance').log_url}
    """
    alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow')
    return alert.execute(context=context)


def failure_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection('slack').password
    slack_msg = f"""
        :red_circle: Failure
        *Task*: {context.get('task_instance').task_id}
        *Dag*: {context.get('task_instance').dag_id}
        *Execution Time*: {context.get('ts')}
        *Log Url*: {context.get('task_instance').log_url}
    """
    alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow')
    return alert.execute(context=context)


def retry_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection('slack').password
    slack_msg = f"""
        :large_yellow_circle: Retry
        *Task*: {context.get('task_instance').task_id}
        *Dag*: {context.get('task_instance').dag_id}
        *Execution Time*: {context.get('ts')}
        *Log Url*: {context.get('task_instance').log_url}
    """
    alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow')
    return alert.execute(context=context)


def success_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection('slack').password
    slack_msg = f"""
        :large_green_circle: Success
        *Dag*: {context.get('task_instance').dag_id}
        *Run ID*: {context.get('task_instance').run_id}
        *Execution Time*: {context.get('ts')}
        *Log Url*: {context.get('task_instance').log_url}
    """
    alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow')
    return alert.execute(context=context)


def get_tasks():
    tasks_list = []
    for job in get_all_job_names():
        tasks_list.append({'task_id': 'upsert_' + job, 'job_name': job})
    return tasks_list


@task_group
def upsert_tables_mapping():
    for task_params in get_tasks():
        task_id_name, job_name = task_params['task_id'], task_params['job_name']

        @task(task_id = 'check_table_' + task_id_name)
        def check_table_task(job_name, schema):
            return check_table(job_name=job_name, schema=schema)
        check_table_task = check_table_task(job_name=job_name, schema=schema)


        @task(task_id='get_customers_' + task_id_name)
        def get_customers_data(job_name, **kwargs):
            comp_id_list = kwargs['dag_run'].conf.get('comp_id_list')
            logging.info(f'comp_id_list: {comp_id_list}')
            return get_customers(table=job_name, comp_id_list=comp_id_list)
        get_customers_task = get_customers_data(job_name=job_name)


        @task(task_id=task_id_name, max_active_tis_per_dag=1)
        def upsert_task(customers_data, job_name, schema=schema):
            stg_load(customer_data=customers_data, job_name=job_name, schema=schema)
        upsert_task = upsert_task.partial(job_name=job_name).expand(customers_data=get_customers_task)

        check_table_task >> upsert_task


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'on_failure_callback': failure_slack_alert,
    # 'on_retry_callback': retry_slack_alert,
    'retries': 5,
    'retry_delay': pendulum.duration(seconds=60)
}


with DAG(
    dag_id='update_everything_mapping',
    max_active_tasks=32,
    schedule='0 8 * * *', # UTC time
    start_date=pendulum.datetime(2023, 4, 4),
    default_args=default_args,
    catchup=False,
) as dag:
    if schema == 'staging':
        start_alert = EmptyOperator(task_id="start_alert", on_success_callback=start_slack_alert)

        upsert_tables_group_mapping = upsert_tables_mapping()

        dbt_run = DbtRunOperator(
            task_id="dbt_run",
            project_dir="/home/ubuntu/dbt/indica",
            profiles_dir="/home/ubuntu/.dbt",
            exclude=["config.materialized:view"]
        )
        dbt_snapshot = DbtSnapshotOperator(
            task_id="dbt_snapshot",
            project_dir="/home/ubuntu/dbt/indica",
            profiles_dir="/home/ubuntu/.dbt",
        )
        dbt_test = DbtTestOperator(
            task_id="dbt_test",
            project_dir="/home/ubuntu/dbt/indica",
            profiles_dir="/home/ubuntu/.dbt",
            retries=0
        )
        success_alert = EmptyOperator(task_id="success_alert", on_success_callback=success_slack_alert)

        start_alert >> upsert_tables_group_mapping >> dbt_snapshot >> dbt_run >> dbt_test >> success_alert

    else:
        upsert_tables_group_mapping = upsert_tables_mapping()
        
        upsert_tables_group_mapping
