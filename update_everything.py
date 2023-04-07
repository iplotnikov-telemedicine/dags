import sys
import os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
import pendulum
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow_dbt_python.operators.dbt import DbtRunOperator, DbtTestOperator, DbtSnapshotOperator
# from airflow.decorators import task, task_group
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
# from airflow.models import Variable
from python.to_stage import stg_load
from python.core.configs import get_all_job_names
from python.core.connections import redshift_conn_dev
from airflow.utils.task_group import TaskGroup


# set up environment: 'staging' - production, 'mock' - development
schema = 'staging'


# Get connection to Redshift DB
redshift_conn = redshift_conn_dev()
cursor = redshift_conn.cursor()


def start_slack_alert(context, **kwargs):
    comp_id_list = kwargs['dag_run'].conf.get('comp_id_list', '')
    reason = f'*Custom run config*: comp_id_list: {comp_id_list}' if comp_id_list != '' else ''
    slack_webhook_token = BaseHook.get_connection('slack').password
    slack_msg = f"""
        :rocket: Start
        *Dag*: {context.get('task_instance').dag_id}
        *Run ID*: {context.get('task_instance').run_id}
        *Execution Time*: {context.get('ts')}
        *Log Url*: {context.get('task_instance').log_url}
        {reason}
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
    # for job in get_all_job_names():
    #     tasks_list.append({'task_id': 'upsert_' + job, 'job_name': [job]})
    tasks_list = [
        {'task_id': 'upsert_' + 'recommendations', 'job_name': ['recommendations']},
        {'task_id': 'upsert_' + 'refund_products', 'job_name': ['refund_products']},
        {'task_id': 'upsert_' + 'tax_excise', 'job_name': ['tax_excise']}
        ]
    return tasks_list


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'on_failure_callback': failure_slack_alert,
    # 'on_retry_callback': retry_slack_alert,
    'retries': 5,
    'retry_delay': pendulum.duration(seconds=60)
}


with DAG(
    dag_id='update_everything',
    max_active_tasks=32,
    schedule='0 8 * * *', # UTC time
    start_date=pendulum.datetime(2023, 4, 7),
    default_args=default_args,
    catchup=False,
    tags=[schema]
) as dag:
    if schema == 'staging':
        start_alert = EmptyOperator(task_id="start_alert", on_success_callback=start_slack_alert)

        with TaskGroup('upsert_tables') as upsert_tables_group:
            for task_params in get_tasks():
                task_id = task_params['task_id']
                op_args = task_params['job_name'] + [schema]
                task = PythonOperator(
                    task_id=task_id,
                    python_callable=stg_load,
                    op_args=op_args,
                )

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

        start_alert >> upsert_tables_group >> dbt_snapshot >> dbt_run >> dbt_test >> success_alert

    else:
        with TaskGroup('upsert_tables') as upsert_tables_group:
            for task_params in get_tasks():
                task_id = task_params['task_id']
                op_args = task_params['job_name'] + [schema]
                task = PythonOperator(
                    task_id=task_id,
                    python_callable=stg_load,
                    op_args=op_args,
                )
        
        upsert_tables_group
