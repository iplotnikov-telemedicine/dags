from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
# from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow_dbt_python.operators.dbt import DbtRunOperator, DbtTestOperator
import logging
from airflow.decorators import task
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.hooks.base import BaseHook


def fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection('slack').password
    slack_msg = f"""
        :red_circle: Failed
        *Task*: {context.get('task_instance').task_id}
        *Dag*: {context.get('task_instance').dag_id}
        *Execution Time*: {context.get('ds')}
        *Log Url*: {context.get('task_instance').log_url}
    """
    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow')
    return failed_alert.execute(context=context)


def retry_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection('slack').password
    slack_msg = f"""
        :large_yellow_circle: Retry
        *Task*: {context.get('task_instance').task_id}
        *Dag*: {context.get('task_instance').dag_id}
        *Execution Time*: {context.get('ds')}
        *Log Url*: {context.get('task_instance').log_url}
    """
    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow')
    return failed_alert.execute(context=context)


@task
def get_customers():
    redshift_hook = RedshiftSQLHook(
        postgres_conn_id='redshift_default',
        schema='dev'
    )
    redshift_conn = redshift_hook.get_conn()
    cursor = redshift_conn.cursor()
    query = '''
        SELECT bullshit
        FROM test.int_customers
    '''
    cursor.execute(query)
    logging.info(query)
    data = cursor.fetchall()
    logging.info(f'The number of companies is being processing: {len(data)}')
    return data


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'on_failure_callback': fail_slack_alert,
    'on_retry_callback': retry_slack_alert,
    'retries': 10,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='test_dag_that_fails',
    schedule_interval='0 8 * * *', # UTC time
    start_date=datetime(year=2023, month=1, day=25),
    default_args=default_args,
    catchup=False,
) as dag:
    customers = get_customers()