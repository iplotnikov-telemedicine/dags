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




SLACK_CONN_ID = 'slack'


def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: Task Failed.
            <@C04JTDR8Z6H>  
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *gsutil URI*: {gsutil_URI} 
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('execution_date'),
            gsutil_URI="gs://swift-airflow-gs/airflow/logs/{0}/{1}/{2}/{3}.log".format(
                context.get('task_instance').dag_id, 
                context.get('task_instance').task_id, 
                context.get('task_instance').execution_date,
                context.get('task_instance').prev_attempted_tries)
            )
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
    'on_failure_callback': task_fail_slack_alert,
    'on_retry_callback': task_fail_slack_alert,
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