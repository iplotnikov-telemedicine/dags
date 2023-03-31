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


# set up environment - 'staging' - production, 'mock' - development
schema = 'mock'


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
    return [
        # {'task_id': 'upsert_brands', 'op_args': ['brands']},
        # {'task_id': 'upsert_company_config', 'op_args': ['company_config']},
        # {'task_id': 'upsert_discounts', 'op_args': ['discounts']},
        # {'task_id': 'upsert_patient_group_ref', 'op_args': ['patient_group_ref']},
        # {'task_id': 'upsert_patient_group', 'op_args': ['patient_group']},
        {'task_id': 'upsert_patients', 'op_args': ['patients']},
        # {'task_id': 'upsert_product_categories', 'op_args': ['product_categories']},
        {'task_id': 'upsert_product_checkins', 'op_args': ['product_checkins']},
        # {'task_id': 'upsert_product_filter_index', 'op_args': ['product_filter_index']},
        # {'task_id': 'upsert_product_office_qty', 'op_args': ['product_office_qty']},
        # {'task_id': 'upsert_product_transactions', 'op_args': ['product_transactions']},
        # {'task_id': 'upsert_product_vendors', 'op_args': ['product_vendors']},
        {'task_id': 'upsert_products', 'op_args': ['products']},
        # {'task_id': 'upsert_register_log', 'op_args': ['register_log']},
        {'task_id': 'upsert_register', 'op_args': ['register']},
        # {'task_id': 'upsert_service_history', 'op_args': ['service_history']},
        # {'task_id': 'upsert_sf_guard_group', 'op_args': ['sf_guard_group']},
        # {'task_id': 'upsert_sf_guard_user_group', 'op_args': ['sf_guard_user_group']},
        # {'task_id': 'upsert_sf_guard_user', 'op_args': ['sf_guard_user']},
        # {'task_id': 'upsert_sf_guard_user_permission', 'op_args': ['sf_guard_user_permission']},
        # {'task_id': 'upsert_tax_payment', 'op_args': ['tax_payment']},
        # {'task_id': 'upsert_user_activity_record', 'op_args': ['user_activity_record']},
        # {'task_id': 'upsert_warehouse_order_logs', 'op_args': ['warehouse_order_logs']},
        # {'task_id': 'upsert_warehouse_orders', 'op_args': ['warehouse_orders']}
    ]


@task_group
def upsert_tables_mapping():
    for task_params in get_tasks():
        task_id_name, job_name = task_params['task_id'], ', '.join(list(map(str, task_params['op_args'])))

        @task(task_id='get_customers_' + task_id_name)
        def get_customers_data(**kwargs):
            comp_id_list = kwargs['dag_run'].conf.get('comp_id_list')
            logging.info(f'comp_id_list: {comp_id_list}')
            return get_customers(table=job_name, comp_id_list=comp_id_list)

        @task(task_id = 'check_table_' + task_id_name)
        def check_table_task(job_name, schema):
            return check_table(job_name=job_name, schema=schema)
        check_table_task = check_table_task(job_name=job_name, schema=schema)


        @task(task_id=task_id_name, max_active_tis_per_dag=1)
        def upsert_task(customers_data, job_name, schema=schema):
            stg_load(customer_data=customers_data, job_name=job_name, schema=schema)
        customer_data=get_customers_data()
        upsert_task = upsert_task.partial(job_name=job_name).expand(customers_data=customer_data)

        check_table_task >> upsert_task


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'on_failure_callback': failure_slack_alert,
    'on_retry_callback': retry_slack_alert,
    'retries': 10,
    'retry_delay': pendulum.duration(seconds=60)
}


with DAG(
    dag_id='update_everything_mapping_test',
    max_active_tasks=32,
    schedule=None, #'0 8 * * *', # UTC time
    start_date=pendulum.datetime(2023, 3, 24),
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
        )
        success_alert = EmptyOperator(task_id="success_alert", on_success_callback=success_slack_alert)

        start_alert >> upsert_tables_group_mapping >> dbt_snapshot >> dbt_run >> dbt_test >> success_alert

    else:
        upsert_tables_group_mapping = upsert_tables_mapping()
        
        upsert_tables_group_mapping
        