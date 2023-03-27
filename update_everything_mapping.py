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
from python.core.connections import redshint_conn_dev
from python.core.configs import get_all_job_names


# set up environment: 'staging' - production, 'mock' - development
schema = 'staging'


# Get connection to Redshift DB
redshift_conn = redshint_conn_dev()
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


def upsert_warehouse_order_items(customer_data, schema, table, date_column):
    (comp_id, ext_schema) = customer_data
    logging.info(f'Task is starting for company {comp_id}')
    # creating temp table with new data increment
    query = f'''
        CREATE temporary TABLE {table}_{comp_id}_temp as
        SELECT {table}.*, warehouse_orders.confirmed_at
        FROM {ext_schema}.{table}
        INNER JOIN {ext_schema}.warehouse_orders
        ON {table}.order_id = warehouse_orders.id
        WHERE {table}.{date_column} > (
            SELECT coalesce(max({date_column}), '1970-01-01 00:00:00'::timestamp)
            FROM {schema}.{table}
            WHERE comp_id = {comp_id}
        ) and {table}.{date_column} < CURRENT_DATE + interval '8 hours'
            and warehouse_orders.confirmed_at IS NOT NULL
    '''
    cursor.execute(query)
    logging.info(f'Temp table is created')
    # deleting from target table data that were updated
    query = f'''
        DELETE FROM {schema}.{table}
        USING {table}_{comp_id}_temp
        WHERE {schema}.{table}.comp_id = {comp_id}
            AND {schema}.{table}.id = {table}_{comp_id}_temp.id
    '''
    cursor.execute(query)
    logging.info(f'{cursor.rowcount} rows deleted for {comp_id} at {pendulum.now()}')
    # inserting increment to target table
    query = f'''
        INSERT INTO {schema}.{table}
        SELECT {comp_id} as comp_id, id, order_id, product_id, "name", descr, price_type, price_per, 
            charge_by, price, qty, qty_free, amount, tax, discount_value, discount_type_bak, total_amount, 
            created_at, updated_at, is_charge_by_order, is_free, free_discount, income, discount_amount, 
            item_type, count, special_id, special_item_id, is_half_eighth, is_returned, returned_amount, 
            discount_type, free_amount, paid_amount, wcii_cart_item, sync_created_at, sync_updated_at, 
            product_checkin_id, is_excise, returned_at, is_marijuana_product, product_is_tax_exempt, 
            is_metrc, is_under_package_control, base_amount, discount_id, delivery_tax, discount_count, 
            is_exchanged, exchanged_at, product_brutto_weight, product_brutto_weight_validation, confirmed_at, current_timestamp as inserted_at
        FROM {table}_{comp_id}_temp
    '''
    cursor.execute(query)
    logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {pendulum.now()}')
    # deleting temp table
    query = f'''
        DROP TABLE {table}_{comp_id}_temp
    '''
    cursor.execute(query)
    logging.info(f'Temp table is dropped')
    # commit to target DB
    redshift_conn.commit()
    logging.info(f'Task is finished for company {comp_id}')


def get_tasks():
    tasks_list = []
    for job in get_all_job_names():
        tasks_list.append({'task_id': 'upsert_' + job, 'job_name': job})
    return tasks_list


@task_group
def upsert_tables_mapping():
    for task_params in get_tasks():
        task_id_name, job_name = task_params['task_id'], task_params['job_name']

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


    @task(task_id = 'check_table_upsert_warehouse_order_items')
    def check_table_task(job_name, schema):
        return check_table(job_name=job_name, schema=schema)
    check_table_task = check_table_task(job_name='warehouse_order_items', schema=schema)


    @task(task_id='get_customers_upsert_warehouse_order_items')
    def get_customers_data(**kwargs):
        comp_id_list = kwargs['dag_run'].conf.get('comp_id_list')
        logging.info(f'comp_id_list: {comp_id_list}')
        return get_customers(table='warehouse_order_items', comp_id_list=comp_id_list)

    @task(task_id='upsert_warehouse_order_items', max_active_tis_per_dag=1)
    def upsert_task(customers_data):
        upsert_warehouse_order_items(customer_data=customers_data, schema=schema, table='warehouse_order_items', date_column='updated_at')
    customer_data=get_customers_data()
    upsert_task = upsert_task.expand(customers_data=customer_data)

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
    dag_id='update_everything_mapping',
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
