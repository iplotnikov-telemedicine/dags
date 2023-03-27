import sys
import os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
# from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow_dbt_python.operators.dbt import DbtRunOperator, DbtTestOperator, DbtSnapshotOperator
import logging
import json
from airflow.decorators import task, task_group
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from python.to_stage import stg_load, get_customers
from airflow.utils.task_group import TaskGroup


# Get connection to Redshift DB
redshift_hook = RedshiftSQLHook(
    redshift_conn_id='redshift_default',
    schema='dev'
)
redshift_conn = redshift_hook.get_conn()
cursor = redshift_conn.cursor()

# set up environment
schema = 'staging'


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


@task
def upsert_warehouse_order_items(schema, table, date_column, **kwargs):
    ti, task_id = kwargs['ti'], kwargs['task'].task_id

    # check if table not exists
    query = f'''
        select 1
        from information_schema.tables
        where table_schema = '{schema}' and table_name = '{table}'
        '''
    cursor.execute(query)
    table_exists = cursor.fetchone()
    logging.info(f'Table exists value: {table_exists}')
    if table_exists is None:
        # creating blank table from schema 'ext_indica_c9928_company' because of this company has not blank tables
        comp_id = 9928 #customers[0][0]
        ext_schema = 'ext_indica_c9928_company' #customers[0][1]
        query = f'''
            create table {schema}.{table} as
            select {comp_id} as comp_id, id, order_id, product_id, "name", descr, price_type, price_per, 
                charge_by, price, qty, qty_free, amount, tax, discount_value, discount_type_bak, total_amount, 
                created_at, updated_at, is_charge_by_order, is_free, free_discount, income, discount_amount, 
                item_type, count, special_id, special_item_id, is_half_eighth, is_returned, returned_amount, 
                discount_type, free_amount, paid_amount, wcii_cart_item, sync_created_at, sync_updated_at, 
                product_checkin_id, is_excise, returned_at, is_marijuana_product, product_is_tax_exempt, 
                is_metrc, is_under_package_control, base_amount, discount_id, delivery_tax, discount_count, 
                is_exchanged, exchanged_at, product_brutto_weight, product_brutto_weight_validation, current_timestamp as confirmed_at, current_timestamp as inserted_at
            from {ext_schema}.{table}
            where false
            '''
        cursor.execute(query)
        redshift_conn.commit()
        logging.info(f'Table {schema}.{table} created successfully')

    # getting customers list for task run
    comp_id_list = kwargs['dag_run'].conf.get('comp_id_list')
    logging.info(f'comp_id_list is: {comp_id_list}')
    if not comp_id_list:
        customers_dict = Variable.get(task_id, dict(), deserialize_json=True)
        logging.info(f'customers_dict from Variable is: {customers_dict}')
        if not customers_dict:
            if table_exists is None:
                logging.info(f'Table {schema}.{table} does not exist before, initiate full load')
                customers_dict = get_customers(table, comp_id_list, is_full_load=True)
            else:
                customers_dict = get_customers(table, comp_id_list)
    else:
        customers_dict = get_customers(table, comp_id_list)
    logging.info(f'Start loading for customers_dict: {customers_dict}')

    for comp_id in list(customers_dict.keys()):
        ext_schema = customers_dict[comp_id]
        logging.info(f'Task is starting for company {comp_id}')
        # creating temp table with new data increment
        query = f'''
            CREATE temporary TABLE {table}_{comp_id}_temp as
            SELECT {table}.*, warehouse_orders.confirmed_at
            FROM {ext_schema}.{table}
            INNER JOIN {ext_schema}.warehouse_orders
            ON {table}.order_id = warehouse_orders.id
            WHERE warehouse_orders.{date_column} > (
                SELECT coalesce(max({date_column}), '1970-01-01 00:00:00'::timestamp)
                FROM {schema}.warehouse_order_items
                WHERE comp_id = {comp_id}
            ) and warehouse_orders.{date_column} < CURRENT_DATE + interval '8 hours'
                and warehouse_orders.{date_column} IS NOT NULL
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
        logging.info(f'{cursor.rowcount} rows deleted for {comp_id} at {datetime.now()}')
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
        logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {datetime.now()}')
        # deleting temp table
        query = f'''
            DROP TABLE {table}_{comp_id}_temp
        '''
        cursor.execute(query)
        logging.info(f'Temp table is dropped')
        # commit to target DB
        redshift_conn.commit()
        logging.info(f'Task is finished for company {comp_id}')
    #     Variable.set(task_id, comp_id)
    # Variable.set(task_id, 0)
        del customers_dict[comp_id]
        logging.info(f'Number of companies left: {len(customers_dict)}')
        Variable.set(task_id, json.dumps(customers_dict))
    Variable.delete(task_id)



def get_tasks():
    return [
        {'task_id': 'upsert_brands', 'op_args': ['brands']},
        {'task_id': 'upsert_company_config', 'op_args': ['company_config']},
        {'task_id': 'upsert_discounts', 'op_args': ['discounts']},
        {'task_id': 'upsert_patient_group_ref', 'op_args': ['patient_group_ref']},
        {'task_id': 'upsert_patient_group', 'op_args': ['patient_group']},
        {'task_id': 'upsert_patients', 'op_args': ['patients']},
        {'task_id': 'upsert_product_categories', 'op_args': ['product_categories']},
        {'task_id': 'upsert_product_checkins', 'op_args': ['product_checkins']},
        {'task_id': 'upsert_product_filter_index', 'op_args': ['product_filter_index']},
        {'task_id': 'upsert_product_office_qty', 'op_args': ['product_office_qty']},
        {'task_id': 'upsert_product_transactions', 'op_args': ['product_transactions']},
        {'task_id': 'upsert_product_vendors', 'op_args': ['product_vendors']},
        {'task_id': 'upsert_products', 'op_args': ['products']},
        {'task_id': 'upsert_register_log', 'op_args': ['register_log']},
        {'task_id': 'upsert_register', 'op_args': ['register']},
        {'task_id': 'upsert_service_history', 'op_args': ['service_history']},
        {'task_id': 'upsert_sf_guard_group', 'op_args': ['sf_guard_group']},
        {'task_id': 'upsert_sf_guard_user_group', 'op_args': ['sf_guard_user_group']},
        {'task_id': 'upsert_sf_guard_user', 'op_args': ['sf_guard_user']},
        {'task_id': 'upsert_sf_guard_user_permission', 'op_args': ['sf_guard_user_permission']},
        {'task_id': 'upsert_tax_payment', 'op_args': ['tax_payment']},
        {'task_id': 'upsert_user_activity_record', 'op_args': ['user_activity_record']},
        {'task_id': 'upsert_warehouse_order_logs', 'op_args': ['warehouse_order_logs']},
        {'task_id': 'upsert_warehouse_orders', 'op_args': ['warehouse_orders']}
    ]


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'on_failure_callback': failure_slack_alert,
    'on_retry_callback': retry_slack_alert,
    'retries': 10,
    'retry_delay': timedelta(seconds=60)
}


with DAG(
    dag_id='update_everything_new_dev',
    max_active_tasks=32,
    schedule='0 8 * * *', # UTC time
    start_date=datetime(year=2022, month=12, day=8),
    default_args=default_args,
    catchup=False,
) as dag:
    if schema == 'staging':
        start_alert = EmptyOperator(task_id="start_alert", on_success_callback=start_slack_alert)

        with TaskGroup('upsert_tables') as upsert_tables_group:
            for task_params in get_tasks():
                task_id = task_params['task_id']
                op_args = task_params['op_args'] + [schema]
                task = PythonOperator(
                    task_id=task_id,
                    python_callable=stg_load,
                    op_args=op_args,
                )
            upsert_warehouse_order_items(schema=schema, table='warehouse_order_items', date_column='confirmed_at')

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

        start_alert >> upsert_tables_group >> dbt_snapshot >> dbt_run >> dbt_test >> success_alert

    else:
        with TaskGroup('upsert_tables') as upsert_tables_group:
            for task_params in get_tasks():
                task_id = task_params['task_id']
                op_args = task_params['op_args'] + [schema]
                task = PythonOperator(
                    task_id=task_id,
                    python_callable=stg_load,
                    op_args=op_args,
                )
            upsert_warehouse_order_items(schema=schema, table='warehouse_order_items', date_column='updated_at')
        
        upsert_tables_group
