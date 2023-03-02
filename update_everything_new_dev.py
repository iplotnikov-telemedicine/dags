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
from airflow.decorators import task, task_group
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from python.to_stage import stg_load
from airflow.utils.task_group import TaskGroup


# Get connection to Redshift DB
redshift_hook = RedshiftSQLHook(
    redshift_conn_id='redshift_default',
    schema='dev'
)
redshift_conn = redshift_hook.get_conn()
cursor = redshift_conn.cursor()


# def start_slack_alert(context):
#     slack_webhook_token = BaseHook.get_connection('slack').password
#     slack_msg = f"""
#         :rocket: Start
#         *Dag*: {context.get('task_instance').dag_id}
#         *Run ID*: {context.get('task_instance').run_id}
#         *Execution Time*: {context.get('execution_date')}
#         *Log Url*: {context.get('task_instance').log_url}
#     """
#     alert = SlackWebhookOperator(
#         task_id='slack_test',
#         http_conn_id='slack',
#         webhook_token=slack_webhook_token,
#         message=slack_msg,
#         username='airflow')
#     return alert.execute(context=context)


# def failure_slack_alert(context):
#     slack_webhook_token = BaseHook.get_connection('slack').password
#     slack_msg = f"""
#         :red_circle: Failure
#         *Task*: {context.get('task_instance').task_id}
#         *Dag*: {context.get('task_instance').dag_id}
#         *Execution Time*: {context.get('execution_date')}
#         *Log Url*: {context.get('task_instance').log_url}
#     """
#     alert = SlackWebhookOperator(
#         task_id='slack_test',
#         http_conn_id='slack',
#         webhook_token=slack_webhook_token,
#         message=slack_msg,
#         username='airflow')
#     return alert.execute(context=context)


# def retry_slack_alert(context):
#     slack_webhook_token = BaseHook.get_connection('slack').password
#     slack_msg = f"""
#         :large_yellow_circle: Retry
#         *Task*: {context.get('task_instance').task_id}
#         *Dag*: {context.get('task_instance').dag_id}
#         *Execution Time*: {context.get('execution_date')}
#         *Log Url*: {context.get('task_instance').log_url}
#     """
#     alert = SlackWebhookOperator(
#         task_id='slack_test',
#         http_conn_id='slack',
#         webhook_token=slack_webhook_token,
#         message=slack_msg,
#         username='airflow')
#     return alert.execute(context=context)


# def success_slack_alert(context):
#     slack_webhook_token = BaseHook.get_connection('slack').password
#     slack_msg = f"""
#         :large_green_circle: Success
#         *Dag*: {context.get('task_instance').dag_id}
#         *Run ID*: {context.get('task_instance').run_id}
#         *Execution Time*: {context.get('execution_date')}
#         *Log Url*: {context.get('task_instance').log_url}
#     """
#     alert = SlackWebhookOperator(
#         task_id='slack_test',
#         http_conn_id='slack',
#         webhook_token=slack_webhook_token,
#         message=slack_msg,
#         username='airflow')
#     return alert.execute(context=context)


@task
def get_customers(ti=None, **kwargs):
    comp_id_list = kwargs['dag_run'].conf.get('comp_id_list')
    if comp_id_list:
        condition = f"int_customers.comp_id IN ({', '.join(list(map(str, comp_id_list)))})"
    else:
        condition = "int_customers.potify_sync_entity_updated_at >= current_date - interval '3 day'"
    cursor = redshift_conn.cursor()
    query = f'''
        SELECT int_customers.comp_id, TRIM(svv_external_schemas.schemaname) as schemaname
        FROM test.int_customers
        INNER JOIN svv_external_schemas
        ON int_customers.comp_db_name = svv_external_schemas.databasename
        WHERE {condition}
        ORDER BY comp_id
    '''
    cursor.execute(query)
    logging.info(query)
    data = cursor.fetchall()
    logging.info(f'The number of companies is being processing: {len(data)}')
    ti.xcom_push(key='customers', value=data)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # 'on_failure_callback': failure_slack_alert,
    # 'on_retry_callback': retry_slack_alert,
    'retries': 10,
    'retry_delay': timedelta(seconds=60)
}


with DAG(
    dag_id='update_everything_new_dev',
    max_active_tasks=32,
    schedule=None, # UTC time
    start_date=datetime(year=2022, month=12, day=8),
    default_args=default_args,
    catchup=False,
) as dag:
    # start_alert = EmptyOperator(task_id="start_alert", on_success_callback=start_slack_alert)

    get_customers_task = get_customers()

    with TaskGroup(group_id='upsert_tables_group') as upsert_tables_group:
        brands = PythonOperator(
            task_id="brands",
            python_callable=stg_load,
            op_args=['brands'],
        )
        company_config = PythonOperator(
            task_id="company_config",
            python_callable=stg_load,
            op_args=['company_config'],
        )
        discounts = PythonOperator(
            task_id="discounts",
            python_callable=stg_load,
            op_args=['discounts'],
        )
        patient_group_ref = PythonOperator(
            task_id="patient_group_ref",
            python_callable=stg_load,
            op_args=['patient_group_ref'],
        )
        patient_group = PythonOperator(
            task_id="patient_group",
            python_callable=stg_load,
            op_args=['patient_group'],
        )
        patients = PythonOperator(
            task_id="patients",
            python_callable=stg_load,
            op_args=['patients'],
        )
        product_categories = PythonOperator(
            task_id="product_categories",
            python_callable=stg_load,
            op_args=['product_categories'],
        )
        product_checkins = PythonOperator(
            task_id="product_checkins",
            python_callable=stg_load,
            op_args=['product_checkins'],
        )
        product_filter_index = PythonOperator(
            task_id="product_filter_index",
            python_callable=stg_load,
            op_args=['product_filter_index'],
        )
        product_office_qty = PythonOperator(
            task_id="product_office_qty",
            python_callable=stg_load,
            op_args=['product_office_qty'],
        )
        product_transactions = PythonOperator(
            task_id="product_transactions",
            python_callable=stg_load,
            op_args=['product_transactions'],
        )
        product_vendors = PythonOperator(
            task_id="product_vendors",
            python_callable=stg_load,
            op_args=['product_vendors'],
        )
        products = PythonOperator(
            task_id="products",
            python_callable=stg_load,
            op_args=['products'],
        )
        register_log = PythonOperator(
            task_id="register_log",
            python_callable=stg_load,
            op_args=['register_log'],
        )
        register = PythonOperator(
            task_id="register",
            python_callable=stg_load,
            op_args=['register'],
        )
        service_history = PythonOperator(
            task_id="service_history",
            python_callable=stg_load,
            op_args=['service_history'],
        )
        sf_guard_group = PythonOperator(
            task_id="sf_guard_group",
            python_callable=stg_load,
            op_args=['sf_guard_group'],
        )
        sf_guard_user_group = PythonOperator(
            task_id="sf_guard_user_group",
            python_callable=stg_load,
            op_args=['sf_guard_user_group'],
        )
        sf_guard_user = PythonOperator(
            task_id="sf_guard_user",
            python_callable=stg_load,
            op_args=['sf_guard_user'],
        )
        tax_payment = PythonOperator(
            task_id="tax_payment",
            python_callable=stg_load,
            op_args=['tax_payment'],
        )
        user_activity_record = PythonOperator(
            task_id="user_activity_record",
            python_callable=stg_load,
            op_args=['user_activity_record'],
        )
        warehouse_order_items = PythonOperator(
            task_id="warehouse_order_items",
            python_callable=stg_load,
            op_args=['warehouse_order_items'],
        )
        warehouse_order_logs = PythonOperator(
            task_id="warehouse_order_logs",
            python_callable=stg_load,
            op_args=['warehouse_order_logs'],
        )
        warehouse_orders = PythonOperator(
            task_id="warehouse_orders",
            python_callable=stg_load,
            op_args=['warehouse_orders'],
        )

    # dbt_run = DbtRunOperator(
    #     task_id="dbt_run",
    #     project_dir="/home/ubuntu/dbt/indica",
    #     profiles_dir="/home/ubuntu/.dbt",
    # )
    # dbt_snapshot = DbtSnapshotOperator(
    #     task_id="dbt_snapshot",
    #     project_dir="/home/ubuntu/dbt/indica",
    #     profiles_dir="/home/ubuntu/.dbt",
    # )
    # dbt_test = DbtTestOperator(
    #     task_id="dbt_test",
    #     project_dir="/home/ubuntu/dbt/indica",
    #     profiles_dir="/home/ubuntu/.dbt",
    # )
    # success_alert = EmptyOperator(task_id="success_alert", on_success_callback=success_slack_alert)

# start_alert >> get_customers_task >> upsert_tables_group >> dbt_run >> dbt_snapshot >> dbt_test >> success_alert
get_customers_task >> upsert_tables_group