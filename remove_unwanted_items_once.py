from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
# from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow_dbt_python.operators.dbt import DbtRunOperator, DbtTestOperator
import logging
from airflow.decorators import task, task_group
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable


# Get connection to Redshift DB
redshift_hook = RedshiftSQLHook(
    redshift_conn_id='redshift_default',
    schema='dev'
)
redshift_conn = redshift_hook.get_conn()
cursor = redshift_conn.cursor()


@task
def get_customers(ti=None):
    cursor = redshift_conn.cursor()
    query = '''
        SELECT int_customers.comp_id, TRIM(svv_external_schemas.schemaname) as schemaname
        FROM test.int_customers
        INNER JOIN svv_external_schemas
        ON int_customers.db_name = svv_external_schemas.databasename
        WHERE int_customers.potify_sync_entity_updated_at >= current_date - interval '3 day'
        ORDER BY 1
    '''
    cursor.execute(query)
    logging.info(query)
    data = cursor.fetchall()
    logging.info(f'The number of companies is being processing: {len(data)}')
    ti.xcom_push(key='customers', value=data)
    

@task
def upsert_warehouse_order_items_new(schema, table, date_column, **kwargs):
    ti, task_id = kwargs['ti'], kwargs['task'].task_id
    customers = ti.xcom_pull(key='customers', task_ids='get_customers')
    # get max_comp_id from target table and filter list of customers
    max_comp_id = int(Variable.get(task_id, 0))
    customers = [c for c in customers if c[0] > max_comp_id]
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
        # create blank table
        comp_id = customers[0][0]
        ext_schema = customers[0][1]
        query = f'''
            CREATE TABLE {schema}.{table} as
            SELECT {comp_id} as comp_id, id, order_id, product_id, "name", descr, price_type, price_per, 
                charge_by, price, qty, qty_free, amount, tax, discount_value, discount_type_bak, total_amount, 
                created_at, updated_at, is_charge_by_order, is_free, free_discount, income, discount_amount, 
                item_type, count, special_id, special_item_id, is_half_eighth, is_returned, returned_amount, 
                discount_type, free_amount, paid_amount, wcii_cart_item, sync_created_at, sync_updated_at, 
                product_checkin_id, is_excise, returned_at, is_marijuana_product, product_is_tax_exempt, 
                is_metrc, is_under_package_control, base_amount, discount_id, delivery_tax, discount_count, 
                is_exchanged, exchanged_at, product_brutto_weight, product_brutto_weight_validation
            FROM {ext_schema}.{table}
            WHERE 1 != 1
            '''
        cursor.execute(query)
        redshift_conn.commit()
        logging.info(f'Table {schema}.{table} created successfully')

    for comp_id, ext_schema in customers:
        logging.info(f'Task is starting for company {comp_id}')
        # creating temp table with new data increment
        # query = f'''
        #     CREATE temporary TABLE {table}_{comp_id}_temp as
        #     WITH orders_for_update AS (
        #         select  
        #             order_id, 
        #             max(CASE type WHEN 8 THEN 1 else 0 end) as is_completed, 
        #             max(CASE type WHEN 24 THEN 1 else 0 end) as has_deleted_items
        #         from staging.warehouse_order_logs
        #         where type in (8, 24) and comp_id = {comp_id}
        #         group by 1
        #         having is_completed = 1 and has_deleted_items = 1
        #     )
        #     SELECT
        #         {comp_id} as comp_id, id, order_id, product_id, "name", descr, price_type, price_per, 
        #         charge_by, price, qty, qty_free, amount, tax, discount_value, discount_type_bak, total_amount, 
        #         created_at, updated_at, is_charge_by_order, is_free, free_discount, income, discount_amount, 
        #         item_type, count, special_id, special_item_id, is_half_eighth, is_returned, returned_amount, 
        #         discount_type, free_amount, paid_amount, wcii_cart_item, sync_created_at, sync_updated_at, 
        #         product_checkin_id, is_excise, returned_at, is_marijuana_product, product_is_tax_exempt, 
        #         is_metrc, is_under_package_control, base_amount, discount_id, delivery_tax, discount_count, 
        #         is_exchanged, exchanged_at, product_brutto_weight, product_brutto_weight_validation
        #     FROM {ext_schema}.{table}
        #     where order_id in (select order_id from orders_for_update)
        # '''
        # cursor.execute(query)
        # logging.info(f'Temp table is created')
        # deleting from target table data that were updated
        query = f'''
            WITH orders_for_update AS (
                select  
                    order_id, 
                    max(CASE type WHEN 8 THEN 1 else 0 end) as is_completed, 
                    max(CASE type WHEN 24 THEN 1 else 0 end) as has_deleted_items
                from staging.warehouse_order_logs
                where type in (8, 24) and comp_id = {comp_id}
                group by 1
                having is_completed = 1 and has_deleted_items = 1
            )
            DELETE FROM {schema}.{table}
            WHERE {schema}.{table}.comp_id = {comp_id}
                AND {schema}.{table}.order_id in (select order_id from orders_for_update);
        '''
        cursor.execute(query)
        logging.info(f'{cursor.rowcount} rows deleted for {comp_id} at {datetime.now()}')
        if cursor.rowcount > 0:
            # inserting increment to target table
            query = f'''
                INSERT INTO {schema}.{table}
                WITH orders_for_update AS (
                    select  
                        order_id, 
                        max(CASE type WHEN 8 THEN 1 else 0 end) as is_completed, 
                        max(CASE type WHEN 24 THEN 1 else 0 end) as has_deleted_items
                    from staging.warehouse_order_logs
                    where type in (8, 24) and comp_id = {comp_id}
                    group by 1
                    having is_completed = 1 and has_deleted_items = 1
                )
                SELECT
                    {comp_id} as comp_id, id, order_id, product_id, "name", descr, price_type, price_per, 
                    charge_by, price, qty, qty_free, amount, tax, discount_value, discount_type_bak, total_amount, 
                    created_at, updated_at, is_charge_by_order, is_free, free_discount, income, discount_amount, 
                    item_type, count, special_id, special_item_id, is_half_eighth, is_returned, returned_amount, 
                    discount_type, free_amount, paid_amount, wcii_cart_item, sync_created_at, sync_updated_at, 
                    product_checkin_id, is_excise, returned_at, is_marijuana_product, product_is_tax_exempt, 
                    is_metrc, is_under_package_control, base_amount, discount_id, delivery_tax, discount_count, 
                    is_exchanged, exchanged_at, product_brutto_weight, product_brutto_weight_validation
                FROM {ext_schema}.{table}
                where order_id in (select order_id from orders_for_update)
            '''
            cursor.execute(query)
            logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {datetime.now()}')
            # deleting temp table
            # query = f'''
            #     DROP TABLE {table}_{comp_id}_temp
            # '''
            # cursor.execute(query)
            # logging.info(f'Temp table is dropped')
            # commit to target DB
            redshift_conn.commit()
        logging.info(f'Task is finished for company {comp_id}')
        Variable.set(task_id, comp_id)
    Variable.set(task_id, 0)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 10,
    'retry_delay': timedelta(seconds=30)
}


@task_group
def upsert_tables(schema='staging'):
    upsert_warehouse_order_items_new(schema, table='warehouse_order_items', date_column='updated_at')


with DAG(
    dag_id='remove_unwanted_items_once',
    max_active_tasks=32,
    schedule='0 8 * * *', # UTC time
    start_date=datetime(year=2022, month=12, day=8),
    default_args=default_args,
    catchup=False,
) as dag:
    get_customers_task = get_customers()
    upsert_tables_group = upsert_tables(schema='staging')

    get_customers_task >> upsert_tables_group