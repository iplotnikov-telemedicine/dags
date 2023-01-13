from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
# from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow_dbt_python.operators.dbt import DbtRunOperator, DbtTestOperator
import logging
from airflow.decorators import task


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['i.plotnikov@telemedicine.ge', 'd.prokopev@telemedicine.ge'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 10,
    'retry_delay': timedelta(minutes=1)
}


@task
def get_customers():
    redshift_hook = RedshiftSQLHook(
        postgres_conn_id='redshift_default',
        schema='dev'
    )
    redshift_conn = redshift_hook.get_conn()
    cursor = redshift_conn.cursor()
    query = '''
        SELECT int_customers.comp_id, TRIM(svv_external_schemas.schemaname) as schemaname
        FROM test.int_customers
        INNER JOIN svv_external_schemas
        ON int_customers.db_name = svv_external_schemas.databasename
        WHERE int_customers.potify_sync_entity_updated_at >= current_date - interval '1 day'
        ORDER BY comp_id
    '''
    cursor.execute(query)
    logging.info(query)
    data = cursor.fetchall()
    logging.info(f'The number of companies is being processing: {len(data)}')
    return data


@task
def upsert_brands(customer_data):
    (comp_id, ext_schema) = customer_data
    redshift_hook = RedshiftSQLHook(
            postgres_conn_id='redshift_default',
            schema='dev'
        )
    redshift_conn = redshift_hook.get_conn()
    logging.info(f'Task is starting for company {comp_id}')
    with redshift_conn.cursor() as cursor:
        query = f'''
            CREATE temporary TABLE brands_{comp_id}_temp as
            SELECT *
            FROM {ext_schema}.brands
            WHERE sync_updated_at > (
                SELECT coalesce(max(sync_updated_at), '1970-01-01 00:00:00'::timestamp)
                FROM staging.brands
                WHERE comp_id = {comp_id}
            ) and sync_updated_at < CURRENT_DATE + interval '8 hours'
        '''
        cursor.execute(query)
        logging.info(f'Temp table is created')
    with redshift_conn.cursor() as cursor:
        query = f'''
            DELETE FROM staging.brands
            USING brands_{comp_id}_temp
            WHERE staging.brands.comp_id = {comp_id}
                AND staging.brands.id = brands_{comp_id}_temp.id
        '''
        cursor.execute(query)
        logging.info(f'{cursor.rowcount} rows deleted for {comp_id} at {datetime.now()}')
    with redshift_conn.cursor() as cursor:
        query = f'''
            INSERT INTO staging.brands
            SELECT {comp_id}, id, brand_id, brand_name, wm_id, sync_created_at, sync_updated_at, description, is_internal
            FROM brands_{comp_id}_temp
        '''
        cursor.execute(query)
        logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {datetime.now()}')
    with redshift_conn.cursor() as cursor:
        query = f'''
            DROP TABLE brands_{comp_id}_temp
        '''
        cursor.execute(query)
        logging.info(f'Temp table is dropped')
    redshift_conn.commit()
    logging.info(f'Task is finished for company {comp_id}')


with DAG(
    dag_id='dynamic_task_mapping_test',
    schedule_interval='0 8 * * *', # UTC time
    start_date=datetime(year=2023, month=1, day=12),
    default_args=default_args,
    catchup=False,
) as dag:
    upsert_brands.expand(customer_data=get_customers())