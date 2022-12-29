from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
# from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.operators.bash_operator import BashOperator
import logging
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task_group, task



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
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
        SELECT companies.comp_id, TRIM(svv_external_schemas.schemaname) as schemaname
        FROM ext_indica_backend.companies
        INNER JOIN svv_external_schemas
        ON companies.db_name = svv_external_schemas.databasename
        WHERE 1=1
            and db_name like '%_company'
            and is_blank = 0
            and comp_project = 'Indica'
            and not domain_prefix like '%prod'
            and not domain_prefix like 'test%'
            and not domain_prefix like '%demo%'
            and (not comp_email like '%maildrop.cc' or comp_email = 'calif@maildrop.cc')
            and not comp_email like '%indica%'
            and not comp_name like 'Blank company%'
            and not comp_name like '%test%'
            and not comp_name like '%Test%'
            and not comp_name like '%xxxx%'
            and plan <> 5
            and comp_id not in (8580, 724, 6805, 8581, 6934, 8584, 
                8585, 3324, 8582, 6022, 3439, 8583, 8586, 6443, 8588, 
                6483, 7900, 8587, 8589, 9471, 7304, 7523, 8911, 213
            ) and potify_sync_entity_updated_at >= current_date - interval '1 day'
            and comp_is_approved = 1
        ORDER BY comp_id
    '''
    cursor.execute(query)
    logging.info(query)
    data = cursor.fetchall()
    logging.info(f'The number of companies is being processing: {len(data)}')
    return map(list, zip(*data))


@task
def upsert_brands(comp_id, ext_schema):
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
            )
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
    dag_id='update_everything',
    schedule_interval='0 8 * * *', # UTC time
    start_date=datetime(year=2022, month=12, day=29),
    default_args=default_args,
    catchup=False,
) as dag:
    customers = get_customers()
    upsert_brands.expand(comp_id=customers[0], ext_schema=customers[1])

