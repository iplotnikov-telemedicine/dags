from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook


def get_customers():
    redshift_hook = RedshiftSQLHook(
        postgres_conn_id='redshift_default',
        schema='dev'
    )
    redshift_conn = redshift_hook.get_conn()
    cursor = redshift_conn.cursor()
    query = '''
        select comp_id, db_name
        from ext_indica_backend.companies
        where 1=1
            and db_name like '%_company'
            and is_blank = 0
            and comp_project = 'Indica'
            and not comp_email like '%maildrop%'
            and not comp_email like '%indica%'
            and not comp_name like 'Blank company%'
            and not comp_name like '%test%'
            and not comp_name like '%Test%'
            and not comp_name like '%xxxx%'
            and plan <> 5
            and comp_id not in (8580, 724, 6805, 8581, 6934, 8584, 
                8585, 3324, 8582, 6022, 3439, 8583, 8586, 6443, 8588, 
                6483, 7900, 8587, 8589, 9471, 7304, 7523, 8911, 213
            ) and potify_sync_entity_updated_at >= current_date - interval '1 week'
        order by comp_id
        limit 5
    '''
    cursor.execute(query)
    return cursor.fetchall()[0]


def upsert_brands(ti):
    customers = ti.xcom_pull(task_ids=['get_customers'])
    if not customers:
        raise Exception('No customers.')
    else:
        for customer in customers:
            comp_id = customer[0]
            db_name = customer[1]
            ext_schema = f'ext_indica_{db_name}'
            redshift_hook = RedshiftSQLHook(
                postgres_conn_id='redshift_default',
                schema='dev'
            )
            redshift_conn = redshift_hook.get_conn()
            cursor = redshift_conn.cursor()
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
            query = f'''
                DELETE FROM staging.brands
                USING brands_{comp_id}_temp
                WHERE staging.brands.comp_id = {comp_id}
                    AND staging.brands.id = brands_{comp_id}_temp.id
            '''
            cursor.execute(query)
            query = f'''
                INSERT INTO staging.brands
                SELECT {comp_id}, *
                FROM brands_{comp_id}_temp
            '''
            cursor.execute(query)
            query = f'''
                DROP TABLE brands_{comp_id}_temp
            '''
            cursor.execute(query)



with DAG(
    dag_id='update_brands_dag',
    schedule_interval='@daily',
    start_date=datetime(year=2022, month=2, day=1),
    catchup=False,
) as dag:
    task_get_customers = PythonOperator(
        task_id='get_customers',
        python_callable=get_customers,
        do_xcom_push=True
    )
    task_upsert_brands = PythonOperator(
        task_id='upsert_brands',
        python_callable=upsert_brands
    )

    task_get_customers >> task_upsert_brands




# for comp_id in comp_ids:
#     select_data = RedshiftSQLOperator(
#         task_id='select_current_date',
#         redshift_conn_id='redshift_default',
#         sql='''SELECT GETDATE()'''
#         # sql="""SELECT count(*) FROM test.warehouse_orders WHERE comp_id = {{ params.comp_id }};""",
#         # parameters={'comp_id': comp_id},
#     )


# get_max_sync_updated_at = RedshiftSQLOperator(
#     task_id='get_max_sync_updated_at',
#     sql='SELECT GETDATE()',
#     redshift_conn_id='redshift_default',
#     parameters={'comp_id': comp_id},
#     autocommit=True
# )


# with DAG(
#     dag_id='postgres_db_dag',
#     schedule_interval='@daily',
#     start_date=datetime(year=2022, month=2, day=1),
#     catchup=False
# ) as dag:
#     for comp_id in comp_ids:
#         get_max_sync_updated_at = PostgresOperator(
#             sql='get_max_sync_updated_at',
#             task_id='get_max_sync_updated_at',
#             postgres_conn_id='redshift',
#             parameters={'comp_id': comp_id}
#         )
#         print(get_max_sync_updated_at.__dict__)
        # task_get_iris_data = PostgresHook(
        #     task_id='get_max_sync_updated_at',
        #     sql='get_max_sync_'
        #     do_xcom_push=False
        # )
