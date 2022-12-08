from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
# from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.utils.dates import days_ago
from airflow_dbt_python.operators.dbt import DbtRunOperator


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
    data = cursor.fetchall()
    return data


with DAG(
    dag_id="example_dbt_artifacts",
    schedule_interval="0 0 * * *",
    start_date=days_ago(1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
) as dag:
    task_get_customers = PythonOperator(
        task_id='get_customers',
        python_callable=get_customers
    )
    dbt_run = DbtRunOperator(
        task_id="dbt_run_daily",
        project_dir="~/dbt/indica",
        profiles_dir="~/.dbt",
        # select=["+tag:daily"],
        # exclude=["tag:deprecated"],
        target="dev",
        profile="indica",
   )