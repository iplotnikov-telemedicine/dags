from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
import logging
import pendulum
from python.core.configs import get_job_config
from python.core.utils import get_fields_for
from airflow.decorators import task


# Get connection to Redshift DB
redshift_hook = RedshiftSQLHook(
    redshift_conn_id='redshift_default',
    schema='dev'
)
redshift_conn = redshift_hook.get_conn()
cursor = redshift_conn.cursor()


@task
def stg_load(job_name, **kwargs):
    job_cfg = get_job_config(job_name)
    load_type = job_cfg.load_type
    target_schema = job_cfg.target.schema
    target_table = job_cfg.target.table
    source_table = job_cfg.source.table
    increment = job_cfg.increment_column
    source_fields = get_fields_for('source', job_cfg.map)
    target_fields = get_fields_for('target', job_cfg.map)
    
    ti, task_id = kwargs['ti'], kwargs['task'].task_id
    customers = ti.xcom_pull(key='customers', task_ids='get_customers')
    # get max_comp_id from target table and filter list of customers
    max_comp_id = int(Variable.get(task_id, 0))
    customers = [c for c in customers if c[0] > max_comp_id]
    # check if table not exists
    query = f'''
        select 1
        from information_schema.tables
        where table_schema = '{target_schema}' and table_name = '{target_table}'
        '''
    cursor.execute(query)
    table_exists = cursor.fetchone()
    logging.info(f'Table exists value: {table_exists}')
    if table_exists is None:
        # create blank table
        comp_id = customers[0][0]
        ext_schema = customers[0][1]
        query = f'''
            create table {target_schema}.{target_table} as
            select {comp_id} as comp_id, {source_fields}, current_timestamp as inserted_at
            from {ext_schema}.{source_table}
            where false
            '''
        cursor.execute(query)
        redshift_conn.commit()
        logging.info(f'Table {target_schema}.{target_table} created successfully')
    for comp_id, ext_schema in customers:
        logging.info(f'Task is starting for company {comp_id}')
        if load_type == 'increment':
            # inserting new data with increment to target
            query = f'''
                INSERT INTO {target_schema}.{target_table}
                SELECT {comp_id} as comp_id, {source_fields}, current_timestamp as inserted_at
                FROM {ext_schema}.{source_table}
                WHERE
                    {increment} > (
                        SELECT coalesce(max({increment}), '1970-01-01 00:00:00'::timestamp)
                        FROM {target_schema}.{target_table}
                        WHERE comp_id = {comp_id} 
                    ) AND {increment} < CURRENT_DATE + interval '8 hours'
                '''
        elif load_type == 'full':
            # inserting new data with full load to target
            # deleting old data from target
            query = f'''
                DELETE FROM {target_schema}.{target_table}
                WHERE comp_id = {comp_id}
            '''
            cursor.execute(query)
            logging.info(f'{cursor.rowcount} rows deleted for {comp_id} at {pendulum.now()}')
            # inserting new data to target
            query = f'''
                INSERT INTO {target_schema}.{target_table}
                SELECT {comp_id}, {source_fields}, current_timestamp as inserted_at
                FROM {ext_schema}.{source_table}
            '''
        cursor.execute(query)
        logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {pendulum.now()}')
        # commit to target DB
        redshift_conn.commit()
        logging.info(f'Task is finished for company {comp_id}')
        Variable.set(task_id, comp_id)
    Variable.set(task_id, 0)