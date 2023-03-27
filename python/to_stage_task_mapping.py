# from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
import logging
import pendulum
# import json
from python.core.configs import get_job_config
from python.core.utils import get_fields_for
from python.core.connections import redshint_conn_dev
# from airflow.decorators import task


env = 'mock' # 'dev' - production, 'mock' - development


# Get connection to Redshift DB
redshift_conn = redshint_conn_dev()
cursor = redshift_conn.cursor()


# getting customers list for loading
def get_customers(table, comp_id_list, is_full_load=False):
    logging.info(f'agr: comp_id_list is: {comp_id_list}')
    if is_full_load:
        condition = 'true'
    elif comp_id_list:
        logging.info(f'comp_id_list is: {comp_id_list}')
        condition = f'''comp_id IN ({', '.join(list(map(str, comp_id_list)))})
        '''
    else:
        condition = f'''
            comp_db_name in (
            select table_schema
            from ext_indica_info.tables
            where table_schema like '%_company' 
                and table_name = '{table}'
                and (
                    update_time >= CURRENT_DATE - INTERVAL '16 HOUR'
                    or table_rows > 0
                )
            )
        '''
    query = f'''
        SELECT int_customers.comp_id, TRIM(svv_external_schemas.schemaname) as schemaname
        FROM test.int_customers
        INNER JOIN svv_external_schemas
        ON int_customers.comp_db_name = svv_external_schemas.databasename
        WHERE {condition}
        ORDER BY comp_id
    '''
    logging.info(query)
    cursor.execute(query)
    customers_dict = {row[0]:row[1] for row in cursor.fetchall()}
    logging.info(f'customers_dict: {customers_dict}')
    logging.info(f'The number of companies is being processed: {len(customers_dict)}')
    return customers_dict



def check_table(job_name, schema):
    job_cfg = get_job_config(job_name)
    target_schema = job_cfg.schema if schema is None else schema
    table = job_cfg.table
    source_fields = get_fields_for('source', job_cfg.map)

    # check if table not exists in target schema
    query = f'''
        select 1
        from information_schema.tables
        where table_schema = '{target_schema}' and table_name = '{table}'
        '''
    logging.info(f'query that check table: {query}')
    cursor.execute(query)
    table_exists = cursor.fetchone()
    logging.info(f'Table exists value: {table_exists}')
    if table_exists is None:
        # creating blank table from schema 'ext_indica_c9928_company' because of this company has not blank tables
        comp_id = 9928
        ext_schema = 'ext_indica_c9928_company'
        query = f'''
            create table {target_schema}.{table} as
            select {comp_id} as comp_id, {source_fields}, current_timestamp as inserted_at
            from {ext_schema}.{table}
            where false
            '''
        logging.info(f'query that create table: {query}')
        cursor.execute(query)
        redshift_conn.commit()
        logging.info(f'Table {target_schema}.{table} created successfully') 



def stg_load(customer_data, job_name, schema):
    (comp_id, ext_schema) = customer_data
    # ti, task_id = kwargs['ti'], kwargs['task'].task_id
    job_cfg = get_job_config(job_name)
    load_type = job_cfg.load_type
    target_schema = job_cfg.schema if schema is None else schema
    table = job_cfg.table
    increment = job_cfg.increment_column
    pk = job_cfg.pk
    source_fields = get_fields_for('source', job_cfg.map)
    # target_fields = get_fields_for('target', job_cfg.map)

    logging.info(f'Start loading with type: {load_type}')
    logging.info(f'Task is starting for company {comp_id}')
    if load_type == 'increment':
        # inserting new data with increment to target
        cursor.execute('BEGIN;')
        # query = f'LOCK {target_schema}.{table}'
        # cursor.execute(query)
        query = f'''
            INSERT INTO {target_schema}.{table}
            SELECT {comp_id} as comp_id, {source_fields}, current_timestamp as inserted_at
            FROM {ext_schema}.{table}
            WHERE
                {increment} > (
                    SELECT coalesce(max({increment}), '1970-01-01 00:00:00'::timestamp)
                    FROM {target_schema}.{table}
                    WHERE comp_id = {comp_id} 
                ) AND {increment} < CURRENT_DATE + interval '8 hours'
            '''
        logging.info(f'''insert query is: 
        {query}''')
        cursor.execute(query)
        logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {pendulum.now()}')
        cursor.execute('END')
    elif load_type == 'full':
        # inserting new data with full load to target
        # deleting old data from target
        query = f'''
            DELETE FROM {target_schema}.{table}
            WHERE comp_id = {comp_id}
        '''
        cursor.execute(query)
        logging.info(f'{cursor.rowcount} rows deleted for {comp_id} at {pendulum.now()}')
        # inserting new data to target
        cursor.execute('BEGIN;')
        # query = f'LOCK {target_schema}.{table}'
        # cursor.execute(query)
        query = f'''
            INSERT INTO {target_schema}.{table}
            SELECT {comp_id}, {source_fields}, current_timestamp as inserted_at
            FROM {ext_schema}.{table}
        '''
        logging.info(f'''insert query is: 
        {query}''')
        cursor.execute(query)
        logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {pendulum.now()}')
        cursor.execute('END')
    elif load_type == 'increment_with_delete':
        # creating temp table with new data increment
        query = f'''
            CREATE temporary TABLE {table}_{comp_id}_temp as
            SELECT {source_fields}
            FROM {ext_schema}.{table}
            WHERE {increment} > (
                SELECT coalesce(max({increment}), '1970-01-01 00:00:00'::timestamp)
                FROM {target_schema}.{table}
                WHERE comp_id = {comp_id}
            ) and {increment} < CURRENT_DATE + interval '8 hours'
        '''
        cursor.execute(query)
        logging.info(f'Temp table is created')
        # deleting from target table data that were updated
        keys_condition = ' '
        for key in pk:
            keys_condition = keys_condition + ' ' + f'AND {target_schema}.{table}.{key} = {table}_{comp_id}_temp.{key}' + '\n'
        logging.info(f'keys_condition is: {keys_condition}')
        cursor.execute('BEGIN;')
        query = f'''
            DELETE FROM {target_schema}.{table}
            USING {table}_{comp_id}_temp
            WHERE {target_schema}.{table}.comp_id = {comp_id}
                {keys_condition}
        '''
        logging.info(f'query for delete is: {query}')
        cursor.execute(query)
        logging.info(f'{cursor.rowcount} rows deleted for {comp_id} at {pendulum.now()}')
        # inserting increment to target table
        # query = f'LOCK {target_schema}.{table}'
        # cursor.execute(query)
        query = f'''
            INSERT INTO {target_schema}.{table}
            SELECT {comp_id}, {source_fields}, current_timestamp as inserted_at
            FROM {table}_{comp_id}_temp
        '''
        logging.info(f'''insert query is: 
        {query}''')
        cursor.execute(query)
        logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {pendulum.now()}')
        cursor.execute('END')
        # deleting temp table
        query = f'''
            DROP TABLE {table}_{comp_id}_temp
        '''
        cursor.execute(query)
        logging.info(f'Temp table is dropped')
    else:
        raise ValueError(f'Incorrect load_type: {load_type}. Expected values: "increment", "full", "increment_with_delete". Please check {job_name}.yml file in section "load_type"')
    # commit to target DB
    redshift_conn.commit()
    logging.info(f'Task is finished for company {comp_id}')
