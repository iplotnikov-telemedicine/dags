from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
import logging
import pendulum
import json
from python.core.configs import get_job_config
from python.core.utils import get_fields_for
# from airflow.decorators import task


# getting connection to Redshift DB
redshift_hook = RedshiftSQLHook(
    redshift_conn_id='redshift_default',
    schema='dev'
)
redshift_conn = redshift_hook.get_conn()
cursor = redshift_conn.cursor()


# getting customers list for loading
def get_customers(table, comp_id_list, is_full_load=False):
    if is_full_load:
        condition = 'true'
    elif comp_id_list:
        condition = f'''comp_id IN ({', '.join(list(map(str, comp_id_list)))})
        '''
    else:
        condition = f'''
            comp_db_name in (
            select table_schema
            from ext_indica_info.tables
            where table_schema like '%_company' 
                and table_name = '{table}'
                and update_time >= CURRENT_DATE - INTERVAL '16 HOUR')
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


# @task()
# def stg_load(job_name, **kwargs):
def stg_load(*op_args, **kwargs):
    ti, task_id = kwargs['ti'], kwargs['task'].task_id
    job_cfg = get_job_config(op_args[0])
    load_type = job_cfg.load_type
    target_schema = job_cfg.schema
    table = job_cfg.table
    increment = job_cfg.increment_column
    pk = job_cfg.pk
    source_fields = get_fields_for('source', job_cfg.map)
    # target_fields = get_fields_for('target', job_cfg.map)
    

    # check if table not exists in target schema
    query = f'''
        select 1
        from information_schema.tables
        where table_schema = '{target_schema}' and table_name = '{table}'
        '''
    cursor.execute(query)
    table_exists = cursor.fetchone()
    logging.info(f'Table exists value: {table_exists}')
    if table_exists is None:
        # creating blank table from schema 'ext_indica_c9928_company' because of this company has not blank tables
        comp_id = 9928 #customers[0][0]
        ext_schema = 'ext_indica_c9928_company' #customers[0][1]
        query = f'''
            create table {target_schema}.{table} as
            select {comp_id} as comp_id, {source_fields}, current_timestamp as inserted_at
            from {ext_schema}.{table}
            where false
            '''
        cursor.execute(query)
        redshift_conn.commit()
        logging.info(f'Table {target_schema}.{table} created successfully')


    # #old aproach for getting customers
    # customers = ti.xcom_pull(key='customers', task_ids='get_customers')
    # # get max_comp_id from target table and filter list of customers
    # max_comp_id = int(Variable.get(task_id, 0))
    # customers = [c for c in customers if c[0] > max_comp_id]
    # for comp_id, ext_schema in customers:

    customers_dict = Variable.get(task_id, dict(), deserialize_json=True)
    if not customers_dict:
        comp_id_list = kwargs['dag_run'].conf.get('comp_id_list')
        # if the table does not exist before then initiate full load
        if table_exists is None:
            logging.info(f'Table {target_schema}.{table} does not exist before, initiate full load')
            customers_dict = get_customers(table, comp_id_list, is_full_load=True)
        else:
            customers_dict = get_customers(table, comp_id_list)
        Variable.set(task_id, json.dumps(customers_dict))
    logging.info(f'Start loading with type: {load_type}')
    for comp_id in list(customers_dict.keys()):
        ext_schema = customers_dict[comp_id]
        logging.info(f'Task is starting for company {comp_id}')
        if load_type == 'increment':
            # inserting new data with increment to target
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
            cursor.execute(query)
            logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {pendulum.now()}')
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
            query = f'''
                INSERT INTO {target_schema}.{table}
                SELECT {comp_id}, {source_fields}, current_timestamp as inserted_at
                FROM {ext_schema}.{table}
            '''
            cursor.execute(query)
            logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {pendulum.now()}')
        elif load_type == 'increment_with_delete':
            # creating temp table with new data increment
            query = f'''
                CREATE temporary TABLE {table}_{comp_id}_temp as
                SELECT *
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
            query = f'''
                DELETE FROM {target_schema}.{table}
                USING {table}_{comp_id}_temp
                WHERE {target_schema}.{table}.comp_id = {comp_id}
                    {keys_condition}
            '''
            cursor.execute(query)
            logging.info(f'{cursor.rowcount} rows deleted for {comp_id} at {pendulum.now()}')
            # inserting increment to target table
            query = f'''
                INSERT INTO {target_schema}.{table}
                SELECT {comp_id}, {source_fields}, current_timestamp as inserted_at
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
        else:
            raise ValueError(f'Incorrect load_type: {load_type}. Expected values: "increment", "full", "increment_with_delete". Please check {op_args[0]}.yml file in section "load_type"')
        # commit to target DB
        redshift_conn.commit()
        logging.info(f'Task is finished for company {comp_id}')
    #     Variable.set(task_id, comp_id)
    # Variable.set(task_id, 0)
        del customers_dict[comp_id]
        logging.info(f'Number of companies left: {len(customers_dict)}')
        Variable.set(task_id, json.dumps(customers_dict))
    Variable.delete(task_id)
