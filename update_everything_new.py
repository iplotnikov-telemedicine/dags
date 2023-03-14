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
from airflow.providers.mysql.hooks.mysql import MySqlHook
import json


# Get connection to Redshift DB
redshift_hook = RedshiftSQLHook(
    redshift_conn_id='redshift_default',
    schema='dev'
)
redshift_conn = redshift_hook.get_conn()
cursor = redshift_conn.cursor()


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


def get_customers(table, comp_id_list):
    if comp_id_list:
        condition = f'''comp_id IN ({', '.join(list(map(str, comp_id_list)))})
        '''
    else:
        condition = '''update_time >= CURRENT_DATE - INTERVAL '16 HOUR'
        '''
    query = f'''
        SELECT int_customers.comp_id, TRIM(svv_external_schemas.schemaname) as schemaname
        FROM test.int_customers
        INNER JOIN svv_external_schemas
        ON int_customers.comp_db_name = svv_external_schemas.databasename
        WHERE int_customers.comp_db_name in (
            select table_schema
            from ext_indica_info.tables
            where table_schema like '%_company' 
                and table_name = '{table}'
                and {condition})
        ORDER BY comp_id
    '''
    cursor.execute(query)
    logging.info(query)
    customers_dict = {row[0]:row[1] for row in cursor.fetchall()}
    logging.info(f'customers_dict: {customers_dict}')
    logging.info(f'The number of companies is being processed: {len(customers_dict)}')
    return customers_dict


# @task
# def get_customers(ti=None, **kwargs):
#     comp_id_list = kwargs['dag_run'].conf.get('comp_id_list')
#     if comp_id_list:
#         condition = f"int_customers.comp_id IN ({', '.join(list(map(str, comp_id_list)))})"
#     else:
#         condition = "int_customers.potify_sync_entity_updated_at >= current_date - interval '3 day'"
#     query = f'''
#         SELECT int_customers.comp_id, TRIM(svv_external_schemas.schemaname) as schemaname
#         FROM test.int_customers
#         INNER JOIN svv_external_schemas
#         ON int_customers.comp_db_name = svv_external_schemas.databasename
#         WHERE {condition}
#         ORDER BY comp_id
#     '''
#     cursor.execute(query)
#     logging.info(query)
#     data = cursor.fetchall()
#     logging.info(f'The number of companies is being processed: {len(data)}')
#     ti.xcom_push(key='customers', value=data)


@task
def upsert_brands(schema, table, date_column, **kwargs):
    ti, task_id = kwargs['ti'], kwargs['task'].task_id
    customers_dict = Variable.get(task_id, dict(), deserialize_json=True)
    if not customers_dict:
        comp_id_list = kwargs['dag_run'].conf.get('comp_id_list')
        customers_dict = get_customers(table, comp_id_list)
        Variable.set(task_id, json.dumps(customers_dict))
    for comp_id in list(customers_dict.keys()):
        ext_schema = customers_dict[comp_id]
        logging.info(f'Task is starting for company {comp_id}')
        cursor.execute('BEGIN')
        # creating temp table with new data increment
        query = f'''
            CREATE temporary TABLE {table}_{comp_id}_temp as
            SELECT *
            FROM {ext_schema}.{table}
            WHERE {date_column} > (
                SELECT coalesce(max({date_column}), '1970-01-01 00:00:00'::timestamp)
                FROM {schema}.{table}
                WHERE comp_id = {comp_id}
            ) and {date_column} < CURRENT_DATE + interval '8 hours'
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
            SELECT {comp_id}, id, brand_id, brand_name, wm_id, sync_created_at, sync_updated_at, description, is_internal, current_timestamp as inserted_at
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
        cursor.execute('END')
        logging.info(f'Task is finished for company {comp_id}')
        del customers_dict[comp_id]
        logging.info(f'Number of companies left: {len(customers_dict)}')
        Variable.set(task_id, json.dumps(customers_dict))
    Variable.delete(task_id)


@task
def upsert_company_config(schema, table, **kwargs):
    ti, task_id = kwargs['ti'], kwargs['task'].task_id
    customers_dict = Variable.get(task_id, dict(), deserialize_json=True)
    if not customers_dict:
        comp_id_list = kwargs['dag_run'].conf.get('comp_id_list')
        customers_dict = get_customers(table, comp_id_list)
        Variable.set(task_id, json.dumps(customers_dict))
    for comp_id in list(customers_dict.keys()):
        ext_schema = customers_dict[comp_id]
        logging.info(f'Task is starting for company {comp_id}')
        # deleting old data from target
        query = f'''
            DELETE FROM {schema}.{table}
            WHERE comp_id = {comp_id}
        '''
        cursor.execute(query)
        logging.info(f'{cursor.rowcount} rows deleted for {comp_id} at {datetime.now()}')
        # inserting new data to target
        query = f'''
            INSERT INTO {schema}.{table}
            SELECT {comp_id}, id, "name", human_name, value, current_timestamp as inserted_at
            FROM {ext_schema}.{table}
        '''
        cursor.execute(query)
        logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {datetime.now()}')
        # commit to target DB
        redshift_conn.commit()
        logging.info(f'Task is finished for company {comp_id}')
        del customers_dict[comp_id]
        logging.info(f'Number of companies left: {len(customers_dict)}')
        Variable.set(task_id, json.dumps(customers_dict))
    Variable.delete(task_id)



@task
def upsert_discounts(schema, table, date_column, **kwargs):
    ti, task_id = kwargs['ti'], kwargs['task'].task_id
    customers_dict = Variable.get(task_id, dict(), deserialize_json=True)
    if not customers_dict:
        comp_id_list = kwargs['dag_run'].conf.get('comp_id_list')
        customers_dict = get_customers(table, comp_id_list)
        Variable.set(task_id, json.dumps(customers_dict))
    for comp_id in list(customers_dict.keys()):
        ext_schema = customers_dict[comp_id]
        logging.info(f'Task is starting for company {comp_id}')
        # creating temp table with new data increment
        query = f'''
            CREATE temporary TABLE {table}_{comp_id}_temp as
            SELECT *
            from {ext_schema}.{table}
            where {date_column} > (
                select coalesce(max({date_column}), '1970-01-01 00:00:00'::timestamp)
                from {schema}.{table}
                where comp_id = {comp_id}
            ) and {date_column} < CURRENT_DATE + interval '8 hours'
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
            SELECT {comp_id}, id, "name", "type", value, sync_created_at, sync_updated_at, 
                deleted_at, use_type, apply_type, is_pos, is_potify, promo_code, status, 
                is_individual_use_only, is_exclude_items_on_special, start_date, end_date, 
                is_ongoing, happy_weekdays, min_subtotal_price, uses_count, is_once_per_patient, 
                bogo_buy, bogo_get, bogo_multiple, is_first_time_patient, is_show_promo_code_on_potify, 
                max_subtotal_price, display_name, is_show_name_on_collection_tile, image, tv_image, 
                product_filter_id, created_at, updated_at, hide_banner, display_priority, current_timestamp as inserted_at
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
        del customers_dict[comp_id]
        logging.info(f'Number of companies left: {len(customers_dict)}')
        Variable.set(task_id, json.dumps(customers_dict))
    Variable.delete(task_id)


@task
def upsert_patient_group_ref(schema, table, date_column, **kwargs):
    ti, task_id = kwargs['ti'], kwargs['task'].task_id
    customers_dict = Variable.get(task_id, dict(), deserialize_json=True)
    if not customers_dict:
        comp_id_list = kwargs['dag_run'].conf.get('comp_id_list')
        customers_dict = get_customers(table, comp_id_list)
        Variable.set(task_id, json.dumps(customers_dict))
    for comp_id in list(customers_dict.keys()):
        ext_schema = customers_dict[comp_id]
        logging.info(f'Task is starting for company {comp_id}')
        # creating temp table with new data increment
        query = f'''
            CREATE temporary TABLE {table}_{comp_id}_temp as
            SELECT *
            FROM {ext_schema}.{table}
            WHERE {date_column} > (
                SELECT coalesce(max({date_column}), '1970-01-01 00:00:00'::timestamp)
                FROM {schema}.{table}
                WHERE comp_id = {comp_id}
            ) and {date_column} < CURRENT_DATE + interval '8 hours'
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
            SELECT {comp_id}, id, patient_id, group_id, sync_created_at, sync_updated_at, current_timestamp as inserted_at
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
        del customers_dict[comp_id]
        logging.info(f'Number of companies left: {len(customers_dict)}')
        Variable.set(task_id, json.dumps(customers_dict))
    Variable.delete(task_id)


@task
def upsert_patient_group(schema, table, date_column, **kwargs):
    ti, task_id = kwargs['ti'], kwargs['task'].task_id
    customers_dict = Variable.get(task_id, dict(), deserialize_json=True)
    if not customers_dict:
        comp_id_list = kwargs['dag_run'].conf.get('comp_id_list')
        customers_dict = get_customers(table, comp_id_list)
        Variable.set(task_id, json.dumps(customers_dict))
    for comp_id in list(customers_dict.keys()):
        ext_schema = customers_dict[comp_id]
        logging.info(f'Task is starting for company {comp_id}')
        # creating temp table with new data increment
        query = f'''
            CREATE temporary TABLE {table}_{comp_id}_temp as
            SELECT *
            FROM {ext_schema}.{table}
            WHERE {date_column} > (
                SELECT coalesce(max({date_column}), '1970-01-01 00:00:00'::timestamp)
                FROM {schema}.{table}
                WHERE comp_id = {comp_id}
            ) and {date_column} < CURRENT_DATE + interval '8 hours'
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
            SELECT {comp_id}, id, "name", sync_created_at, sync_updated_at, is_auto, start_date, end_date, current_timestamp as inserted_at
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
        del customers_dict[comp_id]
        logging.info(f'Number of companies left: {len(customers_dict)}')
        Variable.set(task_id, json.dumps(customers_dict))
    Variable.delete(task_id)


@task
def upsert_patients(schema, table, date_column, **kwargs):
    ti, task_id = kwargs['ti'], kwargs['task'].task_id
    customers_dict = Variable.get(task_id, dict(), deserialize_json=True)
    if not customers_dict:
        comp_id_list = kwargs['dag_run'].conf.get('comp_id_list')
        customers_dict = get_customers(table, comp_id_list)
        Variable.set(task_id, json.dumps(customers_dict))
    for comp_id in list(customers_dict.keys()):
        ext_schema = customers_dict[comp_id]
        logging.info(f'Task is starting for company {comp_id}')
        # creating temp table with new data increment
        query = f'''
            CREATE temporary TABLE {table}_{comp_id}_temp as
            SELECT *
            FROM {ext_schema}.{table}
            WHERE {date_column} > (
                SELECT coalesce(max({date_column}), '1970-01-01 00:00:00'::timestamp)
                FROM {schema}.{table}
                WHERE comp_id = {comp_id}
            ) and {date_column} < CURRENT_DATE + interval '8 hours'
        '''
        cursor.execute(query)
        logging.info(f'Temp table is created')
        # deleting from target table data that were updated
        query = f'''
            DELETE FROM {schema}.{table}
            USING {table}_{comp_id}_temp
            WHERE {schema}.{table}.comp_id = {comp_id}
                AND {schema}.{table}.pat_id = {table}_{comp_id}_temp.pat_id
        '''
        cursor.execute(query)
        logging.info(f'{cursor.rowcount} rows deleted for {comp_id} at {datetime.now()}')
        # inserting increment to target table
        query = f'''
            INSERT INTO {schema}.{table}
            SELECT {comp_id}, pat_id, pat_user_id, pat_user_info, pat_office_id, pat_first_name, pat_middle_name, pat_last_name, 
                pat_gender, pat_photo, pat_dob, pat_phone, pat_alt_phone, pat_email, pat_state_name, pat_city_name, pat_zip_name, 
                pat_address1, pat_address2, pat_dmv, pat_dhc, pat_passport, pat_notes, pat_ref_points, pat_is_hidden, pat_is_disabled, 
                pat_is_veteran, pat_is_military, pat_is_seniou, pat_is_nonpatient, pat_is_facebooked, pat_is_needs_cultivation, 
                pat_is_cancer, pat_status, pat_weight, pat_height, pat_amount, pat_created_at_date, pat_is_deleted, pat_deleted_at, 
                pat_deleted_by_doc_id, pat_stamp, pat_is_appointment, pat_last_visit_date, is_crypted, pat_skype, pat_insurance, 
                pat_general_intake, pat_has_intake_for_approving, 
                cast(is_not_send_mail as BOOLEAN) as is_not_send_mail, 
                cast(is_not_send_email as BOOLEAN) as is_not_send_email, 
                cast(is_not_send_sms as BOOLEAN) as is_not_send_sms, 
                cast(is_not_call as BOOLEAN) as is_not_call, 
                unsubscribe_hash, is_from_shop, pat_ssn, pat_ethnicity, pat_race, pat_address_lon, pat_address_lat, pat_m_phone1, 
                pat_m_phone2, pat_m_phone3, pat_h_phone1, pat_h_phone2, pat_h_phone3, pat_o_phone1, pat_o_phone2, pat_o_phone3, pat_fax, 
                pat_lng, current_rec_valid_to, current_rec_doc_id, current_rec_number, sync_updated_at, sync_created_at, pat_is_ssi, 
                pat_is_compassion, pat_is_vip, pat_is_staff, pat_is_former_staff, pat_is_markedasdeleted, pat_markedasdeleted_reason, 
                pat_markedasdeleted_username, pat_markedasdeleted_date, balance, pat_county_name, pat_location_id, pat_state_id, pat_county_id, 
                pat_city_id, pat_zip_id, is_typist_work, pat_dmv_alt, pat_is_blacklist, pat_blacklist_reason, "type", pat_phone_twilio_lookup, 
                pat_alt_phone_twilio_lookup, is_twilio_lookup, agile_crm_id, potify_id, potify_earned_cashback, potify_spent_cashback, created_at, 
                updated_at, is_tax_exempt, tax_profile_id, phone_is_consented, email_is_consented, phone_consent_given_at, 
                phone_consent_signature, email_consent_given_at, email_consent_signature, deleted_at, tax_tier_id, buyer_status, marketplace, current_timestamp as inserted_at
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
        del customers_dict[comp_id]
        logging.info(f'Number of companies left: {len(customers_dict)}')
        Variable.set(task_id, json.dumps(customers_dict))
    Variable.delete(task_id)


@task
def upsert_product_categories(schema, table, **kwargs):
    ti, task_id = kwargs['ti'], kwargs['task'].task_id
    customers_dict = Variable.get(task_id, dict(), deserialize_json=True)
    if not customers_dict:
        comp_id_list = kwargs['dag_run'].conf.get('comp_id_list')
        customers_dict = get_customers(table, comp_id_list)
        Variable.set(task_id, json.dumps(customers_dict))
    for comp_id in list(customers_dict.keys()):
        ext_schema = customers_dict[comp_id]
        logging.info(f'Task is starting for company {comp_id}')
        # deleting old data from target
        query = f'''
            DELETE FROM {schema}.{table}
            WHERE comp_id = {comp_id}
        '''
        cursor.execute(query)
        logging.info(f'{cursor.rowcount} rows deleted for {comp_id} at {datetime.now()}')
        # inserting new data to target
        query = f'''
            INSERT INTO {schema}.{table}
            SELECT {comp_id}, id, "name", description, photo, lft, rgt, 
                "level", created_at, updated_at, is_system, sync_updated_at, 
                sync_created_at, icon_name, activation_time, 
                label_template_internal_id, icon_color, system_id, current_timestamp as inserted_at
            FROM {ext_schema}.{table}
        '''
        cursor.execute(query)
        logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {datetime.now()}')
        # commit to target DB
        redshift_conn.commit()
        logging.info(f'Task is finished for company {comp_id}')
        del customers_dict[comp_id]
        logging.info(f'Number of companies left: {len(customers_dict)}')
        Variable.set(task_id, json.dumps(customers_dict))
    Variable.delete(task_id)


@task
def upsert_product_filter_index(schema, table, **kwargs):
    ti, task_id = kwargs['ti'], kwargs['task'].task_id
    customers_dict = Variable.get(task_id, dict(), deserialize_json=True)
    if not customers_dict:
        comp_id_list = kwargs['dag_run'].conf.get('comp_id_list')
        customers_dict = get_customers(table, comp_id_list)
        Variable.set(task_id, json.dumps(customers_dict))
    for comp_id in list(customers_dict.keys()):
        ext_schema = customers_dict[comp_id]
        logging.info(f'Task is starting for company {comp_id}')
        # deleting old data from target
        query = f'''
            DELETE FROM {schema}.{table}
            WHERE comp_id = {comp_id}
        '''
        cursor.execute(query)
        logging.info(f'{cursor.rowcount} rows deleted for {comp_id} at {datetime.now()}')
        # inserting new data to target
        query = f'''
            INSERT INTO {schema}.{table}
            SELECT {comp_id}, id, product_id, product_filter_id, current_timestamp as inserted_at
            FROM {ext_schema}.{table}
        '''
        cursor.execute(query)
        logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {datetime.now()}')
        # commit to target DB
        redshift_conn.commit()
        logging.info(f'Task is finished for company {comp_id}')
        del customers_dict[comp_id]
        logging.info(f'Number of companies left: {len(customers_dict)}')
        Variable.set(task_id, json.dumps(customers_dict))
    Variable.delete(task_id)


@task
def upsert_product_transactions(schema, table, date_column, **kwargs):
    ti, task_id = kwargs['ti'], kwargs['task'].task_id
    customers_dict = Variable.get(task_id, dict(), deserialize_json=True)
    if not customers_dict:
        comp_id_list = kwargs['dag_run'].conf.get('comp_id_list')
        customers_dict = get_customers(table, comp_id_list)
        Variable.set(task_id, json.dumps(customers_dict))
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
        comp_id = list(customers_dict.items())[0][0]
        ext_schema = list(customers_dict.items())[0][1]
        query = f'''
            create table {schema}.{table} as
            select {comp_id} as comp_id, id, product_id, office_id, doctor_id, patient_id, user_id, type, 
                qty, price, price_per, total_price, date, note, item_type, 
                transfer_direction, CAST(qty_free as float4) as qty_free, product_checkin_id, product_name, 
                office_to_id, product_to_id, product_to_name, cost, order_id, 
                base_weight, product_checkin_to_id, office_name, current_timestamp as inserted_at
            from {ext_schema}.{table}
            where 1 != 1
            '''
        cursor.execute(query)
        redshift_conn.commit()
        logging.info(f'Table {schema}.{table} created successfully')
    for comp_id in list(customers_dict.keys()):
        ext_schema = customers_dict[comp_id]
        logging.info(f'Task is starting for company {comp_id}')
        # inserting new data with increment to target
        query = f'''
            INSERT INTO {schema}.{table}
            SELECT
                {comp_id} as comp_id, id, product_id, office_id, doctor_id, patient_id, user_id, type, 
                qty, price, price_per, total_price, date, note, item_type, 
                transfer_direction, CAST(qty_free as float4) as qty_free, product_checkin_id, product_name, 
                office_to_id, product_to_id, product_to_name, cost, order_id, 
                base_weight, product_checkin_to_id, office_name, current_timestamp as inserted_at
            FROM {ext_schema}.{table}
            WHERE {date_column} > (
                select coalesce(max({date_column}), '1970-01-01 00:00:00'::timestamp)
                from {schema}.{table}
                where comp_id = {comp_id}
            ) and {date_column} < CURRENT_DATE + interval '8 hours'
        '''
        cursor.execute(query)
        logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {datetime.now()}')
        # commit to target DB
        redshift_conn.commit()
        logging.info(f'Task is finished for company {comp_id}')
        del customers_dict[comp_id]
        logging.info(f'Number of companies left: {len(customers_dict)}')
        Variable.set(task_id, json.dumps(customers_dict))
    Variable.delete(task_id)


@task
def upsert_product_vendors(schema, table, date_column, **kwargs):
    ti, task_id = kwargs['ti'], kwargs['task'].task_id
    customers_dict = Variable.get(task_id, dict(), deserialize_json=True)
    if not customers_dict:
        comp_id_list = kwargs['dag_run'].conf.get('comp_id_list')
        customers_dict = get_customers(table, comp_id_list)
        Variable.set(task_id, json.dumps(customers_dict))
    for comp_id in list(customers_dict.keys()):
        ext_schema = customers_dict[comp_id]
        logging.info(f'Task is starting for company {comp_id}')
        # creating temp table with new data increment
        query = f'''
            CREATE temporary TABLE {table}_{comp_id}_temp as
            SELECT *
            FROM {ext_schema}.{table}
            WHERE {date_column} > (
                SELECT coalesce(max({date_column}), '1970-01-01 00:00:00'::timestamp)
                FROM {schema}.{table}
                WHERE comp_id = {comp_id}
            ) and {date_column} < CURRENT_DATE + interval '8 hours'
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
            SELECT {comp_id}, id, name, description, phone, email, created_at, updated_at, patient_id, address, 
                fax, skype, balance, sync_updated_at, sync_created_at, deleted_at, license, metrc_name, 
                metrc_license, vendor_type, registration_certificate, current_timestamp as inserted_at
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
        del customers_dict[comp_id]
        logging.info(f'Number of companies left: {len(customers_dict)}')
        Variable.set(task_id, json.dumps(customers_dict))
    Variable.delete(task_id)


@task
def upsert_products(schema, table, date_column, **kwargs):
    ti, task_id = kwargs['ti'], kwargs['task'].task_id
    customers_dict = Variable.get(task_id, dict(), deserialize_json=True)
    if not customers_dict:
        comp_id_list = kwargs['dag_run'].conf.get('comp_id_list')
        customers_dict = get_customers(table, comp_id_list)
        Variable.set(task_id, json.dumps(customers_dict))
    for comp_id in list(customers_dict.keys()):
        ext_schema = customers_dict[comp_id]
        logging.info(f'Task is starting for company {comp_id}')
        # creating temp table with new data increment
        query = f'''
            CREATE temporary TABLE {table}_{comp_id}_temp as
            SELECT
                prod_id, prod_name, prod_price, prod_qty_w, prod_joints_qty_w, prod_qty_o, prod_joints_qty_o, 
                prod_vendor_id, prod_category_id, prod_photo, prod_price_type, prod_is_taxable, prod_symbol, cast(prod_gram_prepack_qty_w as BIGINT) as prod_gram_prepack_qty_w, 
                prod_eighth_prepack_qty_w, prod_quarter_prepack_qty_w, prod_half_prepack_qty_w, prod_ounce_prepack_qty_w, prod_gram_prepack_qty_o, 
                prod_eighth_prepack_qty_o, prod_quarter_prepack_qty_o, prod_half_prepack_qty_o, prod_ounce_prepack_qty_o, prod_tax_id_bak, 
                prod_is_tax_included, prod_joint_cost, prod_is_on_shop, prod_is_free_shipping, sync_updated_at, sync_created_at, prod_balance, 
                prod_is_on_weedmaps, prod_is_custom_price, prod_backend_product_id, prod_tax_profile_id, deleted_at, prod_price_preset_id, 
                prod_lab_type, prod_is_print_label, lab_result_id, prod_sku, prod_tv_photo, prod_is_hidden, strain, potify_id, is_marijuana_product, 
                marijuana_product_type, prod_is_excise, is_metrc, is_metrc_compliant, is_only_each, net_weight, net_weight_measure, show_on_leafly, 
                is_preroll, preroll_weight, product_type_id, prod_upc, is_tax_exempt, cast(is_under_package_control as boolean) as is_under_package_control, 
                directions, twcc, product_class, brand_id, potify_brand_product_id, wm_product_id, brand_product_strain_name, 
                prod_tax_tier_version_id, is_city_local_tax_exempt, brutto_weight, brutto_weight_validation, custom_cost
            FROM {ext_schema}.{table}
            WHERE {date_column} > (
                SELECT coalesce(max({date_column}), '1970-01-01 00:00:00'::timestamp)
                FROM {schema}.{table}
                WHERE comp_id = {comp_id}
            ) and {date_column} < CURRENT_DATE + interval '8 hours'
        '''
        cursor.execute(query)
        logging.info(f'Temp table is created')
        # deleting from target table data that were updated
        query = f'''
            DELETE FROM {schema}.{table}
            USING {table}_{comp_id}_temp
            WHERE {schema}.{table}.comp_id = {comp_id}
                AND {schema}.{table}.prod_id = {table}_{comp_id}_temp.prod_id
        '''
        cursor.execute(query)
        logging.info(f'{cursor.rowcount} rows deleted for {comp_id} at {datetime.now()}')
        # inserting increment to target table
        query = f'''
            INSERT INTO {schema}.{table}
            SELECT {comp_id}, prod_id, prod_name, prod_price, prod_qty_w, prod_joints_qty_w, prod_qty_o, prod_joints_qty_o, 
                prod_vendor_id, prod_category_id, prod_photo, prod_price_type, prod_is_taxable, prod_symbol, cast(prod_gram_prepack_qty_w as BIGINT) as prod_gram_prepack_qty_w, 
                prod_eighth_prepack_qty_w, prod_quarter_prepack_qty_w, prod_half_prepack_qty_w, prod_ounce_prepack_qty_w, prod_gram_prepack_qty_o, 
                prod_eighth_prepack_qty_o, prod_quarter_prepack_qty_o, prod_half_prepack_qty_o, prod_ounce_prepack_qty_o, prod_tax_id_bak, 
                prod_is_tax_included, prod_joint_cost, prod_is_on_shop, prod_is_free_shipping, sync_updated_at, sync_created_at, prod_balance, 
                prod_is_on_weedmaps, prod_is_custom_price, prod_backend_product_id, prod_tax_profile_id, deleted_at, prod_price_preset_id, 
                prod_lab_type, prod_is_print_label, lab_result_id, prod_sku, prod_tv_photo, prod_is_hidden, strain, potify_id, is_marijuana_product, 
                marijuana_product_type, prod_is_excise, is_metrc, is_metrc_compliant, is_only_each, net_weight, net_weight_measure, show_on_leafly, 
                is_preroll, preroll_weight, product_type_id, prod_upc, is_tax_exempt, cast(is_under_package_control as boolean) as is_under_package_control, 
                directions, twcc, product_class, brand_id, potify_brand_product_id, wm_product_id, brand_product_strain_name, 
                prod_tax_tier_version_id, is_city_local_tax_exempt, brutto_weight, brutto_weight_validation, custom_cost, current_timestamp as inserted_at
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
        del customers_dict[comp_id]
        logging.info(f'Number of companies left: {len(customers_dict)}')
        Variable.set(task_id, json.dumps(customers_dict))
    Variable.delete(task_id)


@task
def upsert_register_log(schema, table, date_column, **kwargs):
    ti, task_id = kwargs['ti'], kwargs['task'].task_id
    customers_dict = Variable.get(task_id, dict(), deserialize_json=True)
    if not customers_dict:
        comp_id_list = kwargs['dag_run'].conf.get('comp_id_list')
        customers_dict = get_customers(table, comp_id_list)
        Variable.set(task_id, json.dumps(customers_dict))
    for comp_id in list(customers_dict.keys()):
        ext_schema = customers_dict[comp_id]
        logging.info(f'Task is starting for company {comp_id}')
        # inserting new data with increment to target
        query = f'''
            INSERT INTO {schema}.{table}
            SELECT {comp_id} as comp_id, id, register_id, opening_amount, cash_sales, drops, expected_drawer, actual_drawer, 
                over_drawer, created_at, updated_at, "type", service_history_id, sf_guard_user_id, amount, register_type, 
                total_cost, total_profit, discount, tax, total_amount, method1_amount, method2_amount, method3_amount, 
                method4_amount, method5_amount, method6_amount, method7_amount, cash_returns, delivered_amount, pending_amount, 
                dc_cash_change, vehicle_id, current_timestamp as inserted_at
            FROM {ext_schema}.{table}
            WHERE {date_column} > (
                select coalesce(max({date_column}), '1970-01-01 00:00:00'::timestamp)
                from {schema}.{table}
                where comp_id = {comp_id}
            ) and {date_column} < CURRENT_DATE + interval '8 hours'
        '''
        cursor.execute(query)
        logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {datetime.now()}')
        # commit to target DB
        redshift_conn.commit()
        logging.info(f'Task is finished for company {comp_id}')
        del customers_dict[comp_id]
        logging.info(f'Number of companies left: {len(customers_dict)}')
        Variable.set(task_id, json.dumps(customers_dict))
    Variable.delete(task_id)


@task
def upsert_register(schema, table, date_column, **kwargs):
    ti, task_id = kwargs['ti'], kwargs['task'].task_id
    customers_dict = Variable.get(task_id, dict(), deserialize_json=True)
    if not customers_dict:
        comp_id_list = kwargs['dag_run'].conf.get('comp_id_list')
        customers_dict = get_customers(table, comp_id_list)
        Variable.set(task_id, json.dumps(customers_dict))
    for comp_id in list(customers_dict.keys()):
        ext_schema = customers_dict[comp_id]
        logging.info(f'Task is starting for company {comp_id}')
        # creating temp table with new data increment
        query = f'''
            CREATE temporary TABLE {table}_{comp_id}_temp as
            SELECT *
            FROM {ext_schema}.{table}
            WHERE {date_column} > (
                SELECT coalesce(max({date_column}), '1970-01-01 00:00:00'::timestamp)
                FROM {schema}.{table}
                WHERE comp_id = {comp_id}
            ) and {date_column} < CURRENT_DATE + interval '8 hours'
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
            SELECT {comp_id}, id, manager_sf_guard_user_id, name, status, change_status_at, "type", is_active, 
                opening_amount, cash_tenders, drops, expected_drawer, cash_in_drawer, created_at, updated_at, 
                pending_amount, pending_count, delivered_amount, delivered_count, activator_sf_guard_user_id, 
                is_deleted, latitude, longitude, total_weight, "returns", method1_amount::real, method2_amount::real, 
                method3_amount::real, method4_amount::real, method5_amount::real, method6_amount::real, method7_amount::real, 
                all_methods_total::real, port, ip_address, sync_updated_at, sync_created_at, office_id, push_new_patient, 
                push_patient_approved, push_patient_declined, push_new_order, push_order_delivered, platform, tip_amount, 
                poshub_id, eta, eta_updated_at, application_name, application_version, signin_sf_guard_user_id, dc_cash_change, 
                dispatch_orders_based_on_delivery_zones, vehicle_id, current_timestamp as inserted_at
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
        del customers_dict[comp_id]
        logging.info(f'Number of companies left: {len(customers_dict)}')
        Variable.set(task_id, json.dumps(customers_dict))
    Variable.delete(task_id)



@task
def upsert_sf_guard_user(schema, table, date_column, **kwargs):
    ti, task_id = kwargs['ti'], kwargs['task'].task_id
    customers_dict = Variable.get(task_id, dict(), deserialize_json=True)
    if not customers_dict:
        comp_id_list = kwargs['dag_run'].conf.get('comp_id_list')
        customers_dict = get_customers(table, comp_id_list)
        Variable.set(task_id, json.dumps(customers_dict))
    for comp_id in list(customers_dict.keys()):
        ext_schema = customers_dict[comp_id]
        logging.info(f'Task is starting for company {comp_id}')
        # creating temp table with new data increment
        query = f'''
            CREATE temporary TABLE {table}_{comp_id}_temp as
            SELECT *
            FROM {ext_schema}.{table}
            WHERE {date_column} > (
                SELECT coalesce(max({date_column}), '1970-01-01 00:00:00'::timestamp)
                FROM {schema}.{table}
                WHERE comp_id = {comp_id}
            ) and {date_column} < CURRENT_DATE + interval '8 hours'
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
            SELECT {comp_id}, id, staff_category_id, first_name, last_name, last_office, email_address, username, photo, dmv, 
                address, mobile_phone, phone, algorithm, "type", is_admin, access_type, office_access, is_active, is_super_admin, 
                last_login, timetracker_autocheckout_time, timetracker_hour_rate, created_at, updated_at, patient_id, is_verified, 
                is_dmv_verified, whmcs_clientid, whmcs_email, whmcs_password, user_pin, manager_pin, autologin_hash, trial_title, 
                trial_indica_description, trial_medibook_description, trial_photo_url, is_trial_support, sync_created_at, sync_updated_at, 
                wcii_hash, agilecrm_contact_id, deleted_at, storage_access, staff_id, driver_license_number, occupational_driver_license, 
                external_id, potify_driver_id, current_timestamp as inserted_at
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
        del customers_dict[comp_id]
        logging.info(f'Number of companies left: {len(customers_dict)}')
        Variable.set(task_id, json.dumps(customers_dict))
    Variable.delete(task_id)


@task
def upsert_tax_payment(schema, table, date_column, **kwargs):
    ti, task_id = kwargs['ti'], kwargs['task'].task_id
    customers_dict = Variable.get(task_id, dict(), deserialize_json=True)
    if not customers_dict:
        comp_id_list = kwargs['dag_run'].conf.get('comp_id_list')
        customers_dict = get_customers(table, comp_id_list)
        Variable.set(task_id, json.dumps(customers_dict))
    for comp_id in list(customers_dict.keys()):
        ext_schema = customers_dict[comp_id]
        logging.info(f'Task is starting for company {comp_id}')
        # creating temp table with new data increment
        query = f'''
            CREATE temporary TABLE {table}_{comp_id}_temp as
            SELECT *
            FROM {ext_schema}.{table}
            WHERE {date_column} > (
                SELECT coalesce(max({date_column}), '1970-01-01 00:00:00'::timestamp)
                FROM {schema}.{table}
                WHERE comp_id = {comp_id}
            ) and {date_column} < CURRENT_DATE + interval '8 hours'
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
            SELECT {comp_id}, id, order_id, order_item_id, product_id, state, county, city, 
                state_tax, state_mj_tax, county_tax, county_mj_tax, city_tax, city_mj_tax, 
                created_at, updated_at, is_deleted, state_sales_tax, county_sales_tax, 
                city_sales_tax, state_local_tax, county_local_tax, city_local_tax, 
                excise_tax, state_delivery_sales_tax, county_delivery_sales_tax, 
                city_delivery_sales_tax, city_delivery_local_tax, excise_delivery_tax, current_timestamp as inserted_at
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
        del customers_dict[comp_id]
        logging.info(f'Number of companies left: {len(customers_dict)}')
        Variable.set(task_id, json.dumps(customers_dict))
    Variable.delete(task_id)


@task
def upsert_warehouse_orders(schema, table, date_column, **kwargs):
    ti, task_id = kwargs['ti'], kwargs['task'].task_id
    customers_dict = Variable.get(task_id, dict(), deserialize_json=True)
    if not customers_dict:
        comp_id_list = kwargs['dag_run'].conf.get('comp_id_list')
        customers_dict = get_customers(table, comp_id_list)
        Variable.set(task_id, json.dumps(customers_dict))
    for comp_id in list(customers_dict.keys()):
        ext_schema = customers_dict[comp_id]
        logging.info(f'Task is starting for company {comp_id}')
        # creating temp table with new data increment
        query = f'''
            CREATE temporary TABLE {table}_{comp_id}_temp as
            SELECT *
            FROM {ext_schema}.{table}
            WHERE {date_column} > (
                SELECT coalesce(max({date_column}), '1970-01-01 00:00:00'::timestamp)
                FROM {schema}.{table}
                WHERE comp_id = {comp_id}
            ) and {date_column} < CURRENT_DATE + interval '8 hours'
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
            SELECT {comp_id} as comp_id, id, "number", patient_id, "type", status, order_status, payment_status, 
                fulfillment_status, shipment_status, created_at, updated_at, charge_by, amount, referral_discount_value, 
                discount_type_bak, total_amount, discount_has_changed, office_id, sum_tax, sum_discount, sum_free_discount, 
                sum_income, custom_discount_value, custom_discount_type_bak, delivery_address, delivery_city, delivery_state, 
                delivery_zip, delivery_phone, delivery_latitude, delivery_longitude, shipping_method_id, shipping_amount, 
                courier_register_id, "comment", sync_updated_at, sync_created_at, register_id, discount_id, 
                referral_discount_type, custom_discount_type, balance, method1_amount, method2_amount, method3_amount, 
                method4_amount, method5_amount, method6_amount, method7_amount, processing_register_id, photo, 
                delivery_datetime, delivery_address_id, change_amount, tip_amount, placed_at, completed_at, confirmed_at, 
                preferred_payment_method, is_bonus_point_as_discount, marketplace, applied_potify_credits, asap_delivery, 
                cashier_id, is_transit_started, metrc_status, cashier_name, patient_type, register_name, courier_id, 
                courier_name, courier_register_name, is_verified_by_courier, is_shipped, shipping_tracking_number, 
                patient_has_caregiver, patient_is_tax_exempt, metrc_substatus, checkout_staff_id, pos_mode, signature, 
                delivery_method, courier_number, patient_rec_number, office_zip_name, refund_type, returned_at, 
                shipping_method_name, tax_tier_version_id, vehicle, metrc_delivery_status, resend_staff_id, 
                delivery_estimated_time_of_arrival, current_timestamp as inserted_at
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
        del customers_dict[comp_id]
        logging.info(f'Number of companies left: {len(customers_dict)}')
        Variable.set(task_id, json.dumps(customers_dict))
    Variable.delete(task_id)


@task
def upsert_warehouse_order_items(schema, table, date_column, **kwargs):
    ti, task_id = kwargs['ti'], kwargs['task'].task_id
    customers_dict = Variable.get(task_id, dict(), deserialize_json=True)
    if not customers_dict:
        comp_id_list = kwargs['dag_run'].conf.get('comp_id_list')
        customers_dict = get_customers(table, comp_id_list)
        Variable.set(task_id, json.dumps(customers_dict))
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
            WHERE {table}.{date_column} > (
                SELECT coalesce(max({date_column}), '1970-01-01 00:00:00'::timestamp)
                FROM {schema}.{table}
                WHERE comp_id = {comp_id}
            ) and {table}.{date_column} < CURRENT_DATE + interval '8 hours'
                and warehouse_orders.confirmed_at IS NOT NULL
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
        del customers_dict[comp_id]
        logging.info(f'Number of companies left: {len(customers_dict)}')
        Variable.set(task_id, json.dumps(customers_dict))
    Variable.delete(task_id)


@task
def upsert_service_history(schema, table, date_column, **kwargs):
    ti, task_id = kwargs['ti'], kwargs['task'].task_id
    customers_dict = Variable.get(task_id, dict(), deserialize_json=True)
    if not customers_dict:
        comp_id_list = kwargs['dag_run'].conf.get('comp_id_list')
        customers_dict = get_customers(table, comp_id_list)
        Variable.set(task_id, json.dumps(customers_dict))
    for comp_id in list(customers_dict.keys()):
        ext_schema = customers_dict[comp_id]
        logging.info(f'Task is starting for company {comp_id}')
        # creating temp table with new data increment
        query = f'''
            CREATE temporary TABLE {table}_{comp_id}_temp as
            SELECT *
            FROM {ext_schema}.{table}
            WHERE {date_column} > (
                SELECT coalesce(max({date_column}), '1970-01-01 00:00:00'::timestamp)
                FROM {schema}.{table}
                WHERE comp_id = {comp_id}
            ) and {date_column} < CURRENT_DATE + interval '8 hours'
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
            SELECT {comp_id} as comp_id, id, service_id, office_id, user_id, patient_id, doctor_id, 
                object_id, object_type, referal_id, notes, amount, edit_amount, edit_reason, created_at, 
                updated_at, ad_campaign_id, ad_campaign_patient_type_id, intake, exam, has_intake_for_approving, 
                has_exam_for_approving, order_id, amount_by_referral_points, count_referral_points, register_id, 
                balance, method1_amount, method2_amount, method3_amount, method4_amount, method5_amount, method6_amount, 
                method7_amount, profit, is_take_payment, tax, dc_cash_change, is_dejavoo_payment, "type", cost, discount, 
                "free", payment_method, payment_amount, current_timestamp as inserted_at
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
        del customers_dict[comp_id]
        logging.info(f'Number of companies left: {len(customers_dict)}')
        Variable.set(task_id, json.dumps(customers_dict))
    Variable.delete(task_id)


@task
def upsert_product_checkins(schema, table, date_column, **kwargs):
    ti, task_id = kwargs['ti'], kwargs['task'].task_id
    customers_dict = Variable.get(task_id, dict(), deserialize_json=True)
    if not customers_dict:
        comp_id_list = kwargs['dag_run'].conf.get('comp_id_list')
        customers_dict = get_customers(table, comp_id_list)
        Variable.set(task_id, json.dumps(customers_dict))
    for comp_id in list(customers_dict.keys()):
        ext_schema = customers_dict[comp_id]
        logging.info(f'Task is starting for company {comp_id}')
        # creating temp table with new data increment
        query = f'''
            CREATE temporary TABLE {table}_{comp_id}_temp as
            SELECT *
            FROM {ext_schema}.{table}
            WHERE {date_column} > (
                SELECT coalesce(max({date_column}), '1970-01-01 00:00:00'::timestamp)
                FROM {schema}.{table}
                WHERE comp_id = {comp_id}
            ) and {date_column} < CURRENT_DATE + interval '8 hours'
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
            SELECT {comp_id}, id, product_id, vendor_id, user_id, qty, price, "date", 
                status, balance, note, batch_id, lab_result_id, has_lab_result, uid, 
                harvest_date, sync_created_at, sync_updated_at, opc, sale_qty, office_id, 
                is_metrc, available_qty, is_finished, producer_id, vendor_type, vendor_name, 
                facility_id, is_special, packaged_date, best_by_date, deleted_at, production_run, 
                is_under_package_control, is_form_modified, metrc_initial_quantity, external_barcode, 
                packaged_by_id, manifest, is_sample_package, paused_to_datetime, excise_tax_paid, current_timestamp as inserted_at
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
        del customers_dict[comp_id]
        logging.info(f'Number of companies left: {len(customers_dict)}')
        Variable.set(task_id, json.dumps(customers_dict))
    Variable.delete(task_id)


@task
def upsert_product_office_qty(schema, table, **kwargs):
    ti, task_id = kwargs['ti'], kwargs['task'].task_id
    customers_dict = Variable.get(task_id, dict(), deserialize_json=True)
    if not customers_dict:
        comp_id_list = kwargs['dag_run'].conf.get('comp_id_list')
        customers_dict = get_customers(table, comp_id_list)
        Variable.set(task_id, json.dumps(customers_dict))
    for comp_id in list(customers_dict.keys()):
        ext_schema = customers_dict[comp_id]
        logging.info(f'Task is starting for company {comp_id}')
        # deleting old data from target
        query = f'''
            DELETE FROM {schema}.{table}
            WHERE comp_id = {comp_id}
        '''
        cursor.execute(query)
        logging.info(f'{cursor.rowcount} rows deleted for {comp_id} at {datetime.now()}')
        # inserting new data to target
        query = f'''
            INSERT INTO {schema}.{table}
            SELECT {comp_id}, poq_id, poq_prod_id, poq_office_id, poq_qty, poq_item_type, 
                sync_created_at, sync_updated_at, reserved_qty, paused_qty, current_timestamp as inserted_at
            FROM {ext_schema}.{table}
        '''
        cursor.execute(query)
        logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {datetime.now()}')
        # commit to target DB
        redshift_conn.commit()
        logging.info(f'Task is finished for company {comp_id}')
        del customers_dict[comp_id]
        logging.info(f'Number of companies left: {len(customers_dict)}')
        Variable.set(task_id, json.dumps(customers_dict))
    Variable.delete(task_id)


@task
def upsert_warehouse_order_logs(schema, table, date_column, **kwargs):
    ti, task_id = kwargs['ti'], kwargs['task'].task_id
    customers_dict = Variable.get(task_id, dict(), deserialize_json=True)
    if not customers_dict:
        comp_id_list = kwargs['dag_run'].conf.get('comp_id_list')
        customers_dict = get_customers(table, comp_id_list)
        Variable.set(task_id, json.dumps(customers_dict))
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
        comp_id = list(customers_dict.items())[0][0]
        ext_schema = list(customers_dict.items())[0][1]
        query = f'''
            create table {schema}.{table} as
            select {comp_id} as comp_id, id, order_id, "type", sf_guard_user_id, sf_guard_user_name, 
                order_courier_register_id, created_at, register_id, application, current_timestamp as inserted_at
            from {ext_schema}.{table}
            where 1 != 1
            '''
        cursor.execute(query)
        redshift_conn.commit()
        logging.info(f'Table {schema}.{table} created successfully')
    for comp_id in list(customers_dict.keys()):
        ext_schema = customers_dict[comp_id]
        logging.info(f'Task is starting for company {comp_id}')
        # inserting new data with increment to target
        query = f'''
            INSERT INTO {schema}.{table}
            SELECT {comp_id} as comp_id, id, order_id, "type", sf_guard_user_id, sf_guard_user_name, order_courier_register_id, created_at, register_id, application, current_timestamp as inserted_at
            FROM {ext_schema}.{table}
            WHERE
                {date_column} > (
                    SELECT coalesce(max({date_column}), '1970-01-01 00:00:00'::timestamp)
                    FROM {schema}.{table}
                    WHERE comp_id = {comp_id} 
                ) AND {date_column} < CURRENT_DATE + interval '8 hours'
            '''
        cursor.execute(query)
        logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {datetime.now()}')
        # commit to target DB
        redshift_conn.commit()
        logging.info(f'Task is finished for company {comp_id}')
        del customers_dict[comp_id]
        logging.info(f'Number of companies left: {len(customers_dict)}')
        Variable.set(task_id, json.dumps(customers_dict))
    Variable.delete(task_id)


@task
def upsert_user_activity_record(schema, table, date_column, **kwargs):
    ti, task_id = kwargs['ti'], kwargs['task'].task_id
    customers_dict = Variable.get(task_id, dict(), deserialize_json=True)
    if not customers_dict:
        comp_id_list = kwargs['dag_run'].conf.get('comp_id_list')
        customers_dict = get_customers(table, comp_id_list)
        Variable.set(task_id, json.dumps(customers_dict))
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
        comp_id = list(customers_dict.items())[0][0]
        ext_schema = list(customers_dict.items())[0][1]
        query = f'''
            create table {schema}.{table} as
            select {comp_id} as comp_id, id, sf_guard_user_id, "type", description, ip, created_at, updated_at, current_timestamp as inserted_at
            from {ext_schema}.{table}
            where 1 != 1
            '''
        cursor.execute(query)
        redshift_conn.commit()
        logging.info(f'Table {schema}.{table} created successfully')
    for comp_id in list(customers_dict.keys()):
        ext_schema = customers_dict[comp_id]
        logging.info(f'Task is starting for company {comp_id}')
        # creating temp table with new data increment
        query = f'''
            CREATE temporary TABLE {table}_{comp_id}_temp as
            SELECT *
            FROM {ext_schema}.{table}
            WHERE {date_column} > (
                SELECT coalesce(max({date_column}), '1970-01-01 00:00:00'::timestamp)
                FROM {schema}.{table}
                WHERE comp_id = {comp_id}
            ) and {date_column} < CURRENT_DATE + interval '8 hours'
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
            SELECT {comp_id}, id, sf_guard_user_id, "type", description, ip, created_at, updated_at, current_timestamp as inserted_at
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
        del customers_dict[comp_id]
        logging.info(f'Number of companies left: {len(customers_dict)}')
        Variable.set(task_id, json.dumps(customers_dict))
    Variable.delete(task_id)


@task
def upsert_sf_guard_user_group(schema, table, date_column, **kwargs):
    ti, task_id = kwargs['ti'], kwargs['task'].task_id
    customers_dict = Variable.get(task_id, dict(), deserialize_json=True)
    if not customers_dict:
        comp_id_list = kwargs['dag_run'].conf.get('comp_id_list')
        customers_dict = get_customers(table, comp_id_list)
        Variable.set(task_id, json.dumps(customers_dict))
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
        comp_id = list(customers_dict.items())[0][0]
        ext_schema = list(customers_dict.items())[0][1]
        query = f'''
            create table {schema}.{table} as
            select {comp_id} as comp_id, user_id, group_id, created_at, updated_at, current_timestamp as inserted_at
            from {ext_schema}.{table}
            where 1 != 1
            '''
        cursor.execute(query)
        redshift_conn.commit()
        logging.info(f'Table {schema}.{table} created successfully')
    for comp_id in list(customers_dict.keys()):
        ext_schema = customers_dict[comp_id]
        logging.info(f'Task is starting for company {comp_id}')
        # creating temp table with new data increment
        query = f'''
            CREATE temporary TABLE {table}_{comp_id}_temp as
            SELECT *
            FROM {ext_schema}.{table}
            WHERE {date_column} > (
                SELECT coalesce(max({date_column}), '1970-01-01 00:00:00'::timestamp)
                FROM {schema}.{table}
                WHERE comp_id = {comp_id}
            ) and {date_column} < CURRENT_DATE + interval '8 hours'
        '''
        cursor.execute(query)
        logging.info(f'Temp table is created')
        # deleting from target table data that were updated
        query = f'''
            DELETE FROM {schema}.{table}
            USING {table}_{comp_id}_temp
            WHERE {schema}.{table}.comp_id = {comp_id}
                AND {schema}.{table}.user_id = {table}_{comp_id}_temp.user_id
                AND {schema}.{table}.group_id = {table}_{comp_id}_temp.group_id
        '''
        cursor.execute(query)
        logging.info(f'{cursor.rowcount} rows deleted for {comp_id} at {datetime.now()}')
        # inserting increment to target table
        query = f'''
            INSERT INTO {schema}.{table}
            SELECT {comp_id}, user_id, group_id, created_at, updated_at, current_timestamp as inserted_at
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
        del customers_dict[comp_id]
        logging.info(f'Number of companies left: {len(customers_dict)}')
        Variable.set(task_id, json.dumps(customers_dict))
    Variable.delete(task_id)


@task
def upsert_sf_guard_group(schema, table, date_column, **kwargs):
    ti, task_id = kwargs['ti'], kwargs['task'].task_id
    customers_dict = Variable.get(task_id, dict(), deserialize_json=True)
    if not customers_dict:
        comp_id_list = kwargs['dag_run'].conf.get('comp_id_list')
        customers_dict = get_customers(table, comp_id_list)
        Variable.set(task_id, json.dumps(customers_dict))
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
        comp_id = list(customers_dict.items())[0][0]
        ext_schema = list(customers_dict.items())[0][1]
        query = f'''
            create table {schema}.{table} as
            select {comp_id} as comp_id, id, "name", description, is_main, created_at, updated_at, current_timestamp as inserted_at
            from {ext_schema}.{table}
            where 1 != 1
            '''
        cursor.execute(query)
        redshift_conn.commit()
        logging.info(f'Table {schema}.{table} created successfully')
    for comp_id in list(customers_dict.keys()):
        ext_schema = customers_dict[comp_id]
        logging.info(f'Task is starting for company {comp_id}')
        # creating temp table with new data increment
        query = f'''
            CREATE temporary TABLE {table}_{comp_id}_temp as
            SELECT *
            FROM {ext_schema}.{table}
            WHERE {date_column} > (
                SELECT coalesce(max({date_column}), '1970-01-01 00:00:00'::timestamp)
                FROM {schema}.{table}
                WHERE comp_id = {comp_id}
            ) and {date_column} < CURRENT_DATE + interval '8 hours'
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
            SELECT {comp_id}, id, "name", description, is_main, created_at, updated_at, current_timestamp as inserted_at
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
        del customers_dict[comp_id]
        logging.info(f'Number of companies left: {len(customers_dict)}')
        Variable.set(task_id, json.dumps(customers_dict))
    Variable.delete(task_id)


@task_group
def upsert_tables(schema='staging'):
    upsert_brands(schema, table='brands', date_column='sync_updated_at')
    upsert_company_config(schema, table='company_config') 
    upsert_discounts(schema, table='discounts', date_column='updated_at')
    upsert_patient_group_ref(schema, table='patient_group_ref', date_column='sync_updated_at')
    upsert_patient_group(schema, table='patient_group', date_column='sync_updated_at')
    upsert_patients(schema, table='patients', date_column='updated_at')
    upsert_product_categories(schema, table='product_categories')
    upsert_product_checkins(schema, table='product_checkins', date_column='sync_updated_at')
    upsert_product_filter_index(schema, table='product_filter_index')
    upsert_product_office_qty(schema, table='product_office_qty')
    upsert_product_transactions(schema, table='product_transactions', date_column='date')
    upsert_product_vendors(schema, table='product_vendors', date_column='updated_at')
    upsert_products(schema, table='products', date_column='sync_updated_at')
    upsert_register_log(schema, table='register_log', date_column='created_at')
    upsert_register(schema, table='register', date_column='updated_at')
    upsert_service_history(schema, table='service_history', date_column='updated_at')
    upsert_sf_guard_user(schema, table='sf_guard_user', date_column='updated_at')
    upsert_tax_payment(schema, table='tax_payment', date_column='updated_at')
    upsert_warehouse_orders(schema, table='warehouse_orders', date_column='updated_at')
    upsert_warehouse_order_items(schema, table='warehouse_order_items', date_column='updated_at')
    upsert_warehouse_order_logs(schema, table='warehouse_order_logs', date_column='created_at')
    upsert_user_activity_record(schema, table='user_activity_record', date_column='updated_at')
    upsert_sf_guard_user_group(schema, table='sf_guard_user_group', date_column='updated_at')
    upsert_sf_guard_group(schema, table='sf_guard_group', date_column='updated_at')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'on_failure_callback': failure_slack_alert,
    'on_retry_callback': retry_slack_alert,
    'retries': 10,
    'retry_delay': timedelta(seconds=60)
}


with DAG(
    dag_id='update_everything_new',
    max_active_tasks=32,
    schedule='0 8 * * *', # UTC time
    start_date=datetime(year=2022, month=12, day=8),
    default_args=default_args,
    catchup=False,
) as dag:
    start_alert = EmptyOperator(task_id="start_alert", on_success_callback=start_slack_alert)
    # get_customers_task = get_customers()
    upsert_tables_group = upsert_tables()
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

    start_alert >> upsert_tables_group >> dbt_run >> dbt_snapshot >> dbt_test >> success_alert