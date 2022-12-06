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
        SELECT comp_id, db_name
        FROM ext_indica_backend.companies
        WHERE 1=1
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
            and comp_is_approved = 1 
            and comp_is_disabled = 0
            and comp_id in (3628, 4546)
        ORDER BY comp_id
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


def upsert_company_config(ti):
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
                DELETE FROM staging.company_config
                WHERE comp_id = {comp_id}
            '''
            cursor.execute(query)
            query = f'''
                INSERT INTO staging.company_config
                SELECT {comp_id}, *
                FROM {ext_schema}.company_config
            '''
            cursor.execute(query)


def upsert_discounts(ti):
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
                CREATE temporary TABLE discounts_{comp_id}_temp as
                SELECT *
                from {ext_schema}.discounts
                where updated_at > (
                    select coalesce(max(updated_at), '1970-01-01 00:00:00'::timestamp)
                    from staging.discounts
                    where comp_id = {comp_id}
                )
            '''
            cursor.execute(query)
            query = f'''
                DELETE FROM staging.discounts
                USING discounts_{comp_id}_temp
                WHERE staging.discounts.comp_id = {comp_id}
                    AND staging.discounts.id = discounts_{comp_id}_temp.id
            '''
            cursor.execute(query)
            query = f'''
                INSERT INTO staging.discounts
                SELECT {comp_id}, id, "name", "type", value, sync_created_at, sync_updated_at, 
                    deleted_at, use_type, apply_type, is_pos, is_potify, promo_code, status, 
                    is_individual_use_only, is_exclude_items_on_special, start_date, end_date, 
                    is_ongoing, happy_weekdays, min_subtotal_price, uses_count, is_once_per_patient, 
                    bogo_buy, bogo_get, bogo_multiple, is_first_time_patient, is_show_promo_code_on_potify, 
                    max_subtotal_price, display_name, is_show_name_on_collection_tile, image, tv_image, 
                    product_filter_id, created_at, updated_at, hide_banner, display_priority
                FROM discounts_{comp_id}_temp
            '''
            cursor.execute(query)
            query = f'''
                DROP TABLE discounts_{comp_id}_temp
            '''
            cursor.execute(query)


def upsert_patient_group_ref(ti):
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
                CREATE temporary TABLE patient_group_ref_{comp_id}_temp as
                SELECT *
                FROM {ext_schema}.patient_group_ref
                WHERE sync_updated_at > (
                    SELECT coalesce(max(sync_updated_at), '1970-01-01 00:00:00'::timestamp)
                    FROM staging.patient_group_ref
                    WHERE comp_id = {comp_id}
                )
            '''
            cursor.execute(query)
            query = f'''
                DELETE FROM staging.patient_group_ref
                USING patient_group_ref_{comp_id}_temp
                WHERE staging.patient_group_ref.comp_id = {comp_id}
                    AND staging.patient_group_ref.id = patient_group_ref_{comp_id}_temp.id
            '''
            cursor.execute(query)
            query = f'''
                INSERT INTO staging.patient_group_ref
                SELECT {comp_id}, id, patient_id, group_id, sync_created_at, sync_updated_at
                FROM patient_group_ref_{comp_id}_temp
            '''
            cursor.execute(query)
            query = f'''
                DROP TABLE patient_group_ref_{comp_id}_temp
            '''
            cursor.execute(query)


def upsert_patient_group(ti):
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
                CREATE temporary TABLE patient_group_{comp_id}_temp as
                SELECT *
                FROM {ext_schema}.patient_group
                WHERE sync_updated_at > (
                    SELECT coalesce(max(sync_updated_at), '1970-01-01 00:00:00'::timestamp)
                    FROM staging.patient_group
                    WHERE comp_id = {comp_id}
                )
            '''
            cursor.execute(query)
            query = f'''
                DELETE FROM staging.patient_group
                USING patient_group_{comp_id}_temp
                WHERE staging.patient_group.comp_id = {comp_id}
                    AND staging.patient_group.id = patient_group_{comp_id}_temp.id
            '''
            cursor.execute(query)
            query = f'''
                INSERT INTO staging.patient_group
                SELECT {comp_id}, id, "name", sync_created_at, sync_updated_at, is_auto, start_date, end_date
                FROM patient_group_{comp_id}_temp
            '''
            cursor.execute(query)
            query = f'''
                DROP TABLE patient_group_{comp_id}_temp
            '''
            cursor.execute(query)


def upsert_patients(ti):
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
                CREATE temporary TABLE patients_{comp_id}_temp as
                SELECT *
                FROM {ext_schema}.patients
                WHERE sync_updated_at > (
                    SELECT coalesce(max(sync_updated_at), '1970-01-01 00:00:00'::timestamp)
                    FROM staging.patients
                    WHERE comp_id = {comp_id}
                )
            '''
            cursor.execute(query)
            query = f'''
                DELETE FROM staging.patients
                USING patients_{comp_id}_temp
                WHERE staging.patients.comp_id = {comp_id}
                    AND staging.patients.id = patients_{comp_id}_temp.id
            '''
            cursor.execute(query)
            query = f'''
                INSERT INTO staging.patients
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
                    phone_consent_signature, email_consent_given_at, email_consent_signature, deleted_at, tax_tier_id, buyer_status, marketplace
                FROM patients_{comp_id}_temp
            '''
            cursor.execute(query)
            query = f'''
                DROP TABLE patients_{comp_id}_temp
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
    task_upsert_company_config = PythonOperator(
        task_id='upsert_company_config',
        python_callable=upsert_company_config
    )
    task_upsert_discounts = PythonOperator(
        task_id='upsert_discounts',
        python_callable=upsert_discounts
    )
    task_upsert_patient_group_ref = PythonOperator(
        task_id='upsert_patient_group_ref',
        python_callable=upsert_patient_group_ref
    )
    task_upsert_patient_group = PythonOperator(
        task_id='upsert_patient_group',
        python_callable=upsert_patient_group
    )
    task_upsert_patients = PythonOperator(
        task_id='upsert_patients',
        python_callable=upsert_patients
    )

    task_get_customers >> [
        task_upsert_brands, 
        task_upsert_company_config, 
        task_upsert_discounts, 
        task_upsert_patient_group_ref,
        task_upsert_patient_group,
        task_upsert_patients
    ]




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
