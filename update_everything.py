from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
# from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow_dbt_python.operators.dbt import DbtRunOperator, DbtTestOperator
import logging



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['i.plotnikov@telemedicine.ge', 'd.prokopev@telemedicine.ge'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 10,
    'retry_delay': timedelta(minutes=1)
}


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
            and (db_name like '%_company'
	            and is_blank = 0
	            and comp_project = 'Indica'
	            and not domain_prefix like '%prod'
	            and not domain_prefix like 'test%'
	            and not domain_prefix like '%demo%'
	            and not comp_email like '%maildrop.cc'
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
	        )
            or (comp_id in (10461, 9868))
        ORDER BY comp_id
    '''
    cursor.execute(query)
    logging.info(query)
    data = cursor.fetchall()
    logging.info(f'The number of companies is being processing: {len(data)}')
    return data


def upsert_brands(customers):
    if not customers:
        raise Exception('No customers found')
    else:
        redshift_hook = RedshiftSQLHook(
                postgres_conn_id='redshift_default',
                schema='dev'    
            )
        redshift_conn = redshift_hook.get_conn()
        for comp_id, ext_schema in customers:
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


def upsert_company_config(customers):
    if not customers:
        raise Exception('No customers found')
    else:
        redshift_hook = RedshiftSQLHook(
                postgres_conn_id='redshift_default',
                schema='dev'
            )
        redshift_conn = redshift_hook.get_conn()
        for comp_id, ext_schema in customers:
            logging.info(f'Task is starting for company {comp_id}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    DELETE FROM staging.company_config
                    WHERE comp_id = {comp_id}
                '''
                cursor.execute(query)
                logging.info(f'{cursor.rowcount} rows deleted for {comp_id} at {datetime.now()}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    INSERT INTO staging.company_config
                    SELECT {comp_id}, id, "name", human_name, value
                    FROM {ext_schema}.company_config
                '''
                cursor.execute(query)
                logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {datetime.now()}')
            redshift_conn.commit()
            logging.info(f'Task is finished for company {comp_id}')


def upsert_discounts(customers):
    if not customers:
        raise Exception('No customers found')
    else:
        redshift_hook = RedshiftSQLHook(
                postgres_conn_id='redshift_default',
                schema='dev'
            )
        redshift_conn = redshift_hook.get_conn()
        for comp_id, ext_schema in customers:
            logging.info(f'Task is starting for company {comp_id}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    CREATE temporary TABLE discounts_{comp_id}_temp as
                    SELECT *
                    from {ext_schema}.discounts
                    where updated_at > (
                        select coalesce(max(updated_at), '1970-01-01 00:00:00'::timestamp)
                        from staging.discounts
                        where comp_id = {comp_id}
                    ) and updated_at < CURRENT_DATE + interval '8 hours'
                '''
                cursor.execute(query)
                logging.info(f'Temp table is created')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    DELETE FROM staging.discounts
                    USING discounts_{comp_id}_temp
                    WHERE staging.discounts.comp_id = {comp_id}
                        AND staging.discounts.id = discounts_{comp_id}_temp.id
                '''
                cursor.execute(query)
                logging.info(f'{cursor.rowcount} rows deleted for {comp_id} at {datetime.now()}')
            with redshift_conn.cursor() as cursor:
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
                logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {datetime.now()}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    DROP TABLE discounts_{comp_id}_temp
                '''
                cursor.execute(query)
                logging.info(f'Temp table is dropped')
            redshift_conn.commit()
            logging.info(f'Task is finished for company {comp_id}')


def upsert_patient_group_ref(customers):
    if not customers:
        raise Exception('No customers found')
    else:
        redshift_hook = RedshiftSQLHook(
                postgres_conn_id='redshift_default',
                schema='dev'
            )
        redshift_conn = redshift_hook.get_conn()
        for comp_id, ext_schema in customers:
            logging.info(f'Task is starting for company {comp_id}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    CREATE temporary TABLE patient_group_ref_{comp_id}_temp as
                    SELECT *
                    FROM {ext_schema}.patient_group_ref
                    WHERE sync_updated_at > (
                        SELECT coalesce(max(sync_updated_at), '1970-01-01 00:00:00'::timestamp)
                        FROM staging.patient_group_ref
                        WHERE comp_id = {comp_id}
                    ) and sync_updated_at < CURRENT_DATE + interval '8 hours'
                '''
                cursor.execute(query)
                logging.info(f'Temp table is created')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    DELETE FROM staging.patient_group_ref
                    USING patient_group_ref_{comp_id}_temp
                    WHERE staging.patient_group_ref.comp_id = {comp_id}
                        AND staging.patient_group_ref.id = patient_group_ref_{comp_id}_temp.id
                '''
                cursor.execute(query)
                logging.info(f'{cursor.rowcount} rows deleted for {comp_id} at {datetime.now()}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    INSERT INTO staging.patient_group_ref
                    SELECT {comp_id}, id, patient_id, group_id, sync_created_at, sync_updated_at
                    FROM patient_group_ref_{comp_id}_temp
                '''
                cursor.execute(query)
                logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {datetime.now()}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    DROP TABLE patient_group_ref_{comp_id}_temp
                '''
                cursor.execute(query)
                logging.info(f'Temp table is dropped')
            redshift_conn.commit()
            logging.info(f'Task is finished for company {comp_id}')


def upsert_patient_group(customers):
    if not customers:
        raise Exception('No customers found')
    else:
        redshift_hook = RedshiftSQLHook(
                postgres_conn_id='redshift_default',
                schema='dev'
            )
        redshift_conn = redshift_hook.get_conn()
        for comp_id, ext_schema in customers:
            logging.info(f'Task is starting for company {comp_id}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    CREATE temporary TABLE patient_group_{comp_id}_temp as
                    SELECT *
                    FROM {ext_schema}.patient_group
                    WHERE sync_updated_at > (
                        SELECT coalesce(max(sync_updated_at), '1970-01-01 00:00:00'::timestamp)
                        FROM staging.patient_group
                        WHERE comp_id = {comp_id}
                    ) and sync_updated_at < CURRENT_DATE + interval '8 hours'
                '''
                cursor.execute(query)
                logging.info(f'Temp table is created')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    DELETE FROM staging.patient_group
                    USING patient_group_{comp_id}_temp
                    WHERE staging.patient_group.comp_id = {comp_id}
                        AND staging.patient_group.id = patient_group_{comp_id}_temp.id
                '''
                cursor.execute(query)
                logging.info(f'{cursor.rowcount} rows deleted for {comp_id} at {datetime.now()}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    INSERT INTO staging.patient_group
                    SELECT {comp_id}, id, "name", sync_created_at, sync_updated_at, is_auto, start_date, end_date
                    FROM patient_group_{comp_id}_temp
                '''
                cursor.execute(query)
                logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {datetime.now()}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    DROP TABLE patient_group_{comp_id}_temp
                '''
                cursor.execute(query)
                logging.info(f'Temp table is dropped')
            redshift_conn.commit()
            logging.info(f'Task is finished for company {comp_id}')


def upsert_patients(customers):
    if not customers:
        raise Exception('No customers found')
    else:
        redshift_hook = RedshiftSQLHook(
                postgres_conn_id='redshift_default',
                schema='dev'
            )
        redshift_conn = redshift_hook.get_conn()
        for comp_id, ext_schema in customers:
            logging.info(f'Task is starting for company {comp_id}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    CREATE temporary TABLE patients_{comp_id}_temp as
                    SELECT *
                    FROM {ext_schema}.patients
                    WHERE updated_at > (
                        SELECT coalesce(max(updated_at), '1970-01-01 00:00:00'::timestamp)
                        FROM staging.patients
                        WHERE comp_id = {comp_id}
                    ) and updated_at < CURRENT_DATE + interval '8 hours'
                '''
                cursor.execute(query)
                logging.info(f'Temp table is created')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    DELETE FROM staging.patients
                    USING patients_{comp_id}_temp
                    WHERE staging.patients.comp_id = {comp_id}
                        AND staging.patients.pat_id = patients_{comp_id}_temp.pat_id
                '''
                cursor.execute(query)
                logging.info(f'{cursor.rowcount} rows deleted for {comp_id} at {datetime.now()}')
            with redshift_conn.cursor() as cursor:
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
                logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {datetime.now()}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    DROP TABLE patients_{comp_id}_temp
                '''
                cursor.execute(query)
                logging.info(f'Temp table is dropped')
            redshift_conn.commit()
            logging.info(f'Task is finished for company {comp_id}')


def upsert_product_categories(customers):
    if not customers:
        raise Exception('No customers found')
    else:
        redshift_hook = RedshiftSQLHook(
                postgres_conn_id='redshift_default',
                schema='dev'
            )
        redshift_conn = redshift_hook.get_conn()
        for comp_id, ext_schema in customers:
            logging.info(f'Task is starting for company {comp_id}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    DELETE FROM staging.product_categories
                    WHERE comp_id = {comp_id}
                '''
                cursor.execute(query)
                logging.info(f'{cursor.rowcount} rows deleted for {comp_id} at {datetime.now()}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    INSERT INTO staging.product_categories
                    SELECT {comp_id}, id, "name", description, photo, lft, rgt, 
                        "level", created_at, updated_at, is_system, sync_updated_at, 
                        sync_created_at, icon_name, activation_time, 
                        label_template_internal_id, icon_color, system_id
                    FROM {ext_schema}.product_categories
                '''
                cursor.execute(query)
                logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {datetime.now()}')
            redshift_conn.commit()
            logging.info(f'Task is finished for company {comp_id}')


def upsert_product_filter_index(customers):
    if not customers:
        raise Exception('No customers found')
    else:
        redshift_hook = RedshiftSQLHook(
                postgres_conn_id='redshift_default',
                schema='dev'
            )
        redshift_conn = redshift_hook.get_conn()
        for comp_id, ext_schema in customers:
            logging.info(f'Task is starting for company {comp_id}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    DELETE FROM staging.product_filter_index
                    WHERE comp_id = {comp_id}
                '''
                cursor.execute(query)
                logging.info(f'{cursor.rowcount} rows deleted for {comp_id} at {datetime.now()}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    INSERT INTO staging.product_filter_index
                    SELECT {comp_id}, id, product_id, product_filter_id
                    FROM {ext_schema}.product_filter_index
                '''
                cursor.execute(query)
                logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {datetime.now()}')
            redshift_conn.commit()
            logging.info(f'Task is finished for company {comp_id}')


def upsert_product_transactions(customers):
    if not customers:
        raise Exception('No customers found')
    else:
        redshift_hook = RedshiftSQLHook(
                postgres_conn_id='redshift_default',
                schema='dev'
            )
        redshift_conn = redshift_hook.get_conn()
        for comp_id, ext_schema in customers:
            logging.info(f'Task is starting for company {comp_id}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    INSERT INTO staging.product_transactions
                    SELECT
                        {comp_id} as comp_id, id, product_id, office_id, doctor_id, patient_id, user_id, type, 
                        qty, price, price_per, total_price, date, note, item_type, 
                        transfer_direction, qty_free, product_checkin_id, product_name, 
                        office_to_id, product_to_id, product_to_name, cost, order_id, 
                        base_weight, product_checkin_to_id, office_name
                    FROM {ext_schema}.product_transactions
                    WHERE date > (
                        select coalesce(max(date), '1970-01-01 00:00:00'::timestamp)
                        from staging.product_transactions
                        where comp_id = {comp_id}
                    ) and date < CURRENT_DATE + interval '8 hours'
                '''
                cursor.execute(query)
                logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {datetime.now()}')
            redshift_conn.commit()
            logging.info(f'Task is finished for company {comp_id}')


def upsert_product_vendors(customers):
    if not customers:
        raise Exception('No customers found')
    else:
        redshift_hook = RedshiftSQLHook(
                postgres_conn_id='redshift_default',
                schema='dev'
            )
        redshift_conn = redshift_hook.get_conn()
        for comp_id, ext_schema in customers:
            logging.info(f'Task is starting for company {comp_id}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    CREATE temporary TABLE product_vendors_{comp_id}_temp as
                    SELECT *
                    FROM {ext_schema}.product_vendors
                    WHERE updated_at > (
                        SELECT coalesce(max(updated_at), '1970-01-01 00:00:00'::timestamp)
                        FROM staging.product_vendors
                        WHERE comp_id = {comp_id}
                    ) and updated_at < CURRENT_DATE + interval '8 hours'
                '''
                cursor.execute(query)
                logging.info(f'Temp table is created')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    DELETE FROM staging.product_vendors
                    USING product_vendors_{comp_id}_temp
                    WHERE staging.product_vendors.comp_id = {comp_id}
                        AND staging.product_vendors.id = product_vendors_{comp_id}_temp.id
                '''
                cursor.execute(query)
                logging.info(f'{cursor.rowcount} rows deleted for {comp_id} at {datetime.now()}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    INSERT INTO staging.product_vendors
                    SELECT {comp_id}, id, name, description, phone, email, created_at, updated_at, patient_id, address, 
                        fax, skype, balance, sync_updated_at, sync_created_at, deleted_at, license, metrc_name, 
                        metrc_license, vendor_type, registration_certificate
                    FROM product_vendors_{comp_id}_temp
                '''
                cursor.execute(query)
                logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {datetime.now()}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    DROP TABLE product_vendors_{comp_id}_temp
                '''
                cursor.execute(query)
                logging.info(f'Temp table is dropped')
            redshift_conn.commit()
            logging.info(f'Task is finished for company {comp_id}')


def upsert_products(customers):
    if not customers:
        raise Exception('No customers found')
    else:
        redshift_hook = RedshiftSQLHook(
                postgres_conn_id='redshift_default',
                schema='dev'
            )
        redshift_conn = redshift_hook.get_conn()
        for comp_id, ext_schema in customers:
            logging.info(f'Task is starting for company {comp_id}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    CREATE temporary TABLE products_{comp_id}_temp as
                    SELECT *
                    FROM {ext_schema}.products
                    WHERE sync_updated_at > (
                        SELECT coalesce(max(sync_updated_at), '1970-01-01 00:00:00'::timestamp)
                        FROM staging.products
                        WHERE comp_id = {comp_id}
                    ) and sync_updated_at < CURRENT_DATE + interval '8 hours'
                '''
                cursor.execute(query)
                logging.info(f'Temp table is created')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    DELETE FROM staging.products
                    USING products_{comp_id}_temp
                    WHERE staging.products.comp_id = {comp_id}
                        AND staging.products.prod_id = products_{comp_id}_temp.prod_id
                '''
                cursor.execute(query)
                logging.info(f'{cursor.rowcount} rows deleted for {comp_id} at {datetime.now()}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    INSERT INTO staging.products
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
                        prod_tax_tier_version_id, is_city_local_tax_exempt, brutto_weight, brutto_weight_validation, custom_cost
                    FROM products_{comp_id}_temp
                '''
                cursor.execute(query)
                logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {datetime.now()}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    DROP TABLE products_{comp_id}_temp
                '''
                cursor.execute(query)
                logging.info(f'Temp table is dropped')
            redshift_conn.commit()
            logging.info(f'Task is finished for company {comp_id}')


def upsert_register_log(customers):
    if not customers:
        raise Exception('No customers found')
    else:
        redshift_hook = RedshiftSQLHook(
                postgres_conn_id='redshift_default',
                schema='dev'
            )
        redshift_conn = redshift_hook.get_conn()
        for comp_id, ext_schema in customers:
            logging.info(f'Task is starting for company {comp_id}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    INSERT INTO staging.register_log
                    SELECT {comp_id} as comp_id, id, register_id, opening_amount, cash_sales, drops, expected_drawer, actual_drawer, 
                        over_drawer, created_at, updated_at, "type", service_history_id, sf_guard_user_id, amount, register_type, 
                        total_cost, total_profit, discount, tax, total_amount, method1_amount, method2_amount, method3_amount, 
                        method4_amount, method5_amount, method6_amount, method7_amount, cash_returns, delivered_amount, pending_amount, 
                        dc_cash_change, vehicle_id
                    FROM {ext_schema}.register_log
                    WHERE created_at > (
                        select coalesce(max(created_at), '1970-01-01 00:00:00'::timestamp)
                        from staging.register_log
                        where comp_id = {comp_id}
                    ) and created_at < CURRENT_DATE + interval '8 hours'
                '''
                cursor.execute(query)
                logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {datetime.now()}')
            redshift_conn.commit()
            logging.info(f'Task is finished for company {comp_id}')


def upsert_register(customers):
    if not customers:
        raise Exception('No customers found')
    else:
        redshift_hook = RedshiftSQLHook(
                postgres_conn_id='redshift_default',
                schema='dev'
            )
        redshift_conn = redshift_hook.get_conn()
        for comp_id, ext_schema in customers:
            logging.info(f'Task is starting for company {comp_id}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    CREATE temporary TABLE register_{comp_id}_temp as
                    SELECT *
                    FROM {ext_schema}.register
                    WHERE updated_at > (
                        SELECT coalesce(max(updated_at), '1970-01-01 00:00:00'::timestamp)
                        FROM staging.register
                        WHERE comp_id = {comp_id}
                    ) and updated_at < CURRENT_DATE + interval '8 hours'
                '''
                cursor.execute(query)
                logging.info(f'Temp table is created')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    DELETE FROM staging.register
                    USING register_{comp_id}_temp
                    WHERE staging.register.comp_id = {comp_id}
                        AND staging.register.id = register_{comp_id}_temp.id
                '''
                cursor.execute(query)
                logging.info(f'{cursor.rowcount} rows deleted for {comp_id} at {datetime.now()}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    INSERT INTO staging.register
                    SELECT {comp_id}, id, manager_sf_guard_user_id, name, status, change_status_at, "type", is_active, 
                        opening_amount, cash_tenders, drops, expected_drawer, cash_in_drawer, created_at, updated_at, 
                        pending_amount, pending_count, delivered_amount, delivered_count, activator_sf_guard_user_id, 
                        is_deleted, latitude, longitude, total_weight, "returns", method1_amount::real, method2_amount::real, 
                        method3_amount::real, method4_amount::real, method5_amount::real, method6_amount::real, method7_amount::real, 
                        all_methods_total::real, port, ip_address, sync_updated_at, sync_created_at, office_id, push_new_patient, 
                        push_patient_approved, push_patient_declined, push_new_order, push_order_delivered, platform, tip_amount, 
                        poshub_id, eta, eta_updated_at, application_name, application_version, signin_sf_guard_user_id, dc_cash_change, 
                        dispatch_orders_based_on_delivery_zones, vehicle_id
                    FROM register_{comp_id}_temp
                '''
                cursor.execute(query)
                logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {datetime.now()}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    DROP TABLE register_{comp_id}_temp
                '''
                cursor.execute(query)
                logging.info(f'Temp table is dropped')
            redshift_conn.commit()
            logging.info(f'Task is finished for company {comp_id}')


def upsert_tax_payment(customers):
    if not customers:
        raise Exception('No customers found')
    else:
        redshift_hook = RedshiftSQLHook(
                postgres_conn_id='redshift_default',
                schema='dev'
            )
        redshift_conn = redshift_hook.get_conn()
        for comp_id, ext_schema in customers:
            logging.info(f'Task is starting for company {comp_id}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    CREATE temporary TABLE tax_payment_{comp_id}_temp as
                    SELECT *
                    FROM {ext_schema}.tax_payment
                    WHERE updated_at > (
                        SELECT coalesce(max(updated_at), '1970-01-01 00:00:00'::timestamp)
                        FROM staging.tax_payment
                        WHERE comp_id = {comp_id}
                    ) and updated_at < CURRENT_DATE + interval '8 hours'
                '''
                cursor.execute(query)
                logging.info(f'Temp table is created')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    DELETE FROM staging.tax_payment
                    USING tax_payment_{comp_id}_temp
                    WHERE staging.tax_payment.comp_id = {comp_id}
                        AND staging.tax_payment.id = tax_payment_{comp_id}_temp.id
                '''
                cursor.execute(query)
                logging.info(f'{cursor.rowcount} rows deleted for {comp_id} at {datetime.now()}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    INSERT INTO staging.tax_payment
                    SELECT {comp_id}, id, order_id, order_item_id, product_id, state, county, city, 
                        state_tax, state_mj_tax, county_tax, county_mj_tax, city_tax, city_mj_tax, 
                        created_at, updated_at, is_deleted, state_sales_tax, county_sales_tax, 
                        city_sales_tax, state_local_tax, county_local_tax, city_local_tax, 
                        excise_tax, state_delivery_sales_tax, county_delivery_sales_tax, 
                        city_delivery_sales_tax, city_delivery_local_tax, excise_delivery_tax
                    FROM tax_payment_{comp_id}_temp
                '''
                cursor.execute(query)
                logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {datetime.now()}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    DROP TABLE tax_payment_{comp_id}_temp
                '''
                cursor.execute(query)
                logging.info(f'Temp table is dropped')
            redshift_conn.commit()
            logging.info(f'Task is finished for company {comp_id}')


def upsert_warehouse_orders(customers):
    if not customers:
        raise Exception('No customers found')
    else:
        redshift_hook = RedshiftSQLHook(
                postgres_conn_id='redshift_default',
                schema='dev'
            )
        redshift_conn = redshift_hook.get_conn()
        for comp_id, ext_schema in customers:
            logging.info(f'Task is starting for company {comp_id}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    CREATE temporary TABLE warehouse_orders_{comp_id}_temp as
                    SELECT *
                    FROM {ext_schema}.warehouse_orders
                    WHERE updated_at > (
                        SELECT coalesce(max(updated_at), '1970-01-01 00:00:00'::timestamp)
                        FROM staging.warehouse_orders
                        WHERE comp_id = {comp_id}
                    ) and updated_at < CURRENT_DATE + interval '8 hours'
                '''
                cursor.execute(query)
                logging.info(f'Temp table is created')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    DELETE FROM staging.warehouse_orders
                    USING warehouse_orders_{comp_id}_temp
                    WHERE staging.warehouse_orders.comp_id = {comp_id}
                        AND staging.warehouse_orders.id = warehouse_orders_{comp_id}_temp.id
                '''
                cursor.execute(query)
                logging.info(f'{cursor.rowcount} rows deleted for {comp_id} at {datetime.now()}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    INSERT INTO staging.warehouse_orders
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
                        delivery_estimated_time_of_arrival
                    FROM warehouse_orders_{comp_id}_temp
                '''
                cursor.execute(query)
                logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {datetime.now()}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    DROP TABLE warehouse_orders_{comp_id}_temp
                '''
                cursor.execute(query)
                logging.info(f'Temp table is dropped')
            redshift_conn.commit()
            logging.info(f'Task is finished for company {comp_id}')


def upsert_warehouse_order_items(customers):
    if not customers:
        raise Exception('No customers found')
    else:
        redshift_hook = RedshiftSQLHook(
                postgres_conn_id='redshift_default',
                schema='dev'
            )
        redshift_conn = redshift_hook.get_conn()
        for comp_id, ext_schema in customers:
            logging.info(f'Task is starting for company {comp_id}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    CREATE temporary TABLE warehouse_order_items_{comp_id}_temp as
                    SELECT *
                    FROM {ext_schema}.warehouse_order_items
                    WHERE updated_at > (
                        SELECT coalesce(max(updated_at), '1970-01-01 00:00:00'::timestamp)
                        FROM staging.warehouse_order_items
                        WHERE comp_id = {comp_id}
                    ) and updated_at < CURRENT_DATE + interval '8 hours'
                '''
                cursor.execute(query)
                logging.info(f'Temp table is created')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    DELETE FROM staging.warehouse_order_items
                    USING warehouse_order_items_{comp_id}_temp
                    WHERE staging.warehouse_order_items.comp_id = {comp_id}
                        AND staging.warehouse_order_items.id = warehouse_order_items_{comp_id}_temp.id
                '''
                cursor.execute(query)
                logging.info(f'{cursor.rowcount} rows deleted for {comp_id} at {datetime.now()}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    INSERT INTO staging.warehouse_order_items
                    SELECT {comp_id} as comp_id, id, order_id, product_id, "name", descr, price_type, price_per, 
                        charge_by, price, qty, qty_free, amount, tax, discount_value, discount_type_bak, total_amount, 
                        created_at, updated_at, is_charge_by_order, is_free, free_discount, income, discount_amount, 
                        item_type, count, special_id, special_item_id, is_half_eighth, is_returned, returned_amount, 
                        discount_type, free_amount, paid_amount, wcii_cart_item, sync_created_at, sync_updated_at, 
                        product_checkin_id, is_excise, returned_at, is_marijuana_product, product_is_tax_exempt, 
                        is_metrc, is_under_package_control, base_amount, discount_id, delivery_tax, discount_count, 
                        is_exchanged, exchanged_at, product_brutto_weight, product_brutto_weight_validation
                    FROM warehouse_order_items_{comp_id}_temp
                '''
                cursor.execute(query)
                logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {datetime.now()}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    DROP TABLE warehouse_order_items_{comp_id}_temp
                '''
                cursor.execute(query)
                logging.info(f'Temp table is dropped')
            redshift_conn.commit()
            logging.info(f'Task is finished for company {comp_id}')


def upsert_service_history(customers):
    if not customers:
        raise Exception('No customers found')
    else:
        redshift_hook = RedshiftSQLHook(
                postgres_conn_id='redshift_default',
                schema='dev'
            )
        redshift_conn = redshift_hook.get_conn()
        for comp_id, ext_schema in customers:
            logging.info(f'Task is starting for company {comp_id}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    CREATE temporary TABLE service_history_{comp_id}_temp as
                    SELECT *
                    FROM {ext_schema}.service_history
                    WHERE updated_at > (
                        SELECT coalesce(max(updated_at), '1970-01-01 00:00:00'::timestamp)
                        FROM staging.service_history
                        WHERE comp_id = {comp_id}
                    ) and updated_at < CURRENT_DATE + interval '8 hours'
                '''
                cursor.execute(query)
                logging.info(f'Temp table is created')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    DELETE FROM staging.service_history
                    USING service_history_{comp_id}_temp
                    WHERE staging.service_history.comp_id = {comp_id}
                        AND staging.service_history.id = service_history_{comp_id}_temp.id
                '''
                cursor.execute(query)
                logging.info(f'{cursor.rowcount} rows deleted for {comp_id} at {datetime.now()}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    INSERT INTO staging.service_history
                    SELECT {comp_id} as comp_id, id, service_id, office_id, user_id, patient_id, doctor_id, 
                        object_id, object_type, referal_id, notes, amount, edit_amount, edit_reason, created_at, 
                        updated_at, ad_campaign_id, ad_campaign_patient_type_id, intake, exam, has_intake_for_approving, 
                        has_exam_for_approving, order_id, amount_by_referral_points, count_referral_points, register_id, 
                        balance, method1_amount, method2_amount, method3_amount, method4_amount, method5_amount, method6_amount, 
                        method7_amount, profit, is_take_payment, tax, dc_cash_change, is_dejavoo_payment, "type", cost, discount, 
                        "free", payment_method, payment_amount
                    FROM service_history_{comp_id}_temp
                '''
                cursor.execute(query)
                logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {datetime.now()}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    DROP TABLE service_history_{comp_id}_temp
                '''
                cursor.execute(query)
                logging.info(f'Temp table is dropped')
            redshift_conn.commit()
            logging.info(f'Task is finished for company {comp_id}')


def upsert_product_checkins(customers):
    if not customers:
        raise Exception('No customers found')
    else:
        redshift_hook = RedshiftSQLHook(
                postgres_conn_id='redshift_default',
                schema='dev'
            )
        redshift_conn = redshift_hook.get_conn()
        for comp_id, ext_schema in customers:
            logging.info(f'Task is starting for company {comp_id}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    CREATE temporary TABLE product_checkins_{comp_id}_temp as
                    SELECT *
                    FROM {ext_schema}.product_checkins
                    WHERE sync_updated_at > (
                        SELECT coalesce(max(sync_updated_at), '1970-01-01 00:00:00'::timestamp)
                        FROM staging.product_checkins
                        WHERE comp_id = {comp_id}
                    ) and sync_updated_at < CURRENT_DATE + interval '8 hours'
                '''
                cursor.execute(query)
                logging.info(f'Temp table is created')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    DELETE FROM staging.product_checkins
                    USING product_checkins_{comp_id}_temp
                    WHERE staging.product_checkins.comp_id = {comp_id}
                        AND staging.product_checkins.id = product_checkins_{comp_id}_temp.id
                '''
                cursor.execute(query)
                logging.info(f'{cursor.rowcount} rows deleted for {comp_id} at {datetime.now()}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    INSERT INTO staging.product_checkins
                    SELECT {comp_id}, id, product_id, vendor_id, user_id, qty, price, date, status, balance, note, batch_id, lab_result_id, has_lab_result, uid,
                    harvest_date, sync_created_at, sync_updated_at, opc, sale_qty, office_id, is_metrc, available_qty, is_finished, producer_id, 
                    vendor_type, vendor_name, facility_id, is_special, packaged_date, best_by_date, deleted_at, production_run, is_under_package_control, 
                    is_form_modified, metrc_initial_quantity, external_barcode, packaged_by_id, manifest, is_sample_package, paused_to_datetime, 
                    excise_tax_paid, link_to_coa
                    FROM product_checkins_{comp_id}_temp
                '''
                cursor.execute(query)
                logging.info(f'{cursor.rowcount} rows inserted for {comp_id} at {datetime.now()}')
            with redshift_conn.cursor() as cursor:
                query = f'''
                    DROP TABLE product_checkins_{comp_id}_temp
                '''
                cursor.execute(query)
                logging.info(f'Temp table is dropped')
            redshift_conn.commit()
            logging.info(f'Task is finished for company {comp_id}')


customers = get_customers()


with DAG(
    dag_id='update_everything',
    schedule_interval='0 8 * * *', # UTC time
    start_date=datetime(year=2022, month=12, day=8),
    default_args=default_args,
    catchup=False,
) as dag:
    task_get_customers = PythonOperator(
        task_id='get_customers',
        python_callable=get_customers
    )
    task_upsert_brands = PythonOperator(
        task_id='upsert_brands',
        python_callable=upsert_brands,
        op_args=[customers]
    )
    task_upsert_company_config = PythonOperator(
        task_id='upsert_company_config',
        python_callable=upsert_company_config,
        op_args=[customers]
    )
    task_upsert_discounts = PythonOperator(
        task_id='upsert_discounts',
        python_callable=upsert_discounts,
        op_args=[customers]
    )
    task_upsert_patient_group_ref = PythonOperator(
        task_id='upsert_patient_group_ref',
        python_callable=upsert_patient_group_ref,
        op_args=[customers]
    )
    task_upsert_patient_group = PythonOperator(
        task_id='upsert_patient_group',
        python_callable=upsert_patient_group,
        op_args=[customers]
    )
    task_upsert_patients = PythonOperator(
        task_id='upsert_patients',
        python_callable=upsert_patients,
        op_args=[customers]
    )
    task_upsert_product_categories = PythonOperator(
        task_id='upsert_product_categories',
        python_callable=upsert_product_categories,
        op_args=[customers]
    )
    task_upsert_product_filter_index = PythonOperator(
        task_id='upsert_product_filter_index',
        python_callable=upsert_product_filter_index,
        op_args=[customers]
    )
    task_upsert_product_transactions = PythonOperator(
        task_id='upsert_product_transactions',
        python_callable=upsert_product_transactions,
        op_args=[customers]
    )
    task_upsert_product_vendors = PythonOperator(
        task_id='upsert_product_vendors',
        python_callable=upsert_product_vendors,
        op_args=[customers]
    )
    task_upsert_products = PythonOperator(
        task_id='upsert_products',
        python_callable=upsert_products,
        op_args=[customers]
    )
    task_upsert_register_log = PythonOperator(
        task_id='upsert_register_log',
        python_callable=upsert_register_log,
        op_args=[customers]
    )
    task_upsert_register = PythonOperator(
        task_id='upsert_register',
        python_callable=upsert_register,
        op_args=[customers]
    )
    task_upsert_service_history = PythonOperator(
        task_id='upsert_service_history',
        python_callable=upsert_service_history,
        op_args=[customers]
    )
    task_upsert_tax_payment = PythonOperator(
        task_id='upsert_tax_payment',
        python_callable=upsert_tax_payment,
        op_args=[customers]
    )
    task_upsert_warehouse_orders = PythonOperator(
        task_id='upsert_warehouse_orders',
        python_callable=upsert_warehouse_orders,
        op_args=[customers]
    )
    task_upsert_warehouse_order_items = PythonOperator(
        task_id='upsert_warehouse_order_items',
        python_callable=upsert_warehouse_order_items,
        op_args=[customers]
    )
    task_upsert_product_checkins = PythonOperator(
        task_id='upsert_product_checkins',
        python_callable=upsert_product_checkins,
        op_args=[customers]
    )
    dbt_run = DbtRunOperator(
        task_id="dbt_run",
        project_dir="/home/ubuntu/dbt/indica",
        profiles_dir="/home/ubuntu/.dbt",
        # fail_fast=True,
        # task_id="dbt_run_hourly",
        # project_dir="s3://my-bucket/dbt/project/key/prefix/",
        # profiles_dir="s3://my-bucket/dbt/profiles/key/prefix/",
        # select=["+tag:hourly"],
        # exclude=["tag:deprecated"],
        # target="production",
        # profile="my-project",
        # full_refresh=False,
    )
    dbt_test = DbtTestOperator(
        task_id="dbt_test",
        project_dir="/home/ubuntu/dbt/indica",
        profiles_dir="/home/ubuntu/.dbt",
    )
    
    # task_dbt_run_and_test = BashOperator(
    #     task_id='dbt_run_and_test',
    #     bash_command='source /home/ubuntu/dbt/venv/bin/activate; cd /home/ubuntu/dbt/indica; dbt run; dbt test; deactivate;'
    # )

    task_get_customers >> [
        task_upsert_brands, 
        task_upsert_company_config, 
        task_upsert_discounts, 
        task_upsert_patient_group_ref,
        task_upsert_patient_group,
        task_upsert_patients,
        task_upsert_product_categories,
        task_upsert_product_checkins,
        task_upsert_product_filter_index,
        task_upsert_product_transactions,
        task_upsert_product_vendors,
        task_upsert_products,
        task_upsert_register_log,
        task_upsert_register,
        task_upsert_service_history,
        task_upsert_tax_payment,
        task_upsert_warehouse_orders,
        task_upsert_warehouse_order_items,
    ] >> dbt_run >> dbt_test




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
