from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook


# Get connection to Redshift DB on production environment
def redshint_conn_dev():
    redshift_hook = RedshiftSQLHook(
        redshift_conn_id='redshift_default',
        schema='dev'
    )
    redshift_conn = redshift_hook.get_conn()
    # cursor = redshift_conn.cursor()
    return redshift_conn


# Get connection to Redshift DB on production environment
def redshint_conn_mock():
    redshift_hook = RedshiftSQLHook(
        redshift_conn_id='redshift_mock',
        schema='mock'
    )
    redshift_conn = redshift_hook.get_conn()
    # cursor = redshift_conn.cursor()
    return redshift_conn