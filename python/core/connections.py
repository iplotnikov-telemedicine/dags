from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.models import Connection
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_SERIALIZABLE


# Get connection to Redshift DB on production environment
def redshint_conn_dev():
    redshift_hook = RedshiftSQLHook(
        redshift_conn_id='redshift_default',
        schema='dev'
    )
    redshift_conn = redshift_hook.get_conn()
    # cursor = redshift_conn.cursor()
    return redshift_conn


# Get connection to Redshift DB on dev environment
def redshint_conn_mock():
    redshift_hook = RedshiftSQLHook(
        redshift_conn_id='redshift_mock',
        schema='mock'
    )
    redshift_conn = redshift_hook.get_conn()
    # cursor = redshift_conn.cursor()
    return redshift_conn


# Get connection to Redshift DB on production environment using psycopg2
def psycopg2_redshift():
    conn_id = 'redshift_default'
    conn = Connection.get_connection_from_secrets(conn_id)

    redshift_conn = psycopg2.connect(
        host=conn.host,
        port=conn.port,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password
    )
    
    cursor = redshift_conn.cursor()
    cursor.connection.set_isolation_level(ISOLATION_LEVEL_SERIALIZABLE)
    return cursor, redshift_conn
