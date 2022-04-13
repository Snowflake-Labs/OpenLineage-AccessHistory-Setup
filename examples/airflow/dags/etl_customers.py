from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
SNOWFLAKE_DATABASE = 'OPEN_LINEAGE'

default_args = {
    'owner': 'openlineage',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['demo@openlineage.io'],
    'snowflake_conn_id': 'openlineage_snowflake'
}

with DAG('etl_customers',
         schedule_interval='@daily',
         catchup=False,
         default_args=default_args,
         description='Loads newly registered customers.') as dag:

    t1 = SnowflakeOperator(
        task_id='if_not_exists',
        dag=dag,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        sql='''
        CREATE TABLE IF NOT EXISTS food_delivery.customers (
            id         INTEGER,
            created_at TIME,
            updated_at TIME,
            name       STRING,
            email      STRING,
            address    STRING,
            phone      STRING,
            city_id    INTEGER
        )
        '''
    )

    t2 = SnowflakeOperator(
        task_id='etl',
        dag=dag,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        sql='''
        INSERT INTO food_delivery.customers (id, created_at, updated_at, name, email, address, phone, city_id)
        SELECT id, created_at, updated_at, name, email, address, phone, city_id
        FROM food_delivery.tmp_customers
        ''',
        session_parameters={
            'QUERY_TAG': 'etl_customers'
        }
    )

    t1 >> t2
