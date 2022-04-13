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

with DAG('etl_order_status',
         schedule_interval='@daily',
         catchup=False,
         default_args=default_args,
         description='Loads order status updates.') as dag:

    t1 = SnowflakeOperator(
        task_id='if_not_exists',
        dag=dag,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        sql='''
        CREATE TABLE IF NOT EXISTS food_delivery.order_status (
            id              INTEGER,
            transitioned_at TIME,
            status          STRING,
            order_id        INTEGER,
            customer_id     INTEGER,
            restaurant_id   INTEGER,
            driver_id       INTEGER
        )
        '''
    )

    t2 = SnowflakeOperator(
        task_id='etl',
        dag=dag,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        sql='''
        INSERT INTO food_delivery.order_status (
            id,
            transitioned_at,
            status,
            order_id,
            customer_id,
            driver_id,
            restaurant_id
        )
        SELECT id, transitioned_at, status, order_id, customer_id, driver_id, restaurant_id
        FROM food_delivery.tmp_order_status
        ''',
        session_parameters={
            'QUERY_TAG': 'etl_order_status'
        }
    )

    t1 >> t2
