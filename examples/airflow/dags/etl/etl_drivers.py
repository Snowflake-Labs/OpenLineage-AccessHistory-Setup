import os
from pendulum import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')


with DAG(
    'etl_drivers',
    start_date=datetime(2022, 4, 12),
    schedule_interval='@daily',
    catchup=False,
    default_args={
        'owner': 'openlineage',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'email': ['demo@openlineage.io'],
        'snowflake_conn_id': 'openlineage_snowflake',
        'warehouse': SNOWFLAKE_WAREHOUSE,
        'database': SNOWFLAKE_DATABASE,
    },
    description='Loads newly registered drivers.',
) as dag:

    t1 = SnowflakeOperator(
        task_id='if_not_exists',
        sql='''
        CREATE TABLE IF NOT EXISTS food_delivery.drivers (
            id                INTEGER,
            created_at        TIME,
            updated_at        TIME,
            name              STRING,
            email             STRING,
            phone             STRING,
            car_make          STRING,
            car_model         STRING,
            car_year          STRING,
            car_color         STRING,
            car_license_plate STRING
        )
        '''
    )

    t2 = SnowflakeOperator(
        task_id='etl',
        sql='''
        INSERT INTO food_delivery.drivers (
            id,
            created_at,
            updated_at,
            name,
            email,
            phone,
            car_make,
            car_model,
            car_year,
            car_color,
            car_license_plate
        )
        SELECT id,
               created_at,
               updated_at,
               name,
               email,
               phone,
               car_make,
               car_model,
               car_year,
               car_color,
               car_license_plate
        FROM food_delivery.tmp_drivers
        ''',
        session_parameters={
            'QUERY_TAG': 'etl_drivers'
        }
    )

    t1 >> t2
