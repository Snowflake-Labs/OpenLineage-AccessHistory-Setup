from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
SNOWFLAKE_DATABASE = 'OPENLINEAGE'

default_args = {
    'owner': 'openlineage',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['demo@openlineage.io'],
    'snowflake_conn_id': 'openlineage_snowflake'
}

with DAG('etl_menus',
         schedule_interval='@daily',
         catchup=False,
         default_args=default_args,
         description='Loads newly added restaurant menus') as dag:

    t1 = SnowflakeOperator(
        task_id='if_not_exists',
        dag=dag,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        sql='''
        CREATE TABLE IF NOT EXISTS food_delivery.menus (
            id            INTEGER,
            name          STRING,
            restaurant_id INTEGER,
            description   STRING
        )
        '''
    )

    t2 = SnowflakeOperator(
        task_id='etl',
        dag=dag,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        sql='''
        INSERT INTO food_delivery.menus (id, name, restaurant_id, description)
        SELECT id, name, restaurant_id, description
        FROM food_delivery.tmp_menus
        ''',
        session_parameters={
            'QUERY_TAG': 'etl_menus'
        }
    )

    t1 >> t2
