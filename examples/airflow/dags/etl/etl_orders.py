from pendulum import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
SNOWFLAKE_DATABASE = 'OPENLINEAGE'


with DAG(
    'etl_orders',
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
    description='Loads newly placed orders.',
) as dag:

    t1 = SnowflakeOperator(
        task_id='if_not_exists',
        sql='''
        CREATE TABLE IF NOT EXISTS food_delivery.orders (
            id           INTEGER,
            placed_on    TIME,
            menu_item_id INTEGER,
            quantity     INTEGER,
            discount_id  INTEGER,
            comment      STRING
        )
        '''
    )

    t2 = SnowflakeOperator(
        task_id='etl',
        sql='''
        INSERT INTO food_delivery.orders (id, placed_on, menu_item_id, quantity, discount_id, comment)
        SELECT id, placed_on, menu_item_id, quantity, discount_id, comment
        FROM food_delivery.tmp_orders
        ''',
        session_parameters={
            'QUERY_TAG': 'etl_orders'
        }
    )

    t1 >> t2
