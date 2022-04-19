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

with DAG('etl_order_7_days',
         schedule_interval='@weekly',
         catchup=False,
         default_args=default_args,
         description='Loads placed orders weekly.') as dag:

    t1 = SnowflakeOperator(
        task_id='if_not_exists_orders_7_days',
        dag=dag,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        sql='''
        CREATE TABLE IF NOT EXISTS food_delivery.orders_7_days (
            order_id      INTEGER,
            placed_on     TIME,
            discount_id   INTEGER,
            menu_id       INTEGER,
            restaurant_id INTEGER,
            menu_item_id  INTEGER,
            category_id   INTEGER
        )
        '''
    )

    t2 = SnowflakeOperator(
        task_id='insert',
        dag=dag,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        sql='''
        INSERT INTO food_delivery.orders_7_days (
            order_id,
            placed_on,
            discount_id,
            menu_id,
            restaurant_id,
            menu_item_id,
            category_id
        )
        SELECT o.id  AS order_id,
               o.placed_on,
               o.discount_id,
               m.id  AS menu_id,
               m.restaurant_id,
               mi.id AS menu_item_id,
               c.id  AS category_id
        FROM   food_delivery.orders AS o
               inner join food_delivery.menu_items AS mi
                       ON mi.id = o.menu_item_id
               inner join food_delivery.categories AS c
                       ON c.id = mi.category_id
               inner join food_delivery.menus AS m
                       ON m.id = c.menu_id
        WHERE  o.placed_on >= TIMEADD(hour, -168, current_time())  
        ''',
        session_parameters={
            'QUERY_TAG': 'etl_order_7_days'
        }
    )

    t1 >> t2
