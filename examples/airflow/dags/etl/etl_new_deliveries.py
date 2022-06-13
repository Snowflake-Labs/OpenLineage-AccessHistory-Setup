from pendulum import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago


SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
SNOWFLAKE_DATABASE = 'OPENLINEAGE'


with DAG(
    'etl_new_delivery',
    start_date=datetime(2022, 4, 12),
    schedule_interval='@daily',
    catchup=False,
    default_args={
        'owner': 'openlineage',
        'depends_on_past': False,
        'start_date': days_ago(1),
        'email_on_failure': False,
        'email_on_retry': False,
        'email': ['demo@openlineage.io'],
        'snowflake_conn_id': 'openlineage_snowflake',
        'warehouse': SNOWFLAKE_WAREHOUSE,
        'database': SNOWFLAKE_DATABASE,
    },
    description='Add new food delivery data.',
) as dag:

    t1 = SnowflakeOperator(
        task_id='if_not_exists_cities',
        sql='''
        CREATE TABLE IF NOT EXISTS food_delivery.cities (
            id       INTEGER,
            name     STRING,
            state    STRING,
            zip_code STRING
        )
        '''
    )

    t2 = SnowflakeOperator(
        task_id='if_not_exists_business_hours',
        sql='''
        CREATE TABLE IF NOT EXISTS food_delivery.business_hours (
            id          INTEGER,
            day_of_week STRING,
            opens_at    TIME,
            closes_at   TIME
        )
        '''
    )

    t3 = SnowflakeOperator(
        task_id='if_not_exists_discounts',
        sql='''
        CREATE TABLE IF NOT EXISTS food_delivery.discounts (
            id           INTEGER,
            amount_off   INTEGER,
            customers_id INTEGER,
            starts_at    TIME,
            ends_at      TIME
        )
        '''
    )

    t4 = SnowflakeOperator(
        task_id='if_not_exists_tmp_restaurants',
        sql='''
        CREATE TABLE IF NOT EXISTS food_delivery.tmp_restaurants (
            id                INTEGER,
            created_at        TIME,
            updated_at        TIME,
            name              STRING,
            email             STRING,
            address           STRING,
            phone             STRING,
            city_id           INTEGER,
            business_hours_id INTEGER,
            description       STRING
        )
        '''
    )

    t5 = SnowflakeOperator(
        task_id='if_not_exists_tmp_menus',
        sql='''
        CREATE TABLE IF NOT EXISTS food_delivery.tmp_menus (
            id            INTEGER,
            name          STRING,
            restaurant_id INTEGER,
            description   STRING
        )
        ''',
    )

    t6 = SnowflakeOperator(
        task_id='if_not_exists_tmp_menu_items',
        sql='''
        CREATE TABLE IF NOT EXISTS food_delivery.tmp_menu_items (
            id          INTEGER,
            name        STRING,
            price       STRING,
            category_id INTEGER,
            description STRING
        )
        '''
    )

    t7 = SnowflakeOperator(
        task_id='if_not_exists_tmp_categories',
        sql='''
        CREATE TABLE IF NOT EXISTS food_delivery.tmp_categories (
            id          INTEGER,
            name        STRING,
            menu_id     INTEGER,
            description STRING
        )
        '''
    )

    t8 = SnowflakeOperator(
        task_id='if_not_exists_tmp_drivers',
        sql='''
        CREATE TABLE IF NOT EXISTS food_delivery.tmp_drivers (
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

    t9 = SnowflakeOperator(
        task_id='if_not_exists_tmp_customers',
        sql='''
        CREATE TABLE IF NOT EXISTS food_delivery.tmp_customers (
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

    t10 = SnowflakeOperator(
        task_id='if_not_exists_tmp_orders',
        sql='''
        CREATE TABLE IF NOT EXISTS food_delivery.tmp_orders (
            id           INTEGER,
            placed_on    TIME,
            menu_item_id INTEGER,
            quantity     INTEGER,
            discount_id  INTEGER,
            comment      STRING
        )
        '''
    )

    t11 = SnowflakeOperator(
        task_id='if_not_exists_tmp_order_status',
        sql='''
        CREATE TABLE IF NOT EXISTS food_delivery.tmp_order_status (
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
