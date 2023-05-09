from pendulum import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import State
from airflow.decorators import task_group
from utils import _get_execution_date_of


with DAG(
    "etl_delivery_7_days",
    start_date=datetime(2022, 4, 12),
    schedule_interval="@weekly",
    catchup=False,
    default_args={
        "owner": "openlineage",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "email": ["demo@openlineage.io"],
        "snowflake_conn_id": "openlineage_snowflake",
    },
    description="Loads new deliveries for the week.",
) as dag:

    @task_group(group_id="wait_for_upstream")
    def wait_for_upstream():
        for dag_id in [
            "etl_customers",
            "etl_drivers",
            "etl_order_7_days",
            "etl_order_status",
            "etl_restaurants",
        ]:
            ExternalTaskSensor(
                task_id="wait_for_dag_" + dag_id,
                external_dag_id=dag_id,
                failed_states=[State.FAILED],
                execution_date_fn=_get_execution_date_of(dag_id),
                poke_interval=5,
                mode="reschedule",
            )

    wait_group = wait_for_upstream()

    t1 = SnowflakeOperator(
        task_id="if_not_exists_delivery_7_days",
        sql="""
        CREATE TABLE IF NOT EXISTS food_delivery.delivery_7_days (
            order_id            INTEGER,
            order_placed_on     TIME,
            order_dispatched_on TIME,
            order_delivered_on  TIME,
            customer_email      STRING,
            customer_address    STRING,
            discount_id         INTEGER,
            menu_id             INTEGER,
            restaurant_id       INTEGER,
            restaurant_address  STRING,
            menu_item_id        INTEGER,
            category_id         INTEGER,
            driver_id           INTEGER
        )
        """,
    )

    t2 = SnowflakeOperator(
        task_id="insert",
        sql="""
        INSERT INTO food_delivery.delivery_7_days (
            order_id,
            order_placed_on,
            order_dispatched_on,
            order_delivered_on,
            customer_email,
            customer_address,
            discount_id,
            menu_id,
            restaurant_id,
            restaurant_address,
            menu_item_id,
            category_id,
            driver_id
        )
        SELECT o.order_id,
               o.placed_on                        AS order_placed_on,
               (SELECT transitioned_at
                FROM   food_delivery.order_status
                WHERE  order_id = o.order_id
                       AND status = 'DISPATCHED') AS order_dispatched_on,
               (SELECT transitioned_at
                FROM   food_delivery.order_status
                WHERE  order_id = o.order_id
                       AND status = 'DELIVERED')  AS order_delivered_on,
               c.email                            AS customer_email,
               c.address                          AS customer_address,
               o.discount_id,
               o.menu_id,
               o.restaurant_id,
               r.address                          AS restaurant_address,
               o.menu_item_id,
               o.category_id,
               d.id                               AS driver_id
        FROM   food_delivery.orders_7_days AS o
               INNER JOIN food_delivery.order_status AS os
                       ON os.order_id = o.order_id
               INNER JOIN food_delivery.customers AS c
                       ON c.id = os.customer_id
               INNER JOIN food_delivery.restaurants AS r
                       ON r.id = os.restaurant_id
               INNER JOIN food_delivery.drivers AS d
                       ON d.id = os.driver_id
        WHERE  os.transitioned_at >= TIMEADD(hour, -168, current_time())
        """,
        session_parameters={"QUERY_TAG": "etl_delivery_7_days"},
    )

    wait_group >> t1 >> t2
