from pendulum import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import State
from utils import _get_execution_date_of


with DAG(
    "etl_menu_items",
    start_date=datetime(2022, 4, 12),
    schedule_interval="@daily",
    catchup=False,
    default_args={
        "owner": "openlineage",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "email": ["demo@openlineage.io"],
        "snowflake_conn_id": "openlineage_snowflake",
    },
    description="Loads newly added restaurant menu items.",
) as dag:
    wait_for_upstream = ExternalTaskSensor(
        task_id="wait_for_upstream",
        external_dag_id="etl_new_delivery",
        execution_date_fn=_get_execution_date_of("etl_new_delivery"),
        poke_interval=5,
        mode="reschedule",
    )

    t1 = SnowflakeOperator(
        task_id="if_not_exists",
        sql="""
        CREATE TABLE IF NOT EXISTS food_delivery.menu_items (
            id          INTEGER,
            name        STRING,
            price       STRING,
            category_id INTEGER,
            description STRING
        )
        """,
    )

    t2 = SnowflakeOperator(
        task_id="etl",
        sql="""
        INSERT INTO food_delivery.menu_items (id, name, price, category_id, description)
        SELECT id, name, price, category_id, description
        FROM food_delivery.tmp_menu_items
        """,
        session_parameters={"QUERY_TAG": "etl_menu_items"},
    )

    wait_for_upstream >> t1 >> t2
