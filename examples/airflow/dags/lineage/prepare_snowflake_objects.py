import os
from pendulum import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from openlineage.client.facet import PRODUCER, SCHEMA_URI


with DAG(
    "prepare_snowflake_objects",
    start_date=datetime(2022, 4, 12),
    schedule_interval="@once",
    catchup=False,
    default_args={
        "owner": "openlineage",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "email": ["demo@openlineage.io"],
        "snowflake_conn_id": "openlineage_snowflake",
    },
    description="Loads newly added menu categories.",
    params={
        "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT"),
        "SNOWFLAKE_SCHEMA": os.getenv("SNOWFLAKE_SCHEMA"),
        "PRODUCER": PRODUCER,
        "SCHEMA_URI": SCHEMA_URI,
    },
) as dag:

    t1 = SnowflakeOperator(
        task_id="create_view",
        sql="open_lineage_access_history.sql",
        split_statements=True,
    )
