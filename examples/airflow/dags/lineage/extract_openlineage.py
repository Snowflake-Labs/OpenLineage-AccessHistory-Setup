import json
import os
from pendulum import datetime

from airflow import DAG
from airflow.decorators import task
from openlineage.client import OpenLineageClient
from openlineage.client.facet import PRODUCER, SCHEMA_URI
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")


@task
def send_ol_events():
    client = OpenLineageClient.from_environment()

    hook = SnowflakeHook(snowflake_conn_id="openlineage_snowflake")
    ol_view = "OPENLINEAGE_ACCESS_HISTORY"
    ol_event_time_tag = "OL_LATEST_EVENT_TIME"
    ol_events = hook.get_records(
        f"""
                set current_organization='{SNOWFLAKE_ACCOUNT}';
                set producer='{PRODUCER}';
                set schema_uri='{SCHEMA_URI}';
                SELECT * FROM {ol_view}
                WHERE EVENT:eventTime > system$get_tag('{ol_event_time_tag}', '{ol_view}', 'table')
                ORDER BY EVENT:eventTime ASC;
            """
    )
    ol_events = [json.loads(ol_event[0]) for ol_event in ol_events]

    for ol_event in ol_events:
        client.transport.emit(ol_event)

    if len(ol_events) > 0:
        latest_event_time = ol_events[-1]["eventTime"]
        hook.run(
            f"""
            ALTER VIEW {ol_view} SET TAG {ol_event_time_tag} = '{latest_event_time}';
        """
        )


with DAG(
    "etl_openlineage",
    start_date=datetime(2022, 4, 12),
    schedule_interval="@hourly",
    catchup=False,
    is_paused_upon_creation=True,
    default_args={
        "owner": "openlineage",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "email": ["demo@openlineage.io"],
        "snowflake_conn_id": "openlineage_snowflake",
    },
    description="Send OL events every minutes.",
    tags=["extract"],
) as dag:
    send_ol_events()
