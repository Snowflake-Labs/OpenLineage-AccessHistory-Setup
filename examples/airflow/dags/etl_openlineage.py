import json
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from openlineage.client import OpenLineageClient
from snowflake.connector import connect

SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')


def send_ol_events():
    client = OpenLineageClient.from_environment()

    with connect(user=SNOWFLAKE_USER,
                 password=SNOWFLAKE_PASSWORD,
                 account=SNOWFLAKE_ACCOUNT,
                 database='OPENLINEAGE',
                 schema='PUBLIC') as conn:
        with conn.cursor() as cursor:
            ol_view = 'OPENLINEAGE_ACCESS_HISTORY'
            ol_event_time_tag = 'OL_LATEST_EVENT_TIME'

            var_query = f'''
                set current_organization='{SNOWFLAKE_ACCOUNT}';
            '''

            cursor.execute(var_query)

            ol_query = f'''
                SELECT * FROM {ol_view}
                WHERE EVENT:eventTime > system$get_tag('{ol_event_time_tag}', '{ol_view}', 'table')
                ORDER BY EVENT:eventTime ASC;
            '''

            cursor.execute(ol_query)
            ol_events = [json.loads(ol_event[0]) for ol_event in cursor.fetchall()]

            for ol_event in ol_events:
                client.emit(ol_event)

            if len(ol_events) > 0:
                latest_event_time = ol_events[-1]['eventTime']
                cursor.execute(f'''
                    ALTER VIEW {ol_view} SET TAG {ol_event_time_tag} = '{latest_event_time}';
                ''')


default_args = {
    'owner': 'openlineage',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['demo@openlineage.io'],
    'snowflake_conn_id': 'openlineage_snowflake'
}


with DAG('etl_openlineage',
         schedule_interval='@hourly',
         catchup=False,
         default_args=default_args,
         description='Send OL events every minutes',
         tags=["extract"]) as dag:
    t1 = PythonOperator(task_id='ol_event', python_callable=send_ol_events)
