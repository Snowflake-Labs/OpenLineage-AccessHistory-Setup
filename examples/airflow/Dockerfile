ARG AIRFLOW_IMAGE=apache/airflow:2.5.0-python3.7
FROM $AIRFLOW_IMAGE AS airflow
RUN pip install --no-cache-dir openlineage-python
RUN pip install --no-cache-dir apache-airflow-providers-snowflake