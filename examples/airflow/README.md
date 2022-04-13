# Snowflake OpenLineage with Airflow Example 
Example codes consist of airflow DAGs with SnowflakeOperator that runs query, and a DAG with PythonOperator
that sends generated OpenLineage events to the configured backend.

## Prerequisite
* `OPEN_LINEAGE` database and `FOOD_DELIVERY` schema in Snowflake need to be created to run this example.

## Environment Variables
Following environment variables need to be set in order to send the OpenLineage events to the OL backend:
* SNOWFLAKE_USER
* SNOWFLAKE_PASSWORD
* SNOWFLAKE_ACCOUNT
* OPENLINEAGE_URL

## Result
![](./snowflake-openlineage-example.png)