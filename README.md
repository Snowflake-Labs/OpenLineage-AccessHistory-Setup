#  OpenLineage Adapter

##  Overview
Guideline to extract lineage info in [OpenLineage](https://github.com/OpenLineage/OpenLineage) format from Snowflake [ACCESS_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/access_history.html) view. 

##  Code Deployment

### OPENLINEAGE_ACCESS_HISTORY View

#### View Definition

[open_lineage_access_history.sql](https://github.com/Snowflake-Labs/OpenLineage-AccessHistory-Setup/blob/main/examples/airflow/dags/lineage/open_lineage_access_history.sql) is the script to create the view from [ACCESS_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/access_history.html) and [QUERY_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/query_history.html)
that outputs each query that accesses tables in the account in OpenLineage [JsonSchema](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json) specification. 

* The view only shows a query that has non-empty value for `query_tag` column in the [query_history](https://docs.snowflake.com/en/sql-reference/account-usage/query_history.html).
* The `namespace` of each record is in the format of `snowflake://<Organization_name>-<Account_name>`
