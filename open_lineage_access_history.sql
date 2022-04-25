/***********************************************************************************************************************************
Script:             OpenLineage Access History View
Create Date:        2022-04-12
Description:        This script creates a view from snowflake.account_usage.access_history and snowflake.account_usage.query_history
                    that output lineage information in a OpenLineage compatible format.
************************************************************************************************************************************/

create or replace view OPENLINEAGE_ACCESS_HISTORY(
	EVENT
) as
    with namespace as (
        select concat('snowflake://', $current_organization, '-', current_account())
    ),
    row_info as (
        select
            QH.start_time as start_time,
            QH.end_time as end_time,
            AH.query_id as run_id,
            QH.query_tag as job_name,
            AH.base_objects_accessed as raw_inputs,
            AH.objects_modified as raw_outputs
        from snowflake.account_usage.query_history as QH
        inner join snowflake.account_usage.access_history as AH
            on QH.query_tag != '' and AH.query_id = QH.query_id
        qualify row_number() over (partition by QH.query_id order by null) = 1
    ),
    formatted_inputs as (
        select
            any_value(start_time) as start_time,
            any_value(end_time) as end_time,
            any_value(run_id) as run_id,
            any_value(job_name) as job_name,
            any_value(raw_outputs) as raw_outputs,
            any_value(flattened_inputs.value) as flattened_input,
            any_value(flattened_inputs.seq) as seq,
            array_agg(
                case
                when input_columns.value is null then null
                else object_construct('name', input_columns.value:"columnName"::string)
                end
            ) as formatted_columns
        from row_info
        , lateral flatten(input => raw_inputs, outer => true) flattened_inputs
        , lateral flatten(input => flattened_inputs.value:columns, outer => true) input_columns
        group by input_columns.seq
    ),
    aggregated_inputs as (
        select
            any_value(start_time) as start_time,
            any_value(end_time) as end_time,
            any_value(run_id) as run_id,
            any_value(job_name) as job_name,
            any_value(raw_outputs) as raw_outputs,
            array_agg(
                case
                when flattened_input is null then null
                else object_construct(
                    'namespace', concat((select * from namespace), '/', flattened_input:"objectDomain"::string),
                    'name', flattened_input:"objectName"::string,
                    'facets', object_construct('schema', object_construct('fields', formatted_columns))
                )
                end
            ) as inputs
        from formatted_inputs
        group by seq
    ),
    formatted_outputs as (
        select
            any_value(start_time) as start_time,
            any_value(end_time) as end_time,
            any_value(run_id) as run_id,
            any_value(job_name) as job_name,
            any_value(inputs) as inputs,
            any_value(flattened_outputs.value) as flattened_output,
            any_value(flattened_outputs.seq) as seq,
            array_agg(
                case
                when output_columns.value is null then null
                else object_construct('name', output_columns.value:"columnName"::string)
                end
            ) as formatted_columns
        from aggregated_inputs
        , lateral flatten(input => raw_outputs, outer => true) flattened_outputs
        , lateral flatten(input => flattened_outputs.value:columns, outer => true) output_columns
        group by output_columns.seq
    ),
    aggregated_outputs as (
        select
            any_value(start_time) as start_time,
            any_value(end_time) as end_time,
            any_value(run_id) as run_id,
            any_value(job_name) as job_name,
            any_value(inputs) as inputs,
            array_agg(
                case
                when flattened_output is null then null
                else object_construct(
                    'namespace', concat((select * from namespace), '/', flattened_output:"objectDomain"::string),
                    'name', flattened_output:"objectName"::string,
                    'facets', object_construct('schema', object_construct('fields', formatted_columns))
                )
                end
            ) as outputs
        from formatted_outputs
        group by seq
    )
    select
        object_construct(
            'eventType', 'START',
            'eventTime', replace(start_time::datetime::string, ' ', 'T'),
            'run', object_construct('runId', run_id),
            'job', object_construct('namespace', (select * from namespace), 'name', job_name),
            'inputs', inputs,
            'outputs', outputs,
            'producer', 'https://github.com/OpenLineage/OpenLineage/tree/main/client'
        ) as event
    from aggregated_outputs
    union
    select
        object_construct(
            'eventType', 'COMPLETE',
            'eventTime', replace(end_time::datetime::string, ' ', 'T'),
            'run', object_construct('runId', run_id),
            'job', object_construct('namespace', (select * from namespace), 'name', job_name),
            'inputs', inputs,
            'outputs', outputs,
            'producer', 'https://github.com/OpenLineage/OpenLineage/tree/main/client'
        ) as event
    from aggregated_outputs
;
