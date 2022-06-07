
with __dbt__cte__nps_cancel_visit_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "houmdw_test".load_test._airbyte_raw_cancel_visit
select
    case when json_extract_path_text(_airbyte_data, 'address', true) != '' then json_extract_path_text(_airbyte_data, 'address', true) end as address,
    case when json_extract_path_text(_airbyte_data, 'country', true) != '' then json_extract_path_text(_airbyte_data, 'country', true) end as country,
    case when json_extract_path_text(_airbyte_data, 'comments', true) != '' then json_extract_path_text(_airbyte_data, 'comments', true) end as comments,
    case when json_extract_path_text(_airbyte_data, 'appraiser', true) != '' then json_extract_path_text(_airbyte_data, 'appraiser', true) end as appraiser,
    case when json_extract_path_text(_airbyte_data, 'begin_date', true) != '' then json_extract_path_text(_airbyte_data, 'begin_date', true) end as begin_date,
    case when json_extract_path_text(_airbyte_data, 'visit_type', true) != '' then json_extract_path_text(_airbyte_data, 'visit_type', true) end as visit_type,
    case when json_extract_path_text(_airbyte_data, 'property_id', true) != '' then json_extract_path_text(_airbyte_data, 'property_id', true) end as property_id,
    case when json_extract_path_text(_airbyte_data, 'schedule_id', true) != '' then json_extract_path_text(_airbyte_data, 'schedule_id', true) end as schedule_id,
    case when json_extract_path_text(_airbyte_data, 'recommendation', true) != '' then json_extract_path_text(_airbyte_data, 'recommendation', true) end as recommendation,
    case when json_extract_path_text(_airbyte_data, 'answer_timestamp', true) != '' then json_extract_path_text(_airbyte_data, 'answer_timestamp', true) end as answer_timestamp,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    getdate() as _airbyte_normalized_at
from "houmdw_test".load_test._airbyte_raw_cancel_visit as table_alias
-- nps_cancel_visit
where 1 = 1
),  __dbt__cte__nps_cancel_visit_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__nps_cancel_visit_ab1
select
    cast(address as varchar) as address,
    cast(country as varchar) as country,
    cast(comments as varchar) as comments,
    cast(appraiser as varchar) as appraiser,
    cast(begin_date as varchar) as begin_date,
    cast(visit_type as varchar) as visit_type,
    cast(property_id as varchar) as property_id,
    cast(schedule_id as varchar) as schedule_id,
    cast(recommendation as varchar) as recommendation,
    cast(answer_timestamp as varchar) as answer_timestamp,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    getdate() as _airbyte_normalized_at
from __dbt__cte__nps_cancel_visit_ab1
-- nps_cancel_visit
where 1 = 1
)-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__nps_cancel_visit_ab2
select
    md5(cast(coalesce(cast(address as varchar), '') || '-' || coalesce(cast(country as varchar), '') || '-' || coalesce(cast(comments as varchar), '') || '-' || coalesce(cast(appraiser as varchar), '') || '-' || coalesce(cast(begin_date as varchar), '') || '-' || coalesce(cast(visit_type as varchar), '') || '-' || coalesce(cast(property_id as varchar), '') || '-' || coalesce(cast(schedule_id as varchar), '') || '-' || coalesce(cast(recommendation as varchar), '') || '-' || coalesce(cast(answer_timestamp as varchar), '') as varchar)) as _airbyte_cancel_visit_hashid,
    tmp.*
from __dbt__cte__nps_cancel_visit_ab2 tmp
-- nps_cancel_visit
where 1 = 1