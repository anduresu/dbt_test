
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