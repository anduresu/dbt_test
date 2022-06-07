{{ config(
    sort = "_airbyte_emitted_at",
    unique_key = '_airbyte_ab_id',
    schema = "load_test",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('load_test', '_airbyte_raw_cancel_visit') }}
select
    {{ json_extract_scalar('_airbyte_data', ['address'], ['address']) }} as address,
    {{ json_extract_scalar('_airbyte_data', ['country'], ['country']) }} as country,
    {{ json_extract_scalar('_airbyte_data', ['comments'], ['comments']) }} as comments,
    {{ json_extract_scalar('_airbyte_data', ['appraiser'], ['appraiser']) }} as appraiser,
    {{ json_extract_scalar('_airbyte_data', ['begin_date'], ['begin_date']) }} as begin_date,
    {{ json_extract_scalar('_airbyte_data', ['visit_type'], ['visit_type']) }} as visit_type,
    {{ json_extract_scalar('_airbyte_data', ['property_id'], ['property_id']) }} as property_id,
    {{ json_extract_scalar('_airbyte_data', ['schedule_id'], ['schedule_id']) }} as schedule_id,
    {{ json_extract_scalar('_airbyte_data', ['recommendation'], ['recommendation']) }} as recommendation,
    {{ json_extract_scalar('_airbyte_data', ['answer_timestamp'], ['answer_timestamp']) }} as answer_timestamp,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('load_test', '_airbyte_raw_cancel_visit') }} as table_alias
-- nps_cancel_visit
where 1 = 1

