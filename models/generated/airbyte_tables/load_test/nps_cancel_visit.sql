{{ config(
    sort = "_airbyte_emitted_at",
    unique_key = '_airbyte_ab_id',
    schema = "load_test",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('nps_cancel_visit_ab3') }}
select
    address,
    country,
    comments,
    appraiser,
    begin_date,
    visit_type,
    property_id,
    schedule_id,
    recommendation,
    answer_timestamp,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_cancel_visit_hashid
from {{ ref('nps_cancel_visit_ab3') }}
-- nps_cancel_visit from {{ source('load_test', '_airbyte_raw_cancel_visit') }}
where 1 = 1
