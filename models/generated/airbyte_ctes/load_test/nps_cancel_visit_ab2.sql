{{ config(
    sort = "_airbyte_emitted_at",
    unique_key = '_airbyte_ab_id',
    schema = "load_test",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('nps_cancel_visit_ab1') }}
select
    cast(address as {{ dbt_utils.type_string() }}) as address,
    cast(country as {{ dbt_utils.type_string() }}) as country,
    cast(comments as {{ dbt_utils.type_string() }}) as comments,
    cast(appraiser as {{ dbt_utils.type_string() }}) as appraiser,
    cast(begin_date as {{ dbt_utils.type_string() }}) as begin_date,
    cast(visit_type as {{ dbt_utils.type_string() }}) as visit_type,
    cast(property_id as {{ dbt_utils.type_string() }}) as property_id,
    cast(schedule_id as {{ dbt_utils.type_string() }}) as schedule_id,
    cast(recommendation as {{ dbt_utils.type_string() }}) as recommendation,
    cast(answer_timestamp as {{ dbt_utils.type_string() }}) as answer_timestamp,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('nps_cancel_visit_ab1') }}
-- nps_cancel_visit
where 1 = 1

