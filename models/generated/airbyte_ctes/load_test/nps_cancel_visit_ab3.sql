{{ config(
    sort = "_airbyte_emitted_at",
    unique_key = '_airbyte_ab_id',
    schema = "load_test",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('nps_cancel_visit_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'address',
        'country',
        'comments',
        'appraiser',
        'begin_date',
        'visit_type',
        'property_id',
        'schedule_id',
        'recommendation',
        'answer_timestamp',
    ]) }} as _airbyte_cancel_visit_hashid,
    tmp.*
from {{ ref('nps_cancel_visit_ab2') }} tmp
-- nps_cancel_visit
where 1 = 1

