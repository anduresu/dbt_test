

  create  table
    "houmdw_prod".airbyte_gspread."nps_colombia_foto__dbt_tmp"
    
    
      compound sortkey(_airbyte_emitted_at)
  as (
    
with __dbt__cte__nps_colombia_foto_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "houmdw_prod".airbyte_gspread._airbyte_raw_nps_colombia_foto
select
    case when json_extract_path_text(_airbyte_data, 'date', true) != '' then json_extract_path_text(_airbyte_data, 'date', true) end as date,
    case when json_extract_path_text(_airbyte_data, 'time', true) != '' then json_extract_path_text(_airbyte_data, 'time', true) end as "time",
    case when json_extract_path_text(_airbyte_data, 'type', true) != '' then json_extract_path_text(_airbyte_data, 'type', true) end as type,
    case when json_extract_path_text(_airbyte_data, 'Prop id', true) != '' then json_extract_path_text(_airbyte_data, 'Prop id', true) end as "prop id",
    case when json_extract_path_text(_airbyte_data, 'address', true) != '' then json_extract_path_text(_airbyte_data, 'address', true) end as address,
    case when json_extract_path_text(_airbyte_data, 'appraiser', true) != '' then json_extract_path_text(_airbyte_data, 'appraiser', true) end as appraiser,
    case when json_extract_path_text(_airbyte_data, 'Schedule id', true) != '' then json_extract_path_text(_airbyte_data, 'Schedule id', true) end as "schedule id",
    case when json_extract_path_text(_airbyte_data, '¿Cómo evaluarías el servicio de tu Houmer?', true) != '' then json_extract_path_text(_airbyte_data, '¿Cómo evaluarías el servicio de tu Houmer?', true) end as "¿cómo evaluarías el servicio de tu houmer?",
    case when json_extract_path_text(_airbyte_data, '¿Cómo fue la atención del Houmer previo a la visita?', true) != '' then json_extract_path_text(_airbyte_data, '¿Cómo fue la atención del Houmer previo a la visita?', true) end as "¿cómo fue la atención del houmer previo a la visita?",
    case when json_extract_path_text(_airbyte_data, '¿Cómo fue la simplicidad de la página para agendar visitas?', true) != '' then json_extract_path_text(_airbyte_data, '¿Cómo fue la simplicidad de la página para agendar visitas?', true) end as "¿cómo fue la simplicidad de la página para agendar visitas?",
    case when json_extract_path_text(_airbyte_data, '¿Tienes algún comentario que nos ayude a mejorar nuestro servicio?', true) != '' then json_extract_path_text(_airbyte_data, '¿Tienes algún comentario que nos ayude a mejorar nuestro servicio?', true) end as "¿tienes algún comentario que nos ayude a mejorar nuestro servicio?",
    case when json_extract_path_text(_airbyte_data, '¿Cómo fue tu experiencia en la toma de fotografías de tu propiedad?', true) != '' then json_extract_path_text(_airbyte_data, '¿Cómo fue tu experiencia en la toma de fotografías de tu propiedad?', true) end as "¿cómo fue tu experiencia en la toma de fotografías de tu propiedad?",
    case when json_extract_path_text(_airbyte_data, '¿Qué posibilidades hay de que recomiende nuestro servicio a un amigo o colega?', true) != '' then json_extract_path_text(_airbyte_data, '¿Qué posibilidades hay de que recomiende nuestro servicio a un amigo o colega?', true) end as "¿qué posibilidades hay de que recomiende nuestro servicio a un amigo o colega?",
    _airbyte_ab_id,
    _airbyte_emitted_at,
    getdate() as _airbyte_normalized_at
from "houmdw_prod".airbyte_gspread._airbyte_raw_nps_colombia_foto as table_alias
-- nps_colombia_foto
where 1 = 1
),  __dbt__cte__nps_colombia_foto_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__nps_colombia_foto_ab1
select
    cast(date as varchar) as date,
    cast("time" as varchar) as "time",
    cast(type as varchar) as type,
    cast("prop id" as varchar) as "prop id",
    cast(address as varchar) as address,
    cast(appraiser as varchar) as appraiser,
    cast("schedule id" as varchar) as "schedule id",
    cast("¿cómo evaluarías el servicio de tu houmer?" as varchar) as "¿cómo evaluarías el servicio de tu houmer?",
    cast("¿cómo fue la atención del houmer previo a la visita?" as varchar) as "¿cómo fue la atención del houmer previo a la visita?",
    cast("¿cómo fue la simplicidad de la página para agendar visitas?" as varchar) as "¿cómo fue la simplicidad de la página para agendar visitas?",
    cast("¿tienes algún comentario que nos ayude a mejorar nuestro servicio?" as varchar) as "¿tienes algún comentario que nos ayude a mejorar nuestro servicio?",
    cast("¿cómo fue tu experiencia en la toma de fotografías de tu propiedad?" as varchar) as "¿cómo fue tu experiencia en la toma de fotografías de tu propiedad?",
    cast("¿qué posibilidades hay de que recomiende nuestro servicio a un amigo o colega?" as varchar) as "¿qué posibilidades hay de que recomiende nuestro servicio a un amigo o colega?",
    _airbyte_ab_id,
    _airbyte_emitted_at,
    getdate() as _airbyte_normalized_at
from __dbt__cte__nps_colombia_foto_ab1
-- nps_colombia_foto
where 1 = 1
),  __dbt__cte__nps_colombia_foto_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__nps_colombia_foto_ab2
select
    md5(cast(coalesce(cast(date as varchar), '') || '-' || coalesce(cast("time" as varchar), '') || '-' || coalesce(cast(type as varchar), '') || '-' || coalesce(cast("prop id" as varchar), '') || '-' || coalesce(cast(address as varchar), '') || '-' || coalesce(cast(appraiser as varchar), '') || '-' || coalesce(cast("schedule id" as varchar), '') || '-' || coalesce(cast("¿cómo evaluarías el servicio de tu houmer?" as varchar), '') || '-' || coalesce(cast("¿cómo fue la atención del houmer previo a la visita?" as varchar), '') || '-' || coalesce(cast("¿cómo fue la simplicidad de la página para agendar visitas?" as varchar), '') || '-' || coalesce(cast("¿tienes algún comentario que nos ayude a mejorar nuestro servicio?" as varchar), '') || '-' || coalesce(cast("¿cómo fue tu experiencia en la toma de fotografías de tu propiedad?" as varchar), '') || '-' || coalesce(cast("¿qué posibilidades hay de que recomiende nuestro servicio a un amigo o colega?" as varchar), '') as varchar)) as _airbyte_nps_colombia_foto_hashid,
    tmp.*
from __dbt__cte__nps_colombia_foto_ab2 tmp
-- nps_colombia_foto
where 1 = 1
)-- Final base SQL model
-- depends_on: __dbt__cte__nps_colombia_foto_ab3
select
    date,
    "time",
    type,
    "prop id",
    address,
    appraiser,
    "schedule id",
    "¿cómo evaluarías el servicio de tu houmer?",
    "¿cómo fue la atención del houmer previo a la visita?",
    "¿cómo fue la simplicidad de la página para agendar visitas?",
    "¿tienes algún comentario que nos ayude a mejorar nuestro servicio?",
    "¿cómo fue tu experiencia en la toma de fotografías de tu propiedad?",
    "¿qué posibilidades hay de que recomiende nuestro servicio a un amigo o colega?",
    _airbyte_ab_id,
    _airbyte_emitted_at,
    getdate() as _airbyte_normalized_at,
    _airbyte_nps_colombia_foto_hashid
from __dbt__cte__nps_colombia_foto_ab3
-- nps_colombia_foto from "houmdw_prod".airbyte_gspread._airbyte_raw_nps_colombia_foto
where 1 = 1
  );