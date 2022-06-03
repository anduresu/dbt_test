

  create  table
    "houmdw_prod".airbyte_gspread."anuncios__dbt_tmp"
    
    
      compound sortkey(_airbyte_emitted_at)
  as (
    
with __dbt__cte__anuncios_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "houmdw_prod".airbyte_gspread._airbyte_raw_anuncios
select
    case when json_extract_path_text(_airbyte_data, 'Cost', true) != '' then json_extract_path_text(_airbyte_data, 'Cost', true) end as cost,
    case when json_extract_path_text(_airbyte_data, 'Date', true) != '' then json_extract_path_text(_airbyte_data, 'Date', true) end as date,
    case when json_extract_path_text(_airbyte_data, 'Reach', true) != '' then json_extract_path_text(_airbyte_data, 'Reach', true) end as reach,
    case when json_extract_path_text(_airbyte_data, 'Account', true) != '' then json_extract_path_text(_airbyte_data, 'Account', true) end as account,
    case when json_extract_path_text(_airbyte_data, 'Actions', true) != '' then json_extract_path_text(_airbyte_data, 'Actions', true) end as actions,
    case when json_extract_path_text(_airbyte_data, 'Ad name', true) != '' then json_extract_path_text(_airbyte_data, 'Ad name', true) end as "ad name",
    case when json_extract_path_text(_airbyte_data, 'Ad status', true) != '' then json_extract_path_text(_airbyte_data, 'Ad status', true) end as "ad status",
    case when json_extract_path_text(_airbyte_data, 'Ad set name', true) != '' then json_extract_path_text(_airbyte_data, 'Ad set name', true) end as "ad set name",
    case when json_extract_path_text(_airbyte_data, 'Impressions', true) != '' then json_extract_path_text(_airbyte_data, 'Impressions', true) end as impressions,
    case when json_extract_path_text(_airbyte_data, 'Link clicks', true) != '' then json_extract_path_text(_airbyte_data, 'Link clicks', true) end as "link clicks",
    case when json_extract_path_text(_airbyte_data, 'Clicks (all)', true) != '' then json_extract_path_text(_airbyte_data, 'Clicks (all)', true) end as "clicks (all)",
    case when json_extract_path_text(_airbyte_data, 'Campaign name', true) != '' then json_extract_path_text(_airbyte_data, 'Campaign name', true) end as "campaign name",
    case when json_extract_path_text(_airbyte_data, 'Website leads', true) != '' then json_extract_path_text(_airbyte_data, 'Website leads', true) end as "website leads",
    _airbyte_ab_id,
    _airbyte_emitted_at,
    getdate() as _airbyte_normalized_at
from "houmdw_prod".airbyte_gspread._airbyte_raw_anuncios as table_alias
-- anuncios
where 1 = 1
),  __dbt__cte__anuncios_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__anuncios_ab1
select
    cast(cost as varchar) as cost,
    cast(date as varchar) as date,
    cast(reach as varchar) as reach,
    cast(account as varchar) as account,
    cast(actions as varchar) as actions,
    cast("ad name" as varchar) as "ad name",
    cast("ad status" as varchar) as "ad status",
    cast("ad set name" as varchar) as "ad set name",
    cast(impressions as varchar) as impressions,
    cast("link clicks" as varchar) as "link clicks",
    cast("clicks (all)" as varchar) as "clicks (all)",
    cast("campaign name" as varchar) as "campaign name",
    cast("website leads" as varchar) as "website leads",
    _airbyte_ab_id,
    _airbyte_emitted_at,
    getdate() as _airbyte_normalized_at
from __dbt__cte__anuncios_ab1
-- anuncios
where 1 = 1
),  __dbt__cte__anuncios_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__anuncios_ab2
select
    md5(cast(coalesce(cast(cost as varchar), '') || '-' || coalesce(cast(date as varchar), '') || '-' || coalesce(cast(reach as varchar), '') || '-' || coalesce(cast(account as varchar), '') || '-' || coalesce(cast(actions as varchar), '') || '-' || coalesce(cast("ad name" as varchar), '') || '-' || coalesce(cast("ad status" as varchar), '') || '-' || coalesce(cast("ad set name" as varchar), '') || '-' || coalesce(cast(impressions as varchar), '') || '-' || coalesce(cast("link clicks" as varchar), '') || '-' || coalesce(cast("clicks (all)" as varchar), '') || '-' || coalesce(cast("campaign name" as varchar), '') || '-' || coalesce(cast("website leads" as varchar), '') as varchar)) as _airbyte_anuncios_hashid,
    tmp.*
from __dbt__cte__anuncios_ab2 tmp
-- anuncios
where 1 = 1
)-- Final base SQL model
-- depends_on: __dbt__cte__anuncios_ab3
select
    cost,
    date,
    reach,
    account,
    actions,
    "ad name",
    "ad status",
    "ad set name",
    impressions,
    "link clicks",
    "clicks (all)",
    "campaign name",
    "website leads",
    _airbyte_ab_id,
    _airbyte_emitted_at,
    getdate() as _airbyte_normalized_at,
    _airbyte_anuncios_hashid
from __dbt__cte__anuncios_ab3
-- anuncios from "houmdw_prod".airbyte_gspread._airbyte_raw_anuncios
where 1 = 1
  );