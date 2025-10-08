
  
  create view "warehouse"."main_staging"."stg_posthog_events__dbt_tmp" as (
    

with base as (
    select
        cast(distinct_id as varchar)          as user_id,
        cast(event as varchar)                as event_name,
        cast(timestamp as timestamp)          as event_time,
        /* properties is a JSON string; extract safely */
        json_extract_string(properties, '$."$current_url"')      as url,
        json_extract_string(properties, '$.product_id')          as product_id,
        json_extract_string(properties, '$.campaign')            as campaign,
        json_extract_string(properties, '$.region')              as region,
        json_extract_string(properties, '$.order_id')            as order_id,
        /* numeric fields for orders (if present) */
        try_cast(json_extract_string(properties, '$.price') as double)     as price,
        try_cast(json_extract_string(properties, '$.quantity') as int)     as quantity,
        try_cast(json_extract_string(properties, '$.amount') as double)     as amount
    from "warehouse"."main_staging"."raw_posthog_events"
),

-- Normalize event names used by the funnel (your app should send these)
mapped as (
    select
        *,
        case
            when event_name in ('homepage_view', '$pageview') then 'homepage_view'
            when event_name = 'product_view'                  then 'product_view'
            when event_name = 'add_to_cart'                   then 'add_to_cart'
            when event_name = 'checkout_started'              then 'checkout_started'
            when event_name in ('purchase', 'order_completed') then 'purchase'
            else 'other'
        end as event_standard
    from base
)

select * from mapped
  );
