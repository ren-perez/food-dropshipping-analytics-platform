
  
    
    

    create  table
      "warehouse"."main_core"."fact_events__dbt_tmp"
  
    as (
      

with enriched as (
    select
        user_id,
        event_time,
        event_standard as event_name,
        url,
        product_id,
        campaign,
        region,
        order_id,
        price,
        quantity,
        amount,
        case event_standard
            when 'homepage_view'   then 1
            when 'product_view'    then 2
            when 'add_to_cart'     then 3
            when 'checkout_started' then 4
            when 'purchase'        then 5
            else null
        end as funnel_step,
        /* line_revenue: prefer explicit amount; else price*qty */
        coalesce(amount, price * nullif(quantity,0)) as line_revenue
    from "warehouse"."main_staging"."stg_posthog_events"
)

select *
from enriched
    );
  
  