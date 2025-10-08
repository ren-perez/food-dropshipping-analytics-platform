{{ config(materialized='table') }}

with purchases as (
    select *
    from {{ ref('fact_events') }}
    where event_name = 'purchase'
),

orders as (
    select
        order_id,
        min(event_time) as order_time,
        any_value(user_id) as user_id,
        /* sum all lines for the order */
        sum(coalesce(line_revenue, 0)) as order_revenue,
        sum(coalesce(quantity, 0))     as total_items,
        max(campaign)                  as campaign,   -- choose any consistent rule
        max(region)                    as region
    from purchases
    where order_id is not null
    group by order_id
)

select * from orders
