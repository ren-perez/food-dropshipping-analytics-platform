
  
    
    

    create  table
      "warehouse"."main_marts"."daily_sales__dbt_tmp"
  
    as (
      

select
  date_trunc('day', order_time)::date as date_day,
  count(distinct order_id)           as orders,
  sum(order_revenue)                 as gross_revenue
from "warehouse"."main_core"."fact_orders"
group by 1
order by 1
    );
  
  