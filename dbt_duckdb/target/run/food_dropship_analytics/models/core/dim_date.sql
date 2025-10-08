
  
    
    

    create  table
      "warehouse"."main_core"."dim_date__dbt_tmp"
  
    as (
      

with bounds as (
    select
        date_trunc('day', min(event_time)) as start_date,
        date_trunc('day', max(event_time)) as end_date
    from "warehouse"."main_core"."fact_events"
),
calendar as (
    select
        d::date as date_day
    from bounds,
         generate_series(start_date, end_date, interval 1 day) as t(d)
)

select
  date_day,
  extract(year  from date_day)::int as year,
  extract(quarter from date_day)::int as quarter,
  extract(month from date_day)::int as month,
  extract(day   from date_day)::int as day,
  extract(dow   from date_day)::int as day_of_week,
  strftime(date_day, '%A')          as day_name,
  strftime(date_day, '%Y-%m')       as year_month
from calendar
order by date_day
    );
  
  