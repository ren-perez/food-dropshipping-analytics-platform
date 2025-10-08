{{ config(materialized='table') }}

with steps as (
    select funnel_step, count(distinct user_id) as users
    from {{ ref('fact_events') }}
    where funnel_step is not null
    group by funnel_step
),
ordered as (
    select
      funnel_step,
      users,
      lag(users) over (order by funnel_step) as prev_users,
      first_value(users) over (order by funnel_step) as first_users
    from steps
)

select
  funnel_step,
  users,
  prev_users,
  round(100.0 * users / nullif(prev_users,0), 2) as step_conversion_rate,
  round(100.0 * users / nullif(first_users,0), 2) as total_conversion_rate
from ordered
order by funnel_step
