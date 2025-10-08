{{ config(materialized='view') }}

select *
from {{ source('raw', 'posthog_events') }}
