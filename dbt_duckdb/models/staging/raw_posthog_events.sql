{{ config(materialized='view') }}

select *
from read_parquet('../data/raw/posthog/*/*/*/*.parquet')
