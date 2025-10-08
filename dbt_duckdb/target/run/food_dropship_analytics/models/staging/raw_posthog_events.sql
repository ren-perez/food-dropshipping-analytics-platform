
  
create view "warehouse"."main_staging"."raw_posthog_events__dbt_tmp" as (
    

select *
from read_parquet('../data/raw/posthog/*/*/*/*.parquet')
  );
