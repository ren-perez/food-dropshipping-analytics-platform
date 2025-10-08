
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        event_standard as value_field,
        count(*) as n_records

    from "warehouse"."main_staging"."stg_posthog_events"
    group by event_standard

)

select *
from all_values
where value_field not in (
    'homepage_view','product_view','add_to_cart','checkout_started','purchase','other'
)



  
  
      
    ) dbt_internal_test