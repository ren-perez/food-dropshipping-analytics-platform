
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select order_revenue
from "warehouse"."main_core"."fact_orders"
where order_revenue is null



  
  
      
    ) dbt_internal_test