
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select last_updated_dt
from `workspace`.`gold_gold`.`gold_orders`
where last_updated_dt is null



  
  
      
    ) dbt_internal_test