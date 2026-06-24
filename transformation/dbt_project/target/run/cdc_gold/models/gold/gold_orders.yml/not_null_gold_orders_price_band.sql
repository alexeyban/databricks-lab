
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select price_band
from `workspace`.`gold_gold`.`gold_orders`
where price_band is null



  
  
      
    ) dbt_internal_test