
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select id
from `workspace`.`gold_gold`.`gold_orders`
where id is null



  
  
      
    ) dbt_internal_test