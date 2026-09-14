
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select color
from `workspace`.`gold_gold`.`gold_products`
where color is null



  
  
      
    ) dbt_internal_test