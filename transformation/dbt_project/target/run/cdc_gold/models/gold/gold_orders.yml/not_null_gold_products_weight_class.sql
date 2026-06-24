
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select weight_class
from `workspace`.`gold_gold`.`gold_products`
where weight_class is null



  
  
      
    ) dbt_internal_test