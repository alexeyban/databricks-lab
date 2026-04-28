
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select id
from `workspace`.`gold`.`my_first_dbt_model`
where id is null



  
  
      
    ) dbt_internal_test