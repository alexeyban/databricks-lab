
  
    
        create or replace table `workspace`.`gold_gold`.`gold_products`
      
      
    using delta
  
      
      
      
      
      
      
      
      as
      with source_products as (
    select
        id,
        product_name,
        weight,
        color,
        created_at,
        updated_at
    from `workspace`.`silver`.`silver_products`
),

prepared as (
    select
        id,
        product_name,
        weight,
        color,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        case
            when weight < 0.5 then 'light'
            when weight < 1.5 then 'medium'
            else 'heavy'
        end as weight_class
    from source_products
)

select *
from prepared
  