
    
    

select
    id as unique_field,
    count(*) as n_records

from `workspace`.`gold_gold`.`gold_orders`
where id is not null
group by id
having count(*) > 1


