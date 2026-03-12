select
    s.product_name,
    s.color as color,
    sum(o.price) as total_amount
from {{ source('silver', 'silver_orders') }} o
inner join {{ source('silver', 'silver_products') }} s
    on o.product_id = s.id
group by
    s.product_name,
    s.color
