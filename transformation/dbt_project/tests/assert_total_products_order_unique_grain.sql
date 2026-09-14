select
    product_name,
    color,
    count(*) as row_count
from {{ ref('total_products_order') }}
group by
    product_name,
    color
having count(*) > 1
