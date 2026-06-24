select
    product_name,
    color,
    total_amount
from {{ ref('total_products_order') }}
where total_amount <= 0
