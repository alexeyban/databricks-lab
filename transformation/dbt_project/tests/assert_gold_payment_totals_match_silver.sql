-- Gold vs Silver sum reconciliation for payment totals.
-- Returns rows where the discrepancy exceeds $0.01 — test fails if any rows returned.
with gold_totals as (
    select
        rental_id,
        total_paid as gold_total
    from {{ ref('gold_rental') }}
    where total_paid is not null
),

silver_totals as (
    select
        rental_id,
        sum(amount) as silver_total
    from {{ source('silver', 'silver_payment') }}
    group by rental_id
),

reconciliation as (
    select
        g.rental_id,
        g.gold_total,
        s.silver_total,
        abs(coalesce(g.gold_total, 0) - coalesce(s.silver_total, 0)) as discrepancy
    from gold_totals g
    full outer join silver_totals s using (rental_id)
)

select *
from reconciliation
where discrepancy > 0.01
