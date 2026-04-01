{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='rental_id',
    on_schema_change='sync_all_columns'
) }}

with source_rental as (
    select
        rental_id,
        rental_date,
        inventory_id,
        customer_id,
        return_date,
        staff_id,
        last_updated_dt
    from {{ source('silver', 'silver_rental') }}
),

payment_totals as (
    select
        rental_id,
        sum(amount) as total_paid
    from {{ source('silver', 'silver_payment') }}
    group by rental_id
),

prepared as (
    select
        r.rental_id,
        cast(r.rental_date as timestamp)  as rental_date,
        r.inventory_id,
        r.customer_id,
        cast(r.return_date as timestamp)  as return_date,
        r.staff_id,
        case
            when r.return_date is null then 'open'
            else 'returned'
        end                               as rental_status,
        cast(p.total_paid as decimal(10,2)) as total_paid,
        r.last_updated_dt
    from source_rental r
    left join payment_totals p on r.rental_id = p.rental_id
)

select *
from prepared
{% if is_incremental() %}
where last_updated_dt >= (
    select coalesce(max(last_updated_dt), cast('1900-01-01' as timestamp))
    from {{ this }}
)
{% endif %}
