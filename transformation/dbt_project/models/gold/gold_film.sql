{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='film_id',
    on_schema_change='sync_all_columns'
) }}

with source_film as (
    select
        film_id,
        title,
        description,
        release_year,
        rental_duration,
        rental_rate,
        length,
        replacement_cost,
        rating,
        last_update
    from {{ source('silver', 'silver_film') }}
),

prepared as (
    select
        film_id,
        title,
        description,
        release_year,
        rental_duration,
        cast(rental_rate as decimal(4,2))      as rental_rate,
        length,
        cast(replacement_cost as decimal(5,2)) as replacement_cost,
        rating,
        cast(last_update as timestamp)         as last_update,
        case
            when rental_rate < 1.00 then 'budget'
            when rental_rate < 3.00 then 'standard'
            else 'premium'
        end as rental_rate_tier
    from source_film
)

select *
from prepared
{% if is_incremental() %}
where last_update >= (
    select coalesce(max(last_update), cast('1900-01-01' as timestamp))
    from {{ this }}
)
{% endif %}
