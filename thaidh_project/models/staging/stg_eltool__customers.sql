{{
    config(
        
        tags=['dbt']
    )
}}
with source as (
    select *
    from {{ ref('customers_snapshot') }}
),

renamed as (
    select
        customer_id,
        zipcode,
        city,
        state_code,
        datetime_created::TIMESTAMP as datetime_created,
        datetime_updated::TIMESTAMP as datetime_updated,
        dbt_valid_from,
        dbt_valid_to
    from source
)

select *
from renamed
