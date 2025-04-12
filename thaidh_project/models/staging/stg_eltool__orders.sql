{{
    config(
        
        tags=['dbt']
    )
}}
with source as (
    select *
    from {{ source('raw_layer', 'orders') }}
),

renamed as (
    select
        order_id,
        customer_id,
        order_status,
        COALESCE(NULLIF(order_purchase_timestamp, ''), NULL)::TIMESTAMP AS order_purchase_timestamp,
        COALESCE(NULLIF(order_approved_at, ''), NULL)::TIMESTAMP AS order_approved_at,
        COALESCE(NULLIF(order_delivered_carrier_date, ''), NULL)::TIMESTAMP AS order_delivered_carrier_date,
        COALESCE(NULLIF(order_delivered_customer_date, ''), NULL)::TIMESTAMP AS order_delivered_customer_date,
        COALESCE(NULLIF(order_estimated_delivery_date, ''), NULL)::TIMESTAMP AS order_estimated_delivery_date
    from source
)

select *
from renamed

