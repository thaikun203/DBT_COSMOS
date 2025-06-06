{{
    config(
        
        tags=['dbt']
    )
}}
with orders as (
    select *
    from {{ ref('fct_orders')}}
),

customers as (
    select *
    from {{ ref('dim_customers')}}
)

select
    o.order_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    c.customer_id,
    c.zipcode as customer_zipcode,
    c.city as customer_city,
    c.state_code as customer_state_code,
    c.state_name as customer_state_name
from orders as o
inner join customers as c on
    o.customer_id = c.customer_id
    and o.order_purchase_timestamp >= c.valid_from
    and o.order_purchase_timestamp <= c.valid_to
