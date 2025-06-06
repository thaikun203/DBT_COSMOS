
{% snapshot customers_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='customer_id',

      strategy='timestamp',
      updated_at='datetime_updated',
      tags=['dbt']
    )
}}

select * from {{ source('raw_layer', 'customers') }}

{% endsnapshot %}
