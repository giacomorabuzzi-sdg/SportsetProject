{{ config(
    materialized='incremental',
    unique_key='product_key'
) }}

select
    product_key,
    sku,
    product,
    replace(replace(standard_cost, '$', ''), ',', '')::decimal(10, 2) as standard_cost,
    color,
    replace(replace(list_price, '$', ''), ',', '')::decimal(10, 2) as list_price,
    model,
    subcategory,
    category,
    current_timestamp() as last_update_timestamp
from {{ source('raw_data', 'products_raw') }}

{% if is_incremental() %}
where product_key not in (select distinct product_key from {{ this }})
{% endif %}
