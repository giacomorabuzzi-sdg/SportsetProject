{{ config(
    materialized='incremental',
    unique_key='sales_order_line_key'
) }}

select
    sales_order_line_key,
    reseller_key,
    customer_key,
    product_key,
    sales_territory_key,
    order_date_key,
    due_date_key,
    ship_date_key,
    order_quantity,
    replace(replace(unit_price, '$', ''), ',', '')::decimal(10, 2) as unit_price,
    replace(replace(extended_amount, '$', ''), ',', '')::decimal(10, 2) as extended_amount,
    replace(replace(unit_price_discount_pct, '%', ''), ',', '')::decimal(10, 2) as unit_price_discount_pct,
    replace(replace(product_standard_cost, '$', ''), ',', '')::decimal(10, 2) as product_standard_cost,
    replace(replace(total_product_cost, '$', ''), ',', '')::decimal(10, 2) as total_product_cost,
    replace(replace(sales_amount, '$', ''), ',', '')::decimal(10, 2) as sales_amount,
    current_timestamp() as last_update_timestamp
from {{ source('raw_data', 'orders_raw') }}

{% if is_incremental() %}
where sales_order_line_key not in (select distinct sales_order_line_key from {{ this }})
{% endif %}
