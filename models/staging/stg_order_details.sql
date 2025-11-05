{{ config(
    materialized='incremental',
    unique_key='sales_order_line_key'
) }}

select
    *,
    current_timestamp() as last_update_timestamp
from {{ source('raw_data', 'order_details_raw') }}

{% if is_incremental() %}
where sales_order_line_key not in (select distinct sales_order_line_key from {{ this }})
{% endif %}
