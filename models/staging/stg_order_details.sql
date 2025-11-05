{{ config(
    materialized='incremental',
    unique_key='order_detail_id'
) }}

select
    *,
    current_timestamp() as last_update_timestamp
from {{ source('raw_data', 'order_details_raw') }}

{% if is_incremental() %}
where order_detail_id not in (select distinct order_detail_id from {{ this }})
{% endif %}
