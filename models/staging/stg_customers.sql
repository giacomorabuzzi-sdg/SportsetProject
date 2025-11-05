{{ config(
    materialized='incremental',
    unique_key='customer_id'
) }}

select
    *,
    current_timestamp() as last_update_timestamp
from {{ source('raw_data', 'customers_raw') }}

{% if is_incremental() %}
  where customer_id not in (select distinct customer_id from {{ this }})
{% endif %}