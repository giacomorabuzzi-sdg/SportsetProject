{{ config(
    materialized='incremental',
    unique_key='reseller_key'
) }}

select
    *,
    current_timestamp() as last_update_timestamp
from {{ source('raw_data', 'reseller_raw') }}

{% if is_incremental() %}
where reseller_key not in (select distinct reseller_key from {{ this }})
{% endif %}
