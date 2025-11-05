{{ config(
    materialized='incremental',
    unique_key='date_key'
) }}

select
    *,
    current_timestamp() as last_update_timestamp
from {{ source('raw_data', 'dates_raw') }}

{% if is_incremental() %}
where date_key not in (select distinct date_key from {{ this }})
{% endif %}
