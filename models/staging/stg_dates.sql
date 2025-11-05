{{ config(
    materialized='incremental',
    unique_key='date_id'
) }}

select
    *,
    current_timestamp() as last_update_timestamp
from {{ source('raw_data', 'dates_raw') }}

{% if is_incremental() %}
where date_id not in (select distinct date_id from {{ this }})
{% endif %}
