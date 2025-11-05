{{ config(
    materialized='incremental',
    unique_key='sales_territory_key'
) }}

select
    *,
    current_timestamp() as last_update_timestamp
from {{ source('raw_data', 'sales_territory_raw') }}

{% if is_incremental() %}
where sales_territory_key not in (select distinct sales_territory_key from {{ this }})
{% endif %}
