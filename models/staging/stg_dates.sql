select
    * from {{ source('raw_data', 'dates_raw') }}