select
    * from {{ source('raw_data', 'order_details_raw') }}