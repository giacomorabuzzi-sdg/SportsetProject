select
    * from {{ source('raw_data', 'reseller_raw') }}