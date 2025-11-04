select
    * from {{ source('raw_data', 'sales_territory_raw') }}