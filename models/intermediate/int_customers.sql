select 
    customer_key as customer_id,
	customer_id as customer_cd,
	customer as customer_ds,
	city as city_ds,
	state_province as state_province_ds,
	country_region as country_region_ds,
	postal_code as postal_code_ds

from {{reference('stg_customers')}}