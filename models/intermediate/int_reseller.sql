select
	reseller_key as reseller_id,
	reseller_id as reseller_cd,
	business_type as business_type_ds,
	reseller as reseller_ds,
	city as city_ds,
	state_province as state_province_ds,
	country_region as country_region_ds,
	postal_code as postal_code_ds

from {{reference('stg_reseller')}}