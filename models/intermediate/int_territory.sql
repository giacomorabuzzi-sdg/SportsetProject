select
	sales_territory_key as sales_territory_id,
	region as region_ds,
	country as country_ds,
	group_ as group_ds,
	last_update_timestamp as last_update_ts

from {{ref('stg_territory')}}