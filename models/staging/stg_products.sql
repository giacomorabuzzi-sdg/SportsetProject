select
    product_key,
	sku,
	product,
	replace(replace(standard_cost, '$', ''), ',', '')::decimal(10, 2) as standard_cost,
	color,
	replace(replace(list_price, '$', ''), ',', '')::decimal(10, 2) as list_price,
	model,
	subcategory,
	category

from {{ source('raw_data', 'products_raw') }}


    