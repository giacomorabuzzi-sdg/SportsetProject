select
	product_key as product_id,
	sku as product_cd,
	product as product_ds,
	standard_cost as standard_cost_vl,
	color as color_ds,
	list_price as list_price_vl,
	model as model_ds,
	subcategory as subcategory_ds,
	category as category_ds
    
from {{ref('stg_products')}}