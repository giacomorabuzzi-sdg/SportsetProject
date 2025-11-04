select
    sales_order_line_key as order_id,
	reseller_key as reseller_id,
	customer_key as customer_id,
	product_key as product_id,
	sales_territory_key as sales_territory_id,
	order_date_key as order_date_id,
	due_date_key as due_date_id,
	ship_date_key as ship_date_id,
	order_quantity as order_qt,
	unit_price as unit_price_vl,
	extended_amount as extended_amount_vl,
	unit_price_discount_pct as unit_price_discount_pct,
	product_standard_cost as product_standard_cost_vl,
	total_product_cost as total_product_cost_vl,
	sales_amount as sales_vl

from {{reference('stg_orders')}}