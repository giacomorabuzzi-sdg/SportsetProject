select
    sales_order_line_key as order_id,
	channel as channel_ds,
	sales_order as sales_order,
	sales_order_line as sales_order_line

from {{ref('stg_order_details')}}