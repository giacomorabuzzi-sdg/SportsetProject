select 
    category_ds,
    subcategory_ds,
    sum(sales_vl) as total_revenue,
    sum(total_product_cost_vl) as total_costs,
    sum(order_qt) as order_quantity,
    sum(sales_vl - total_product_cost_vl) as total_profit,
    (total_revenue - total_costs)/order_quantity as total_profit_per_piece

from {{ ref('fct_orders')}} as ord
left join {{ ref('dim_products')}} as prod 
    using (product_id)
group by category_ds, subcategory_ds