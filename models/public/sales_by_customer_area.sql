select 
    cus.state_province_ds,
    cus.country_region_ds,
    sum(sales_vl) as total_revenue,
    sum(total_product_cost_vl) as total_costs,
    sum(order_qt) as order_quantity,
    (total_revenue - total_costs) as total_profit

from {{ ref('fct_orders')}} as ord
left join {{ ref('dim_customers')}} as cus
    using (customer_id)
group by cus.state_province_ds, cus.country_region_ds