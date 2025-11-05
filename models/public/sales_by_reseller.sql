select 
    ord.reseller_id,
    reseller_ds,
    business_type_ds,
    country_region_ds,
    sum(sales_vl) as total_revenue,
    sum(total_product_cost_vl) as total_costs,
    sum(order_qt) as order_quantity,
    (total_revenue - total_costs) as total_profit

from {{ ref('fct_orders')}} as ord
left join {{ ref('dim_reseller')}} as res
    using (reseller_id)
group by     ord.reseller_id, reseller_ds, business_type_ds, country_region_ds