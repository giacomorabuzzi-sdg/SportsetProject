select 
    month(date_dt) as month_number,
    year(date_dt) as year_number,
    sum(sales_vl) as total_revenue,
    sum(total_product_cost_vl) as total_costs,
    sum(order_qt) as order_quantity,
    (total_revenue - total_costs) as total_profit

from {{ ref('fct_orders')}} as ord
left join {{ ref('dim_dates')}} as dat
    on order_date_id = dat.date_id
group by month_number, year_number
order by year_number, month_number