select 
    year(dat.date_dt) as sales_year,
    ter.group_ds as area,
    sum(sales_vl) as total_sales,
    lag(total_sales, 1) 
        over (
            partition by ter.group_ds 
            order by year(dat.date_dt)
        ) as total_sales_prev_year,
    sum(sales_vl-total_product_cost_vl) as total_profit,
    lag(total_profit, 1) 
        over (
            partition by ter.group_ds 
            order by year(dat.date_dt)
        ) as total_profit_prev_year,
    100 * (total_sales - total_sales_prev_year)/total_sales_prev_year as pct_sales_variation,
    100 * (total_profit - total_profit_prev_year)/total_profit_prev_year as pct_profit_variation


from {{ ref('fct_orders') }}

left join {{ ref('dim_territory') }} as ter
    using (sales_territory_id)
left join {{ ref('dim_dates') }} as dat 
    on order_date_id = dat.date_id

group by
    sales_year,
    ter.group_ds

order by
    sales_year,
    area