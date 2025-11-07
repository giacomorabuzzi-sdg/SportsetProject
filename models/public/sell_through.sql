select
    dat.date_dt as date_dt,
    sum(total_product_cost_vl) as sell_in,
    sum(sales_vl) as sell_out,
    sell_out / sell_in *100 as sell_through

from {{ ref('fct_orders') }} as ord
left join {{ ref('dim_dates') }} as dat
    on ord.order_date_id = dat.date_id

group by
    dat.date_dt

order by
    dat.date_dt