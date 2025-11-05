select
    prod.category_ds,
    ter.country_ds,
    group_ds,
    avg(datediff(day,dat_o.date_dt, dat_s.date_dt)) as avg_ship_delay,
    avg(datediff(day,dat_s.date_dt, dat_d.date_dt)) as avg_due_delay

from {{ ref('fct_orders')}} as ord
left join {{ ref('dim_dates')}} as dat_o
    on order_date_id = dat_o.date_id
left join {{ ref('dim_dates')}} as dat_s
    on ship_date_id = dat_s.date_id
left join {{ ref('dim_dates')}} as dat_d
    on due_date_id = dat_d.date_id
left join {{ ref('dim_territory')}} as ter
    using (sales_territory_id)
left join {{ ref('dim_products')}} as prod
    using (product_id)

group by prod.category_ds, ter.country_ds, group_ds
