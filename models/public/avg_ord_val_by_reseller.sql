select 
    reseller_id,
    reseller_ds,
    month(dat.date_dt) as sales_month,
    year(dat.date_dt) as sales_year,
    avg(sales_vl) as aov,

    lag(avg(sales_vl), 1) 
        over (
            partition by reseller_id 
            order by year(dat.date_dt), month(dat.date_dt)
        ) as aov_prev_month,

    lead(avg(sales_vl), 1) 
        over (
            partition by reseller_id 
            order by year(dat.date_dt), month(dat.date_dt)
        ) as aov_next_month,
    
    (aov+aov_prev_month+aov_next_month)/3 as smoothed_aov

from {{ ref('fct_orders') }} as ord

left join {{ ref('dim_reseller') }} as res 
    using (reseller_id)
left join {{ ref('dim_dates') }} as dat 
    on order_date_id = dat.date_id

group by reseller_id,
    reseller_ds,
    sales_month,
    sales_year

order by
    sales_year,
    sales_month,
    reseller_id