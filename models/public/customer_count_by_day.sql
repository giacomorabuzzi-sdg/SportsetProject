select
    date_dt,
    count(distinct(ord.customer_id)) as daily_number_customers

from {{ ref('fct_orders')}} as ord

left join {{ ref('dim_dates') }} as dat
    on ord.order_date_id = dat.date_id
left join {{ ref('dim_customers')}} as cus
    using(customer_id)

group by
    date_dt

order by
    date_dt
