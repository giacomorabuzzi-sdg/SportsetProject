with cumulative_order_count as (

select
    cus.customer_id,
    dat.date_dt as date_dt,
    month(date_dt) as month_number,
    year(date_dt) as year_number,
    count(*) over (
            partition by cus.customer_id
            order by year(date_dt), month(date_dt)
            rows between unbounded preceding and current row) as cumulative_orders,

from  {{ ref('fct_orders') }} as ord
left join {{ ref('dim_customers') }} as cus
    on ord.customer_id = cus.customer_id
left join {{ ref('dim_dates') }} as dat
    on ord.order_date_id = dat.date_id

where
    cus.customer_id != -1
order by
    year_number,
    month_number,
    cus.customer_id
),

customer_repeat_rate as (
select
    month_number,
    year_number,
    (count(distinct case when cumulative_orders > 1 then customer_id end) /
    count(distinct customer_id)) as repeat_rate
from cumulative_order_count
where date_dt >= '2019-01-01'
group by
    year_number,
    month_number
)

select *
from customer_repeat_rate
order by
    year_number,
    month_number

