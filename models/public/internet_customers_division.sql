with internet_sales as (
select
    cus.customer_id, 
    customer_ds,
    sum(sales_vl) as total_sales,

from  {{ ref('fct_orders') }} as ord

left join {{ ref('dim_customers') }} as cus
    on ord.customer_id = cus.customer_id
left join {{ ref('fct_order_details') }} as det
    using (order_id)

where det.channel_ds = 'Internet'
    and cus.customer_id != -1

group by
    cus.customer_id,
    customer_ds
),

quantiles as (
    select
        approx_percentile(total_sales, 0.5) as q1_low_medium,
        approx_percentile(total_sales, 0.8) as q2_medium_high,
        approx_percentile(total_sales, 0.99) as q3_high_top
    from internet_sales
)

select
    *,
    case 
        when total_sales <= (select q1_low_medium from quantiles) then 'Low'
        when total_sales > (select q1_low_medium from quantiles)
            and total_sales <= (select q2_medium_high from quantiles) then 'Medium'
        when total_sales > (select q2_medium_high from quantiles)
            and total_sales <= (select q3_high_top from quantiles) then 'High'
        else 'Top'
    end as customer_tier
from internet_sales
order by
    total_sales desc

