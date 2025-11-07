with customers as (
select
    cus.customer_id,
    customer_ds,
    sum(sales_vl) as total_sales,
    count(*) as order_quantity

from  {{ ref('fct_orders') }} as ord

left join {{ ref('dim_customers') }} as cus
    on ord.customer_id = cus.customer_id

where cus.customer_id != -1

group by
    cus.customer_id,
    customer_ds
),

rev_quantiles as (
    select
        approx_percentile(total_sales, 0.5) as q1_low_medium,
        approx_percentile(total_sales, 0.8) as q2_medium_high,
        approx_percentile(total_sales, 0.99) as q3_high_top
    from customers
),

quan_quantiles as (
    select
        approx_percentile(order_quantity, 0.5) as q1_low_medium,
        approx_percentile(order_quantity, 0.8) as q2_medium_high,
        approx_percentile(order_quantity, 0.99) as q3_high_top
    from customers
)

select
    *,
    case 
        when total_sales <= (select q1_low_medium from rev_quantiles) then 'Low'
        when total_sales > (select q1_low_medium from rev_quantiles)
            and total_sales <= (select q2_medium_high from rev_quantiles) then 'Medium'
        when total_sales > (select q2_medium_high from rev_quantiles)
            and total_sales <= (select q3_high_top from rev_quantiles) then 'High'
        else 'Top'
    end as customer_rev_tier,
    case 
        when order_quantity <= (select q1_low_medium from quan_quantiles) then 'Low'
        when order_quantity > (select q1_low_medium from quan_quantiles)
            and order_quantity <= (select q2_medium_high from quan_quantiles) then 'Medium'
        when order_quantity > (select q2_medium_high from quan_quantiles)
            and order_quantity <= (select q3_high_top from quan_quantiles) then 'High'
        else 'Top'
    end as customer_fid_tier
from customers
order by
    customer_id

