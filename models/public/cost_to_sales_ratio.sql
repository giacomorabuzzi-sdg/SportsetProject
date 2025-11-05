select 
    product_id,
    category_ds,
    list_price_vl/standard_cost_vl as cost_to_sales_ratio,
    avg(list_price_vl/standard_cost_vl) over (
        partition by category_ds
    ) as avg_cost_to_sales_ratio_by_category,
    100*(cost_to_sales_ratio - avg_cost_to_sales_ratio_by_category)/cost_to_sales_ratio as pct_diff_from_category_avg


from {{ ref('dim_products') }}
order by 
    pct_diff_from_category_avg desc
