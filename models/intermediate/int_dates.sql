select
	date_key as date_id,
	date_ as date_dt,
	fiscal_year as fiscal_year_ds,
	fiscal_quarter as fiscal_quarter_ds,
	month as month_ds,
	full_date as full_date_ds,
	month_key as month_year_cd 

from {{ref('stg_dates')}}