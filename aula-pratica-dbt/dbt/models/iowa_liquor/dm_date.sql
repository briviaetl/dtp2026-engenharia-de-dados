select distinct
    date_key,  -- yyyymmdd = 20260508
    sale_date as full_date,
    extract(year from sale_date) as year_number,
    extract(quarter from sale_date) as quarter_number,
    extract(month from sale_date) as month_number,
    format_date('%B', sale_date) as month_name,
    extract(isoweek from sale_date) as iso_week_number,
    extract(day from sale_date) as day_of_month,
    extract(dayofweek from sale_date) as day_of_week_number,
    format_date('%A', sale_date) as day_name,
    case
        when extract(dayofweek from sale_date) in (1, 7) then true else false
    end as is_weekend
from {{ ref("vw_iowa_sales_clean") }}
