select
    store_key,
    store_name,
    store_address,
    city,
    zip_code,
    county_number,
    county,
    store_location,
    longitude,
    latitude
from {{ ref("vw_iowa_sales_clean") }}
qualify
    row_number() over (
        partition by store_key order by sale_date desc, invoice_item_id desc
    )
    = 1
