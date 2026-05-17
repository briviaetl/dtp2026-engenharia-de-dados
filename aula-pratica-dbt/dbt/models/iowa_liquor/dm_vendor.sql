select vendor_key, vendor_name
from {{ ref("vw_iowa_sales_clean") }}
qualify
    row_number() over (
        partition by vendor_key order by sale_date desc, invoice_item_id desc
    )
    = 1
