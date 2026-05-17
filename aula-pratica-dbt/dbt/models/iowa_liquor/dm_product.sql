select
    product_key,
    product_name,
    pack,
    bottle_volume_ml,
    category_key as current_category_key,
    category_name as current_category_name
from
    {{ ref("vw_iowa_sales_clean") }}
    {{ dedup(partition_by="product_key", order_by="sale_date") }}
