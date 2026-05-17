select category_key, category_name
from
    {{ ref("vw_iowa_sales_clean") }}
    {{ dedup(partition_by="category_key", order_by="sale_date") }}
