{{
    config(
        materialized="table",
        partition_by={
            "field": "sale_date",
            "data_type": "date",
            "granularity": "day",
        },
        cluster_by=["store_key", "product_key", "vendor_key", "category_key"],
    )
}}

select
    invoice_item_id,
    sale_date,
    date_key,
    store_key,
    vendor_key,
    product_key,
    category_key,
    is_return,

    1 as line_count,
    bottles_sold,
    unit_cost_amount,
    unit_retail_amount,
    sale_amount,
    cost_amount,
    gross_profit_amount,
    volume_sold_liters,
    volume_sold_gallons
from {{ ref("vw_iowa_sales_clean") }}
