"""Authoritative Gold ownership, grain, and publication grouping."""

from transformation.gold import (
    dim_customer_scd2,
    fact_customer_daily,
    fact_daily_revenue,
    fact_delivery_daily,
    fact_inventory_eod,
    fact_product_channel_daily,
    fact_product_rating_daily,
    fact_return_daily,
    fact_voucher_daily,
)

DOMAIN_MODELS = {
    "finance": [fact_daily_revenue],
    "operations": [fact_delivery_daily, fact_return_daily],
    "customer": [
        fact_customer_daily,
        fact_voucher_daily,
        fact_product_rating_daily,
        dim_customer_scd2,
    ],
    "product": [fact_inventory_eod, fact_product_channel_daily],
}
