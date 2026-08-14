-- Finance: revenue by direct/marketplace channel.
SELECT metric_date, sales_channel, revenue_basis, net_revenue, discount_cost, refund_amount
FROM shopvn.gold.fact_daily_revenue
ORDER BY metric_date, sales_channel, revenue_basis;

-- Finance: top ten direct customers by spend for June.
SELECT customer_id, sum(net_spend) AS monthly_spend
FROM shopvn.gold.fact_customer_daily
WHERE metric_date BETWEEN DATE '2026-06-01' AND DATE '2026-06-30'
GROUP BY customer_id
ORDER BY monthly_spend DESC
LIMIT 10;

-- Operations: carrier success and re-delivery rates.
SELECT metric_date, carrier,
       sum(delivered_count) * 1.0 / nullif(sum(shipment_count), 0) AS success_rate,
       sum(failed_count) * 1.0 / nullif(sum(shipment_count), 0) AS failure_rate,
       sum(redelivery_count) * 1.0 / nullif(sum(shipment_count), 0) AS redelivery_rate,
       sum(avg_delivery_hours * delivered_count) / nullif(sum(delivered_count), 0)
         AS weighted_avg_delivery_hours
FROM shopvn.gold.fact_delivery_daily
GROUP BY metric_date, carrier
ORDER BY metric_date, carrier;

-- Marketing: voucher effectiveness.
SELECT metric_date, voucher_code, usage_count, net_revenue, discount_cost
FROM shopvn.gold.fact_voucher_daily
ORDER BY metric_date, net_revenue DESC;

-- Product: return rate by category and sales channel.
SELECT metric_date, category, sales_channel,
       returned_units * 1.0 / nullif(sold_units, 0) AS return_rate
FROM shopvn.gold.fact_return_daily
ORDER BY metric_date, return_rate DESC;

-- Inventory: current loss-making products and EOD stock.
SELECT i.snapshot_date, i.product_id, p.category, i.eod_stock_qty, p.is_loss_making
FROM shopvn.gold.fact_inventory_eod i
JOIN shopvn.silver.products p ON p.product_id = i.product_id
ORDER BY i.snapshot_date DESC, i.eod_stock_qty;
