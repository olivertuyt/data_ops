-- June 7 flash-sale revenue after discounts and refunded returns.
SELECT metric_date, sales_channel, revenue_basis, gross_revenue, discount_cost,
       refund_amount, commission_cost, net_revenue
FROM shopvn.serving.fact_daily_revenue
WHERE metric_date = DATE '2026-06-07' AND revenue_basis = 'earned'
ORDER BY sales_channel;

-- Marketplace contribution for June.
SELECT sales_channel, sum(net_revenue) AS june_net_revenue
FROM shopvn.serving.fact_daily_revenue
WHERE metric_date BETWEEN DATE '2026-06-01' AND DATE '2026-06-30'
  AND revenue_basis = 'earned' AND channel_group = 'marketplace'
GROUP BY sales_channel ORDER BY june_net_revenue DESC;

-- Top customers by June spend.
SELECT customer_id, max(month_to_date_spend) AS june_spend
FROM shopvn.serving.fact_customer_daily
WHERE metric_date BETWEEN DATE '2026-06-01' AND DATE '2026-06-30'
GROUP BY customer_id ORDER BY june_spend DESC LIMIT 10;

-- Carrier delivery performance.
SELECT carrier,
       sum(delivered_count) * 1.0 / nullif(sum(shipment_count), 0) AS success_rate,
       sum(failed_count) * 1.0 / nullif(sum(shipment_count), 0) AS failure_rate,
       sum(redelivery_count) * 1.0 / nullif(sum(shipment_count), 0) AS redelivery_rate
FROM shopvn.serving.fact_delivery_daily
GROUP BY carrier ORDER BY carrier;

-- Voucher/campaign effectiveness.
SELECT campaign_id, sum(usage_count) AS uses, sum(net_revenue) AS revenue,
       sum(discount_cost) AS discount_cost
FROM shopvn.serving.fact_voucher_daily
GROUP BY campaign_id ORDER BY revenue DESC;

-- Products forecast to stock out within seven days.
SELECT metric_date, product_id, sales_channel, current_stock_qty,
       avg_daily_units_sold_7d, days_to_stockout
FROM shopvn.serving.fact_product_channel_daily
WHERE days_to_stockout BETWEEN 0 AND 7
ORDER BY days_to_stockout;
