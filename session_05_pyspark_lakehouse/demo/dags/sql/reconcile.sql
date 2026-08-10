-- Reconcile one run date entirely in the engine (no client-side scan of the raw data).
-- Returns a single row the reconcile task asserts on:
--   fact_rows        rows in the silver fact for the date
--   fact_distinct    distinct event_ids (== fact_rows proves dedup held)
--   fact_impressions impression events in the fact
--   agg_impressions  impressions summed from the gold aggregate (must equal fact_impressions)
--   bad_rows         gold rows breaking a business rule
--   prev_rows        fact rows for the previous day (day-over-day volume check)
SELECT
    (
        SELECT count(*)
        FROM delta.silver.fact_ad_events
        WHERE event_date = DATE '{ds}'
    ) AS fact_rows,
    (
        SELECT count(DISTINCT event_id)
        FROM delta.silver.fact_ad_events
        WHERE event_date = DATE '{ds}'
    ) AS fact_distinct,
    (
        SELECT count(*)
        FROM delta.silver.fact_ad_events
        WHERE event_date = DATE '{ds}' AND event_type = 'impression'
    ) AS fact_impressions,
    (
        SELECT coalesce(sum(impressions), 0)
        FROM delta.gold.agg_ad_daily
        WHERE event_date = DATE '{ds}'
    ) AS agg_impressions,
    (
        SELECT count(*)
        FROM delta.gold.agg_ad_daily
        WHERE
            event_date = DATE '{ds}'
            AND (
                clicks > impressions
                OR impressions < 0
                OR clicks < 0
                OR ctr < 0
                OR ctr > 1
                OR unique_users > impressions + clicks
            )
    ) AS bad_rows,
    (
        SELECT count(*)
        FROM delta.silver.fact_ad_events
        WHERE event_date = DATE '{ds}' - INTERVAL '1' DAY
    ) AS prev_rows
