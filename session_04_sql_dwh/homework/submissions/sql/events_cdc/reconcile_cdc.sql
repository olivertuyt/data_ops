SELECT
    (SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM silver.events) = 0 AS no_dupes,
    (SELECT COUNT(*) FROM silver.events AS s WHERE EXISTS (
        SELECT 1 FROM (
            SELECT
                event_id,
                op
            FROM bronze.events_raw
            WHERE event_date = DATE '{{ ds }}'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY updated_at DESC) = 1
        ) AS l WHERE l.event_id = s.event_id AND l.op = 'D'
    )) = 0 AS no_tombstone;
