BEGIN;

-- 1) thu batch về 1 dòng/key (event mới nhất) TRƯỚC khi merge
CREATE OR REPLACE TEMP TABLE _latest AS
SELECT
    event_id,
    user_id,
    amount,
    op,
    updated_at
FROM bronze.events_raw
WHERE event_date = DATE '{{ ds }}'
QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY updated_at DESC) = 1;

-- 2) apply insert / update / tombstone trong 1 câu
MERGE INTO silver.events AS t
USING _latest AS s ON t.event_id = s.event_id
WHEN MATCHED AND s.op = 'D'
THEN DELETE
WHEN MATCHED
THEN
    UPDATE SET
        user_id = s.user_id,
        amount = s.amount,
        updated_at = s.updated_at
WHEN NOT MATCHED AND s.op <> 'D'
THEN
    INSERT (event_id, user_id, amount, updated_at)
    VALUES (s.event_id, s.user_id, s.amount, s.updated_at);

COMMIT;
