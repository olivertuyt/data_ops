"""
Generate 50M orders into a local DuckDB and compare broken vs fix query plans.
Run from the session_06_performance_optimization/ directory:
    python3 scripts/generate_hw3_large.py
"""
import logging
import os
import time

import duckdb

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(filename)s:%(lineno)d %(message)s"
)
log = logging.getLogger(__name__)

DATA_DIR = os.environ.get("DATA_DIR", "data")
DB_PATH = os.path.join(DATA_DIR, "hw3_large.duckdb")
N_ROWS = 50_000_000

BROKEN_QUERY = """
EXPLAIN ANALYZE
SELECT *
FROM orders
WHERE order_approved_at >= NOW() - INTERVAL '30 days'
  AND order_status = 'delivered'
ORDER BY order_approved_at DESC;
"""

FIX_QUERY = """
EXPLAIN ANALYZE
SELECT order_id, customer_id, order_approved_at, order_status
FROM orders
WHERE order_approved_at >= '2026-06-22'
  AND order_approved_at < '2026-07-22'
  AND order_status = 'delivered'
ORDER BY order_approved_at DESC;
"""


def generate(con):
    log.info("Generating %s rows into %s ...", f"{N_ROWS:,}", DB_PATH)
    t0 = time.time()
    con.execute("DROP TABLE IF EXISTS orders")
    con.execute(f"""
        CREATE TABLE orders AS
        SELECT
            'ord_' || lpad(range::VARCHAR, 9, '0')                                   AS order_id,
            'cust_' || lpad(((random() * 499999)::BIGINT + 1)::VARCHAR, 8, '0')      AS customer_id,
            CASE
                WHEN random() < 0.60 THEN 'delivered'
                WHEN random() < 0.75 THEN 'shipped'
                WHEN random() < 0.85 THEN 'processing'
                WHEN random() < 0.93 THEN 'cancelled'
                ELSE 'unavailable'
            END                                                                        AS order_status,
            (TIMESTAMP '2026-07-01' + ((random() * 1814400)::BIGINT * INTERVAL '1 second'))
                                                                                       AS order_purchase_timestamp,
            (TIMESTAMP '2026-07-01' + ((random() * 1814400 + random() * 172800)::BIGINT * INTERVAL '1 second'))
                                                                                       AS order_approved_at,
            CASE WHEN random() < 0.60
                 THEN (TIMESTAMP '2026-07-01' + ((random() * 1814400 + 172800 + random() * 950400)::BIGINT * INTERVAL '1 second'))
            END                                                                        AS order_delivered_timestamp,
            (DATE '2026-07-01' + ((random() * 1814400)::BIGINT + 864000) * INTERVAL '1 second')::DATE
                                                                                       AS order_estimated_delivery_date,
            CASE WHEN random() < 0.33 THEN 'web'
                 WHEN random() < 0.66 THEN 'mobile_app'
                 ELSE 'marketplace' END                                                AS order_channel,
            CASE WHEN random() < 0.33 THEN 'desktop'
                 WHEN random() < 0.66 THEN 'ios'
                 ELSE 'android' END                                                    AS device_type,
            CASE WHEN random() < 0.20 THEN upper(md5(random()::VARCHAR)[:10]) END     AS coupon_code,
            md5(random()::VARCHAR) || md5(random()::VARCHAR)                           AS customer_note
        FROM range(1, {N_ROWS} + 1);
    """)
    elapsed = time.time() - t0
    n = con.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    log.info("Generated %s rows in %.1fs", f"{n:,}", elapsed)


def run_query(con, label, query):
    log.info("Running %s ...", label)
    t0 = time.time()
    plan = con.execute(query).fetchall()
    elapsed = time.time() - t0
    log.info("%s plan (wall %.2fs):\n%s", label, elapsed, "\n".join(row[1] for row in plan))


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    con = duckdb.connect(DB_PATH)
    generate(con)
    run_query(con, "BROKEN", BROKEN_QUERY)
    run_query(con, "FIX", FIX_QUERY)
    con.close()


if __name__ == "__main__":
    main()
