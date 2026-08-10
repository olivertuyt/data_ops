import logging
import os

import duckdb

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(filename)s:%(lineno)d %(message)s"
)
log = logging.getLogger(__name__)

DATA_DIR = os.environ.get(
    "DATA_DIR",
    os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../data")),
)
DB_PATH = os.path.join(DATA_DIR, "hw3_large.duckdb")
FLAT_DIR = os.path.join(DATA_DIR, "orders_by_day")
DATE = "2026-07-07"

# Prepare: export 22 daily flat files with opaque names (no hive structure)
if not os.path.exists(FLAT_DIR) or not os.listdir(FLAT_DIR):
    os.makedirs(FLAT_DIR, exist_ok=True)
    log.info("Writing 22 flat daily files -> %s", FLAT_DIR)
    con = duckdb.connect(DB_PATH)
    for i, day in enumerate(range(1, 23)):
        date = f"2026-07-{day:02d}"
        con.execute(f"""
            COPY (
                SELECT * FROM orders
                WHERE order_purchase_timestamp::DATE = '{date}'
            )
            TO '{FLAT_DIR}/part_{i:02d}.parquet' (FORMAT PARQUET)
        """)
    con.close()
    log.info("Done writing flat files")

# Broken: 22 files, no hive partitioning — DuckDB must open all files
con = duckdb.connect()
plan = con.execute(f"""
    EXPLAIN ANALYZE
    SELECT order_id, customer_id, order_status, order_approved_at
    FROM read_parquet('{FLAT_DIR}/*.parquet')
    WHERE order_purchase_timestamp::DATE = '{DATE}'
      AND order_status = 'delivered'
    ORDER BY order_approved_at DESC
""").fetchall()

print("\n--- EXPLAIN ANALYZE (broken) ---")
for row in plan:
    print(row[1])

log.info("hw3 broken done — look at 'Total Files Read' above")
con.close()
