from __future__ import annotations

import csv
import io
import logging
import random
from datetime import date

from minio import Minio

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

PARTNERS = ["shopee", "lazada", "tiki"]
STATUSES = ["completed", "completed", "completed", "refunded", "pending"]
DEMO_DATES = [date(2026, 6, 13), date(2026, 6, 14), date(2026, 6, 15)]
ROWS_PER_DAY = 500

client = Minio("localhost:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)


def make_csv(ds: date) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["order_id", "partner", "customer_email", "amount", "order_ts", "status"])
    for i in range(ROWS_PER_DAY):
        writer.writerow(
            [
                f"ORD-{ds.strftime('%Y%m%d')}-{i:05d}",
                random.choice(PARTNERS),
                f"user{random.randint(1000, 9999)}@example.com",
                round(random.uniform(50_000, 5_000_000), 0),
                f"{ds}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:00",
                random.choice(STATUSES),
            ]
        )
    return buf.getvalue().encode()


for ds in DEMO_DATES:
    key = f"orders/{ds}.csv"
    data = make_csv(ds)
    client.put_object("bronze", key, io.BytesIO(data), length=len(data), content_type="text/csv")
    log.info("Uploaded bronze/%s (%d bytes)", key, len(data))
