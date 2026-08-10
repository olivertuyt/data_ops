from __future__ import annotations

import argparse
import csv
import io
import logging

from minio import Minio

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

INCIDENT_DATE = "2026-06-15"
OBJECT_KEY = f"orders/{INCIDENT_DATE}.csv"

client = Minio("localhost:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)

parser = argparse.ArgumentParser()
parser.add_argument("--restore", action="store_true")
args = parser.parse_args()


def _rewrite_header(old_name: str, new_name: str) -> None:
    response = client.get_object("bronze", OBJECT_KEY)
    content = response.read()
    response.close()

    reader = csv.reader(io.StringIO(content.decode()))
    rows = list(reader)
    header = rows[0]

    if old_name not in header:
        log.info("Column '%s' not found — nothing to do.", old_name)
        return

    header[header.index(old_name)] = new_name
    rows[0] = header

    buf = io.StringIO()
    csv.writer(buf).writerows(rows)
    data = buf.getvalue().encode()

    client.put_object(
        "bronze", OBJECT_KEY, io.BytesIO(data), length=len(data), content_type="text/csv"
    )
    log.info("Rewrote bronze/%s: '%s' -> '%s'", OBJECT_KEY, old_name, new_name)


if args.restore:
    _rewrite_header("revenue", "amount")
    log.info("Incident restored. Rerun the DAG for %s.", INCIDENT_DATE)
else:
    _rewrite_header("amount", "revenue")
    log.info(
        "Incident injected. Trigger the DAG with execution date %sT00:00:00+00:00.", INCIDENT_DATE
    )
