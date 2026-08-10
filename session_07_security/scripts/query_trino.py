import argparse
import logging
import os

import trino
from trino.auth import BasicAuthentication

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("query_trino")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--sql", required=True)
    parser.add_argument("--host", default=os.environ.get("TRINO_HOST", "trino"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("TRINO_PORT", 8443)))
    args = parser.parse_args()
    password = args.password

    ca_cert = os.environ.get("TRINO_CA_CERT", "")
    if not ca_cert or not os.path.exists(ca_cert):
        raise RuntimeError(f"TRINO_CA_CERT not set or file missing: {ca_cert!r}")
    conn = trino.dbapi.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        catalog="delta",
        schema="gold",
        http_scheme="https",
        auth=BasicAuthentication(args.user, password),
        verify=ca_cert,
    )
    cur = conn.cursor()
    cur.execute(args.sql)
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()

    logger.info(f"--- user={args.user} ---")
    logger.info(" | ".join(columns))
    for row in rows:
        logger.info(" | ".join(str(v) for v in row))
    logger.info(f"({len(rows)} rows)")


if __name__ == "__main__":
    main()
