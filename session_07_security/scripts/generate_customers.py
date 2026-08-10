import argparse
import logging
import os
import random
import uuid

import psycopg2
from faker import Faker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("generate_customers")

fake = Faker()
SEGMENTS = ["retail", "wholesale", "enterprise"]
REGIONS = ["US", "UK"]


def random_phone() -> str:
    return "09" + "".join(str(random.randint(0, 9)) for _ in range(8))


def random_cccd() -> str:
    return "".join(str(random.randint(0, 9)) for _ in range(12))


def generate_row():
    dob = fake.date_of_birth(minimum_age=18, maximum_age=75)
    registered_at = fake.date_time_between(start_date="-3y", end_date="now")
    return (
        str(uuid.uuid4()),
        fake.name(),
        random_phone(),
        fake.unique.email(),
        random_cccd(),
        dob,
        fake.address().replace("\n", ", "),
        random.choice(SEGMENTS),
        random.choice(REGIONS),
        registered_at,
        round(random.uniform(0, 500_000_000), 2),
        random.randint(400, 850),
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=os.environ.get("PGHOST", "postgres-source"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("PGPORT", 5432)))
    parser.add_argument("--database", default=os.environ.get("PGDATABASE", "dataops_source"))
    parser.add_argument("--user", default=os.environ.get("PGUSER", "postgres"))
    parser.add_argument("--password", default=os.environ.get("PGPASSWORD", "postgres"))
    parser.add_argument("--rows", type=int, default=200)
    args = parser.parse_args()

    conn = psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.database,
        user=args.user,
        password=args.password,
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM raw.customers")
        existing = cur.fetchone()[0]
        if existing >= args.rows:
            logger.info(f"raw.customers already has {existing} rows, skipping seed.")
            return

        rows = [generate_row() for _ in range(args.rows - existing)]
        cur.executemany(
            """
            INSERT INTO raw.customers (
                customer_id, full_name, phone, email, cccd, date_of_birth,
                address, segment, region, registered_at, account_balance, credit_score
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (customer_id) DO NOTHING
            """,
            rows,
        )
        logger.info(f"Inserted {len(rows)} rows into raw.customers.")

    conn.close()


if __name__ == "__main__":
    main()
