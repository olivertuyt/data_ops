from __future__ import annotations

import argparse
import csv
import logging
import os
import random
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("generate_ad_events")

NUM_ADS = 200
NUM_USERS = 10_000
NUM_CAMPAIGNS = 20
CLICK_RATE = 0.03  # ~3% of events are clicks, the rest impressions (views)
DUP_RATE = 0.02  # ~2% of rows are duplicate event_ids (ad servers retry)
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
BASE_ROWS = 50_000  # weekday baseline; actual volume varies by day (see daily_volume)

# The source team rolls out schema changes over time. Generating consecutive days
# reproduces the rollout: Day 2 adds device_type; Day 3 adds cost_micros and ships
# campaign_id as a string code ("cmp_07") instead of an integer.
DEVICE_FROM = "2026-06-27"
COST_FROM = "2026-06-28"


def daily_volume(ds: str) -> int:
    """Deterministic per-day row count. Real ad traffic drifts day to day and
    dips on weekends, so a fixed count per day looks synthetic and makes the
    day-over-day reconcile check meaningless. Seeded by the date so a given ds
    always yields the same volume; kept within the pipeline's 50% drift bound."""
    d = datetime.strptime(ds, "%Y-%m-%d")
    vrng = random.Random(int(ds.replace("-", "")))
    weekend = 0.82 if d.weekday() >= 5 else 1.0  # Sat/Sun lower
    jitter = vrng.uniform(0.90, 1.10)  # ±10% day-to-day noise
    return int(round(BASE_ROWS * weekend * jitter, -2))


def _campaign(ad_index: int) -> int:
    return ad_index % NUM_CAMPAIGNS + 1


def make_event(rng: random.Random, ds: str, i: int, day_start: datetime) -> dict:
    ad_index = rng.randrange(NUM_ADS)
    campaign = _campaign(ad_index)
    event = {
        "event_id": f"EVT-{ds}-{i:08d}",
        "ad_id": f"AD-{ad_index:04d}",
        "campaign_id": f"cmp_{campaign:02d}" if ds >= COST_FROM else campaign,
        "user_id": f"U-{rng.randrange(NUM_USERS):05d}",
        "event_type": "click" if rng.random() < CLICK_RATE else "impression",
        "event_ts": (day_start + timedelta(seconds=rng.randrange(86400))).strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
    }
    if ds >= DEVICE_FROM:
        event["device_type"] = rng.choice(DEVICE_TYPES)
    if ds >= COST_FROM:
        event["cost_micros"] = rng.randrange(50_000, 5_000_000)  # $0.05–$5.00 per event
    return event


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ds", required=True, help="Date YYYY-MM-DD")
    parser.add_argument(
        "--rows",
        type=int,
        default=None,
        help="Override the row count; default varies realistically by date.",
    )
    parser.add_argument("--output-dir", default="data/bronze/ad_events")
    args = parser.parse_args()

    rows = args.rows if args.rows is not None else daily_volume(args.ds)

    # Seed from the date so the same ds always yields the same file (reproducible).
    rng = random.Random(int(args.ds.replace("-", "")))
    day_start = datetime.strptime(args.ds, "%Y-%m-%d")

    n_unique = int(rows * (1 - DUP_RATE))
    events = [make_event(rng, args.ds, i, day_start) for i in range(n_unique)]
    # Duplicates: replay earlier events verbatim, as a retrying ad server would.
    duplicates = [dict(rng.choice(events)) for _ in range(rows - n_unique)]
    rows = events + duplicates
    rng.shuffle(rows)

    fields = ["event_id", "ad_id", "campaign_id", "user_id", "event_type", "event_ts"]
    if args.ds >= DEVICE_FROM:
        fields.append("device_type")
    if args.ds >= COST_FROM:
        fields.append("cost_micros")

    os.makedirs(args.output_dir, exist_ok=True)
    output_path = os.path.join(args.output_dir, f"{args.ds}.csv")
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    log.info("Generated %d rows (%d unique event_ids) -> %s", len(rows), n_unique, output_path)
    log.info("Columns: %s", ", ".join(fields))


if __name__ == "__main__":
    main()
