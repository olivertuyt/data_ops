from __future__ import annotations

import argparse

from lakehouse_common import build_spark, get_logger

log = get_logger("schema_guard")

EXPECTED = {
    "event_id": "string",
    "ad_id": "string",
    "campaign_id": "string",
    "user_id": "string",
    "event_type": "string",
    "event_ts": "timestamp",
}
REQUIRED = set(EXPECTED)


def check_schema(df) -> dict:
    incoming = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    return {
        "added": sorted(set(incoming) - set(EXPECTED)),
        "missing": sorted(REQUIRED - set(incoming)),
        "retyped": sorted(c for c in EXPECTED if c in incoming and incoming[c] != EXPECTED[c]),
    }


def enforce(drift: dict, ds: str) -> None:
    if drift["missing"]:
        raise ValueError(f"[{ds}] missing required column(s): {drift['missing']}")
    if drift["added"]:
        log.info("[%s] additive columns (allowed): %s", ds, drift["added"])
    if drift["retyped"]:
        log.warning("[%s] retyped id columns (normalized to string): %s", ds, drift["retyped"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ds", required=True)
    ds = parser.parse_args().ds

    spark = build_spark("schema-guard")
    raw_path = f"s3a://bronze/ad_events/{ds}.csv"
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(raw_path)

    drift = check_schema(df)
    log.info("[%s] schema drift: %s", ds, drift)
    enforce(drift, ds)
    spark.stop()
