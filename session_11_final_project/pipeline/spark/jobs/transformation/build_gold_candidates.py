"""Build one domain's Gold candidates in the non-serving work namespace."""

from __future__ import annotations

import argparse
import time

from common.audit import write_run_audit
from common.config import validate_date_range, validate_run_id
from common.iceberg import assert_unique_non_null, merge_upsert
from common.spark_session import create_spark_session

from transformation.gold.registry import DOMAIN_MODELS


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--domain", choices=sorted(DOMAIN_MODELS), required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.run_id = validate_run_id(args.run_id)
    validate_date_range(args.start_date, args.end_date)
    spark = create_spark_session(f"shopvn-candidate-{args.domain}")
    try:
        for model in DOMAIN_MODELS[args.domain]:
            started = time.monotonic()
            try:
                frame = model.build(spark, args).cache()
                count = frame.count()
                if count == 0 and not getattr(model, "ALLOW_EMPTY", False):
                    raise RuntimeError(f"Unexpected empty candidate for {model.TARGET}")
                assert_unique_non_null(frame, model.TARGET, model.KEYS)
                merge_upsert(
                    spark,
                    frame,
                    f"polaris.work.{model.TARGET}_candidate",
                    ["run_id", *model.KEYS],
                    model.PARTITION,
                )
                write_run_audit(
                    spark,
                    run_id=args.run_id,
                    stage="candidate",
                    object_name=model.TARGET,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    status="BUILT",
                    source_count=count,
                    target_count=count,
                    duration_ms=int((time.monotonic() - started) * 1000),
                )
                frame.unpersist()
            except Exception as exc:
                write_run_audit(
                    spark,
                    run_id=args.run_id,
                    stage="candidate",
                    object_name=model.TARGET,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    status="FAIL",
                    duration_ms=int((time.monotonic() - started) * 1000),
                    error=exc,
                )
                raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
