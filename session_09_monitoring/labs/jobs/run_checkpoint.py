import json
import logging
import sys
from pathlib import Path

import duckdb
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

JOBS_DIR = Path(__file__).resolve().parent
SUITE_PATH = JOBS_DIR / "orders_lab_suite.json"
DEFAULT_DATASET = JOBS_DIR.parent.parent / "demo" / "data" / "orders_broken.parquet"


def run(dataset: Path) -> bool:
    df = duckdb.connect().execute("SELECT * FROM read_parquet(?)", [str(dataset)]).df()

    # This lab uses ephemeral mode in GE.
    context = gx.get_context(mode="ephemeral")
    asset = context.sources.add_pandas("lab").add_dataframe_asset("orders")
    batch_request = asset.build_batch_request(dataframe=df)
    suite = context.add_expectation_suite(
        expectation_suite=ExpectationSuite(**json.loads(SUITE_PATH.read_text()))
    )
    validator = context.get_validator(batch_request=batch_request, expectation_suite=suite)
    result = validator.validate()

    failures = 0
    for r in result["results"]:
        etype = r["expectation_config"]["expectation_type"]
        col = r["expectation_config"]["kwargs"].get("column", "-")
        pct = r["result"].get("unexpected_percent")
        pct_str = f"{pct:.2f}% rows failed" if pct is not None else "table-level"
        status = "PASS" if r["success"] else "FAIL"
        if not r["success"]:
            failures += 1
        logger.info("[%s] %-45s col=%-12s %s", status, etype, col, pct_str)

    logger.info("---")
    logger.info("Dataset: %s", dataset.name)
    logger.info("Expectations: %d | Failed: %d", len(result["results"]), failures)
    return result.success


if __name__ == "__main__":
    ds = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_DATASET
    success = run(ds)
    sys.exit(0 if success else 1)
