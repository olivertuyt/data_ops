from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

import duckdb
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)

from dataops_common.notifications import send_slack_alert

log = logging.getLogger(__name__)


def run_ge_checkpoint(
    warehouse: Path,
    ge_dir: Path,
    suite_name: str,
    sql: str,
) -> None:
    con = duckdb.connect(str(warehouse), read_only=True)
    df = con.execute(sql).df()
    con.close()

    context = gx.get_context(context_root_dir=str(ge_dir))
    datasource = context.sources.add_or_update_pandas("runtime")
    asset = datasource.add_dataframe_asset(suite_name)
    batch_request = asset.build_batch_request(dataframe=df)

    suite_dict = json.loads((ge_dir / "expectations" / f"{suite_name}.json").read_text())
    suite = context.add_or_update_expectation_suite(
        expectation_suite=ExpectationSuite(**suite_dict)
    )
    validator = context.get_validator(batch_request=batch_request, expectation_suite=suite)

    run_id = RunIdentifier(run_name=f"airflow_{datetime.now().strftime('%Y%m%dT%H%M%S')}")
    result = validator.validate(run_id=run_id)

    val_id = ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier(suite_name),
        run_id=run_id,
        batch_identifier=f"{suite_name}_batch",
    )
    context.validations_store.set(val_id, result)
    context.build_data_docs()

    report_path = ge_dir / "uncommitted" / "data_docs" / "local_site" / "index.html"
    log.info("GE report: %s", report_path)

    if not result.success:
        failed = [
            r["expectation_config"]["expectation_type"]
            for r in result["results"]
            if not r["success"]
        ]
        msg = f"GE checkpoint FAILED — suite={suite_name}\nFailed: {', '.join(failed)}"
        send_slack_alert(msg, level="critical")
        raise ValueError(msg)

    log.info("GE checkpoint passed — suite=%s", suite_name)
