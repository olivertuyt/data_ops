from __future__ import annotations

from pathlib import Path

from airflow.models import DagBag


def test_shopvn_dag_imports_and_has_domain_groups() -> None:
    dag_folder = Path(__file__).resolve().parents[1] / "dags"
    bag = DagBag(dag_folder=str(dag_folder), include_examples=False)
    assert bag.import_errors == {}
    # Read the already parsed DAG directly; DagBag.get_dag queries the metadata DB.
    dag = bag.dags.get("shopvn_daily")
    assert dag is not None
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert set(dag.params) == {"start_date", "end_date"}
    assert dag.params.get_param("start_date").schema["format"] == "date"
    assert dag.params.get_param("end_date").schema["format"] == "date"
    task_ids = set(dag.task_ids)
    for domain in ("finance", "operations", "customer", "product"):
        assert f"{domain}.build_candidates" in task_ids
        assert f"{domain}.validate_candidates" in task_ids
        assert f"{domain}.publish_gold" in task_ids
