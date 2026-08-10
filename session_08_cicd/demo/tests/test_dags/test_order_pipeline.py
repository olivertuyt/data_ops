def test_dag_loads(dagbag):
    assert "session_08_order_pipeline" in dagbag.dags
    assert len(dagbag.import_errors) == 0


def test_no_catchup(dagbag):
    dag = dagbag.dags["session_08_order_pipeline"]
    assert dag.catchup is False


def test_no_cycle(dagbag):
    from airflow.exceptions import AirflowDagCycleException

    dag = dagbag.dags["session_08_order_pipeline"]
    try:
        dag.topological_sort()
    except AirflowDagCycleException as e:
        raise AssertionError("Circular dependency detected in DAG") from e


def test_task_dependency_order(dagbag):
    dag = dagbag.dags["session_08_order_pipeline"]
    load_bronze = dag.get_task("load_bronze")
    load_gold = dag.get_task("load_gold")
    reconcile = dag.get_task("reconcile")
    assert "init_schema" in load_bronze.upstream_task_ids
    assert "load_gold" in reconcile.upstream_task_ids
    assert "silver_transform" in load_gold.upstream_task_ids


def test_task_count(dagbag):
    dag = dagbag.dags["session_08_order_pipeline"]
    assert len(dag.tasks) == 5
