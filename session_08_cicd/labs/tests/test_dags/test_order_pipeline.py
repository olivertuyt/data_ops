def test_no_catchup(dagbag):
    # Bug1: catchup=True will silently backfill all history on first deploy
    # TODO: assert dag.catchup is False
    raise NotImplementedError


def test_task_dependency_order(dagbag):
    # Bug2: reconcile runs before load_gold — the gold table is never populated
    # TODO: assert load_gold is upstream of reconcile, not the other way round
    raise NotImplementedError


def test_dag_loads_without_error(dagbag):
    # TODO: assert "session_08_order_pipeline" in dagbag.dags and no import_errors
    raise NotImplementedError


def test_task_count(dagbag):
    # TODO: assert len(dag.tasks) == 4
    raise NotImplementedError
