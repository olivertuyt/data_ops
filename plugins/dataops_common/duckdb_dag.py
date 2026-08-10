"""Shared config for the session-04 DuckDB DAGs.

DuckDB is single-writer: the 1-slot `duckdb_pool` serializes warehouse access
across every session-04 DAG so two runs never fight over the file lock.
"""

from __future__ import annotations

from datetime import timedelta

from dataops_common.notifications import notify_on_failure

# split_statements: run each ; statement in a file (incl. BEGIN/COMMIT).
LOAD_OPTS = {"split_statements": True, "autocommit": True}


def duckdb_default_args(*, with_retries: bool = True) -> dict:
    """default_args shared by the session-04 DAGs. Set with_retries=False for
    starter/lab DAGs that leave the fault-tolerance policy as a TODO."""
    args = {
        "owner": "dataops-class",
        "conn_id": "duckdb_default",
        "pool": "duckdb_pool",
    }
    if with_retries:
        args.update(
            retries=2,
            retry_delay=timedelta(minutes=1),
            on_failure_callback=notify_on_failure,
        )
    return args
