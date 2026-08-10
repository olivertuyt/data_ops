"""Slack alert callbacks. Set the Airflow Variable 'slack_webhook_url' to enable;
if it is missing the callbacks log and no-op, so local runs work without Slack."""

from __future__ import annotations

import logging

import requests
from airflow.models import Variable

log = logging.getLogger(__name__)


def _post_slack(text: str) -> None:
    url = Variable.get("slack_webhook_url", default_var=None)
    if not url:
        log.info("slack_webhook_url not set — skipping. Message would be: %s", text)
        return
    try:
        resp = requests.post(url, json={"text": text}, timeout=10)
        resp.raise_for_status()
    except Exception as exc:
        log.warning("Slack post failed (non-fatal): %s", exc)


def notify_on_failure(context: dict) -> None:
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id = context["run_id"]
    logical_date = context.get("logical_date", "unknown")
    _post_slack(
        f":red_circle: *{dag_id}* › `{task_id}` failed\n"
        f"Run: `{run_id}` | Date: `{logical_date}`"
    )


def notify_on_sla_miss(dag, _task_list, _blocking_task_list, slas, _blocking_tis) -> None:
    missed = ", ".join(f"`{sla.task_id}`" for sla in slas)
    _post_slack(f":warning: *{dag.dag_id}* SLA missed — tasks: {missed}")


def send_slack_alert(message: str, level: str = "warning") -> None:
    icon = ":red_circle:" if level == "critical" else ":warning:"
    _post_slack(f"{icon} *DataOps Alert*\n{message}")
