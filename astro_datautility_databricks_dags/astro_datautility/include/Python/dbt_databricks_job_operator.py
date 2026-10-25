"""
Optional execution mode: run each dbt slice as a Databricks Jobs one-time run
with a `dbt_task` (Jobs API 2.1 `runs/submit`), instead of running dbt on the
Airflow worker.

Enable with DBT_EXECUTION_MODE=databricks_job on the Airflow deployment and
provide the job settings in include/Configuration/dbt_databricks_job.json (or the
DBT_DATABRICKS_JOB_CONFIG env var holding the same JSON). See
dbt_databricks_job.example.json for every key.

IMPORTANT - what this mode needs on the Databricks side
  * The dbt project must be reachable by the job through a Git source
    (`git_source`); Databricks cannot run a dbt project from DBFS.
  * The project reads its catalogs/credentials with env_var(). A job cluster
    therefore needs them as `spark_env_vars`. Non-secret DATABRICKS_* values
    present on the Airflow deployment are forwarded automatically; secrets
    (DATABRICKS_TOKEN, ...) must be given as secret references such as
    "{{secrets/<scope>/<key>}}" under `secret_env_vars` in the job config.
  * Use a custom profile (`profiles_directory`, e.g. PROD_PROFILES) so the
    project's own profile - which uses those env vars - is what dbt reads.

This mode is NOT exercised by the default setup and has not been run against a
live workspace; validate it in DEV first. The default `airflow_cli` mode follows
the dbt project README and is the supported path.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from airflow.exceptions import AirflowSkipException
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

from include.Python import dbt_databricks_common as C
from include.Python.dbt_databricks_operators import node_is_selected

JOB_CONFIG_PATH = os.environ.get(
    "DBT_DATABRICKS_JOB_CONFIG_PATH", str(Path(C.CONFIG_DIR) / "dbt_databricks_job.json")
)
# Never forwarded as plain spark_env_vars (per-user / service-principal secrets).
_NEVER_FORWARD = {"DATABRICKS_TOKEN", "DATABRICKS_DEV_CLIENT", "DATABRICKS_DEV_SECRET"}


def load_job_config() -> dict[str, Any]:
    raw = os.environ.get("DBT_DATABRICKS_JOB_CONFIG")
    if raw:
        cfg = json.loads(raw)
    else:
        try:
            cfg = json.loads(Path(JOB_CONFIG_PATH).read_text(encoding="utf-8"))
        except FileNotFoundError as exc:
            raise FileNotFoundError(
                f"DBT_EXECUTION_MODE=databricks_job needs a job config: {JOB_CONFIG_PATH} "
                f"(or env DBT_DATABRICKS_JOB_CONFIG). See dbt_databricks_job.example.json."
            ) from exc
    for key in ("git_source", "project_directory", "job_cluster"):
        if not cfg.get(key):
            raise ValueError(f"dbt_databricks_job config is missing required key: {key}")
    return cfg


def _spark_env_vars(cfg: dict[str, Any]) -> dict[str, str]:
    env = {
        k: v for k, v in os.environ.items()
        if k.startswith(("DATABRICKS_", "DBT_THREADS", "DBT_UPLOAD_ARTIFACTS")) and k not in _NEVER_FORWARD and v
    }
    env.update(cfg.get("spark_env_vars", {}))
    env.update(cfg.get("secret_env_vars", {}))
    env["ASTRO_DEPLOYMENT_NAME"] = C.DEPLOYMENT
    return env


def _raw(value: Any) -> Any:
    """Protect config-derived strings from Jinja.

    The operator's `json` field is templated, but Databricks secret references look like
    `{{secrets/<scope>/<key>}}` - the same braces Jinja uses. Without this they would fail to render
    (or silently become empty). Only the dbt command line is meant to be templated.
    """
    if isinstance(value, str):
        return "{% raw %}" + value + "{% endraw %}" if ("{{" in value or "{%" in value or "{#" in value) else value
    if isinstance(value, dict):
        return {k: _raw(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_raw(v) for v in value]
    return value


class DbtDatabricksJobOperator(DatabricksSubmitRunOperator):
    """DatabricksSubmitRunOperator that honours select_nodes/skip_nodes and builds the dbt_task payload."""

    def __init__(self, *, dbt_node_id: str | None, dbt_domain: str | None, select: str, job_config: dict[str, Any], **kwargs: Any):
        C.validate_selector(select, "select")
        cfg = job_config
        # profiles_directory in the job is repo-relative: <project_directory>/<DEPLOYMENT>_PROFILES
        profiles_directory = cfg.get("profiles_directory") or f"{cfg['project_directory'].rstrip('/')}/{C.DEPLOYMENT}_PROFILES"

        # dbt commands are templated so DAG params (dbt_command, threads, full_refresh) apply per run.
        run_cmd = (
            "dbt {{ params.dbt_command }} --select " + select +
            " --threads {{ params.threads }}"
            "{% if params.full_refresh %} --full-refresh{% endif %}"
            "{% if params.fail_fast %} --fail-fast{% endif %}"
            "{% if params.exclude %} --exclude {{ params.exclude }}{% endif %}"
        )
        dbt_task: dict[str, Any] = {
            "project_directory": cfg["project_directory"],
            "profiles_directory": profiles_directory,
            "commands": ["dbt deps", run_cmd],
            "source": "GIT",
        }
        if cfg.get("warehouse_id"):
            dbt_task["warehouse_id"] = cfg["warehouse_id"]

        cluster = dict(cfg["job_cluster"])
        cluster["spark_env_vars"] = {**cluster.get("spark_env_vars", {}), **_spark_env_vars(cfg)}
        cluster = _raw(cluster)

        payload = {
            "run_name": f"airflow-dbt-{dbt_node_id or 'adhoc'}",
            "git_source": _raw(cfg["git_source"]),
            "timeout_seconds": int(cfg.get("timeout_seconds", 21600)),
            "tasks": [
                {
                    "task_key": (dbt_node_id or "adhoc").replace(".", "_")[:100],
                    "dbt_task": dbt_task,
                    "new_cluster": cluster,
                    "libraries": [{"pypi": {"package": cfg.get("dbt_pip_spec", "dbt-databricks>=1.7.17,<1.8")}}],
                }
            ],
        }
        super().__init__(
            json=payload,
            databricks_conn_id=C.DATABRICKS_CONN_ID,
            polling_period_seconds=int(cfg.get("polling_period_seconds", 30)),
            databricks_retry_limit=3,
            **kwargs,
        )
        self.dbt_node_id = dbt_node_id
        self.dbt_domain = dbt_domain

    def pre_execute(self, context: Any) -> None:
        if self.dbt_node_id and not node_is_selected(self.dbt_node_id, self.dbt_domain or "", dict(context["params"])):
            raise AirflowSkipException(f"{self.dbt_node_id} excluded by select_nodes/skip_nodes")
