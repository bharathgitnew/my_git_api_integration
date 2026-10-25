"""
Operators for running the dbt_databricks project from Airflow.

DbtDatabricksBashOperator
    Default execution mode. Runs dbt from the `dbt_venv` on the Airflow worker;
    dbt sends SQL to the Databricks SQL warehouse (as described in the dbt
    project README).

build_databricks_job_operator()
    Optional execution mode (DBT_EXECUTION_MODE=databricks_job). Submits the same
    dbt command as a Databricks Jobs one-time run with a `dbt_task`.
"""
from __future__ import annotations

import json
import os
from typing import Any

try:  # Airflow 3 / standard provider
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:  # Airflow 2
    from airflow.operators.bash import BashOperator

try:
    from airflow.exceptions import AirflowSkipException
except ImportError:  # pragma: no cover
    from airflow.sdk.exceptions import AirflowSkipException

from include.Python import dbt_databricks_common as C

ALLOWED_COMMANDS = ("run", "build", "test")


def csv_set(value: Any) -> set[str]:
    """'a, b,,c' / ['a','b'] -> {'a','b','c'} (lower-cased)."""
    if not value:
        return set()
    if isinstance(value, str):
        value = value.split(",")
    return {str(v).strip().lower() for v in value if str(v).strip()}


def node_is_selected(node_id: str, domain: str, params: dict[str, Any]) -> bool:
    """`select_nodes` (empty = all) and `skip_nodes` match on node id or domain."""
    keys = {node_id.lower(), domain.lower()}
    selected, skipped = csv_set(params.get("select_nodes")), csv_set(params.get("skip_nodes"))
    if skipped & keys:
        return False
    return not selected or bool(selected & keys)


def bool_param(value: Any) -> bool:
    return str(value).strip().lower() in ("true", "1", "yes", "y")


class DbtDatabricksBashOperator(BashOperator):
    """
    One dbt invocation (run / build / test) for a fixed or parameter-supplied selector.

    dbt_node_id / dbt_domain : set for pipeline tasks; used by the `select_nodes` / `skip_nodes` params.
    select           : fixed dbt selector (space separated), e.g. "path:models/tea_service/bronze".
    select_param     : alternative: names of DAG params/conf keys to read the selector from.
    """

    ui_color = "#ffb199"
    template_fields = ("select",)  # bash_command is a constant and is intentionally NOT templated

    def __init__(
        self,
        *,
        select: str | None = None,
        select_param: tuple[str, ...] = (),
        dbt_node_id: str | None = None,
        dbt_domain: str | None = None,
        **kwargs: Any,
    ) -> None:
        # Placeholder keeps BashOperator happy; the real script is set in execute()
        # (after templating) so Jinja never sees the shell code.
        kwargs.setdefault("do_xcom_push", False)  # don't store dbt's last stdout line as an XCom
        super().__init__(bash_command="true", append_env=True, **kwargs)
        self.select = select
        self.select_param = select_param
        self.dbt_node_id = dbt_node_id
        self.dbt_domain = dbt_domain

    def execute(self, context: Any) -> Any:
        params = dict(context["params"])
        ti = context["ti"]
        # Existing DRIVER_* DAGs trigger with {"model_name": ...}; honour conf keys that are not declared params.
        conf = getattr(context.get("dag_run"), "conf", None) or {}
        for key in ("select", "model_name"):
            if not params.get(key) and conf.get(key):
                params[key] = conf[key]

        if self.dbt_node_id and not node_is_selected(self.dbt_node_id, self.dbt_domain or "", params):
            raise AirflowSkipException(f"{self.dbt_node_id} excluded by select_nodes/skip_nodes")

        command = str(params.get("dbt_command") or "run").lower()
        if command not in ALLOWED_COMMANDS:
            raise ValueError(f"dbt_command must be one of {ALLOWED_COMMANDS}, got {command!r}")

        select = self.select
        if not select:
            for key in self.select_param:
                if params.get(key):
                    select = str(params[key])
                    break
        if not select:
            raise ValueError(f"No dbt selector supplied (looked at: {self.select_param or 'static select'})")
        select = C.validate_selector(select, "select")
        exclude = C.validate_selector(str(params.get("exclude") or ""), "exclude")
        dbt_vars = str(params.get("dbt_vars") or "").strip()

        profiles, target = C.profiles_dir()
        env: dict[str, str] = {
            "DBT_VENV_ACTIVATE": C.DBT_VENV_ACTIVATE,
            "DBT_PROJECT_DIR": C.DBT_PROJECT_DIR,
            "TEL_DEPLOYMENT": C.DEPLOYMENT,
            "TEL_PROFILES_DIR": profiles,
            "TEL_DBT_TARGET": target or "",
            "TEL_RUN_KEY": C.run_key(context["dag"].dag_id, context["run_id"], ti.task_id),
            "TEL_TRY_NUMBER": str(getattr(ti, "try_number", 1) or 1),
            "TEL_DBT_COMMAND": command,
            "TEL_DBT_SELECT": select,
            "TEL_DBT_EXCLUDE": exclude,
            "TEL_DBT_THREADS": str(int(params.get("threads") or 4)),
            "TEL_DBT_FULL_REFRESH": "true" if bool_param(params.get("full_refresh")) else "false",
            "TEL_DBT_FAIL_FAST": "true" if bool_param(params.get("fail_fast")) else "false",
            "TEL_DBT_VARS": dbt_vars,
            "TEL_USE_DBT_RETRY": "true" if bool_param(params.get("use_dbt_retry", True)) else "false",
        }

        # Fill any DATABRICKS_HOST / HTTP_PATH / TOKEN not already set on the worker
        # from the Airflow connection. Existing env vars always win.
        if any(not os.environ.get(k) for k in C.CONNECTION_BACKED_ENV):
            for key, value in C.env_from_connection().items():
                if not os.environ.get(key):
                    env[key] = value

        self.env = env
        self.bash_command = C.DBT_SHELL_SCRIPT
        return super().execute(context)
