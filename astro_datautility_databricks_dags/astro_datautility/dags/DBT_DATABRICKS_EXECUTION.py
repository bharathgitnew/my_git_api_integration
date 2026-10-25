"""
### DBT_DATABRICKS_EXECUTION

Run **any dbt selector** against the `dbt_databricks` project on Databricks - the Databricks
counterpart of `DBT_EXECUTION_NEW_PLATFORM`. Use it for ad-hoc runs, or let a driver / ingestion DAG
trigger it after new data lands.

Trigger with config, for example:

    {"select": "source:SUPPLIER+"}                       # everything downstream of one source
    {"select": "+tag:finance", "dbt_command": "build"}   # a tag, with its parents, models + tests
    {"select": "path:models/tea_service/bronze"}         # one layer of one domain
    {"model_name": "BRONZE__TEH_DATA_OPS__KANBANIZE_CARDS"}   # same key the existing DRIVER_* DAGs send

`select` is a normal dbt selector (space separated = union). `model_name` is accepted as an alias.
For a dependency-ordered run of the *whole* project use `DBT_DATABRICKS_PIPELINE`.

Execution: dbt from `dbt_venv` on the worker -> Databricks SQL warehouse. (The optional Databricks
Jobs `dbt_task` mode, `DBT_EXECUTION_MODE=databricks_job`, applies to `DBT_DATABRICKS_PIPELINE` only;
this DAG always runs in the default worker mode.)
"""
from __future__ import annotations

import os
from datetime import timedelta

import pendulum

from include.Python import dbt_databricks_common as C
from include.Python.dbt_databricks_airflow_compat import DAG, Param, task
from include.Python.dbt_databricks_operators import DbtDatabricksBashOperator

RETRIES = int(os.environ.get("DBT_TASK_RETRIES", "1" if C.IS_PROD else "0"))

PARAMS = {
    "select": Param("", type="string", description="dbt selector, e.g. 'source:SUPPLIER+' or '+tag:finance'"),
    "model_name": Param("", type="string", description="Alias for `select` (used by existing DRIVER_* DAGs)"),
    "dbt_command": Param("run", type="string", enum=["run", "build", "test"]),
    "full_refresh": Param(False, type="boolean"),
    "threads": Param(int(os.environ.get("DBT_THREADS", "4")), type="integer", minimum=1, maximum=64),
    "exclude": Param("", type="string"),
    "dbt_vars": Param("", type="string", description="dbt --vars as YAML/JSON"),
    "fail_fast": Param(False, type="boolean"),
    "use_dbt_retry": Param(True, type="boolean"),
    "skip_preflight": Param(False, type="boolean"),
    "strict_graph_check": Param(False, type="boolean", description="Not used by ad-hoc runs; graph freshness is checked by the pipeline DAG"),
}

with DAG(
    dag_id="DBT_DATABRICKS_EXECUTION",
    description="Run an arbitrary dbt selector on Databricks (dbt_databricks project)",
    doc_md=__doc__,
    start_date=pendulum.datetime(2025, 9, 1, tz="America/Chicago"),
    schedule=None,
    catchup=False,
    max_active_runs=4,
    params=PARAMS,
    default_args={"owner": "data-platform", "retries": RETRIES, "retry_delay": timedelta(minutes=5)},
    tags=["dbt", "databricks", "dbt_databricks"],
) as dag:

    @task(task_id="preflight", retries=0)
    def preflight(**context):
        from include.Python.dbt_databricks_airflow_compat import AirflowFailException
        from include.Python.dbt_databricks_preflight import run_preflight

        params = dict(context["params"])
        conf = getattr(context.get("dag_run"), "conf", None) or {}
        if not (params.get("select") or params.get("model_name") or conf.get("select") or conf.get("model_name")):
            raise AirflowFailException("Provide a dbt selector: trigger with {\"select\": \"...\"} (or \"model_name\").")
        return run_preflight(params, check_graph_file=False)

    run = DbtDatabricksBashOperator(
        task_id="dbt",
        select_param=("select", "model_name"),
        execution_timeout=timedelta(hours=float(os.environ.get("DBT_NODE_TIMEOUT_HOURS", "6"))),
    )
    preflight() >> run
