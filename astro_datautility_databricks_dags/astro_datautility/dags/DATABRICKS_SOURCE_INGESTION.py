"""
### DATABRICKS_SOURCE_INGESTION

Loads **one source system into the Databricks landing zone**, then (optionally) refreshes everything in
dbt that depends on it.

    plan -> check_landing -> extract_load (one mapped task per table) -> validate_load -> trigger dbt

* **Source -> Databricks**: the existing extractors do the work, unchanged -
  `TelTools_SQLServer.main` / `TelTools_Oracle.main` read the source in chunks and write to the landing
  table via `databricks_teltools` (parquet -> Unity Catalog Volume -> `INSERT ... SELECT`), recording each
  run in `<LZ catalog>.<SCHEMA>.CONTROLTABLE`.
* **check_landing** fails in seconds if a landing table is missing (the loader never auto-creates tables).
* **validate_load** confirms every table wrote a CONTROLTABLE row during this run and logs row counts.
* **trigger dbt** runs `DBT_DATABRICKS_EXECUTION` with `source:<name>+` for the dbt sources this config
  feeds (mapping generated into `dbt_databricks_graph.json`), so bronze -> silver -> gold refresh in order.

Trigger with config, for example:

    {"config_filename": "TelTools_SQLServer_SUPPLIER_Extract.json"}
    {"config_filename": "TelTools_Oracle_Wisdom_Extract.json", "tables": "MAC_MAIN,SAP_SURB", "run_dbt": false}

| param | meaning |
|---|---|
| `config_filename` | an extract config in `include/Configuration` (SQL Server or Oracle) |
| `tables` | comma list of *source* table names; empty = every table in the config |
| `run_dbt` | trigger the dependent dbt run afterwards (default true) |
| `dbt_command` | `run` (default) / `build` / `test` for that dbt run |
| `check_landing_tables` | verify landing tables exist first (default true) |
| `fail_on_empty_load` | fail if any table loaded 0 rows (default false: normal for incremental loads) |

Sources not covered here (SAP R3 via `getSapR3Data`, HVR replication, file/API pulls, ...) keep their
existing DAGs; `DBT_DATABRICKS_PIPELINE` picks their landed data up.
"""
from __future__ import annotations

import os
from datetime import timedelta

import pendulum

from include.Python import dbt_databricks_common as C
from include.Python import databricks_ingestion as ING
from include.Python.dbt_databricks_airflow_compat import (
    DAG, Param, TriggerDagRunOperator, TriggerRule, get_current_context, task,
)

MAX_PARALLEL_TABLES = int(os.environ.get("INGEST_MAX_PARALLEL_TABLES", "6"))
RETRIES = int(os.environ.get("INGEST_TASK_RETRIES", "2" if C.IS_PROD else "0"))
CONFIG_CHOICES = [""] + ING.discover_config_files()

PARAMS = {
    "config_filename": Param("", type="string", enum=CONFIG_CHOICES,
                             description="Extract config to load (TelTools_SQLServer_*_Extract.json / TelTools_Oracle_*_Extract.json)"),
    "tables": Param("", type="string", description="Comma list of source tables; empty = all in the config"),
    "run_dbt": Param(True, type="boolean", description="Trigger the dependent dbt run after loading"),
    "dbt_command": Param("run", type="string", enum=["run", "build", "test"]),
    "check_landing_tables": Param(True, type="boolean"),
    "fail_on_empty_load": Param(False, type="boolean"),
}

with DAG(
    dag_id="DATABRICKS_SOURCE_INGESTION",
    description="Extract one source system and load it into the Databricks landing zone, then refresh dependent dbt models",
    doc_md=__doc__,
    start_date=pendulum.datetime(2025, 9, 1, tz="America/Chicago"),
    schedule=None,
    catchup=False,
    max_active_runs=int(os.environ.get("INGEST_MAX_ACTIVE_RUNS", "6")),
    params=PARAMS,
    default_args={"owner": "data-platform", "retries": RETRIES, "retry_delay": timedelta(minutes=3)},
    tags=["databricks", "ingestion", "teltools"],
) as dag:

    @task(task_id="plan", retries=0)
    def plan(**context):
        p = context["params"]
        result = ING.plan_ingestion(p.get("config_filename", ""), p.get("tables", ""))
        print(f"{result['config_filename']}: {len(result['tables'])} table(s) -> "
              f"{result['landing_schema']} schema; dbt selector: {result['dbt_select'] or '(none)'}")
        return result

    @task(task_id="table_list", retries=0)
    def table_list(plan_result: dict):
        return plan_result["tables"]  # dynamic task mapping needs a list-valued XCom, not a dict key

    @task(task_id="check_landing", retries=0)
    def check_landing(plan_result: dict, **context):
        if not context["params"].get("check_landing_tables", True):
            print("check_landing_tables=false: skipped")
            return {}
        return ING.check_landing(plan_result)

    @task(
        task_id="extract_load",
        map_index_template="{{ ingest_label }}",
        max_active_tis_per_dag=MAX_PARALLEL_TABLES,
        execution_timeout=timedelta(hours=float(os.environ.get("INGEST_TABLE_TIMEOUT_HOURS", "4"))),
    )
    def extract_load(plan_result: dict, table: str):
        get_current_context()["ingest_label"] = f"{table}"
        ING.extract_one(plan_result["config_filename"], table)

    @task(task_id="validate_load", retries=0)
    def validate_load(plan_result: dict, **context):
        return ING.validate_load(plan_result, fail_on_empty=bool(context["params"].get("fail_on_empty_load")))

    @task.short_circuit(task_id="dbt_wanted", trigger_rule=TriggerRule.ALL_SUCCESS)
    def dbt_wanted(plan_result: dict, **context):
        if not context["params"].get("run_dbt", True):
            print("run_dbt=false: dbt refresh skipped")
            return False
        if not plan_result["dbt_select"]:
            print("No dbt source is mapped to this config; dbt refresh skipped")
            return False
        return True

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt",
        trigger_dag_id="DBT_DATABRICKS_EXECUTION",
        trigger_run_id="ingest_{{ dag_run.run_id }}",
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
        conf={
            "select": "{{ ti.xcom_pull(task_ids='plan')['dbt_select'] }}",
            "dbt_command": "{{ params.dbt_command }}",
        },
    )

    planned = plan()
    landing_ok = check_landing(planned)
    loaded = extract_load.partial(plan_result=planned).expand(table=table_list(planned))
    landing_ok >> loaded
    validated = validate_load(planned)
    loaded >> validated
    validated >> dbt_wanted(planned) >> trigger_dbt
