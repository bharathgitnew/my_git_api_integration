"""
### DATABRICKS_END_TO_END

The **full workflow in one DAG**: source systems -> Databricks landing -> dbt bronze/silver/gold.

    check_databricks -> plan_ingestion -> ingest_source (one DATABRICKS_SOURCE_INGESTION run per source,
    in parallel) -> dbt_wanted -> run_dbt_pipeline (DBT_DATABRICKS_PIPELINE, dependency ordered)

* Every source ingestion run is a separate `DATABRICKS_SOURCE_INGESTION` DAG run (visible, retryable on
  its own). They are triggered with `run_dbt=false`; dbt runs **once** at the end over the whole graph so
  models that combine several sources see all of them freshly loaded.
* By default a failed ingestion stops the dbt stage (data would be inconsistent). Set the deployment
  variable `E2E_CONTINUE_ON_INGESTION_FAILURE=true` to run dbt anyway, or exclude the failing source with
  the `sources` param / `skip_ingestion`.
* Sources that are not extracted by Airflow's SQL Server/Oracle extractors (SAP R3, HVR, files, APIs) keep
  their own DAGs; run them first or on their own schedule - dbt simply reads whatever has landed.

Schedule: none by default. Set `DATABRICKS_E2E_SCHEDULE` (cron, e.g. `0 2 * * *`) on the deployment.

| param | meaning |
|---|---|
| `sources` | comma list of extractor names (`SUPPLIER`, `Wisdom`, `sqlserver:QVICS`, or a config file name); empty = all |
| `skip_ingestion` | only run dbt |
| `skip_dbt` | only ingest |
| `dbt_command`, `full_refresh`, `threads`, `select_nodes`, `skip_nodes`, `exclude`, `dbt_vars`, `fail_fast` | passed to `DBT_DATABRICKS_PIPELINE` |
"""
from __future__ import annotations

import os
from datetime import timedelta

import pendulum

from include.Python import dbt_databricks_common as C
from include.Python.dbt_databricks_airflow_compat import (
    DAG, AirflowFailException, Param, TriggerDagRunOperator, TriggerRule, task,
)

SCHEDULE = os.environ.get("DATABRICKS_E2E_SCHEDULE") or None
INGEST_PARALLEL_SOURCES = int(os.environ.get("E2E_MAX_PARALLEL_SOURCES", "4"))
CONTINUE_ON_INGESTION_FAILURE = os.environ.get("E2E_CONTINUE_ON_INGESTION_FAILURE", "false").strip().lower() == "true"

PARAMS = {
    "sources": Param("", type="string", description="Comma list of extractor names / config files; empty = all"),
    "skip_ingestion": Param(False, type="boolean"),
    "skip_dbt": Param(False, type="boolean"),
    "dbt_command": Param("run", type="string", enum=["run", "build", "test"]),
    "full_refresh": Param(False, type="boolean"),
    "threads": Param(int(os.environ.get("DBT_THREADS", "4")), type="integer", minimum=1, maximum=64),
    "select_nodes": Param("", type="string"),
    "skip_nodes": Param("", type="string"),
    "exclude": Param("", type="string"),
    "dbt_vars": Param("", type="string"),
    "fail_fast": Param(False, type="boolean"),
}

with DAG(
    dag_id="DATABRICKS_END_TO_END",
    description="Source systems -> Databricks landing -> dbt (bronze/silver/gold), end to end",
    doc_md=__doc__,
    start_date=pendulum.datetime(2025, 9, 1, tz="America/Chicago"),
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    params=PARAMS,
    default_args={"owner": "data-platform", "retries": 0},
    tags=["databricks", "end_to_end", "dbt_databricks"],
) as dag:

    @task(task_id="check_databricks", retries=1, retry_delay=timedelta(minutes=2))
    def check_databricks():
        """Cheap connectivity test (SELECT 1 on the SQL warehouse) before launching anything long-running."""
        from include.Python.dbt_databricks_common import env_from_connection

        env = {k: os.environ.get(k) for k in C.CONNECTION_BACKED_ENV}
        if not all(env.values()):
            env.update({k: v for k, v in env_from_connection().items() if not env.get(k)})
        if not all(env.values()):
            raise AirflowFailException(
                "No Databricks credentials: set DATABRICKS_HOST / DATABRICKS_HTTP_PATH / DATABRICKS_TOKEN "
                f"or configure the Airflow connection '{C.DATABRICKS_CONN_ID}'."
            )
        from databricks import sql

        with sql.connect(server_hostname=env["DATABRICKS_HOST"], http_path=env["DATABRICKS_HTTP_PATH"],
                         access_token=env["DATABRICKS_TOKEN"]) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone()[0] == 1
        print("Databricks SQL warehouse reachable")

    @task(task_id="plan_ingestion", retries=0)
    def plan_ingestion(**context):
        """Turn the `sources` param into one DATABRICKS_SOURCE_INGESTION conf per extract config."""
        p = context["params"]
        if p.get("skip_ingestion"):
            print("skip_ingestion=true: nothing to ingest")
            return []
        from include.Python.databricks_ingestion import discover_config_files, classify

        available = discover_config_files()
        wanted = [s.strip() for s in str(p.get("sources") or "").split(",") if s.strip()]
        if not wanted:
            chosen = available
        else:
            chosen, unknown = [], []
            for w in wanted:
                name = w.split(":")[-1].lower()  # accepts 'SUPPLIER', 'sqlserver:SUPPLIER' or a file name
                hits = [f for f in available if f.lower() == w.lower() or classify(f)[1].lower() == name]
                if hits:
                    chosen.extend(hits)
                else:
                    unknown.append(w)
            if unknown:
                raise AirflowFailException(f"Unknown source(s): {unknown}. Available: {[classify(f)[1] for f in available]}")
        confs = [{"config_filename": f, "tables": "", "run_dbt": False, "dbt_command": p.get("dbt_command", "run"),
                  "check_landing_tables": True, "fail_on_empty_load": False} for f in dict.fromkeys(chosen)]
        print(f"Ingesting {len(confs)} source(s): {[c['config_filename'] for c in confs]}")
        return confs

    ingest_states = ["success", "failed"] if CONTINUE_ON_INGESTION_FAILURE else ["success"]
    ingest = TriggerDagRunOperator.partial(
        task_id="ingest_source",
        trigger_dag_id="DATABRICKS_SOURCE_INGESTION",
        trigger_run_id="e2e_{{ run_id }}_{{ ti.map_index }}",
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=ingest_states,
        failed_states=[] if CONTINUE_ON_INGESTION_FAILURE else ["failed"],
        max_active_tis_per_dag=INGEST_PARALLEL_SOURCES,
    )

    @task.short_circuit(task_id="dbt_wanted", trigger_rule=TriggerRule.NONE_FAILED)
    def dbt_wanted(**context):
        wanted = not context["params"].get("skip_dbt")
        print("running dbt stage" if wanted else "skip_dbt=true: dbt stage skipped")
        return wanted

    @task(task_id="pipeline_conf", retries=0)
    def pipeline_conf(**context):
        p = context["params"]
        keys = ("dbt_command", "full_refresh", "threads", "select_nodes", "skip_nodes", "exclude", "dbt_vars", "fail_fast")
        return {k: p[k] for k in keys if k in p}

    run_dbt = TriggerDagRunOperator(
        task_id="run_dbt_pipeline",
        trigger_dag_id="DBT_DATABRICKS_PIPELINE",
        trigger_run_id="e2e_{{ run_id }}",
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=120,
        conf=pipeline_conf(),
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    ingest_stage = ingest.expand(conf=plan_ingestion())
    check_databricks() >> ingest_stage
    ingest_stage >> dbt_wanted() >> run_dbt
