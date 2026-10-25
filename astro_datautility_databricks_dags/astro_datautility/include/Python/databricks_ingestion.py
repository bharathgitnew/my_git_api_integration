"""
Source-system -> Databricks landing helpers used by DATABRICKS_SOURCE_INGESTION.

The extraction + load itself is done by the EXISTING extractors, unchanged:

    include/Python/TelTools_SQLServer.py   main(config_file, table)
    include/Python/TelTools_Oracle.py      main(config_file, table)

Those read the source (SQL Server / Oracle), page it in chunks and write it to the
landing table through TELTools.putSnowflakeDataFrame, which routes to
include/Python/databricks_teltools.py (parquet -> UC Volume -> INSERT ... SELECT)
and records each run in <LZ catalog>.<SCHEMA>.CONTROLTABLE.

What this module adds around them:
    plan_ingestion      which tables a config file loads, and which dbt selector depends on them
    check_landing       fail fast if the landing schema / tables / CONTROLTABLE do not exist
                        (the loader never auto-creates tables)
    extract_one         the call into the right extractor
    validate_load       confirm every table wrote a CONTROLTABLE row during this run

Connection settings for the load are the existing ones (DATABRICKS_DEV_HOST,
DATABRICKS_DEV_HTTP_PATH, DATABRICKS_DEV_CLIENT, DATABRICKS_DEV_SECRET, and
SNOWFLAKE_LZ for the landing catalog name) - see databricks_teltools.py.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import re
from pathlib import Path
from typing import Any

from include.Python import dbt_databricks_common as C

log = logging.getLogger("airflow.task")

LOADER_ENV = ("DATABRICKS_DEV_HOST", "DATABRICKS_DEV_HTTP_PATH", "DATABRICKS_DEV_CLIENT", "DATABRICKS_DEV_SECRET", "SNOWFLAKE_LZ")
_EXTRACTOR_PATTERNS = (
    ("sqlserver", re.compile(r"^TelTools_SQLServer_(.+)_Extract\.json$")),
    ("oracle", re.compile(r"^TelTools_Oracle_(.+)_Extract\.json$")),
)


def discover_config_files(config_dir: str = C.CONFIG_DIR) -> list[str]:
    """File names of every SQL Server / Oracle extract config present."""
    try:
        names = sorted(os.listdir(config_dir))
    except FileNotFoundError:
        return []
    return [n for n in names if any(p.match(n) for _, p in _EXTRACTOR_PATTERNS)]


def classify(config_filename: str) -> tuple[str, str]:
    """(kind, server_name) for a config file name; raises for unsupported files."""
    base = os.path.basename(config_filename)
    for kind, pattern in _EXTRACTOR_PATTERNS:
        m = pattern.match(base)
        if m:
            return kind, m.group(1)
    raise ValueError(
        f"Unsupported extract config {config_filename!r}. Expected TelTools_SQLServer_<name>_Extract.json "
        f"or TelTools_Oracle_<name>_Extract.json"
    )


def _csv(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, str):
        value = value.split(",")
    return [str(v).strip() for v in value if str(v).strip()]


def plan_ingestion(config_filename: str, tables: Any = "") -> dict[str, Any]:
    """Resolve what one ingestion run will load. JSON-serialisable (goes through XCom)."""
    config_filename = os.path.basename(config_filename or "")
    if not config_filename:
        raise ValueError("config_filename is required")
    kind, server = classify(config_filename)
    path = Path(C.CONFIG_DIR) / config_filename
    if not path.is_file():
        raise FileNotFoundError(f"Extract config not found: {path}")

    data = json.loads(path.read_text(encoding="utf-8-sig"))
    datasets = [
        {"table": v["table"], "landing_table": v["sftablename"]}
        for ds in data.values() if isinstance(ds, dict)
        for k, v in ds.items() if k.startswith("DataSet") and isinstance(v, dict)
    ]
    requested = _csv(tables)
    if requested:
        known = {d["table"].lower() for d in datasets}
        unknown = [t for t in requested if t.lower() not in known]
        if unknown:
            raise ValueError(f"Tables not in {config_filename}: {unknown}")
        wanted = {t.lower() for t in requested}
        datasets = [d for d in datasets if d["table"].lower() in wanted]
    if not datasets:
        raise ValueError(f"No datasets found in {config_filename}")

    # same derivation the extractors use for the landing schema
    landing_schema = server.upper().split("-")[0] if kind == "sqlserver" else server.upper()

    dbt_select = ""
    try:
        for ex in C.load_graph().get("extractors", []):
            if ex["config_filename"] == config_filename:
                dbt_select = ex.get("dbt_select", "")
                break
    except Exception as exc:  # noqa: BLE001 - graph is optional for ingestion
        log.warning("dbt graph unavailable, downstream dbt selector left empty: %s", exc)

    return {
        "config_filename": config_filename,
        "kind": kind,
        "server": server,
        "landing_schema": landing_schema,
        "datasets": datasets,
        "tables": [d["table"] for d in datasets],
        "dbt_select": dbt_select,
        "run_start": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),  # same clock/format the extractors stamp RUN_TIME with
    }


def _require_loader_env() -> None:
    missing = [k for k in LOADER_ENV if not os.environ.get(k)]
    if missing:
        raise RuntimeError(
            "Databricks loader environment is incomplete, missing: " + ", ".join(missing) +
            ". These are the variables include/Python/databricks_teltools.py reads."
        )


def _landing_connection(schema: str):
    from include.Python import databricks_teltools as D
    return D, D._open(os.environ["SNOWFLAKE_LZ"], schema)


def check_landing(plan: dict[str, Any]) -> dict[str, Any]:
    """Verify the landing schema, every target table and CONTROLTABLE exist before extracting anything."""
    _require_loader_env()
    D, conn = _landing_connection(plan["landing_schema"])
    try:
        schema_fq = f"`{D._catalog(os.environ['SNOWFLAKE_LZ'])}`.`{D._ident(plan['landing_schema'])}`"
        with conn.cursor() as cur:
            cur.execute(f"SHOW TABLES IN {schema_fq}")
            present = {str(r[1]).lower() for r in cur.fetchall()}  # (database, tableName, isTemporary)
    finally:
        conn.close()

    expected = {d["landing_table"].lower() for d in plan["datasets"]} | {"controltable"}
    missing = sorted(expected - present)
    if missing:
        raise RuntimeError(
            f"Landing tables missing in {schema_fq}: {missing}. The loader never auto-creates landing tables - "
            f"provision them with the DDL generator (include/Python/createLandingZone.py / lz DDL) first."
        )
    log.info("Landing check OK: %d tables present in %s", len(expected), schema_fq)
    return {"schema": schema_fq, "tables_checked": len(expected)}


def extract_one(config_filename: str, table: str) -> None:
    """Extract one source table and load it to Databricks via the existing extractor."""
    _require_loader_env()
    kind, _ = classify(config_filename)
    path = str(Path(C.CONFIG_DIR) / os.path.basename(config_filename))
    if kind == "sqlserver":
        import include.Python.TelTools_SQLServer as extractor
    else:
        import include.Python.TelTools_Oracle as extractor
    log.info("Extracting %s from %s", table, config_filename)
    extractor.main(path, table)
    log.info("Loaded %s", table)


def validate_load(plan: dict[str, Any], fail_on_empty: bool = False) -> list[dict[str, Any]]:
    """Every table must have written a CONTROLTABLE row during this run (RUN_TIME >= run_start)."""
    _require_loader_env()
    D, conn = _landing_connection(plan["landing_schema"])
    since = (dt.datetime.strptime(plan["run_start"], "%Y-%m-%d %H:%M:%S.%f") - dt.timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
    fq = D._fq(os.environ["SNOWFLAKE_LZ"], plan["landing_schema"], "CONTROLTABLE")
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT TABLE_NAME, ROW_COUNT, RUN_TIME FROM {fq} "
                f"WHERE CAST(RUN_TIME AS TIMESTAMP) >= CAST(:since AS TIMESTAMP) "
                f"QUALIFY row_number() OVER (PARTITION BY TABLE_NAME ORDER BY RUN_TIME DESC) = 1",
                {"since": since},
            )
            rows = {str(r[0]).lower(): r for r in cur.fetchall()}
    finally:
        conn.close()

    report, missing, empty = [], [], []
    for t in plan["tables"]:
        row = rows.get(t.lower())
        if row is None:
            missing.append(t)
            continue
        n = int(row[1]) if row[1] not in (None, "") else None
        report.append({"table": t, "rows": n, "run_time": str(row[2])})
        if n == 0:
            empty.append(t)
        log.info("%-60s rows=%s run_time=%s", t, n, row[2])
    if missing:
        raise RuntimeError(f"No CONTROLTABLE entry for this run (load did not complete?): {missing}")
    if empty and fail_on_empty:
        raise RuntimeError(f"Loads returned 0 rows and fail_on_empty_load=true: {empty}")
    if empty:
        log.warning("%d table(s) loaded 0 rows (normal for incremental loads with no new data): %s", len(empty), empty[:10])
    return report
