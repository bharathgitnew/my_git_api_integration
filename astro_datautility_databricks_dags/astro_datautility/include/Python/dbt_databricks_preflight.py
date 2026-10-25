"""
Pre-flight checks run as the first task of the dbt DAGs.

Purpose: fail in seconds with an actionable message instead of letting ~100
dbt tasks fail one by one. Each check returns (level, message); level is
"error" (stops the run) or "warn".

Checks
  project     dbt project folder, profiles folder, dbt_venv present
  name        project `name:` must not equal an adapter package name (dbt aborts at parse)
  env         required DATABRICKS_* variables (or an Airflow connection supplying host/path/token)
  graph       committed dependency graph matches the project's ref()/source() calls
  materialize every `materialized=` used by models is a dbt built-in or defined as a macro
  connection  `dbt debug` reaches the Databricks SQL warehouse
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
from pathlib import Path
from typing import Any

from include.Python import dbt_databricks_common as C

log = logging.getLogger("airflow.task")

Finding = tuple[str, str]  # (level, message)


def _truthy(v: Any) -> bool:
    return str(v).strip().lower() in ("true", "1", "yes", "y")


def run_in_dbt_venv(cmd: str, extra_env: dict[str, str] | None = None, timeout: int = 180) -> subprocess.CompletedProcess:
    """Run a command with the dbt venv activated, inside the dbt project directory."""
    env = {**os.environ, **(extra_env or {})}
    env["DBT_VENV_ACTIVATE"] = C.DBT_VENV_ACTIVATE
    env["DBT_PROJECT_DIR"] = C.DBT_PROJECT_DIR
    return subprocess.run(
        ["bash", "-c", f'source "$DBT_VENV_ACTIVATE" && cd "$DBT_PROJECT_DIR" && {cmd}'],
        env=env, capture_output=True, text=True, timeout=timeout,
    )


def check_project() -> list[Finding]:
    out: list[Finding] = []
    proj = Path(C.DBT_PROJECT_DIR)
    if not (proj / "dbt_project.yml").is_file():
        return [("error", f"dbt project not found at {proj} (set DBT_DATABRICKS_PROJECT_DIR)")]
    pdir, _ = C.profiles_dir()
    if not (Path(pdir) / "profiles.yml").is_file():
        out.append(("error", f"profiles.yml not found in {pdir}"))
    if not Path(C.DBT_VENV_ACTIVATE).is_file():
        out.append(("error", f"dbt virtualenv not found: {C.DBT_VENV_ACTIVATE} (see Dockerfile: dbt_venv)"))
    return out


def check_project_name() -> list[Finding]:
    name = C.project_name()
    if name in C.ADAPTER_PACKAGE_NAMES:
        return [("error",
                 f"dbt_project.yml `name: {name}` collides with the dbt-databricks adapter's own package of the same "
                 f"name, so every dbt command aborts with 'found more than one package with the name \"{name}\"' "
                 f"(reproduced on dbt-core 1.7.1, 1.9 and 1.11). Rename the project, e.g. `name: dbt_databricks_proj`, "
                 f"and rename the matching `models:` key in dbt_project.yml (keep `profile: dbt_databricks`). "
                 f"A ready-made patch is in patches/dbt_project_rename.patch.")]
    return []


def check_env() -> list[Finding]:
    missing = C.missing_dbt_env()
    if missing:
        return [("error", "Missing required environment variables: " + ", ".join(missing) +
                 f" (DATABRICKS_HOST/HTTP_PATH/TOKEN may instead come from Airflow connection '{C.DATABRICKS_CONN_ID}')")]
    return []


def check_graph(strict: bool) -> list[Finding]:
    try:
        graph = C.load_graph()
    except Exception as exc:  # noqa: BLE001
        return [("error", f"dbt graph file problem: {exc}")]
    current = C.dependency_fingerprint()
    if current != graph.get("dependency_fingerprint"):
        msg = ("The committed dbt dependency graph is STALE: model files or their ref()/source() calls changed since it "
               "was generated (generated_at=%s). Regenerate with `python include/Python/generate_dbt_databricks_graph.py "
               "--parse` and redeploy. Until then cross-domain ordering may be wrong." % graph.get("generated_at"))
        return [("error" if strict else "warn", msg)]
    return []


def check_materializations() -> list[Finding]:
    bad = C.unresolved_materializations()
    if not bad:
        return []
    detail = ", ".join(f"'{k}' ({v} models)" for k, v in sorted(bad.items(), key=lambda kv: -kv[1]))
    return [("error",
             f"Models use materializations that do not exist for Databricks: {detail}. dbt would fail each of these "
             f"models at run time. Convert the models to a dbt-databricks materialization (e.g. incremental with "
             f"merge/insert_overwrite) or define the materialization macro. (The dbt project README notes the "
             f"Snowflake-specific materialization was removed and the SQL is not yet converted.)")]


def check_connection() -> list[Finding]:
    pdir, target = C.profiles_dir()
    extra = {"DBT_PROFILES_DIR": pdir}
    if target:
        extra["DBT_TARGET"] = target
    extra.update({k: v for k, v in _connection_env().items() if not os.environ.get(k)})
    try:
        res = run_in_dbt_venv("dbt debug", extra, timeout=240)
    except subprocess.TimeoutExpired:
        return [("error", "`dbt debug` timed out after 240s (is the SQL warehouse starting up, or the network blocked?)")]
    text = (res.stdout or "") + (res.stderr or "")
    if res.returncode != 0 or "All checks passed" not in text:
        tail = "\n".join(text.strip().splitlines()[-25:])
        return [("error", f"`dbt debug` failed (exit {res.returncode}):\n{tail}")]
    return []


def _connection_env() -> dict[str, str]:
    try:
        return C.env_from_connection()
    except Exception:  # noqa: BLE001
        return {}


def run_preflight(params: dict[str, Any], *, check_graph_file: bool = True) -> dict[str, Any]:
    """Run all checks; raise on errors. Returns a small summary for XCom."""
    if _truthy(params.get("skip_preflight")):
        log.warning("skip_preflight=true: pre-flight checks were NOT run")
        return {"skipped": True}

    findings: list[Finding] = []
    findings += check_project()
    if not any(level == "error" for level, _ in findings):
        findings += check_project_name()
        findings += check_env()
        if check_graph_file:
            findings += check_graph(strict=_truthy(params.get("strict_graph_check", True)))
        findings += check_materializations()
    if not any(level == "error" for level, _ in findings):
        findings += check_connection()  # only worth the round trip if everything else is sane

    for level, msg in findings:
        (log.error if level == "error" else log.warning)("[preflight:%s] %s", level, msg)
    errors = [m for level, m in findings if level == "error"]
    if errors:
        from airflow.exceptions import AirflowFailException
        raise AirflowFailException(
            f"Pre-flight failed with {len(errors)} problem(s). Fix them, or re-run with skip_preflight=true "
            f"to bypass (not recommended).\n\n" + "\n\n".join(f"- {m}" for m in errors)
        )
    return {"skipped": False, "warnings": [m for level, m in findings if level == "warn"]}
