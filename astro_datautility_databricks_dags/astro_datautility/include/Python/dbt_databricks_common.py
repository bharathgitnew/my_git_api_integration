"""
Shared helpers for the dbt_databricks Airflow DAGs.

Used by:
    dags/DBT_DATABRICKS_PIPELINE.py       full dependency-ordered dbt run
    dags/DBT_DATABRICKS_EXECUTION.py      ad-hoc / selector-driven dbt run
    dags/DATABRICKS_SOURCE_INGESTION.py   source system -> Databricks landing
    dags/DATABRICKS_END_TO_END.py         ingestion -> dbt orchestrator

Design notes
------------
* The DAG structure comes from include/Configuration/dbt_databricks_graph.json,
  which is generated from dbt's manifest by generate_dbt_databricks_graph.py.
  The full manifest is ~170 MB, so it must never be read at DAG-parse time.
* Each pipeline task runs ONE dbt invocation for one "node" (a domain/layer
  slice such as tea_service/bronze). dbt itself orders the models inside the
  slice; Airflow orders the slices using the edges taken from the dbt graph.
* Everything user-controlled (selectors, vars, flags) reaches the shell through
  environment variables, never by string-formatting into the command, so the
  shell script below is a constant.
* dbt runs in the existing `dbt_venv` (see Dockerfile) on the Airflow worker and
  sends SQL to the Databricks SQL warehouse configured in the profiles.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable

# --------------------------------------------------------------------------- locations
_PYTHON_DIR = Path(__file__).resolve().parent                # .../include/Python
_ASTRO_ROOT = _PYTHON_DIR.parents[1]                           # .../astro_datautility

DBT_PROJECT_DIR = os.environ.get(
    "DBT_DATABRICKS_PROJECT_DIR", str(_ASTRO_ROOT / "dags" / "dbt" / "dbt_databricks")
)
GRAPH_PATH = os.environ.get(
    "DBT_DATABRICKS_GRAPH_PATH", str(_ASTRO_ROOT / "include" / "Configuration" / "dbt_databricks_graph.json")
)
CONFIG_DIR = os.environ.get("TEL_CONFIG_DIR", str(_ASTRO_ROOT / "include" / "Configuration"))
DBT_VENV_ACTIVATE = os.environ.get(
    "DBT_VENV_ACTIVATE", f"{os.environ.get('AIRFLOW_HOME', '/usr/local/airflow')}/dbt_venv/bin/activate"
)

DEPLOYMENT = os.environ.get("ASTRO_DEPLOYMENT_NAME", "DEV").strip().upper() or "DEV"
IS_PROD = DEPLOYMENT == "PROD"

# Connection used by provider operators/hooks and as a fallback source for the
# dbt credentials (host / http_path / token) when the env vars are not set.
DATABRICKS_CONN_ID = os.environ.get("DBT_DATABRICKS_CONN_ID", "databricks_default")

# Adapter-internal dbt packages. A root project with one of these names fails
# at parse time ("found more than one package with the name ...").
ADAPTER_PACKAGE_NAMES = {"dbt_databricks", "dbt_spark"}

# Required by dbt parsing even when selecting one domain (see dbt_databricks/README.md).
REQUIRED_DBT_ENV = (
    "DATABRICKS_HOST",
    "DATABRICKS_HTTP_PATH",
    "DATABRICKS_TOKEN",
    "DATABRICKS_CATALOG",
    "DATABRICKS_LZ_CATALOG",
    "DATABRICKS_LZ_HVR_CATALOG",
    "DATABRICKS_BRONZE_CATALOG",
    "DATABRICKS_SILVER_CATALOG",
    "DATABRICKS_GOLD_CATALOG",
    "DATABRICKS_ADMIN_CATALOG",
    "DATABRICKS_PUB_CATALOG",
    "DATABRICKS_ODS_CATALOG",
    "DATABRICKS_SNOWFLAKE_ACCOUNT_USAGE_CATALOG",
)
# These three can be satisfied from the Airflow connection instead.
CONNECTION_BACKED_ENV = ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")

BUILTIN_MATERIALIZATIONS = {"table", "view", "incremental", "ephemeral", "materialized_view", "streaming_table", "seed", "snapshot"}

_SELECTOR_OK = re.compile(r"^[A-Za-z0-9_.:+@,*/\-\[\]\s]*$")


# --------------------------------------------------------------------------- project info
def project_name(project_dir: str = DBT_PROJECT_DIR) -> str | None:
    """`name:` from dbt_project.yml (read with a regex; the file is small and PyYAML is not needed)."""
    try:
        text = (Path(project_dir) / "dbt_project.yml").read_text(encoding="utf-8")
    except OSError:
        return None
    m = re.search(r"^name:\s*['\"]?([A-Za-z0-9_]+)['\"]?\s*$", text, re.M)
    return m.group(1) if m else None


def profiles_dir(project_dir: str = DBT_PROJECT_DIR, deployment: str = DEPLOYMENT) -> tuple[str, str | None]:
    """
    Returns (profiles_dir, target_override).

    `<DEPLOYMENT>_PROFILES/` (DEV_PROFILES, QA_PROFILES, SIT_PROFILES, PROD_PROFILES)
    each carry their own default target, so no override is needed. If the
    folder is missing, fall back to the root profiles.yml, which selects its
    target from ASTRO_DEPLOYMENT_NAME (dev/qa/prod only).
    """
    candidate = Path(project_dir) / f"{deployment}_PROFILES"
    if candidate.is_dir():
        return str(candidate), None
    return str(project_dir), deployment.lower()


# --------------------------------------------------------------------------- graph
@lru_cache(maxsize=1)
def load_graph(path: str = GRAPH_PATH) -> dict[str, Any]:
    """Load and validate the pre-computed dbt dependency graph."""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            graph = json.load(fh)
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"dbt graph file not found: {path}. Generate it with "
            f"`python include/Python/generate_dbt_databricks_graph.py --parse` and deploy it with the DAGs."
        ) from exc

    if graph.get("schema_version") != 1:
        raise ValueError(f"Unsupported graph schema_version: {graph.get('schema_version')}")

    ids = [n["id"] for n in graph["nodes"]]
    if len(ids) != len(set(ids)):
        raise ValueError("Duplicate node ids in dbt graph file")
    known = set(ids)
    for up, down in graph["edges"]:
        if up not in known or down not in known:
            raise ValueError(f"Edge references unknown node: {up} -> {down}")
    assert_acyclic(ids, graph["edges"])
    return graph


def assert_acyclic(ids: Iterable[str], edges: Iterable[Iterable[str]]) -> None:
    """Kahn's algorithm; raises ValueError naming the nodes stuck in a cycle."""
    ids = list(ids)
    indeg = {i: 0 for i in ids}
    adj: dict[str, list[str]] = {i: [] for i in ids}
    for up, down in edges:
        adj[up].append(down)
        indeg[down] += 1
    ready = [i for i, d in indeg.items() if d == 0]
    seen = 0
    while ready:
        cur = ready.pop()
        seen += 1
        for nxt in adj[cur]:
            indeg[nxt] -= 1
            if indeg[nxt] == 0:
                ready.append(nxt)
    if seen != len(ids):
        stuck = sorted(i for i, d in indeg.items() if d > 0)
        raise ValueError(f"Cycle in dbt task graph involving: {stuck[:10]}")


# --------------------------------------------------------------------------- fingerprints & static checks
_REF_RE = re.compile(r"""\bref\(\s*(?:['"][^'"]+['"]\s*,\s*)?['"]([^'"]+)['"]""")
_SRC_RE = re.compile(r"""\bsource\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]""")
_MAT_RE = re.compile(r"""materialized\s*=\s*['"]([A-Za-z0-9_]+)['"]""")
_MATERIALIZATION_DEF_RE = re.compile(r"""\{%-?\s*materialization\s+([A-Za-z0-9_]+)\s*,""")


def _model_files(project_dir: str) -> list[Path]:
    root = Path(project_dir) / "models"
    return sorted(p for p in root.rglob("*") if p.is_file() and p.suffix.lower() == ".sql")


def dependency_fingerprint(project_dir: str = DBT_PROJECT_DIR) -> str:
    """
    Hash of every model file path plus the ref()/source() calls inside it.

    This is what the dependency graph is derived from, so if it changes the
    committed graph JSON is stale. Whitespace/formatting edits and ordinary SQL
    changes do NOT change it. (Refs produced by macros are not visible here;
    dbt still orders models correctly inside a task, only cross-task ordering
    could be affected.)
    """
    h = hashlib.sha1()
    base = Path(project_dir)
    for f in _model_files(project_dir):
        text = f.read_text(encoding="utf-8", errors="ignore")
        refs = sorted(set(_REF_RE.findall(text)))
        srcs = sorted({f"{a}.{b}" for a, b in _SRC_RE.findall(text)})
        h.update(f"{f.relative_to(base).as_posix().lower()}|{','.join(refs)}|{','.join(srcs)}\n".encode())
    return h.hexdigest()


def unresolved_materializations(project_dir: str = DBT_PROJECT_DIR) -> dict[str, int]:
    """
    Materializations named in model configs that are neither dbt built-ins nor
    defined as a `{% materialization name, adapter='...' %}` macro in the
    project or its installed packages. Returns {name: model_count}.
    """
    defined = set(BUILTIN_MATERIALIZATIONS)
    for sub in ("macros", "dbt_packages", "dbt_internal_packages"):
        root = Path(project_dir) / sub
        if root.is_dir():
            for f in root.rglob("*.sql"):
                try:
                    defined.update(_MATERIALIZATION_DEF_RE.findall(f.read_text(encoding="utf-8", errors="ignore")))
                except OSError:
                    continue
    counts: dict[str, int] = {}
    for f in _model_files(project_dir):
        for name in set(_MAT_RE.findall(f.read_text(encoding="utf-8", errors="ignore"))):
            if name not in defined:
                counts[name] = counts.get(name, 0) + 1
    return counts


# --------------------------------------------------------------------------- runtime env
def missing_dbt_env(environ: dict[str, str] | None = None, allow_connection: bool = True) -> list[str]:
    environ = os.environ if environ is None else environ
    missing = [k for k in REQUIRED_DBT_ENV if not environ.get(k)]
    if allow_connection:
        missing = [k for k in missing if k not in CONNECTION_BACKED_ENV]
        if any(not environ.get(k) for k in CONNECTION_BACKED_ENV):
            # verify the connection can actually supply them
            try:
                supplied = env_from_connection()
            except Exception:  # noqa: BLE001 - connection absent is a normal "missing" case
                supplied = {}
            missing += [k for k in CONNECTION_BACKED_ENV if not environ.get(k) and not supplied.get(k)]
    return missing


def env_from_connection(conn_id: str = DATABRICKS_CONN_ID) -> dict[str, str]:
    """
    DATABRICKS_HOST / HTTP_PATH / TOKEN derived from an Airflow Databricks
    connection (host, extra.http_path, password or extra.token). Only called
    at task run time.
    """
    try:
        from airflow.sdk import Connection  # Airflow 3
        conn = Connection.get(conn_id)
    except ImportError:  # Airflow 2
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection(conn_id)

    extra = conn.extra_dejson if hasattr(conn, "extra_dejson") else json.loads(conn.extra or "{}")
    host = (conn.host or "").replace("https://", "").replace("http://", "").strip("/")
    out = {
        "DATABRICKS_HOST": host,
        "DATABRICKS_HTTP_PATH": extra.get("http_path", ""),
        "DATABRICKS_TOKEN": conn.password or extra.get("token", ""),
    }
    return {k: v for k, v in out.items() if v}


def validate_selector(value: str, label: str = "selector") -> str:
    value = (value or "").strip()
    if not _SELECTOR_OK.match(value):
        raise ValueError(f"Unsupported characters in dbt {label}: {value!r}")
    return value


def run_key(*parts: str) -> str:
    """Filesystem-safe key for per-task dbt target/log directories."""
    return "/".join(re.sub(r"[^A-Za-z0-9_.-]+", "_", p or "x") for p in parts)


# --------------------------------------------------------------------------- shell script
# Constant on purpose: all dynamic input arrives via TEL_* / DBT_* env vars.
DBT_SHELL_SCRIPT = r"""
set -euo pipefail
set -f                                   # selectors contain '*'/'+': never glob them

# shellcheck disable=SC1090
source "${DBT_VENV_ACTIVATE}"
cd "${DBT_PROJECT_DIR}"

RUN_ROOT="${DBT_RUN_ROOT:-/tmp/dbt_databricks_runs}"
PP_DIR="${DBT_PARTIAL_PARSE_DIR:-/tmp/dbt_databricks_partial_parse}/${TEL_DEPLOYMENT}"
mkdir -p "${RUN_ROOT}" "${PP_DIR}" "${RUN_ROOT}/${TEL_RUN_KEY}/target" "${RUN_ROOT}/${TEL_RUN_KEY}/logs"

# One directory per task (not per try) so `dbt retry` can find run_results.json.
export DBT_TARGET_PATH="${RUN_ROOT}/${TEL_RUN_KEY}/target"
export DBT_LOG_PATH="${RUN_ROOT}/${TEL_RUN_KEY}/logs"
export DBT_PROFILES_DIR="${TEL_PROFILES_DIR}"
export DBT_SEND_ANONYMOUS_USAGE_STATS=false
export DBT_USE_COLORS=false
if [ -n "${TEL_DBT_TARGET:-}" ]; then export DBT_TARGET="${TEL_DBT_TARGET}"; fi

# ---- 1. dbt packages: once per worker, serialised across concurrent tasks ----
DEPS_STAMP="dbt_packages/.deps_ok_$({ cat packages.yml package-lock.yml 2>/dev/null || true; } | sha1sum | cut -c1-16)"
(
  flock 9
  if [ ! -f "${DEPS_STAMP}" ]; then
    echo "[dbt-databricks] installing dbt packages"
    dbt deps
    touch "${DEPS_STAMP}"
  fi
) 9>"${RUN_ROOT}/.deps.lock"

# ---- 2. partial-parse cache: a cold parse of this project takes minutes ----
# The cache lives per worker and is built once under a lock; later tasks reuse it.
PP_FILE="${PP_DIR}/partial_parse.msgpack"
(
  flock 9
  if [ ! -s "${PP_FILE}" ]; then
    echo "[dbt-databricks] building partial-parse cache (first task on this worker)"
    DBT_TARGET_PATH="${PP_DIR}" DBT_LOG_PATH="${PP_DIR}/logs" dbt parse
  fi
) 9>"${PP_DIR}/.lock"
if [ -s "${PP_FILE}" ]; then export DBT_PARTIAL_PARSE_FILE_PATH="${PP_FILE}"; fi

# ---- 3. result summary (also on failure) ----
summarize() {
  local rc=$?
  if [ -f "${DBT_TARGET_PATH}/run_results.json" ]; then
    python - "${DBT_TARGET_PATH}/run_results.json" <<'PY' || true
import json, sys, collections
res = json.load(open(sys.argv[1])).get("results", [])
by = collections.Counter(r.get("status") for r in res)
print("[dbt-databricks] result summary:", dict(by))
bad = [r for r in res if r.get("status") in ("error", "fail")]
for r in bad[:25]:
    print("  FAILED", r.get("unique_id"), "-", (r.get("message") or "").replace("\n", " ")[:300])
if len(bad) > 25:
    print(f"  ... and {len(bad) - 25} more")
PY
  fi
  find "${RUN_ROOT}" -mindepth 1 -maxdepth 1 -type d -mtime +2 -exec rm -rf {} + 2>/dev/null || true
  return $rc
}
trap summarize EXIT

# ---- 4. dbt invocation ----
read -r -a SELECT <<< "${TEL_DBT_SELECT:-}"
read -r -a EXCLUDE <<< "${TEL_DBT_EXCLUDE:-}"
ARGS=(--threads "${TEL_DBT_THREADS:-4}")
if [ -n "${TEL_DBT_EXCLUDE:-}" ]; then ARGS+=(--exclude "${EXCLUDE[@]}"); fi
if [ "${TEL_DBT_FAIL_FAST:-false}" = "true" ]; then ARGS+=(--fail-fast); fi
if [ "${TEL_DBT_FULL_REFRESH:-false}" = "true" ] && [ "${TEL_DBT_COMMAND}" != "test" ]; then ARGS+=(--full-refresh); fi
if [ -n "${TEL_DBT_VARS:-}" ] && [ "${TEL_DBT_VARS}" != "{}" ]; then ARGS+=(--vars "${TEL_DBT_VARS}"); fi

# On an Airflow retry, re-run only what failed/was skipped (state in run_results.json).
if [ "${TEL_TRY_NUMBER:-1}" -gt 1 ] && [ "${TEL_USE_DBT_RETRY:-true}" = "true" ] \
   && [ -f "${DBT_TARGET_PATH}/run_results.json" ]; then
  echo "[dbt-databricks] retry attempt ${TEL_TRY_NUMBER}: dbt retry"
  dbt retry --threads "${TEL_DBT_THREADS:-4}"
else
  echo "[dbt-databricks] dbt ${TEL_DBT_COMMAND} --select ${SELECT[*]} (threads=${TEL_DBT_THREADS:-4})"
  dbt "${TEL_DBT_COMMAND}" --select "${SELECT[@]}" "${ARGS[@]}"
fi
"""
