#!/usr/bin/env python3
"""
Generate include/Configuration/dbt_databricks_graph.json from dbt's manifest.

WHY A GENERATED FILE
    The project has ~5,500 models and its manifest is ~170 MB; parsing it takes
    minutes. Airflow re-imports DAG files constantly, so the DAGs read this small
    pre-computed graph instead. Re-run this script (CI step or by hand) whenever
    models are added/removed or their ref()/source() calls change. The DAGs'
    pre-flight task compares a dependency fingerprint and fails loudly if the
    graph is stale.

WHAT IT BUILDS
    * one NODE per domain/layer folder  (models/<domain>/<layer>), e.g. tea_service/bronze
      plus one node per installed dbt package that project models depend on
      (dbt_artifacts)
    * EDGES between nodes from the model-level ref() graph
    * nodes that sit in a cycle at this coarser level (e.g. teh_finance
      silver <-> gold, where individual models are acyclic) are merged into one
      node, so the Airflow graph is always a DAG; dbt orders the models inside it
    * edges are transitively reduced (same ordering, far fewer arrows)
    * source -> nodes and extractor-config -> source maps used by the ingestion DAGs

USAGE
    python generate_dbt_databricks_graph.py --parse            # deps + parse + generate
    python generate_dbt_databricks_graph.py --manifest path/to/manifest.json

    --parse runs `dbt deps` and `dbt parse` with placeholder connection values
    (parsing never opens a connection) using the `dbt` on PATH, e.g. the one in dbt_venv.
"""
from __future__ import annotations

import argparse
import collections
import datetime as dt
import glob
import json
import os
import re
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))
from include.Python import dbt_databricks_common as C  # noqa: E402

PARSE_PLACEHOLDER_ENV = {
    "DATABRICKS_HOST": "placeholder.cloud.databricks.com",
    "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/placeholder",
    "DATABRICKS_TOKEN": "placeholder",
    "DATABRICKS_CATALOG": "main",
    "DATABRICKS_LZ_CATALOG": "lz",
    "DATABRICKS_LZ_HVR_CATALOG": "lz_hvr",
    "DATABRICKS_BRONZE_CATALOG": "bronze",
    "DATABRICKS_SILVER_CATALOG": "silver",
    "DATABRICKS_GOLD_CATALOG": "gold",
    "DATABRICKS_ADMIN_CATALOG": "admin",
    "DATABRICKS_PUB_CATALOG": "pub",
    "DATABRICKS_ODS_CATALOG": "ods",
    "DATABRICKS_SNOWFLAKE_ACCOUNT_USAGE_CATALOG": "sfau",
    "ASTRO_DEPLOYMENT_NAME": "DEV",
    "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
}


# --------------------------------------------------------------------------- manifest
def run_parse(project_dir: Path, profiles: str) -> Path:
    env = {**PARSE_PLACEHOLDER_ENV, **os.environ}   # real values win over placeholders
    env.pop("DBT_TARGET", None)
    for cmd in (["dbt", "deps"], ["dbt", "parse", "--profiles-dir", profiles]):
        print("+", " ".join(cmd), flush=True)
        subprocess.run(cmd, cwd=project_dir, env=env, check=True)
    return project_dir / "target" / "manifest.json"


def group_of(model: dict, root_pkg: str) -> tuple[str, str, str]:
    """(domain, layer, selector path) for a model node."""
    if model["package_name"] != root_pkg:
        return model["package_name"], "package", f"package:{model['package_name']}"
    parts = model["original_file_path"].replace("\\", "/").split("/")   # models/<domain>/<layer>/file.sql
    if len(parts) >= 4:
        return parts[1], parts[2], f"path:{'/'.join(parts[:3])}"
    return parts[1] if len(parts) > 2 else "root", "_", f"path:{'/'.join(parts[:-1])}"


def tarjan_scc(nodes: list, adj: dict) -> list[list]:
    index, low, on, stack, out, counter = {}, {}, set(), [], [], [0]

    def visit(v):
        # iterative to stay safe on deep graphs
        work = [(v, iter(adj.get(v, ())))]
        index[v] = low[v] = counter[0]; counter[0] += 1
        stack.append(v); on.add(v)
        while work:
            node, it = work[-1]
            advanced = False
            for w in it:
                if w not in index:
                    index[w] = low[w] = counter[0]; counter[0] += 1
                    stack.append(w); on.add(w)
                    work.append((w, iter(adj.get(w, ()))))
                    advanced = True
                    break
                elif w in on:
                    low[node] = min(low[node], index[w])
            if advanced:
                continue
            work.pop()
            if work:
                low[work[-1][0]] = min(low[work[-1][0]], low[node])
            if low[node] == index[node]:
                comp = []
                while True:
                    w = stack.pop(); on.discard(w); comp.append(w)
                    if w == node:
                        break
                out.append(sorted(comp))

    for n in nodes:
        if n not in index:
            visit(n)
    return out


def transitive_reduction(ids: list[str], edges: set[tuple[str, str]]) -> set[tuple[str, str]]:
    adj = collections.defaultdict(set)
    for a, b in edges:
        adj[a].add(b)
    reach: dict[str, set[str]] = {}

    def descendants(n: str) -> set[str]:
        if n in reach:
            return reach[n]
        seen: set[str] = set()
        for m in adj[n]:
            seen.add(m)
            seen |= descendants(m)
        reach[n] = seen
        return seen

    sys.setrecursionlimit(10000)
    keep = set()
    for a, b in edges:
        # drop a->b if b is reachable from a through another direct successor
        if not any(b in descendants(m) for m in adj[a] if m != b):
            keep.add((a, b))
    return keep


# --------------------------------------------------------------------------- sources & extractors
def source_catalog_info(project_dir: Path) -> dict[str, dict]:
    """source name -> {catalog_env, schema_env, schema_default} read from the raw source YAMLs."""
    try:
        import yaml
    except ImportError:
        return {}
    info: dict[str, dict] = {}
    env_re = re.compile(r"env_var\(\s*'([A-Z0-9_]+)'(?:\s*,\s*'([^']*)')?")
    for f in sorted((project_dir / "models").rglob("*.yml")):
        try:
            doc = yaml.safe_load(f.read_text(encoding="utf-8")) or {}
        except Exception:  # noqa: BLE001
            continue
        for src in doc.get("sources", []) or []:
            name = src.get("name")
            if not name:
                continue
            cat = env_re.search(str(src.get("database", "")))
            sch = env_re.search(str(src.get("schema", "")))
            rec = info.setdefault(name, {"catalog_env": set(), "schema_env": set(), "schema_default": set()})
            if cat:
                rec["catalog_env"].add(cat.group(1))
            if sch:
                rec["schema_env"].add(sch.group(1))
                if sch.group(2):
                    rec["schema_default"].add(sch.group(2))
    return {k: {kk: sorted(vv) for kk, vv in v.items()} for k, v in info.items()}


def discover_extractors(config_dir: Path) -> list[dict]:
    out = []
    for kind, prefix in (("sqlserver", "TelTools_SQLServer_"), ("oracle", "TelTools_Oracle_")):
        for f in sorted(glob.glob(str(config_dir / f"{prefix}*_Extract.json"))):
            name = re.search(rf"{prefix}(.+)_Extract\.json$", os.path.basename(f)).group(1)
            data = json.loads(Path(f).read_text(encoding="utf-8-sig"))
            datasets = []
            for server_key, ds in data.items():
                if not isinstance(ds, dict):
                    continue
                for k, v in ds.items():
                    if k.startswith("DataSet") and isinstance(v, dict):
                        datasets.append({"table": v["table"], "landing_table": v["sftablename"]})
            # landing schema exactly as the extractor modules derive it
            landing_schema = name.upper().split("-")[0] if kind == "sqlserver" else name.upper()
            out.append({
                "id": f"{kind}:{name}",
                "kind": kind,
                "config_filename": os.path.basename(f),
                "landing_schema": landing_schema,
                "datasets": datasets,
            })
    return out


# --------------------------------------------------------------------------- main
def build(manifest_path: Path, project_dir: Path, config_dir: Path) -> dict:
    m = json.loads(manifest_path.read_text(encoding="utf-8"))
    root_pkg = m["metadata"].get("project_name") or project_dir.name
    nodes = m["nodes"]
    models = {k: v for k, v in nodes.items() if v["resource_type"] == "model"}
    tests = [v for v in nodes.values() if v["resource_type"] == "test"]

    grp = {k: group_of(v, root_pkg) for k, v in models.items()}
    keys = sorted(set(grp.values()))
    gid = lambda g: f"{g[0]}__{g[1]}"  # noqa: E731

    # model-level edges aggregated to (domain, layer) level
    edges: set[tuple[tuple, tuple]] = set()
    src_by_group: dict[tuple, set[str]] = collections.defaultdict(set)
    for k, v in models.items():
        for d in v["depends_on"]["nodes"]:
            if d in models:
                if grp[d] != grp[k]:
                    edges.add((grp[d], grp[k]))
            elif d.startswith("source."):
                src_by_group[grp[k]].add(m["sources"][d]["source_name"])

    # package models are only worth running if some project model needs them
    needed_pkgs = {a for a, b in edges if a[1] == "package"}
    keys = [g for g in keys if g[1] != "package" or g in needed_pkgs]
    edges = {(a, b) for a, b in edges if a in keys and b in keys}

    adj = collections.defaultdict(set)
    for a, b in edges:
        adj[a].add(b)
    comps = tarjan_scc(keys, adj)
    comp_of = {g: i for i, comp in enumerate(comps) for g in comp}

    counts = collections.Counter(grp[k] for k in models)
    test_counts: collections.Counter = collections.Counter()
    path_to_group = {}
    for g in keys:
        if g[1] != "package":
            path_to_group[f"models/{g[0]}/{g[1]}/"] = g
    for t in tests:
        p = t["original_file_path"].replace("\\", "/")
        for prefix, g in path_to_group.items():
            if p.startswith(prefix):
                test_counts[g] += 1
                break

    out_nodes = []
    node_name: dict[int, str] = {}
    for i, comp in enumerate(comps):
        domains = sorted({g[0] for g in comp})
        layers = [g[1] for g in comp]
        order = {"bronze": 0, "silver": 1, "gold": 2}
        comp_sorted = sorted(comp, key=lambda g: (order.get(g[1], 9), g[1]))
        if len(domains) == 1:
            domain = domains[0]
            nid = f"{domain}__" + "_".join(g[1] for g in comp_sorted)
            task = "_".join(g[1] for g in comp_sorted)
        else:
            domain = "merged"
            nid = "merged__" + "__".join(gid(g) for g in comp_sorted)
            task = "__".join(gid(g) for g in comp_sorted)
        node_name[i] = nid
        out_nodes.append({
            "id": nid,
            "group": domain,
            "task": task,
            "members": [gid(g) for g in comp_sorted],
            "selector": " ".join(g[2] for g in comp_sorted),
            "models": sum(counts[g] for g in comp),
            "tests": sum(test_counts[g] for g in comp),
            "sources": sorted(set().union(*[src_by_group[g] for g in comp])),
            "merged_cycle": len(comp) > 1,
        })

    node_edges = {(node_name[comp_of[a]], node_name[comp_of[b]]) for a, b in edges if comp_of[a] != comp_of[b]}
    ids = [n["id"] for n in out_nodes]
    reduced = transitive_reduction(ids, node_edges)
    C.assert_acyclic(ids, reduced)

    # sources
    cat_info = source_catalog_info(project_dir)
    nodes_by_source: dict[str, set[str]] = collections.defaultdict(set)
    for g, names in src_by_group.items():
        for s in names:
            nodes_by_source[s].add(node_name[comp_of[g]] if g in comp_of else gid(g))
    src_tables: dict[str, dict] = collections.defaultdict(lambda: {"schemas": set(), "tables": set()})
    for s in m["sources"].values():
        rec = src_tables[s["source_name"]]
        rec["schemas"].add(s["schema"])
        rec["tables"].add(s.get("identifier") or s["name"])
    sources = {}
    for name, rec in sorted(src_tables.items()):
        sources[name] = {
            "schemas": sorted(rec["schemas"]),
            "tables": sorted(rec["tables"]),
            "catalog_env": cat_info.get(name, {}).get("catalog_env", []),
            "nodes": sorted(nodes_by_source.get(name, [])),
        }

    # extractor configs -> dbt sources (match on landing schema + the LZ catalog)
    extractors = discover_extractors(config_dir) if config_dir.is_dir() else []
    for ex in extractors:
        matched = [
            n for n, s in sources.items()
            if any(sc.upper() == ex["landing_schema"] for sc in s["schemas"])
            and (not s["catalog_env"] or "DATABRICKS_LZ_CATALOG" in s["catalog_env"])
        ]
        ex["dbt_sources"] = sorted(matched)
        ex["dbt_select"] = " ".join(f"source:{n}+" for n in sorted(matched))
        dbt_tables = {t.lower() for n in matched for t in sources[n]["tables"]}
        ex["landing_tables_known_to_dbt"] = sum(1 for d in ex["datasets"] if d["landing_table"].lower() in dbt_tables)

    return {
        "schema_version": 1,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "dbt": {"project_name": root_pkg, "dbt_version": m["metadata"].get("dbt_version")},
        "dependency_fingerprint": C.dependency_fingerprint(str(project_dir)),
        "stats": {
            "models_in_graph": sum(n["models"] for n in out_nodes),
            "tests_in_graph": sum(n["tests"] for n in out_nodes),
            "tasks": len(out_nodes),
            "edges": len(reduced),
            "edges_before_reduction": len(node_edges),
            "merged_cycle_nodes": [n["id"] for n in out_nodes if n["merged_cycle"]],
            "disabled_models": sum(1 for v in m["disabled"].values() for x in v if x["resource_type"] == "model"),
        },
        "nodes": sorted(out_nodes, key=lambda n: (n["group"], n["task"])),
        "edges": sorted([list(e) for e in reduced]),
        "sources": sources,
        "extractors": extractors,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--project-dir", default=C.DBT_PROJECT_DIR)
    ap.add_argument("--manifest", help="existing manifest.json (default: <project>/target/manifest.json)")
    ap.add_argument("--parse", action="store_true", help="run `dbt deps` + `dbt parse` first")
    ap.add_argument("--profiles-dir", default=None, help="default: <project>/DEV_PROFILES")
    ap.add_argument("--config-dir", default=C.CONFIG_DIR)
    ap.add_argument("--out", default=C.GRAPH_PATH)
    a = ap.parse_args()

    project = Path(a.project_dir).resolve()
    manifest = Path(a.manifest) if a.manifest else project / "target" / "manifest.json"
    if a.parse:
        manifest = run_parse(project, a.profiles_dir or str(project / "DEV_PROFILES"))
    if not manifest.is_file():
        sys.exit(f"manifest not found: {manifest} (use --parse or --manifest)")

    graph = build(manifest, project, Path(a.config_dir))
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    Path(a.out).write_text(json.dumps(graph, indent=1, sort_keys=False) + "\n", encoding="utf-8")
    s = graph["stats"]
    print(f"wrote {a.out}: {s['tasks']} tasks, {s['edges']} edges "
          f"({s['edges_before_reduction']} before reduction), {s['models_in_graph']} models, {s['tests_in_graph']} tests")
    if s["merged_cycle_nodes"]:
        print("merged to break group-level cycles:", ", ".join(s["merged_cycle_nodes"]))


if __name__ == "__main__":
    main()
