"""Generates the check-inventory table for DATA_QUALITY.md from the real dbt
manifest and the real GX suite - not a hand-maintained list that can drift.

Run with the GX venv (needs great_expectations importable to introspect
validate_raw.py's suite; manifest.json itself is read as plain JSON, no dbt
install required):

    .venv-gx/bin/python scripts/generate_dq_inventory.py

Requires include/dbt/target/manifest.json to exist (run `dbt parse` or any
other dbt command first if it's missing or stale).
"""
import contextlib
import io
import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MANIFEST_PATH = REPO_ROOT / "include" / "dbt" / "target" / "manifest.json"

sys.path.insert(0, str(REPO_ROOT / "include" / "gx"))

# --- Quality-dimension classification -----------------------------------
# Neither dbt nor GX records which quality dimension a check belongs to -
# this is the one piece of judgment this script adds on top of the real
# test/expectation definitions below.

DBT_TEST_DIMENSION = {
    "unique": "Uniqueness",
    "unique_combination_of_columns": "Uniqueness",
    "not_null": "Completeness",
    "accepted_values": "Validity",
    "relationships": "Consistency",
    "expression_is_true": "Validity",
    "test_value_between": "Validity",
    "assert_salt_sodium_consistent": "Consistency",
    "assert_macronutrient_sum_in_range": "Consistency",
}

GX_TYPE_DIMENSION = {
    "ExpectColumnValuesToNotBeNull": "Completeness",
    "ExpectColumnValuesToBeUnique": "Uniqueness",
    "ExpectColumnValuesToMatchRegex": "Validity",
    "ExpectTableRowCountToBeBetween": "Completeness",
    "ExpectTableColumnsToMatchSet": "Validity",
}

# UnexpectedRowsExpectation shares one class name for 6 different checks in
# validate_raw.py - disambiguated by the fixed description text written there.
GX_UNEXPECTED_ROWS_DIMENSION = {
    "row count did not drop more than 30% vs the previous load_date": "Consistency",
    "product_name at least 95% complete": "Completeness",
    "brands at least 80% complete": "Completeness",
    "categories_tags at least 35% complete": "Completeness",
    "nutriments at least 55% complete": "Completeness",
    "loaded_at is within the last 26 hours": "Timeliness",
}


def load_manifest() -> dict:
    if not MANIFEST_PATH.exists():
        raise SystemExit(f"{MANIFEST_PATH} not found - run a dbt command (e.g. `dbt parse`) first.")
    return json.loads(MANIFEST_PATH.read_text())


def node_layer(manifest: dict, node_id: str) -> str:
    if node_id.startswith("seed."):
        return "seed"
    node = manifest["nodes"].get(node_id, {})
    return node.get("schema", "unknown")


def tested_model_name(kwargs: dict, dep_nodes: list) -> str:
    """The model actually under test - from test_metadata.kwargs.model
    (get_where_subquery(ref('X'))), which is unambiguous even for
    relationships tests where depends_on lists two models."""
    match = re.search(r"ref\('([^']+)'\)", kwargs.get("model", ""))
    if match:
        return match.group(1)
    return dep_nodes[0].split(".")[-1] if dep_nodes else "?"


def dbt_rows(manifest: dict) -> list:
    rows = []
    for node in manifest["nodes"].values():
        if node.get("resource_type") != "test":
            continue
        tm = node.get("test_metadata")
        dep = node.get("depends_on", {}).get("nodes", [])
        column = node.get("column_name")
        config = node["config"]
        severity = config["severity"]
        warn_if = config["warn_if"]
        error_if = config["error_if"]

        if tm:
            test_type = tm["name"]
            namespace = tm.get("namespace") or "dbt"
            kwargs = tm.get("kwargs", {})
            model = tested_model_name(kwargs, dep)
            model_node_id = next((d for d in dep if d.endswith(f".{model}")), dep[0] if dep else "")
            layer = node_layer(manifest, model_node_id)

            if test_type == "unique":
                check = f"unique({column}) on {model}"
            elif test_type == "not_null":
                check = f"not_null({column}) on {model}"
            elif test_type == "accepted_values":
                check = f"accepted_values({column} in {kwargs.get('values', [])}) on {model}"
            elif test_type == "relationships":
                to_match = re.search(r"ref\('([^']+)'\)", kwargs.get("to", ""))
                to_model = to_match.group(1) if to_match else "?"
                check = f"relationships({column} -> {to_model}.{kwargs.get('field')}) on {model}"
            elif test_type == "expression_is_true":
                check = f"expression_is_true({column} {kwargs.get('expression')}) on {model}"
            elif test_type == "unique_combination_of_columns":
                cols = ", ".join(kwargs.get("combination_of_columns", []))
                check = f"unique_combination_of_columns({cols}) on {model}"
            elif test_type == "test_value_between":
                where = config.get("where") or ""
                where_txt = f", where {where}" if where else ""
                check = (
                    f"test_value_between({column} in [{kwargs.get('min_value')}, "
                    f"{kwargs.get('max_value')}]{where_txt}) on {model}"
                )
            else:
                check = f"{test_type}({column}) on {model}" if column else f"{test_type} on {model}"

            dimension = DBT_TEST_DIMENSION.get(test_type, "Unclassified")
            tool = f"dbt test ({namespace})" if namespace != "dbt" else "dbt test (generic)"
        else:
            test_type = node["name"]
            check = config.get("description") or test_type
            layer = node_layer(manifest, dep[0]) if dep else "unknown"
            dimension = DBT_TEST_DIMENSION.get(test_type, "Unclassified")
            tool = "dbt test (singular)"

        if severity.upper() == "ERROR":
            sev_desc = "Error (hard fail, any failing row)"
            on_failure = "`dbt build` exits non-zero -> Airflow `dbt_build` task fails"
        else:
            sev_desc = f"Warn if {warn_if}, error if {error_if}"
            on_failure = (
                f"WARN below the error threshold ({error_if}) - build continues; ERROR above it - "
                "`dbt_build` fails. Failing rows always stored in `audit`."
            )

        rows.append(
            {
                "check": check,
                "layer": layer,
                "tool": tool,
                "dimension": dimension,
                "severity": sev_desc,
                "on_failure": on_failure,
            }
        )
    return rows


def source_freshness_row(manifest: dict) -> dict:
    src = manifest["sources"]["source.food_warehouse.raw.products"]
    fr = src["freshness"]
    warn, err = fr["warn_after"], fr["error_after"]
    return {
        "check": f"source freshness on raw.products.{src['loaded_at_field']}",
        "layer": "raw",
        "tool": "dbt source freshness",
        "dimension": "Timeliness",
        "severity": f"Warn after {warn['count']} {warn['period']}s, error after {err['count']} {err['period']}s",
        "on_failure": (
            "Reported by `dbt source freshness` (run before `dbt build` in the DAG); a stale/error "
            "result signals raw.products hasn't been reloaded recently."
        ),
    }


def gx_rows() -> list:
    import great_expectations as gx
    import validate_raw  # noqa: E402  (path inserted above; only constructs objects, no DB call)

    # ExpectationSuite.add_expectation() needs an active GX context registered
    # first (a global singleton) - validate_raw.py's own main() always does
    # this before calling build_suite(); this script needs the same, even
    # though it never actually connects to a datasource.
    gx.get_context(mode="ephemeral")

    # GX prints (plain print(), not logging/warnings - confirmed unfilterable
    # during Day 4B) one noisy line per UnexpectedRowsExpectation built
    # without a {batch} placeholder. Harmless, but would otherwise leak into
    # this script's stdout table output.
    with contextlib.redirect_stdout(io.StringIO()):
        suite = validate_raw.build_suite("2026-01-01")  # placeholder date, only used to render SQL text
    rows = []
    for exp in suite.expectations:
        type_name = type(exp).__name__
        kwargs = exp.configuration.kwargs
        description = exp.description or type_name

        if type_name == "UnexpectedRowsExpectation":
            dimension = GX_UNEXPECTED_ROWS_DIMENSION.get(description, "Unclassified")
        else:
            dimension = GX_TYPE_DIMENSION.get(type_name, "Unclassified")

        if "mostly" in kwargs:
            threshold = f"mostly >= {kwargs['mostly']}"
        elif type_name == "ExpectTableRowCountToBeBetween":
            threshold = f"{kwargs['min_value']} <= row_count <= {kwargs['max_value']}"
        elif type_name == "ExpectTableColumnsToMatchSet":
            threshold = f"all {len(kwargs.get('column_set', []))} expected columns must be present"
        else:
            threshold = "no mostly set - any failing row fails the check (100% required)"

        rows.append(
            {
                "check": description,
                "layer": "raw",
                "tool": "Great Expectations",
                "dimension": dimension,
                "severity": f"Hard fail ({threshold})",
                "on_failure": "`validate_raw` task fails (retries=0) -> `dbt_build` shows upstream_failed, never runs",
            }
        )
    return rows


def render_markdown_table(rows: list) -> str:
    lines = [
        "| Check | Layer | Tool | Dimension | Severity / threshold | On failure |",
        "|---|---|---|---|---|---|",
    ]
    for r in rows:
        lines.append(
            f"| {r['check']} | {r['layer']} | {r['tool']} | {r['dimension']} | {r['severity']} | {r['on_failure']} |"
        )
    return "\n".join(lines)


def main() -> None:
    manifest = load_manifest()
    rows = dbt_rows(manifest)
    rows.append(source_freshness_row(manifest))
    rows.extend(gx_rows())
    print(
        f"<!-- {len(rows)} checks total, generated from "
        "include/dbt/target/manifest.json + include/gx/validate_raw.py -->"
    )
    print(render_markdown_table(rows))


if __name__ == "__main__":
    main()
