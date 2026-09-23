# Day 4: Great Expectations (GX Core 1.x) suite + checkpoint that validates
# raw.products for the latest load_date (row count, barcode format, completeness,
# freshness), wired into the DAG's validate_raw task.
#
# Note: GX prints one line per UnexpectedRowsExpectation that doesn't use a
# {batch} placeholder (deliberate here - the freshness/drop-vs-previous checks
# intentionally query outside the current batch), plus a tqdm progress bar
# during validation. Both are emitted via plain print()/tqdm, not logging or
# warnings, so neither is filterable from here - cosmetic noise around the
# real summary below, not a functional issue.
import argparse
import logging
import os
import sys
from datetime import date
from pathlib import Path

import great_expectations as gx
import great_expectations.expectations as gxe

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[2]
# Written under include/gx/ (bind-mounted from the host in the Airflow
# containers, same as the rest of include/) rather than a container-local
# path like extract.py's cache - unlike that cache, Data Docs are small
# static files meant to be opened in a browser, so they need to land
# somewhere visible from the laptop regardless of which context ran this.
DATA_DOCS_DIR = Path(os.environ.get("GX_DATA_DOCS_DIR") or (REPO_ROOT / "include" / "gx" / "data_docs"))

# raw.products' real columns (warehouse/init/02_products_table.sql) - checked
# with exact_match=False, so this only fails if an expected column goes
# missing, not if raw gains new ones we haven't started validating yet.
EXPECTED_COLUMNS = [
    "barcode",
    "product_name",
    "brands",
    "categories_tags",
    "nutriments",
    "quantity",
    "nutriscore_grade",
    "last_modified_t",
    "countries_tags",
    "product_quantity",
    "product_quantity_unit",
    "brands_tags",
    "nova_group",
    "environmental_score_grade",
    "completeness",
    "created_t",
    "labels_tags",
    "data_quality_warnings_tags",
    "unique_scans_n",
    "nutrition_data_per",
    "load_date",
    "loaded_at",
]


def build_connection_string() -> str:
    host = os.environ.get("WAREHOUSE_HOST", "localhost")
    port = os.environ.get("WAREHOUSE_PORT", "5433")
    user = os.environ.get("WAREHOUSE_USER", "postgres")
    password = os.environ.get("WAREHOUSE_PASSWORD", "postgres")
    dbname = os.environ.get("WAREHOUSE_DB", "warehouse")
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{dbname}"


def build_completeness_expectation(
    incomplete_condition: str, mostly: float, description: str, load_date_str: str
) -> gxe.UnexpectedRowsExpectation:
    """
    UnexpectedRowsExpectation is a plain pass/fail on "did this query return
    rows" - it has no built-in `mostly`, so the tolerance threshold is baked
    into the query itself. Also lets `incomplete_condition` treat an empty
    JSON array / empty string as incomplete, not just SQL NULL - confirmed
    during planning that plain NULL checks miss most of the real gaps here
    (e.g. product_name has 0 true NULLs but 1,748 empty-array rows).
    """
    sql = f"""
        with counts as (
            select
                count(*) as total,
                count(*) filter (where {incomplete_condition}) as incomplete
            from raw.products
            where load_date = '{load_date_str}'
        )
        select * from counts
        where incomplete > total * {1 - mostly}
    """
    return gxe.UnexpectedRowsExpectation(description=description, unexpected_rows_query=sql)


def build_suite(load_date_str: str) -> gx.ExpectationSuite:
    # Overridable for the failure-test demo only - real runs leave this unset.
    min_row_count = int(os.environ.get("GX_MIN_ROW_COUNT_OVERRIDE", "50000"))

    suite = gx.ExpectationSuite(name="raw_products_suite")

    # Roughly half / double every row count we've seen (109,767 on both
    # loads to date) - catches a catastrophic partial load or a runaway
    # duplicate load, not day-to-day variation.
    suite.add_expectation(
        gxe.ExpectTableRowCountToBeBetween(
            min_value=min_row_count, max_value=200000, description="row count within expected range"
        )
    )

    drop_check_sql = f"""
        with current_count as (
            select count(*) as n from raw.products where load_date = '{load_date_str}'
        ),
        previous_load as (
            select max(load_date) as load_date from raw.products where load_date < '{load_date_str}'
        ),
        previous_count as (
            select count(*) as n from raw.products, previous_load
            where raw.products.load_date = previous_load.load_date
        )
        select current_count.n as current_n, previous_count.n as previous_n
        from current_count, previous_count
        where previous_count.n > 0
          and current_count.n < previous_count.n * 0.7
    """
    suite.add_expectation(
        gxe.UnexpectedRowsExpectation(
            description="row count did not drop more than 30% vs the previous load_date",
            unexpected_rows_query=drop_check_sql,
        )
    )

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="barcode", description="barcode is never null"))
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeUnique(column="barcode", description="barcode is unique within this load")
    )
    suite.add_expectation(
        gxe.ExpectColumnValuesToMatchRegex(
            column="barcode",
            regex=r"^[0-9]{8,14}$",
            mostly=0.99,
            description="barcode matches ^[0-9]{8,14}$ (measured pass rate: 99.685%)",
        )
    )

    # Completeness thresholds set a few points below today's measured rate
    # (see Day 4B planning), for margin against normal day-to-day noise:
    #   product_name 98.41%, brands 84.18%, categories_tags 39.18%, nutriments 61.47%
    suite.add_expectation(
        build_completeness_expectation(
            "product_name is null or product_name = '[]'::jsonb",
            0.95,
            "product_name at least 95% complete",
            load_date_str,
        )
    )
    suite.add_expectation(
        build_completeness_expectation(
            "brands is null or trim(brands) = ''",
            0.80,
            "brands at least 80% complete",
            load_date_str,
        )
    )
    suite.add_expectation(
        build_completeness_expectation(
            "categories_tags is null or categories_tags = '[]'::jsonb",
            0.35,
            "categories_tags at least 35% complete",
            load_date_str,
        )
    )
    suite.add_expectation(
        build_completeness_expectation(
            "nutriments is null or nutriments = '[]'::jsonb",
            0.55,
            "nutriments at least 55% complete",
            load_date_str,
        )
    )
    # No empty-string variant exists for this column (confirmed during
    # planning) - a plain not-null mostly-check is accurate here, unlike
    # the four above.
    suite.add_expectation(
        gxe.ExpectColumnValuesToNotBeNull(
            column="nutriscore_grade", mostly=0.995, description="nutriscore_grade at least 99.5% complete"
        )
    )

    freshness_sql = f"""
        select max(loaded_at) as latest
        from raw.products
        where load_date = '{load_date_str}'
        having max(loaded_at) < now() - interval '26 hours'
    """
    suite.add_expectation(
        gxe.UnexpectedRowsExpectation(
            description="loaded_at is within the last 26 hours", unexpected_rows_query=freshness_sql
        )
    )

    suite.add_expectation(
        gxe.ExpectTableColumnsToMatchSet(
            column_set=EXPECTED_COLUMNS, exact_match=False, description="expected columns exist"
        )
    )

    return suite


def print_summary(result) -> None:
    logger.info("=" * 70)
    for validation_result in result.run_results.values():
        for r in validation_result.results:
            observed = r.result.get("observed_value")
            if observed is None and "unexpected_count" in r.result:
                pct = r.result.get("unexpected_percent", 0)
                observed = f"{r.result['unexpected_count']} unexpected ({pct:.3f}%)"
            status = "PASS" if r.success else "FAIL"
            description = r.expectation_config.description or r.expectation_config.type
            logger.info("[%s] %s -> observed: %s", status, description, observed)
    logger.info("=" * 70)
    logger.info("Overall: %s", "PASSED" if result.success else "FAILED")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # GX's own loggers are extremely verbose at INFO (repeats one warning
    # about unexpected_rows_query hundreds of times) - this is expected,
    # since our completeness/freshness/drop checks deliberately query outside
    # the batch, not a real problem to fix. Keep our own summary readable.
    logging.getLogger("great_expectations").setLevel(logging.WARNING)

    parser = argparse.ArgumentParser(description="Validate raw.products for a load_date with Great Expectations.")
    parser.add_argument("--load-date", default=date.today().isoformat(), type=date.fromisoformat)
    args = parser.parse_args()
    load_date_str = args.load_date.isoformat()

    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_postgres("warehouse", connection_string=build_connection_string())
    data_asset = data_source.add_query_asset(
        name="raw_products",
        query=f"SELECT * FROM raw.products WHERE load_date = '{load_date_str}'",
    )
    batch_definition = data_asset.add_batch_definition(name="raw_products_batch")

    suite = context.suites.add(build_suite(load_date_str))

    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(name="raw_products_validation", data=batch_definition, suite=suite)
    )

    checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name="raw_products_checkpoint",
            validation_definitions=[validation_definition],
            actions=[gx.checkpoint.UpdateDataDocsAction(name="update_data_docs")],
        )
    )

    # An ephemeral context already ships a default "local_site" (pointed at a
    # random temp dir) - redirect it rather than adding a second one
    # (add_data_docs_site errors if the name already exists).
    DATA_DOCS_DIR.mkdir(parents=True, exist_ok=True)
    context.update_data_docs_site(
        site_name="local_site",
        site_config={
            "class_name": "SiteBuilder",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": str(DATA_DOCS_DIR),
            },
            "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
        },
    )

    result = checkpoint.run()
    context.build_data_docs()

    logger.info("Validated raw.products for load_date=%s", load_date_str)
    print_summary(result)
    logger.info("Data Docs written to %s", DATA_DOCS_DIR / "index.html")

    if not result.success:
        sys.exit(1)


if __name__ == "__main__":
    main()
