"""Day 6: loads tests/fixtures/sample_products.csv into raw.products, used by
CI so `dbt build` has data to run against without hitting Hugging Face.

Reuses warehouse/init/01_schemas.sql and 02_products_table.sql verbatim (CI's
Postgres service container starts with neither - unlike local dev, where
docker-entrypoint-initdb.d already ran them once) rather than duplicating
the DDL here.

Reads WAREHOUSE_* env vars, same defaults as every other script (localhost:5433
for a laptop run; CI sets these to its postgres service instead).
"""
import csv
import logging
import os
from datetime import date, datetime, timezone
from pathlib import Path

import psycopg

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
FIXTURE_CSV_PATH = REPO_ROOT / "tests" / "fixtures" / "sample_products.csv"
SCHEMAS_DDL_PATH = REPO_ROOT / "warehouse" / "init" / "01_schemas.sql"
PRODUCTS_DDL_PATH = REPO_ROOT / "warehouse" / "init" / "02_products_table.sql"

# The fixture is always loaded under this fixed load_date, so dbt build
# --vars '{"load_date": ...}' has a stable value to target in CI.
FIXTURE_LOAD_DATE = date(2024, 1, 1)

# raw.products' real columns (warehouse/init/02_products_table.sql), minus
# load_date/loaded_at - the loader stamps both itself, same split as
# extract.py's own cached parquet.
RAW_COLUMNS = [
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
]
INT_COLUMNS = {"last_modified_t", "nova_group", "created_t", "unique_scans_n"}
FLOAT_COLUMNS = {"completeness"}


def parse_value(column: str, raw_value: str):
    # JSON columns are kept as raw JSON text, not parsed into Python objects -
    # psycopg's text-format COPY writes a jsonb column's text representation
    # verbatim (Postgres itself parses it on the way in), the same way
    # extract.py's cached parquet already stores these columns as JSON text.
    if raw_value == "":
        return None
    if column in INT_COLUMNS:
        return int(raw_value)
    if column in FLOAT_COLUMNS:
        return float(raw_value)
    return raw_value


def read_fixture_rows() -> list[tuple]:
    with open(FIXTURE_CSV_PATH, newline="") as f:
        reader = csv.DictReader(f)
        return [tuple(parse_value(col, row[col]) for col in RAW_COLUMNS) for row in reader]


def connect() -> psycopg.Connection:
    return psycopg.connect(
        host=os.environ.get("WAREHOUSE_HOST", "localhost"),
        port=os.environ.get("WAREHOUSE_PORT", "5433"),
        user=os.environ.get("WAREHOUSE_USER", "postgres"),
        password=os.environ.get("WAREHOUSE_PASSWORD", "postgres"),
        dbname=os.environ.get("WAREHOUSE_DB", "warehouse"),
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    rows = read_fixture_rows()
    schemas_sql = SCHEMAS_DDL_PATH.read_text()
    products_sql = PRODUCTS_DDL_PATH.read_text()
    copy_sql = f"COPY raw.products ({', '.join(RAW_COLUMNS)}, load_date, loaded_at) FROM STDIN"

    with connect() as conn, conn.transaction():
        with conn.cursor() as cur:
            cur.execute(schemas_sql)
            cur.execute(products_sql)
            cur.execute("DELETE FROM raw.products WHERE load_date = %s", [FIXTURE_LOAD_DATE])
            loaded_at = datetime.now(timezone.utc)
            with cur.copy(copy_sql) as copy:
                for row in rows:
                    copy.write_row((*row, FIXTURE_LOAD_DATE, loaded_at))

    logger.info("load_sample: loaded %d fixture rows into raw.products (load_date=%s)", len(rows), FIXTURE_LOAD_DATE)


if __name__ == "__main__":
    main()
