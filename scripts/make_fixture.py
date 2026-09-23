"""Day 6: builds tests/fixtures/sample_products.csv - a small, deterministic
fixture CI loads instead of the real ~7.3GB Open Food Facts source.

Two parts, not one:
  1. ~170 REAL rows pulled from the live warehouse, ~13 per category_l1 (the
     already-built marts.dim_product/dim_category tell us which barcodes
     land in which category), for realistic diversity "for free".
  2. ~25 hand-written SYNTHETIC rows appended, so every edge case this
     fixture is meant to exercise is guaranteed present regardless of what
     today's live OFF snapshot happens to contain.

Run once, by hand, against the real warehouse (not part of CI):
    .venv-dbt/bin/python scripts/make_fixture.py   # or any venv with psycopg
(WAREHOUSE_HOST/PORT default to localhost:5433, same as every other script
run directly from the laptop.)
"""
import csv
import json
import os
import sys
from pathlib import Path

import psycopg

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_PATH = REPO_ROOT / "tests" / "fixtures" / "sample_products.csv"
ROWS_PER_CATEGORY = 13

sys.path.insert(0, str(REPO_ROOT / "include" / "extract"))
from extract import RAW_COLUMNS  # noqa: E402  (path inserted above - reuses the real column list, doesn't redeclare it)

# Which of RAW_COLUMNS are jsonb in raw.products (warehouse/init/02_products_table.sql) -
# needed to serialize Python values back to JSON text for the CSV.
JSON_COLUMNS = {
    "product_name",
    "categories_tags",
    "nutriments",
    "countries_tags",
    "brands_tags",
    "labels_tags",
    "data_quality_warnings_tags",
}


def connect() -> psycopg.Connection:
    return psycopg.connect(
        host=os.environ.get("WAREHOUSE_HOST", "localhost"),
        port=os.environ.get("WAREHOUSE_PORT", "5433"),
        user=os.environ.get("WAREHOUSE_USER", "postgres"),
        password=os.environ.get("WAREHOUSE_PASSWORD", "postgres"),
        dbname=os.environ.get("WAREHOUSE_DB", "warehouse"),
    )


def fetch_real_rows(conn: psycopg.Connection) -> list[tuple]:
    """~13 real barcodes per category_l1 (deterministic: lowest barcode
    first), then those barcodes' real raw.products rows for the current
    (max) load_date."""
    cols = ", ".join(RAW_COLUMNS[:-1])  # everything except load_date - not needed, we pin our own fixture date
    query = f"""
        with current_load as (
            select max(load_date) as load_date from raw.products
        ),
        ranked as (
            select
                dp.product_key as barcode,
                row_number() over (partition by dc.category_l1 order by dp.product_key) as rn
            from marts.dim_product dp
            join marts.dim_category dc on dc.category_key = dp.category_key
        ),
        picked as (
            select barcode from ranked where rn <= %(n)s
        )
        select {cols}
        from raw.products r, current_load
        where r.load_date = current_load.load_date
          and r.barcode in (select barcode from picked)
        order by r.barcode
    """
    with conn.cursor() as cur:
        cur.execute(query, {"n": ROWS_PER_CATEGORY})
        return cur.fetchall()


def synthetic_rows() -> list[dict]:
    """Hand-written edge cases - see the module docstring. Every column not
    set explicitly defaults to a plausible, boring value via _row()."""

    def _row(**overrides) -> dict:
        base = {
            "barcode": "9999900001",
            "product_name": [{"lang": "nl", "text": "Testproduct"}],
            "brands": "Testmerk",
            "categories_tags": ["en:snacks"],
            "nutriments": [
                {"name": "energy-kcal", "100g": 250, "unit": "kcal"},
                {"name": "fat", "100g": 10, "unit": "g"},
                {"name": "carbohydrates", "100g": 40, "unit": "g"},
                {"name": "proteins", "100g": 5, "unit": "g"},
                {"name": "fiber", "100g": 2, "unit": "g"},
                {"name": "salt", "100g": 1.0, "unit": "g"},
                {"name": "sodium", "100g": 0.4, "unit": "g"},
            ],
            "quantity": "100 g",
            "nutriscore_grade": "c",
            "last_modified_t": 1700000000,
            "countries_tags": ["en:netherlands"],
            "product_quantity": "100",
            "product_quantity_unit": "g",
            "brands_tags": ["testmerk"],
            "nova_group": 3,
            "environmental_score_grade": "c",
            "completeness": 0.8,
            "created_t": 1690000000,
            "labels_tags": [],
            "data_quality_warnings_tags": [],
            "unique_scans_n": 10,
            "nutrition_data_per": "100g",
        }
        base.update(overrides)
        return base

    rows = []

    # Duplicate barcodes with different last_modified_t - exercises
    # stg_products' row_number() ... order by last_modified_t desc dedup.
    for i, (barcode, older, newer) in enumerate(
        [("9999910001", 1650000000, 1750000000), ("9999910002", 1600000000, 1720000000)]
    ):
        rows.append(_row(barcode=barcode, last_modified_t=older, brands=f"OldSpelling{i}"))
        rows.append(_row(barcode=barcode, last_modified_t=newer, brands=f"NewSpelling{i}"))

    # 'unknown' / 'not-applicable' Nutri-Score - stg_products.sql already
    # maps anything outside a-e to null; confirms that mapping, not a break.
    rows.append(_row(barcode="9999920001", nutriscore_grade="unknown"))
    rows.append(_row(barcode="9999920002", nutriscore_grade="not-applicable"))

    # No brand at all - exercises dim_brand's 'unknown' fallback row.
    rows.append(_row(barcode="9999930001", brands=None, brands_tags=[]))
    rows.append(_row(barcode="9999930002", brands="", brands_tags=[]))

    # No categories at all - exercises int_products_categorised's
    # 'no_categories' mapping_status.
    rows.append(_row(barcode="9999940001", categories_tags=[]))
    rows.append(_row(barcode="9999940002", categories_tags=[]))

    # Per-100ml beverages - exercises dim_product's nutrition_basis heuristic.
    rows.append(
        _row(
            barcode="9999950001",
            categories_tags=["en:water"],
            nutrition_data_per="100ml",
            product_quantity_unit="ml",
            quantity="500 ml",
        )
    )
    rows.append(
        _row(
            barcode="9999950002",
            categories_tags=["en:soda-carbonated"],
            nutrition_data_per=None,
            product_quantity_unit="cl",
            quantity="33 cl",
        )
    )

    # Leading-zero barcodes - confirms barcode stays text, never becomes a number.
    rows.append(_row(barcode="00012345678905"))
    rows.append(_row(barcode="00098765432108"))

    # Plausibility WARNING rows - one row each, every threshold is in the
    # hundreds so a single row can never reach an error, only a warning.
    rows.append(_row(barcode="99999-6000-1"))  # fails ^[0-9]{8,14}$
    rows.append(
        _row(
            barcode="9999960002",
            nutriments=[
                {"name": "fat", "100g": 40, "unit": "g"},
                {"name": "carbohydrates", "100g": 40, "unit": "g"},
                {"name": "proteins", "100g": 20, "unit": "g"},
                {"name": "fiber", "100g": 5, "unit": "g"},
                {"name": "salt", "100g": 3, "unit": "g"},
            ],
        )
    )  # macronutrient sum 108 > 105
    rows.append(
        _row(barcode="9999960003", nutriments=[{"name": "proteins", "100g": 120, "unit": "g"}])
    )  # mass nutrient > 100 in [0,100]
    rows.append(
        _row(barcode="9999960004", nutriments=[{"name": "energy-kcal", "100g": 950, "unit": "kcal"}])
    )  # energy-kcal > 900 in [0,900]
    rows.append(
        _row(
            barcode="9999960005",
            nutriments=[
                {"name": "salt", "100g": 5.0, "unit": "g"},
                {"name": "sodium", "100g": 0.5, "unit": "g"},
            ],
        )
    )  # salt/sodium ratio well outside 15%-or-0.02g tolerance

    return rows


def synthetic_to_tuples(rows: list[dict]) -> list[tuple]:
    return [tuple(row[col] for col in RAW_COLUMNS[:-1]) for row in rows]


def write_csv(rows: list[tuple]) -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(RAW_COLUMNS[:-1])  # header, everything except load_date
        for row in rows:
            serialized = []
            for col, value in zip(RAW_COLUMNS[:-1], row):
                if value is None:
                    serialized.append("")
                elif col in JSON_COLUMNS:
                    serialized.append(json.dumps(value))
                else:
                    serialized.append(value)
            writer.writerow(serialized)


def main() -> None:
    with connect() as conn:
        real_rows = fetch_real_rows(conn)
    print(f"make_fixture: {len(real_rows)} real rows from the live warehouse")

    synth_rows = synthetic_to_tuples(synthetic_rows())
    print(f"make_fixture: {len(synth_rows)} synthetic edge-case rows")

    write_csv(real_rows + synth_rows)
    size_kb = OUTPUT_PATH.stat().st_size / 1024
    print(f"make_fixture: wrote {len(real_rows) + len(synth_rows)} rows to {OUTPUT_PATH} ({size_kb:.1f} KB)")


if __name__ == "__main__":
    main()
