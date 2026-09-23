# Day 1: extracts Dutch food products from Open Food Facts into raw.products.
import argparse
import logging
import os
import time
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import psycopg
import requests

logger = logging.getLogger(__name__)

# DuckDB's parquet reader issues many small ranged HTTP requests to scan a
# remote file's row groups (one signed-redirect resolve per range read on
# this dataset's storage backend), which blows through Hugging Face's rate
# limit for a file this size (~7.9GB) even when authenticated. So we download
# the whole source file once with a single streamed request, and only ever
# point DuckDB at the local copy.
HF_RESOLVE_URL = "https://huggingface.co/datasets/openfoodfacts/product-database/resolve/main/food.parquet"
REPO_ROOT = Path(__file__).resolve().parents[2]
# Locally this defaults to data/raw (repo-relative); inside the Airflow
# containers, EXTRACT_CACHE_DIR points at a container-local path instead (see
# .env.example) so the cache never touches the host-bind-mounted folders.
CACHE_DIR = Path(os.environ.get("EXTRACT_CACHE_DIR") or (REPO_ROOT / "data" / "raw"))
SOURCE_PATH = CACHE_DIR / "_source" / "food.parquet"
DDL_PATH = REPO_ROOT / "warehouse" / "init" / "02_products_table.sql"
DUCKDB_MEMORY_LIMIT = os.environ.get("DUCKDB_MEMORY_LIMIT", "2GB")

# Matches raw.products' column order (warehouse/init/02_products_table.sql),
# minus loaded_at, which the load stage stamps at insert time instead of caching it.
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
    "load_date",
]


def cache_path_for(load_date: date) -> Path:
    return CACHE_DIR / f"products_nl_{load_date.isoformat()}.parquet"


def download_source_file(refresh: bool) -> Path:
    if SOURCE_PATH.exists() and not refresh:
        logger.info("extract: reusing downloaded source file %s", SOURCE_PATH)
        return SOURCE_PATH

    SOURCE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = SOURCE_PATH.with_suffix(".parquet.tmp")
    headers = {}
    hf_token = os.environ.get("HF_TOKEN")
    if hf_token:
        headers["Authorization"] = f"Bearer {hf_token}"

    start = time.perf_counter()
    logger.info("extract: downloading source file from %s", HF_RESOLVE_URL)
    try:
        with requests.get(HF_RESOLVE_URL, headers=headers, stream=True, timeout=60) as response:
            response.raise_for_status()
            with open(tmp_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                    f.write(chunk)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise

    tmp_path.replace(SOURCE_PATH)
    size_mb = SOURCE_PATH.stat().st_size / (1024 * 1024)
    logger.info(
        "extract: downloaded %.0fMB source file in %.1fs", size_mb, time.perf_counter() - start
    )
    return SOURCE_PATH


def extract_to_cache(load_date: date, refresh: bool) -> Path:
    cache_path = cache_path_for(load_date)
    if cache_path.exists() and not refresh:
        logger.info("extract: using cached file %s (pass --refresh to redownload)", cache_path)
        return cache_path

    source_path = download_source_file(refresh)

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    # Write to a temp path and rename into place only on success, so a failed
    # or interrupted run never leaves a corrupt file that a later run would
    # mistake for a valid cache.
    tmp_path = cache_path.with_suffix(".parquet.tmp")
    start = time.perf_counter()

    con = duckdb.connect()
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}';")
    try:
        con.execute(
            f"""
            COPY (
                SELECT
                    code AS barcode,
                    to_json(product_name) AS product_name,
                    brands,
                    to_json(categories_tags) AS categories_tags,
                    to_json(nutriments) AS nutriments,
                    quantity,
                    nutriscore_grade,
                    last_modified_t,
                    to_json(countries_tags) AS countries_tags,
                    product_quantity,
                    product_quantity_unit,
                    to_json(brands_tags) AS brands_tags,
                    nova_group,
                    environmental_score_grade,
                    completeness,
                    created_t,
                    to_json(labels_tags) AS labels_tags,
                    to_json(data_quality_warnings_tags) AS data_quality_warnings_tags,
                    unique_scans_n,
                    DATE '{load_date.isoformat()}' AS load_date
                FROM read_parquet('{source_path}')
                WHERE list_contains(countries_tags, 'en:netherlands')
            ) TO '{tmp_path}' (FORMAT parquet)
            """
        )
        row_count = con.execute(f"SELECT count(*) FROM read_parquet('{tmp_path}')").fetchone()[0]
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise
    finally:
        con.close()

    tmp_path.replace(cache_path)
    logger.info(
        "extract: wrote %d NL rows to %s in %.1fs", row_count, cache_path, time.perf_counter() - start
    )
    return cache_path


def load_cache_to_warehouse(load_date: date, cache_path: Path) -> int:
    if not cache_path.exists():
        raise FileNotFoundError(
            f"{cache_path} does not exist. Run --stage extract (or all) for {load_date} first."
        )

    start = time.perf_counter()

    cache_con = duckdb.connect()
    cache_con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}';")
    rows = cache_con.execute(f"SELECT * FROM read_parquet('{cache_path}')").fetchall()
    cache_con.close()

    conn_info = {
        "host": os.environ.get("WAREHOUSE_HOST", "localhost"),
        "port": os.environ.get("WAREHOUSE_PORT", "5433"),
        "user": os.environ.get("WAREHOUSE_USER", "postgres"),
        "password": os.environ.get("WAREHOUSE_PASSWORD", "postgres"),
        "dbname": os.environ.get("WAREHOUSE_DB", "warehouse"),
    }
    ddl_sql = DDL_PATH.read_text()
    copy_sql = f"COPY raw.products ({', '.join(RAW_COLUMNS)}, loaded_at) FROM STDIN"

    with psycopg.connect(**conn_info) as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(ddl_sql)
                cur.execute("DELETE FROM raw.products WHERE load_date = %s", [load_date])
                loaded_at = datetime.now(timezone.utc)
                with cur.copy(copy_sql) as copy:
                    for row in rows:
                        copy.write_row((*row, loaded_at))

    row_count = len(rows)
    logger.info(
        "load: replaced %d rows for load_date=%s in %.1fs", row_count, load_date, time.perf_counter() - start
    )
    return row_count


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(
        description="Extract NL products from Open Food Facts and load them into raw.products."
    )
    parser.add_argument("--load-date", default=date.today().isoformat(), type=date.fromisoformat)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--stage", choices=["extract", "load", "all"], default="all")
    args = parser.parse_args()

    cache_path = cache_path_for(args.load_date)

    if args.stage in ("extract", "all"):
        cache_path = extract_to_cache(args.load_date, args.refresh)

    if args.stage in ("load", "all"):
        load_cache_to_warehouse(args.load_date, cache_path)


if __name__ == "__main__":
    main()
