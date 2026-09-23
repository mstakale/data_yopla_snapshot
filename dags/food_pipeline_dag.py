import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, get_current_context, task

logger = logging.getLogger(__name__)
EXTRACT_SCRIPT = Path(__file__).resolve().parents[1] / "include" / "extract" / "extract.py"
DBT_BIN = Path("/usr/local/airflow/dbt_venv/bin/dbt")
DBT_PROJECT_DIR = Path(__file__).resolve().parents[1] / "include" / "dbt"


def get_run_load_date() -> str:
    context = get_current_context()
    dag_run = context["dag_run"]
    # Airflow 3: a manually/asset-triggered run can have logical_date=None;
    # run_after is always populated, so it's the safe fallback.
    run_moment = dag_run.logical_date or dag_run.run_after
    return run_moment.date().isoformat()


def run_dbt(*args: str) -> None:
    env = {
        **os.environ,
        "DBT_PROFILES_DIR": str(DBT_PROJECT_DIR),
        "DBT_PROJECT_DIR": str(DBT_PROJECT_DIR),
        # Keep build artifacts out of the host-bind-mounted include/ folder.
        "DBT_TARGET_PATH": "/tmp/dbt_target",
        "DBT_LOG_PATH": "/tmp/dbt_logs",
    }
    result = subprocess.run([str(DBT_BIN), *args], env=env, capture_output=True, text=True)
    if result.stdout:
        logger.info(result.stdout)
    if result.stderr:
        logger.info(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"dbt {args[0]} failed (exit {result.returncode})")


@dag(
    dag_id="food_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["food", "elt", "portfolio"],
    doc_md="""
    ### Dutch Food Data Pipeline
    extract_load -> validate_raw -> dbt_build. Extracts NL products from Open Food
    Facts into raw.products (idempotent per load_date), then (from Day 4) gates on
    Great Expectations, then builds the dbt star schema for that same load_date.
    """,
)
def food_pipeline():

    @task
    def extract_load() -> str:
        load_date = get_run_load_date()

        result = subprocess.run(
            [sys.executable, str(EXTRACT_SCRIPT), "--load-date", load_date],
            capture_output=True,
            text=True,
        )
        if result.stdout:
            logger.info(result.stdout)
        if result.stderr:
            logger.info(result.stderr)
        if result.returncode != 0:
            raise RuntimeError(f"extract.py failed for load_date={load_date} (exit {result.returncode})")
        return load_date

    @task
    def validate_raw(load_date: str) -> str:
        logger.info("TODO: Day 4 - Great Expectations gate for load_date=%s", load_date)
        return load_date

    @task
    def dbt_build(load_date: str) -> None:
        run_dbt("deps")
        run_dbt("build", "--vars", json.dumps({"load_date": load_date}))

    dbt_build(validate_raw(extract_load()))


food_pipeline()
