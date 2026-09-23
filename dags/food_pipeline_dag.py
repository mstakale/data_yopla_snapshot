import logging
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, get_current_context, task

logger = logging.getLogger(__name__)
EXTRACT_SCRIPT = Path(__file__).resolve().parents[1] / "include" / "extract" / "extract.py"


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
    Great Expectations, then (from Day 3) builds the dbt star schema.
    """,
)
def food_pipeline():

    @task
    def extract_load() -> str:
        context = get_current_context()
        dag_run = context["dag_run"]
        # Airflow 3: a manually/asset-triggered run can have logical_date=None;
        # run_after is always populated, so it's the safe fallback.
        run_moment = dag_run.logical_date or dag_run.run_after
        load_date = run_moment.date().isoformat()

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
        logger.info("TODO: Day 3 - dbt build for load_date=%s", load_date)

    dbt_build(validate_raw(extract_load()))


food_pipeline()
