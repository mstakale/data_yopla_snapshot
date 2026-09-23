# CLAUDE.md

Working rules for this repo. PLAN.md is the source of truth for scope and day-by-day plan; this file is the quick-reference for conventions and how we collaborate.

## Conventions (from PLAN.md §3)

- **Repo root = Astro project.** `astro dev init` creates `dags/`, `include/`, `plugins/`, `Dockerfile`, `requirements.txt`. Extraction code, the GX suite and the dbt project live under `include/` so they're mounted into the Airflow containers and hot-reload.
- **Warehouse Postgres lives in `docker-compose.override.yml`.** One `astro dev start` brings up Airflow + warehouse together.
  - From Airflow containers: host `warehouse`, port `5432`.
  - From the laptop: `localhost:5433` (host port 5432 is taken by Astro's metadata DB).
- **Config comes from environment variables only.**
  - Scripts and `profiles.yml` read `WAREHOUSE_HOST`, `WAREHOUSE_PORT`, `WAREHOUSE_USER`, `WAREHOUSE_PASSWORD`, `WAREHOUSE_DB`.
  - The Airflow connection is `AIRFLOW_CONN_WAREHOUSE`, set in `.env` — never clicked in manually via the UI.
  - `.env` is gitignored; `.env.example` is committed and kept complete.
- **dbt runs in its own virtualenv inside the Airflow image** (built in the `Dockerfile`), to keep its deps isolated from Airflow's. The DAG calls that venv's `dbt` binary.
- **`profiles.yml` lives in the repo** at `include/dbt/profiles.yml` and uses `env_var()`, so it works unchanged locally, in Airflow and in CI.
- **Pin every dependency version.** No unpinned packages anywhere (`requirements.txt`, `requirements-dbt.txt`, `packages.yml`, `packages.txt`, CI installs).
- **Work one step at a time, and commit after each working step.**

## Working rules (how we collaborate)

- Work one step at a time. After each step, explain what was done and why, then stop and wait for explicit go-ahead before continuing.
- Never start the next Day's work without an explicit go-ahead — even if the current Day looks done.
- Pin all dependency versions. Never commit secrets (`.env`, credentials, tokens).
- Keep code simple and readable — every line must be explainable in an interview. No cleverness, no speculative abstraction, no unused error handling.
