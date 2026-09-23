# PLAN.md: Dutch Food Data Pipeline

A portfolio project: an end-to-end ELT pipeline for Dutch food products from Open Food Facts. It is a modern rebuild of my category-taxonomy and nutrition-data work at Yopla.

**One-line pitch:** Airflow extracts Dutch food products from Open Food Facts into PostgreSQL. Great Expectations gates the raw layer, and dbt models the data into a tested, documented, lineage-tracked star schema. Everything runs locally in Docker.

---

## 1. Architecture

```
Open Food Facts (Parquet on Hugging Face)
        │  DuckDB: filter to NL remotely, select ~20 columns
        ▼
PostgreSQL "warehouse"  ── raw.products        (bronze, one snapshot per load_date)
        │  Great Expectations quality gate (pipeline stops on failure)
        ▼
dbt ── staging.stg_products                    (silver: typed, cleaned, deduplicated)
    ── intermediate.int_products_categorised   (seed-based taxonomy mapping)
    ── marts.dim_product / dim_brand / dim_category / fct_nutrition   (gold)
        ▲
Airflow (Astro CLI) orchestrates: extract_load → validate_raw → dbt_build
GitHub Actions runs dbt build against a sample dataset on every push.
```

## 2. Stack

Python 3.11, DuckDB, PostgreSQL 16, dbt Core (dbt-postgres), Great Expectations Core 1.x, Apache Airflow via the Astro CLI, Docker, and GitHub Actions.

## 3. Conventions (these apply every day)

- **The repo root is the Astro project.** `astro dev init` creates `dags/`, `include/`, `plugins/`, `Dockerfile` and `requirements.txt`. The extraction code, GX suite and dbt project all live under `include/`, so they are mounted into the Airflow containers and hot-reload.
- **The warehouse Postgres is defined in `docker-compose.override.yml`**, so a single `astro dev start` brings up Airflow and the warehouse together.
  - From Airflow containers, connect to host `warehouse` on port `5432`.
  - From the laptop, connect to `localhost:5433`. Port 5432 on the host is already taken by Astro's metadata DB.
- **Configuration comes from environment variables only.**
  - Scripts and `profiles.yml` read `WAREHOUSE_HOST`, `WAREHOUSE_PORT`, `WAREHOUSE_USER`, `WAREHOUSE_PASSWORD` and `WAREHOUSE_DB`.
  - The Airflow connection is set as `AIRFLOW_CONN_WAREHOUSE` in `.env`, not by clicking in the UI, so setup is reproducible.
  - `.env` is gitignored; `.env.example` is committed.
- **dbt runs in its own virtualenv inside the Airflow image,** created in the `Dockerfile`, to avoid dependency conflicts with Airflow. The DAG calls that venv's `dbt` binary.
- **`profiles.yml` lives in the repo** (`include/dbt/profiles.yml`) and uses `env_var()`, so the same file works locally, in Airflow and in CI.
- **Pin every dependency version.**
- **Work one step at a time, and commit after each working step.**

## 4. Day-by-day plan

### Day 1: Setup, warehouse and extraction
1. Install Docker Desktop, Git, VS Code, Python 3.11, the Astro CLI and Claude Code. Create the GitHub repo `dutch-food-data-pipeline`.
2. Create the directory skeleton (see section 5). Run `astro dev init`.
3. Add the `warehouse` service (Postgres 16) to `docker-compose.override.yml`. Include an init SQL script that creates the database `warehouse` and the schemas `raw`, `staging`, `intermediate` and `marts`.
4. Write `include/extract/extract.py`:
   - Use DuckDB (with the `httpfs` extension) to read the Open Food Facts Parquet file from Hugging Face (`openfoodfacts/product-database`). **Verify the current file name** before using it.
   - Filter on `list_contains(countries_tags, 'en:netherlands')` *before* materialising anything.
   - Select about 20 columns: `code` (barcode), `product_name`, `brands`, `categories_tags`, `nutriments`, `quantity`, `nutriscore_grade`, `last_modified_t`, `countries_tags`, and similar.
   - Leave nested fields such as `nutriments` and multilingual `product_name` as JSON/JSONB in raw. Flattening them is dbt's job, which is the ELT principle.
   - Add a `load_date` column (the run date) and a `loaded_at` timestamp.
   - Write to `raw.products` with an **idempotent load**: delete the rows for that `load_date`, then insert them again, inside one transaction.
   - Accept `--load-date YYYY-MM-DD` (defaulting to today) so Airflow can pass its logical date.

**Done when:** `raw.products` holds tens of thousands of rows, and running the script twice for the same date does not duplicate rows.

### Day 2: Airflow orchestration
1. Run `astro dev start`. The UI opens at `localhost:8080`.
2. Set `AIRFLOW_CONN_WAREHOUSE` in `.env`, and add `apache-airflow-providers-postgres` if it is needed.
3. Write `dags/food_pipeline_dag.py` using the TaskFlow API and whatever Airflow version the current Astro Runtime ships:
   - The tasks run in the order `extract_load` → `validate_raw` → `dbt_build`. The last two are placeholders for now.
   - Schedule it `@daily` with `catchup=False` and sensible retries.
   - `extract_load` passes the run's logical date (`{{ ds }}`) as `--load-date`.
4. Trigger the DAG. Then re-trigger the same date and confirm the row counts are unchanged.

**Done when:** triggering the DAG loads fresh data, and re-running the same date is idempotent.

**Interview point:** "Re-runs replace a date partition instead of appending, so backfills and retries are safe."

### Day 3: dbt transformations (the core of the project)
1. Set up the dbt project `food_warehouse` in `include/dbt/`. Configure the staging, intermediate and marts layers (the medallion architecture), each materialising into its own schema.
2. Declare `raw.products` as a source, with a freshness check on `loaded_at`.
3. **Staging:** build `stg_products`. It should:
   - take only the latest `load_date` snapshot;
   - rename and cast columns;
   - trim and lowercase text, and convert `last_modified_t` from a Unix timestamp to a timestamp;
   - map Nutri-Score values `unknown` and `not-applicable` to null;
   - flatten the nutrients out of the `nutriments` JSON;
   - deduplicate on barcode, keeping the latest `last_modified`.
4. **Intermediate:** build `int_products_categorised`.
   - Create the seed `seeds/category_mapping.csv`, mapping Open Food Facts category tags to my own taxonomy (protein, dairy, vegetable, fruit, grain, snack, beverage, etc.).
   - Resolve each product to one primary category, and document the tie-break rule.
   - **Interview point:** this is exactly the taxonomy work I did at Yopla.
5. **Marts:**
   - `dim_product`: one row per barcode, with name, quantity, Nutri-Score, brand key and category key.
   - `dim_brand`: one row per brand. Split the comma-separated `brands` field, and document how the primary brand is chosen.
   - `dim_category`: one row per taxonomy category.
   - `fct_nutrition`: one row per product per nutrient (a long format). Columns are `product_key`, `nutrient`, `value_per_100`, `unit` (`g`, `mg`, `kcal`, `kJ`) and `basis` (`100g` or `100ml`).
6. Replace the `dbt_build` placeholder in the DAG with a real `dbt build` call using the dbt venv.

**Done when:** `dbt build` runs green, locally and from Airflow.

### Day 4: Data quality and governance
1. **dbt tests:**
   - `unique` and `not_null` on all keys.
   - `relationships` from `fct_nutrition` and `dim_product` to their dimensions.
   - `accepted_values` on Nutri-Score (`a` to `e`, with nulls allowed).
   - A custom generic test that mass nutrients per 100 g are between 0 and 100. Energy (kcal and kJ) is excluded, because it legitimately exceeds 100.
2. **Great Expectations (GX Core 1.x only).** Many tutorials use the old 0.x API; ignore them.
   - Build a suite on `raw.products` for the latest `load_date`. It checks:
     - the row count is within an expected range;
     - barcodes match `^[0-9]{8,14}$`;
     - key columns meet a completeness threshold;
     - the data is fresh (`loaded_at` falls within the last 26 hours).
   - Plug it into `validate_raw` so that a failed validation fails the task and stops the DAG.
3. **`DATA_QUALITY.md`:** a table mapping every test to a data quality dimension (completeness, uniqueness, validity, consistency or timeliness), the layer it runs on, and what happens when it fails.
4. **Governance metadata in the dbt YAML:**
   - A description for every model and column.
   - `meta` fields for `owner`, `source` and `classification: public` / `contains_pii: false`.
   - This becomes the data dictionary.

**Done when:** all tests pass, a deliberately broken raw load stops the DAG at `validate_raw`, and `DATA_QUALITY.md` is complete.

### Day 5: Lineage, docs, contract and end-to-end runs
1. Run `dbt docs generate` and `dbt docs serve`, and save screenshots of the lineage graph to `docs/images/`.
2. Run the full DAG end to end at least three times, fixing whatever breaks.
3. **Data contract** for `fct_nutrition`:
   - Enable dbt's native model contract (`contract: {enforced: true}`), with explicit column data types and constraints.
   - Add `contracts/fct_nutrition.yml`, which documents the schema, quality thresholds, owner and SLA in a readable form.

**Done when:** three clean end-to-end runs, the lineage screenshots are saved, and the contract is enforced.

### Day 6: CI/CD and README
1. Add `.github/workflows/ci.yml`, triggered on push and on pull requests. It:
   - starts a Postgres service container;
   - installs dbt from a pinned file;
   - runs `scripts/load_sample.py` to load `tests/fixtures/sample_products.csv` into `raw.products`;
   - runs `dbt deps`, `dbt seed` and `dbt build`.
2. Write the `README.md`:
   - a one-line pitch;
   - a Mermaid architecture diagram;
   - the stack;
   - how to run it in three commands (`cp .env.example .env`, `astro dev start`, then trigger the DAG);
   - screenshots of the Airflow DAG and the dbt lineage graph;
   - **Design decisions:** why ELT, why a medallion architecture, how idempotency works, and why the quality gates run before the transforms;
   - **What I'd do in production:** Databricks or Fabric, incremental models, alerting (Slack or email on failure), Terraform, secrets management;
   - an honest "Limitations" note.

**Done when:** CI is green on GitHub and the README renders with its diagram and screenshots.

### Day 7: Polish and apply
1. Clean the repo:
   - check for secrets in the git history;
   - make sure `.env.example` is complete and every dependency is pinned;
   - run `ruff` and `sqlfluff` if time allows.
2. Record a 60-second demo (GIF or Loom) of the DAG running, and put it in the README.
3. Update the CV, under Projects and labelled as a portfolio project (never as work experience). Example: *Built an end-to-end ELT pipeline ingesting [X]k Dutch food products from Open Food Facts, orchestrated with Airflow, modelled in dbt (staging → marts) on PostgreSQL, with Great Expectations quality gates, dbt lineage and documentation, and GitHub Actions CI.*
4. Update LinkedIn, write a short post about the project, and apply to Data Engineer roles the same day.

## 5. Target directory structure

```
dutch-food-data-pipeline/
├── .astro/                         # created by astro dev init
├── .github/workflows/ci.yml
├── dags/
│   └── food_pipeline_dag.py
├── include/
│   ├── extract/
│   │   └── extract.py
│   ├── gx/
│   │   └── validate_raw.py         # GX Core 1.x suite + checkpoint logic
│   └── dbt/                        # dbt project: food_warehouse
│       ├── dbt_project.yml
│       ├── profiles.yml            # uses env_var(), safe to commit
│       ├── packages.yml
│       ├── models/
│       │   ├── staging/            # _sources.yml, _stg_models.yml, stg_products.sql
│       │   ├── intermediate/       # int_products_categorised.sql + yml
│       │   └── marts/              # dim_*.sql, fct_nutrition.sql, _marts_models.yml
│       ├── seeds/category_mapping.csv
│       ├── macros/
│       └── tests/generic/          # custom nutrient range test
├── warehouse/init/01_schemas.sql   # run by the warehouse container on first start
├── scripts/load_sample.py          # CI: load fixture CSV into raw.products
├── tests/fixtures/sample_products.csv
├── contracts/fct_nutrition.yml
├── docs/images/                    # screenshots, demo GIF
├── plugins/                        # created by astro dev init
├── Dockerfile                      # Astro Runtime + dbt virtualenv
├── docker-compose.override.yml     # warehouse Postgres service
├── requirements.txt                # Airflow-image deps (pinned)
├── requirements-dbt.txt            # dbt venv deps (pinned)
├── packages.txt
├── .env.example
├── .gitignore
├── CLAUDE.md
├── PLAN.md
├── DATA_QUALITY.md
└── README.md
```

## 6. If I fall behind, cut in this order
1. The data contract.
2. CI/CD.
3. Great Expectations. The dbt tests alone still cover data quality.

**Never cut Airflow, dbt or the README.** Those are what recruiters filter on.