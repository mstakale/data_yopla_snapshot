# Run Log

Three end-to-end runs of the pipeline (PLAN.md Day 5B), all performed live against
real infrastructure on 2026-09-23. All timings are wall-clock; all row counts are
from live `psql` queries at the time noted, not recalled or estimated.

Note on Run 1: the repo was cloned from `https://github.com/mstakale/data_yopla_snapshot.git`
at commit `0a998e3` (the latest commit pushed to GitHub at clone time - Day 4c). The
fixes found during Run 1 are committed locally (this session) but **not yet pushed** -
a fresh clone done right now would still hit the two bugs below until the fixes are
pushed.

## Run 1 — Fresh clone (2026-09-23, ~21:57–22:23 CEST)

**Goal:** prove the repo runs in exactly three commands: `cp .env.example .env`,
`astro dev start`, trigger the DAG - from a fresh clone, fresh Postgres volume, no
other manual steps.

**Setup:** stopped the main repo's Astro project (`astro dev stop`, 21:56:59) to free
ports 8080/5433. Cloned into `/tmp/yopla-fresh-clone` (a fresh directory gets its own
Docker Compose project name/volumes automatically, so the warehouse volume was
genuinely empty).

### Issues found (both are real repo bugs, both fixed)

| # | Issue | Fix |
|---|---|---|
| 1 | `astro dev start` does not forward the project's `.env` into `docker-compose.override.yml`'s `${WAREHOUSE_USER}`/`${WAREHOUSE_PASSWORD}`/`${WAREHOUSE_DB}` interpolation (confirmed: plain `docker compose -f docker-compose.override.yml config` from the same directory resolves them correctly; only Astro's own merge step doesn't). On a genuinely fresh volume this means Postgres starts with an empty password and crash-loops (`Error: Database is uninitialized and superuser password is not specified`). This was invisible for the whole project's life because the main repo's volume was already initialized once, and Postgres ignores those env vars on every restart after first init. | Added `${VAR:-default}` fallbacks in `docker-compose.override.yml`, matching `.env.example`'s values. |
| 2 | Airflow pauses every DAG by default on first sight. A manually triggered run of a paused DAG creates the DagRun but never schedules its tasks (confirmed: task states stayed blank for minutes until the DAG was explicitly unpaused via `airflow dags unpause`). This means "trigger the DAG" alone wasn't enough. | Added `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False` to `.env.example`/`.env`. |

**Confirmed non-issues** (checked, not bugs):
- `HF_TOKEN` empty in `.env.example` — `extract_load` still succeeded downloading the
  full ~7.3GB source file unauthenticated (the current single-stream-download
  implementation doesn't hit the old per-row-group rate limit the original code
  comment warned about).
- Astro CLI printed `Airflow UI: http://yopla-fresh-clone.localhost:6563` instead of
  `:8080` — this is Astro's own local reverse proxy giving each project a friendly
  vanity hostname/port (`--no-proxy` disables it); the actual Docker port mapping was
  correctly `8080` the whole time (confirmed via `astro dev ps` and a direct `curl`).
- `.astro/config.yaml` being committed to git is standard/expected Astro CLI
  convention, not a leftover machine-specific file.

### Timings (after both fixes were applied)

| Task | Start (UTC) | End (UTC) | Duration | Notes |
|---|---|---|---|---|
| `extract_load` | 20:05:51 | 20:21:56 | 16m 5s | Fresh container, no cache — full ~7.3GB source download from Hugging Face, unauthenticated |
| `validate_raw` | 20:21:57 | 20:22:17 | 20s | |
| `dbt_build` | 20:22:18 | 20:22:56 | 38s | |

### Result

All three tasks green: `PASS=48 WARN=5 ERROR=0` (same 5 audit-table checks flagging as
the established baseline, with slightly different absolute counts since Open Food
Facts is a live, changing dataset).

| Table | Row count |
|---|---|
| `raw.products` (load_date=2026-09-23) | 109,929 |
| `marts.dim_product` | 109,929 |
| `marts.dim_brand` | 18,452 |
| `marts.dim_category` | 48 |
| `marts.fct_nutrition` | 884,386 |

Stopped the fresh-clone project (`astro dev stop`, 22:23:45) and returned to the main
repo. Both fixes were committed to the main repo (commit `2e25508`).

## Run 2 — Idempotency (2026-09-23, ~22:24–22:37 CEST)

**Goal:** prove that triggering the DAG twice for the same logical date produces
byte-identical row counts in `raw` and `marts` — not just "the DAG ran twice without
error."

**Setup:** started the main repo's Astro project (image rebuilt, since
`docker-compose.override.yml`/`Dockerfile` had changed — ~4 minutes). Pre-existing
warehouse data was confirmed intact (109,767 rows each for load_date 2026-09-20 and
2026-09-23 — unaffected by the rebuild). The DAG was already unpaused automatically
(confirming Run 1's fix #2 works). Both runs manually triggered today resolve to
`load_date=2026-09-23`, which already had data from earlier in the day — a real test
of `extract.py`'s `DELETE FROM raw.products WHERE load_date = %s` + re-insert pattern
and dbt's full-table-rebuild materialization, not a no-op.

### Run A (run_id `manual__2026-09-23T20:33:41.617355+00:00`)

| Task | Start (UTC) | End (UTC) | Duration |
|---|---|---|---|
| `extract_load` | 20:33:42 | 20:34:01 | 19s |
| `validate_raw` | 20:34:01 | 20:34:23 | 22s |
| `dbt_build` | 20:34:23 | 20:35:27 | 64s |

(`extract_load` was fast, not the ~16 minutes of Run 1: the image rebuild only forced
recreation of the `warehouse` service, not the Airflow containers, so the container-
local extract cache from earlier today's real work was still warm.)

### Run B (run_id `manual__2026-09-23T20:35:44.892250+00:00`, triggered immediately after A)

| Task | Start (UTC) | End (UTC) | Duration |
|---|---|---|---|
| `extract_load` | 20:35:46 | 20:36:01 | 15s |
| `validate_raw` | 20:36:02 | 20:36:17 | 15s |
| `dbt_build` | 20:36:18 | 20:37:22 | 64s |

### Row count comparison

| Table | After Run A | After Run B | Identical? |
|---|---|---|---|
| `raw.products` (2026-09-20) | 109,767 | 109,767 | Yes |
| `raw.products` (2026-09-23) | 109,767 | 109,767 | Yes |
| `raw.products` (2026-09-23) `loaded_at` | 20:33:50 | 20:35:49 | **Different** (proves a real reload happened, not a skip) |
| `marts.dim_product` | 109,767 | 109,767 | Yes |
| `marts.dim_brand` | 18,376 | 18,376 | Yes |
| `marts.dim_category` | 48 | 48 | Yes |
| `marts.fct_nutrition` | 882,877 | 882,877 | Yes |

**No issues found.** Idempotency confirmed: every row count is byte-identical between
the two runs, while `loaded_at` genuinely advanced — proving the pipeline actually
redid the work (`DELETE`+re-`COPY` in `raw.products`; `DROP`+`CREATE` for every
`+materialized: table` mart) and arrived at the same result, rather than merely
skipping.

## Run 3 — Failure and recovery (2026-09-23, ~22:44–23:12 CEST)

**Goal:** force `validate_raw` to fail using the Day 4B row-count override, show
`dbt_build` is skipped (not run), then recover the *same* DAG run (not a new trigger)
by clearing the failed task after removing the override.

### First attempt (methodology correction, not a repo bug)

Set `GX_MIN_ROW_COUNT_OVERRIDE=999999999` in `.env`, restarted, triggered
(run_id `manual__2026-09-23T20:47:25.964780+00:00`). `validate_raw` failed in 11s —
but with a GX config-validation exception (`min_value (999999999) must be less than
or equal to max_value (200000)`), not the intended "the row-count check ran and
legitimately failed." `dbt_build` still correctly showed `upstream_failed` and never
executed (confirmed: no dbt log directory was created for that task attempt at all).
Corrected the override to `150000` (between the real count and the suite's own
`max_value=200000`) for a faithful demo of an actual failing check, rather than a
mis-configured one.

### Forced failure (run_id `manual__2026-09-23T20:57:30.152043+00:00`)

| Task | Start (UTC) | End (UTC) | Result |
|---|---|---|---|
| `extract_load` | 20:57:31 | 20:57:49 | success |
| `validate_raw` | 20:57:49 | 20:58:08 | **FAILED** — `[FAIL] row count within expected range -> observed: 109767` (all 11 other GX expectations passed) |
| `dbt_build` | 20:58:09 | 20:58:09 | `upstream_failed` — confirmed it never actually ran: no dbt log directory exists for this attempt |

### Recovery

Removed `GX_MIN_ROW_COUNT_OVERRIDE` from `.env` and restarted. **Methodology mistake
caught and corrected in real time:** the first attempt to clear `validate_raw` was
issued via the Airflow REST API (`POST /api/v2/dags/food_pipeline/clearTaskInstances`,
scoped to this exact `dag_run_id` — the same underlying call the Airflow UI's "Clear"
button makes; no browser-automation tool is available in this environment, so the
identical API call was used instead of a UI click) *before* the `astro dev restart`
had actually finished swapping in the container with the cleaned environment. The
still-live scheduler picked up the cleared task almost immediately and re-ran it with
the *stale* override still in its process environment, failing identically a second
time (confirmed via the scheduler container's creation timestamp, which was ~9 minutes
*after* this second attempt started). Corrected the sequence — waited for the restart
to fully complete and verified the container's environment was clean
(`GX_MIN_ROW_COUNT_OVERRIDE` empty) — then cleared again.

| Task (attempt) | Start (UTC) | End (UTC) | Result |
|---|---|---|---|
| `validate_raw` (3rd attempt) | 21:10:19 | 21:10:41 | **success** |
| `dbt_build` | 21:10:42 | 21:11:32 | **success** |

All three tasks ended green **within the same `dag_run_id`**
(`manual__2026-09-23T20:57:30.152043+00:00`) — no new DAG run was created for the
recovery, exactly as required.

**No repo bugs found in Run 3** — both the forced-failure and recovery paths behaved
exactly as designed (`retries=0` on `validate_raw`, default `trigger_rule` correctly
skipping `dbt_build` on an upstream failure, `store_failures`/audit unaffected by any
of this since GX has no `audit` schema of its own).

## Summary of all issues found across the three runs

1. **`docker-compose.override.yml` env-var interpolation** (Run 1, critical —
   fresh-volume-breaking) — fixed with hardcoded fallback defaults.
2. **DAGs paused by default, blocking manual-trigger scheduling** (Run 1, breaks the
   three-command promise) — fixed with `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False`.
3. Runs 2 and 3 found **no repo bugs** — idempotency and failure/recovery both behaved
   exactly as designed once the Run 1 fixes were in place.

Both fixes are committed locally (`2e25508`) but not yet pushed to GitHub — push
before considering the "runs in three commands" claim actually true for an outside
clone.
