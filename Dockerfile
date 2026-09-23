FROM astrocrpublic.azurecr.io/runtime:3.3-7

# Day 3: dbt runs in its own virtualenv, isolated from Airflow's own
# dependencies (dbt's deps overlap several packages Airflow itself needs).
COPY requirements-dbt.txt /tmp/requirements-dbt.txt
RUN python3 -m venv /usr/local/airflow/dbt_venv && \
    /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt

# Day 4B: great_expectations 1.x requires Python <3.14, but this image's own
# Python is 3.14 - a venv on that same interpreter (like dbt_venv above)
# can't fix a Python-version ceiling, so uv (already used to bootstrap this
# image's own Python) provisions a second, independent Python 3.12 for it.
COPY requirements-gx.txt /tmp/requirements-gx.txt
RUN uv python install 3.12 && \
    uv venv --python 3.12 /usr/local/airflow/gx_venv && \
    uv pip install --python /usr/local/airflow/gx_venv/bin/python -r /tmp/requirements-gx.txt
