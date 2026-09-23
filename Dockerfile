FROM astrocrpublic.azurecr.io/runtime:3.3-7

# Day 3: dbt runs in its own virtualenv, isolated from Airflow's own
# dependencies (dbt's deps overlap several packages Airflow itself needs).
COPY requirements-dbt.txt /tmp/requirements-dbt.txt
RUN python3 -m venv /usr/local/airflow/dbt_venv && \
    /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt
