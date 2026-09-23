# Day 2: TaskFlow DAG orchestrating extract_load -> validate_raw -> dbt_build,
# scheduled @daily with catchup=False. validate_raw and dbt_build become real
# tasks on Day 4 and Day 3 respectively. Not implemented yet.
