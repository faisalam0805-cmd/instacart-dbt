from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta


# === PATH CONFIGURATION ===

DBT_PROJECT_DIR = "/opt/airflow/dbt/instacart_market_basket"
DBT_PROFILES_DIR = "/opt/airflow/dbt/instacart_market_basket"
DBT_EXECUTABLE = "/opt/airflow/dbt_venv/bin/dbt"


# === DEFAULT ARGUMENTS ===

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# === DAG DEFINITION ===

with DAG(
    dag_id="instacart_dbt_pipeline",
    description="Instacart dbt pipeline using Airflow + Docker + dbt venv",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "instacart", "venv"],
) as dag:

    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        {DBT_EXECUTABLE} debug --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        {DBT_EXECUTABLE} deps --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        {DBT_EXECUTABLE} seed --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        {DBT_EXECUTABLE} run --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        {DBT_EXECUTABLE} test --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        {DBT_EXECUTABLE} docs generate --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    dbt_debug >> dbt_deps >> dbt_seed >> dbt_run >> dbt_test >> dbt_docs