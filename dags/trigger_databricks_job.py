"""
DAG: Trigger a Databricks Job (deployed via Asset Bundle)
=========================================================

This DAG triggers an existing Databricks job by name using
DatabricksRunNowOperator. The job must already be deployed
to the workspace via `databricks bundle deploy`.

Usage:
    1. Deploy the bundle:  databricks bundle deploy -t dev
    2. Start Airflow:      ./scripts/start_airflow.sh
    3. Trigger this DAG from the UI or CLI
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator,
)


# ─── DAG Configuration ────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DATABRICKS_CONN_ID = "databricks_default"
JOB_NAME = "[dev kalyanroyinfo] my_etl_job"  # Must match the job name in databricks.yml


# ─── DAG Definition ───────────────────────────────────────────

with DAG(
    dag_id="trigger_databricks_job",
    default_args=default_args,
    description="Trigger a Databricks job deployed via Asset Bundle",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",          # Run daily; change as needed
    catchup=False,
    tags=["databricks", "etl", "dab"],
) as dag:

    # ─── Task: Trigger the Databricks Job ─────────────────────

    run_my_etl_job = DatabricksRunNowOperator(
        task_id="run_my_etl_job",
        databricks_conn_id=DATABRICKS_CONN_ID,

        # Use job_name to resolve the job dynamically (no hardcoded ID)
        job_name=JOB_NAME,

        # Pass parameters to the notebook
        # These override base_parameters defined in databricks.yml
        notebook_params={
            "env": "dev",
            "run_date": "{{ ds }}",   # Airflow execution date (YYYY-MM-DD)
        },

        # Polling: Airflow waits for the Databricks run to finish
        polling_period_seconds=30,

        # deferrable=True offloads polling to the Triggerer (saves worker slots)
        # Requires: `airflow triggerer` running as a separate process
        # deferrable=True,
    )

    run_my_etl_job
