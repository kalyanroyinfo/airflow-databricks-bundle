"""
DAG: Deploy Bundle + Run Databricks Job
========================================

This DAG handles BOTH deployment and execution:
  1. Validates the Databricks Asset Bundle
  2. Deploys the bundle to the workspace
  3. Triggers the job and waits for completion

Use this approach when you want Airflow to manage the full lifecycle.
For production, prefer separating deploy (CI/CD) from run (Airflow).

Prerequisites:
  - Databricks CLI installed on the Airflow worker
  - DATABRICKS_HOST and DATABRICKS_TOKEN environment variables or ~/.databrickscfg
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator,
)


# ─── Configuration ────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# Path to the repo root (where databricks.yml lives)
# Adjust this path to match your deployment
BUNDLE_PROJECT_DIR = os.environ.get(
    "DAB_PROJECT_DIR",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..")),
)

TARGET = "dev"  # Change to "prod" for production
DATABRICKS_CONN_ID = "databricks_default"
JOB_NAME = "my_etl_job"


# ─── DAG Definition ───────────────────────────────────────────

with DAG(
    dag_id="deploy_and_run_databricks",
    default_args=default_args,
    description="Deploy DAB bundle and run the Databricks job",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=["databricks", "dab", "deploy"],
) as dag:

    # ─── Task 1: Validate the bundle ─────────────────────────

    validate_bundle = BashOperator(
        task_id="validate_bundle",
        bash_command=f"cd {BUNDLE_PROJECT_DIR} && databricks bundle validate -t {TARGET}",
    )

    # ─── Task 2: Deploy the bundle ───────────────────────────

    deploy_bundle = BashOperator(
        task_id="deploy_bundle",
        bash_command=f"cd {BUNDLE_PROJECT_DIR} && databricks bundle deploy -t {TARGET}",
    )

    # ─── Task 3: Run the job ─────────────────────────────────

    run_job = DatabricksRunNowOperator(
        task_id="run_my_etl_job",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_name=JOB_NAME,
        notebook_params={
            "env": TARGET,
            "run_date": "{{ ds }}",
        },
        polling_period_seconds=30,
    )

    # ─── Pipeline ────────────────────────────────────────────

    validate_bundle >> deploy_bundle >> run_job
