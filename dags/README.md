# DAGs Directory

Airflow DAG definitions for the Airflow + Databricks Asset Bundle project.

## Available DAGs

| DAG ID | File | Schedule | Purpose |
|--------|------|----------|---------|
| `trigger_databricks_job` | `trigger_databricks_job.py` | `@daily` | Triggers an **existing** Databricks job by name. Use when the bundle is already deployed (e.g., via CI/CD). |
| `deploy_and_run_databricks` | `deploy_and_run_databricks.py` | Manual only | Validates, deploys the bundle, **then** triggers the job. Use for dev/testing when you want Airflow to manage the full lifecycle. |

## Prerequisites

Both DAGs require:

1. A Databricks connection named `databricks_default` configured in Airflow
   (see [README Step 10](../README.md#step-10-set-up-the-airflow--databricks-connection))
2. The Databricks Asset Bundle deployed to the workspace
   (see [README Step 8](../README.md#step-8-deploy-the-bundle-to-databricks))

## Configuration

Key constants are defined at the top of each DAG file:

- `DATABRICKS_CONN_ID` — Airflow connection ID (default: `databricks_default`)
- `JOB_NAME` — Must match the job name deployed via `databricks bundle deploy`
- `TARGET` — DAB target environment (`dev` or `prod`) — only in `deploy_and_run_databricks.py`
