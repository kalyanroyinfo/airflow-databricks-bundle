# Config Directory

Configuration files for the Airflow + Databricks project.

## airflow_variables.json

Airflow Variables used by the DAGs. Import them into your Airflow instance:

```bash
airflow variables import config/airflow_variables.json
```

Before importing, replace the placeholder values with your actual configuration.

| Variable | Description | Default |
|----------|-------------|---------|
| `databricks_workspace_host` | Full URL of your Databricks workspace | `https://<your-workspace>.cloud.databricks.com` |
| `databricks_job_name` | Name of the DAB-deployed job to trigger | `my_etl_job` |
| `databricks_target` | Databricks Asset Bundle target (`dev` or `prod`) | `dev` |
| `etl_default_env` | Default environment passed to the ETL notebook | `dev` |

> **Note:** These variables can also be set individually via the Airflow UI
> at **Admin → Variables**, or via the CLI:
> ```bash
> airflow variables set databricks_workspace_host "https://your-workspace.cloud.databricks.com"
> ```
