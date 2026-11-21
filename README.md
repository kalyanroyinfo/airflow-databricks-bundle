# Airflow + Databricks Asset Bundles — Step-by-Step Guide

![Python](https://img.shields.io/badge/Python-3.9--3.12-blue)
![Airflow](https://img.shields.io/badge/Airflow-2.10.4-017CEE)
![License](https://img.shields.io/badge/License-MIT-green)

Run Databricks jobs from a locally installed Apache Airflow using Databricks Asset Bundles (DABs).

## Architecture

```
┌─────────────────────┐       deploy        ┌──────────────────────────┐
│  Local Machine      │ ──────────────────►  │  Databricks Workspace    │
│                     │  databricks bundle   │                          │
│  ┌───────────────┐  │       deploy         │  ┌────────────────────┐  │
│  │ databricks.yml│  │                      │  │  Job: my_etl_job   │  │
│  └───────────────┘  │                      │  └────────────────────┘  │
│                     │                      │            ▲             │
│  ┌───────────────┐  │   trigger & monitor  │            │             │
│  │ Apache Airflow│  │ ─────────────────►   │            │             │
│  │  (native)     │  │  DatabricksRunNow    │     Runs notebook       │
│  └───────────────┘  │      Operator        │     on cluster          │
└─────────────────────┘                      └──────────────────────────┘
```

---

## Prerequisites

| Tool         | Version  | Purpose                                |
|--------------|----------|----------------------------------------|
| Python       | 3.9–3.12 | Runtime for Airflow                    |
| pip          | latest   | Package management                     |
| Databricks CLI | ≥ 0.218 | Bundle deploy & manage                |
| Databricks workspace | — | Where jobs run                      |
| Personal Access Token | — | Authentication                     |

---

## Step 1: Clone This Repo

```bash
git clone <your-repo-url> airflow-databricks-bundle
cd airflow-databricks-bundle
```

---

## Step 2: Create a Python Virtual Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
```

---

## Step 3: Install Apache Airflow (Native / Local)

```bash
# Set Airflow home to a local directory (keeps everything in the project)
export AIRFLOW_HOME=$(pwd)/airflow_home

# Install Airflow with constraints (important for compatibility)
AIRFLOW_VERSION=2.10.4
PYTHON_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

---

## Step 4: Install the Databricks Provider

```bash
pip install apache-airflow-providers-databricks
```

Or install everything at once:

```bash
pip install -r requirements.txt
```

---

## Step 5: Install the Databricks CLI

```bash
# Option A: via pip
pip install databricks-cli

# Option B: via curl (standalone — recommended)
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
```

Verify:

```bash
databricks --version
```

---

## Step 6: Configure Databricks Authentication

### 6a. Create a Personal Access Token (PAT)

1. Go to your **Databricks workspace** → **Settings** → **Developer** → **Access Tokens**
2. Click **Generate New Token**
3. Copy the token (you won't see it again)

### 6b. Configure the Databricks CLI

```bash
databricks configure
```

Enter when prompted:
- **Host**: `https://<your-workspace>.cloud.databricks.com`
- **Token**: your PAT from step 6a

This creates `~/.databrickscfg`:

```ini
[DEFAULT]
host  = https://<your-workspace>.cloud.databricks.com
token = dapi_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

---

## Step 7: Configure `databricks.yml` (Asset Bundle)

Copy the template and edit it with your workspace URL:

```bash
cp databricks.yml.example databricks.yml
```

Edit `databricks.yml` and replace the placeholders:

> ⚠️ **Important**: `databricks.yml` is gitignored to protect your workspace URL.
> Only `databricks.yml.example` (with placeholders) is committed to the repository.

```yaml
bundle:
  name: airflow_databricks_demo

workspace:
  host: https://<your-workspace>.cloud.databricks.com   # ← REPLACE

resources:
  jobs:
    my_etl_job:
      name: "my_etl_job"
      tasks:
        - task_key: "run_sample_notebook"
          notebook_task:
            notebook_path: ./src/sample_notebook.py
          new_cluster:
            spark_version: "13.3.x-scala2.12"
            node_type_id: "i3.xlarge"           # ← Adjust for your cloud
            num_workers: 1

targets:
  dev:
    mode: development
    default: true
  prod:
    mode: production
```

---

## Step 8: Deploy the Bundle to Databricks

```bash
# Validate first
databricks bundle validate

# Deploy to dev target
databricks bundle deploy -t dev
```

Expected output:
```
Uploading bundle files to /Users/<you>/.bundle/airflow_databricks_demo/dev/files...
Successfully deployed resources:
  Job: my_etl_job (ID: 123456789)
```

**Note the Job ID** — you'll need it for Airflow (or use `job_name` to resolve automatically).

Optionally, test the job directly:

```bash
databricks bundle run my_etl_job -t dev
```

---

## Step 9: Initialize Airflow

```bash
export AIRFLOW_HOME=$(pwd)/airflow_home

# Initialize the database
airflow db migrate

# Create an admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@example.com \
    --role Admin \
    --password admin
```

---

## Step 10: Set Up the Airflow → Databricks Connection

### Option A: Via CLI (recommended for automation)

```bash
airflow connections add databricks_default \
    --conn-type databricks \
    --conn-host "https://<your-workspace>.cloud.databricks.com" \
    --conn-password "dapi_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
```

### Option B: Via Airflow UI

1. Start Airflow (see Step 11)
2. Go to **Admin → Connections → + Add**
3. Fill in:
   - **Connection Id**: `databricks_default`
   - **Connection Type**: `Databricks`
   - **Host**: `https://<your-workspace>.cloud.databricks.com`
   - **Password**: your PAT

---

## Step 11: Link the DAG and Start Airflow

```bash
export AIRFLOW_HOME=$(pwd)/airflow_home

# Point Airflow to our dags folder
# (edit airflow.cfg or set the env var)
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags

# Terminal 1 — Start the scheduler
airflow scheduler &

# Terminal 2 — Start the webserver
airflow webserver --port 8080 &
```

Or use the convenience script:

```bash
chmod +x scripts/start_airflow.sh
./scripts/start_airflow.sh
```

Open **http://localhost:8080** and log in with `admin` / `admin`.

---

## Step 12: Trigger the DAG

### From the UI

1. Find `trigger_databricks_job` in the DAG list
2. Toggle it **ON**
3. Click the ▶ **Trigger DAG** button

### From the CLI

```bash
airflow dags trigger trigger_databricks_job
```

### Monitor

```bash
# Check the run status
airflow dags list-runs -d trigger_databricks_job

# Check task logs
airflow tasks logs trigger_databricks_job run_my_etl_job <run_id>
```

---

## Project Structure

```
airflow-databricks-bundle/
├── README.md                  # This guide
├── LICENSE                    # MIT License
├── databricks.yml             # DAB config — local (gitignored)
├── databricks.yml.example     # DAB config — template for GitHub
├── requirements.txt           # Python dependencies
├── .env.example               # Environment variables template
├── .gitignore
├── src/
│   └── sample_notebook.py     # Sample Databricks notebook
├── dags/
│   ├── README.md              # DAGs documentation
│   ├── trigger_databricks_job.py          # DAG: trigger existing job
│   └── deploy_and_run_databricks.py       # DAG: deploy bundle + run
├── scripts/
│   ├── setup.sh               # One-shot setup script
│   └── start_airflow.sh       # Start Airflow services
└── config/
    ├── README.md              # Config directory documentation
    └── airflow_variables.json # Airflow variables template
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `Connection refused` on Databricks | Check your host URL and token in the Airflow connection |
| `Job not found` | Make sure `databricks bundle deploy` succeeded and `job_name` matches |
| DAG not showing in UI | Check `AIRFLOW__CORE__DAGS_FOLDER` points to `./dags` |
| `ModuleNotFoundError: databricks` | Run `pip install apache-airflow-providers-databricks` in the same venv |
| Airflow DB errors | Run `airflow db migrate` |

---

## Next Steps

- [ ] Set up CI/CD to auto-deploy bundles on git push
- [ ] Add Slack/email alerts on job failure
- [ ] Parameterize jobs with Airflow Variables
- [ ] Use `deferrable=True` for resource-efficient polling
- [ ] Add more tasks and dependencies to the DAG

---

## License

MIT
