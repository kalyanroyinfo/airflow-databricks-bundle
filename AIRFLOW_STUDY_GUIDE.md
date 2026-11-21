# Apache Airflow — Complete Beginner's Guide

> *Last updated: November 2026*

A comprehensive study material for anyone starting with Apache Airflow from scratch.

---

## Table of Contents

1. [What is Apache Airflow?](#1-what-is-apache-airflow)
2. [Why Do We Need Airflow?](#2-why-do-we-need-airflow)
3. [Core Concepts](#3-core-concepts)
4. [Architecture](#4-architecture)
5. [Your First DAG — Line by Line](#5-your-first-dag--line-by-line)
6. [Operators — The Building Blocks](#6-operators--the-building-blocks)
7. [Task Dependencies](#7-task-dependencies)
8. [Scheduling](#8-scheduling)
9. [XComs — Passing Data Between Tasks](#9-xcoms--passing-data-between-tasks)
10. [Connections & Hooks](#10-connections--hooks)
11. [Variables](#11-variables)
12. [Airflow UI Walkthrough](#12-airflow-ui-walkthrough)
13. [Jinja Templating](#13-jinja-templating)
14. [Task Flow API (Modern Way)](#14-task-flow-api-modern-way)
15. [Best Practices](#15-best-practices)
16. [Common Commands Cheat Sheet](#16-common-commands-cheat-sheet)
17. [Glossary](#17-glossary)

---

## 1. What is Apache Airflow?

Apache Airflow is an **open-source workflow orchestration tool**. Think of it as a **smart scheduler** that runs your tasks in the right order, at the right time, and handles failures gracefully.

### Simple Analogy

Imagine you're a restaurant manager:

| Restaurant | Airflow |
|-----------|---------|
| Recipe (steps in order) | **DAG** (workflow) |
| Each cooking step | **Task** |
| "Chop vegetables BEFORE cooking" | **Dependencies** |
| "Start cooking at 7 AM daily" | **Schedule** |
| Chef who does the cooking | **Worker** |
| Kitchen manager who assigns work | **Scheduler** |
| Dashboard showing order status | **Web UI** |

### What Airflow is NOT

- ❌ **Not a data streaming tool** (use Kafka/Flink for that)
- ❌ **Not a data processing engine** (it triggers Spark/Databricks/SQL, doesn't process data itself)
- ❌ **Not a storage system** (it orchestrates, not stores)

> [!IMPORTANT]
> **Key idea**: Airflow doesn't DO the work itself — it **tells other systems WHEN to do the work and in WHAT order**.

---

## 2. Why Do We Need Airflow?

### Without Airflow (The Problem)

```
❌ Cron job 1: Extract data at 2:00 AM
❌ Cron job 2: Transform data at 2:30 AM  (but what if job 1 is still running?)
❌ Cron job 3: Load data at 3:00 AM       (what if job 2 failed?)
❌ No visibility — did it run? Did it fail?
❌ No retries — failed? You find out next morning
❌ No dependency management — jobs run regardless of previous step
```

### With Airflow (The Solution)

```
✅ Extract → Transform → Load (runs in ORDER)
✅ Transform waits for Extract to finish
✅ If Extract fails → Transform doesn't run → you get an alert
✅ Auto-retry on failure (configurable)
✅ Full visibility in a web dashboard
✅ Historical logs for every run
```

---

## 3. Core Concepts

### 3.1 DAG (Directed Acyclic Graph)

A **DAG** is your workflow — a collection of tasks with defined execution order.

```
         ┌──────────┐
         │  Extract  │
         └────┬─────┘
              │
         ┌────▼─────┐
         │ Transform │
         └────┬─────┘
              │
         ┌────▼─────┐
         │   Load    │
         └──────────┘
```

- **Directed**: Tasks flow in one direction (Extract → Transform → Load)
- **Acyclic**: No loops (Task A can't depend on Task C if C depends on A)
- **Graph**: Tasks are connected nodes

```python
# A DAG is defined in a Python file
from airflow import DAG
from datetime import datetime

with DAG(
    dag_id="my_first_dag",          # Unique name
    start_date=datetime(2026, 1, 1), # When to start scheduling
    schedule_interval="@daily",      # How often to run
) as dag:
    # tasks go here
    pass
```

### 3.2 Task

A **Task** is a single unit of work — one step in your workflow.

```python
# Examples of tasks:
# - Run a Python function
# - Execute a SQL query
# - Trigger a Databricks job
# - Send an email
# - Copy a file
```

### 3.3 Task Instance

A **Task Instance** is a specific run of a task at a specific time.

```
Task:           "extract_data"
Task Instance:  "extract_data on 2026-01-15"  ← This is one specific execution
Task Instance:  "extract_data on 2026-01-16"  ← This is another execution
```

### 3.4 DAG Run

A **DAG Run** is a specific execution of the entire DAG at a specific time.

```
DAG:      "my_etl_pipeline"
DAG Run:  "my_etl_pipeline on 2026-01-15 at 00:00 UTC"
DAG Run:  "my_etl_pipeline on 2026-01-16 at 00:00 UTC"
```

### 3.5 Operator

An **Operator** is a template for a task — it defines WHAT kind of work to do.

### 3.5.1 Commonly Used Operators in Data Engineering

Data engineers frequently use the following operators (and their provider‑specific variants) to build robust pipelines:

- **BashOperator** – Execute arbitrary shell commands. Great for quick scripts, file movements, or invoking CLI tools.
- **PythonOperator** – Run a Python callable. Used for custom logic, data validation, or lightweight transformations.
- **EmailOperator** – Send email notifications (e.g., success/failure alerts).
- **EmptyOperator** (formerly DummyOperator) – No‑op placeholder for structuring DAGs or branching.
- **BranchPythonOperator** – Choose downstream path at runtime based on a Python condition.
- **ShortCircuitOperator** – Skip downstream tasks when a condition is false.
- **TriggerDagRunOperator** – Trigger another DAG, enabling modular pipeline composition.
- **PostgresOperator**, **MySqlOperator**, **SqliteOperator** – Execute SQL statements against relational databases.
- **SnowflakeOperator**, **BigQueryInsertJobOperator**, **RedshiftSQLOperator** – Run queries on cloud data warehouses.
- **S3CreateBucketOperator**, **S3DeleteObjectsOperator**, **S3FileTransformOperator** – Interact with Amazon S3 for staging data.
- **GoogleCloudStorageCreateBucketOperator**, **GCSToS3Operator**, **S3ToGCSOperator** – Cross‑cloud storage transfers.
- **DatabricksRunNowOperator**, **DatabricksSubmitRunOperator** – Launch Databricks notebooks or jobs.
- **KubernetesPodOperator** – Run a task inside an isolated Kubernetes pod (useful for containerized workloads).
- **HttpSensor / SimpleHttpOperator** – Poll HTTP endpoints or make REST calls.
- **FileSensor** – Wait for a file to appear in a filesystem or object store before proceeding.
- **DockerOperator** – Execute a Docker container as a task.

These operators cover the majority of ETL, ELT, data movement, and orchestration patterns seen in modern data‑engineering workflows.

### 3.5.2 Spark & PySpark Operators

| Operator | Typical Use |
|---|---|
| `SparkSubmitOperator` | Submit a Spark/PySpark job via `spark-submit`. |
| `SparkSqlOperator` | Run ad‑hoc Spark SQL statements. |
| `DatabricksRunNowOperator` / `DatabricksSubmitRunOperator` | Execute notebooks or jobs on Databricks (covers PySpark & Spark SQL). |
| `KubernetesPodOperator` (with `spark-submit`) | Run Spark jobs inside an isolated Kubernetes pod. |
| `PythonOperator` (import `pyspark`) | Simple one‑liner PySpark code when the worker already has Spark installed. |

#### Sample snippets

```python
# SparkSubmitOperator
SparkSubmitOperator(
    task_id="run_pyspark",
    application="/path/to/script.py",
    conn_id="spark_default",
    driver_memory="4g",
    executor_memory="4g",
    num_executors=4,
)
```

```python
# SparkSqlOperator
SparkSqlOperator(
    task_id="create_view",
    sql="""CREATE OR REPLACE TEMP VIEW recent AS
           SELECT * FROM table WHERE ds = '{{ ds }}';""",
    conn_id="spark_default",
)
```

```python
# DatabricksRunNowOperator (example)
DatabricksRunNowOperator(
    task_id="run_notebook",
    notebook_path="/Users/example@example.com/ETL",
    spark_submit_task={"new_cluster": {...}, "notebook_task": {"notebook_path": "/..."}}
)
```

```python
# KubernetesPodOperator with spark-submit
KubernetesPodOperator(
    task_id="spark_in_k8s",
    namespace="airflow",
    image="spark:latest",
    cmds=["spark-submit"],
    arguments=["--master", "k8s://...", "/scripts/etl.py"],
)
```

```python
# PythonOperator for lightweight PySpark
def simple_job():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("quick_job").getOrCreate()
    df = spark.range(10)
    df.show()

PythonOperator(task_id="lightweight_pyspark", python_callable=simple_job)
```

> **Tip:** Create a **Spark connection** (`Admin → Connections → Add → spark`) that stores the master URL, deploy mode, and any authentication details. Use `conn_id="spark_default"` in the operators above.



```
Operator = Blueprint     →    Task = Actual instance built from that blueprint
BashOperator             →    task: "run_my_script"
PythonOperator           →    task: "process_data"
DatabricksRunNowOperator →    task: "trigger_databricks"
```

---

## 4. Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                        AIRFLOW                                │
│                                                               │
│  ┌─────────────┐   ┌──────────────┐   ┌──────────────────┐  │
│  │  Web Server  │   │  Scheduler   │   │  Metadata DB     │  │
│  │  (Flask UI)  │   │  (heartbeat) │   │  (SQLite/        │  │
│  │              │   │              │   │   PostgreSQL)     │  │
│  │  - View DAGs │   │  - Parse DAGs│   │                  │  │
│  │  - Trigger   │   │  - Schedule  │   │  - DAG state     │  │
│  │  - Logs      │   │  - Monitor   │   │  - Task state    │  │
│  └──────┬───────┘   └──────┬───────┘   │  - Connections   │  │
│         │                  │           │  - Variables      │  │
│         │                  │           └────────▲─────────┘  │
│         │                  │                    │             │
│         └──────────┬───────┘                    │             │
│                    │                            │             │
│              ┌─────▼──────┐                     │             │
│              │  Executor   │─────────────────────┘             │
│              │             │                                   │
│              │  - Local    │   ┌──────────┐  ┌──────────┐    │
│              │  - Celery   │──►│ Worker 1 │  │ Worker 2 │    │
│              │  - K8s      │   └──────────┘  └──────────┘    │
│              └─────────────┘                                   │
└──────────────────────────────────────────────────────────────┘
```

### Components Explained

| Component | What It Does | Analogy |
|-----------|-------------|---------|
| **Web Server** | Shows the UI at `localhost:8080` | Dashboard / Control Panel |
| **Scheduler** | Decides when to run tasks, monitors them | Manager who assigns work |
| **Metadata DB** | Stores state of all DAGs, tasks, runs | Filing cabinet / memory |
| **Executor** | Decides HOW to run tasks | Hiring strategy |
| **Worker** | Actually runs the tasks | Employee doing the work |

### Executor Types (How tasks are run)

| Executor | Description | Best For |
|----------|-------------|----------|
| **SequentialExecutor** | Runs one task at a time | Testing only |
| **LocalExecutor** | Runs tasks in parallel on one machine | Small / local setups |
| **CeleryExecutor** | Distributes tasks across multiple machines | Production |
| **KubernetesExecutor** | Runs each task in a Kubernetes pod | Cloud-native production |

---

## 5. Your First DAG — Line by Line

```python
# ──────────────────────────────────────────────
# File: dags/my_first_dag.py
# ──────────────────────────────────────────────

# Step 1: Import required modules
from airflow import DAG                           # The DAG class
from airflow.operators.bash import BashOperator   # To run bash commands
from airflow.operators.python import PythonOperator  # To run Python functions
from datetime import datetime, timedelta

# Step 2: Define default settings for all tasks
default_args = {
    "owner": "kalyan",               # Who owns this DAG
    "retries": 2,                     # Retry failed tasks 2 times
    "retry_delay": timedelta(minutes=5),  # Wait 5 min between retries
    "email_on_failure": False,        # Don't email on failure
}

# Step 3: Define a Python function (will be used by PythonOperator)
def greet(name):
    print(f"Hello, {name}! Welcome to Airflow!")
    return f"Greeted {name}"

# Step 4: Create the DAG
with DAG(
    dag_id="my_first_dag",                 # Unique identifier
    default_args=default_args,             # Apply default settings
    description="My first Airflow DAG",    # Description for the UI
    start_date=datetime(2026, 1, 1),       # Don't schedule before this
    schedule_interval="@daily",            # Run once per day
    catchup=False,                         # Don't run for past dates
    tags=["tutorial", "beginner"],         # Tags for filtering in UI
) as dag:

    # Step 5: Define Task 1 — Run a bash command
    task_hello = BashOperator(
        task_id="say_hello",               # Unique task name
        bash_command='echo "Hello from Airflow! Today is $(date)"',
    )

    # Step 6: Define Task 2 — Run a Python function
    task_greet = PythonOperator(
        task_id="greet_user",
        python_callable=greet,             # The function to run
        op_kwargs={"name": "Kalyan"},      # Arguments to pass
    )

    # Step 7: Define Task 3 — Another bash command
    task_done = BashOperator(
        task_id="all_done",
        bash_command='echo "Pipeline complete! ✅"',
    )

    # Step 8: Set the order (dependencies)
    task_hello >> task_greet >> task_done
    # This means: say_hello runs FIRST
    #             greet_user runs AFTER say_hello
    #             all_done runs AFTER greet_user
```

### What happens when this DAG runs:

```
1. Scheduler finds this file in the dags/ folder
2. Scheduler parses it and registers the DAG
3. At the scheduled time (daily), it creates a DAG Run
4. Task "say_hello" runs first → prints date
5. Task "greet_user" runs next → prints "Hello, Kalyan!"
6. Task "all_done" runs last  → prints "Pipeline complete!"
7. All tasks green ✅ → DAG Run marked as SUCCESS
```

---

## 6. Operators — The Building Blocks

Operators define what kind of work a task does. Here are the most common ones:

### 6.1 BashOperator — Run Shell Commands

```python
from airflow.operators.bash import BashOperator

run_script = BashOperator(
    task_id="run_script",
    bash_command="python3 /scripts/process.py --date {{ ds }}",
)
```

### 6.2 PythonOperator — Run Python Functions

```python
from airflow.operators.python import PythonOperator

def process_data(file_path, batch_size):
    print(f"Processing {file_path} with batch size {batch_size}")
    # your logic here

task = PythonOperator(
    task_id="process_data",
    python_callable=process_data,
    op_kwargs={
        "file_path": "/data/input.csv",
        "batch_size": 100,
    },
)
```

### 6.3 EmailOperator — Send Emails

```python
from airflow.operators.email import EmailOperator

send_report = EmailOperator(
    task_id="send_report",
    to="team@company.com",
    subject="Daily Report - {{ ds }}",
    html_content="<h1>Report ready!</h1>",
)
```

### 6.4 DummyOperator / EmptyOperator — Do Nothing (for structure)

```python
from airflow.operators.empty import EmptyOperator

start = EmptyOperator(task_id="start")
end = EmptyOperator(task_id="end")

start >> [task_a, task_b, task_c] >> end
```

### 6.5 Provider Operators (External Systems)

```python
# Databricks
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

# AWS S3
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator

# Google BigQuery
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Postgres
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Slack
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
```

### Operator Summary Table

| Operator | Purpose | Example |
|----------|---------|---------|
| `BashOperator` | Run shell commands | `bash_command="echo hello"` |
| `PythonOperator` | Run Python functions | `python_callable=my_func` |
| `EmailOperator` | Send emails | `to="user@email.com"` |
| `EmptyOperator` | Placeholder / grouping | Start/end markers |
| `BranchPythonOperator` | Choose which task to run next | If/else logic |
| `ShortCircuitOperator` | Skip downstream if condition false | Conditional runs |
| `TriggerDagRunOperator` | Trigger another DAG | Cross-DAG orchestration |
| `DatabricksRunNowOperator` | Run Databricks job | `job_name="my_job"` |
| `PostgresOperator` | Run SQL on Postgres | `sql="SELECT ..."` |

---

## 7. Task Dependencies

Dependencies define the **order** tasks run in.

### 7.1 Linear (Sequential)

```python
task_a >> task_b >> task_c

# A runs first → then B → then C
```

```
A ──► B ──► C
```

### 7.2 Fan-Out (One to Many)

```python
task_a >> [task_b, task_c, task_d]

# A runs first → then B, C, D run IN PARALLEL
```

```
       ┌──► B
A ─────┼──► C
       └──► D
```

### 7.3 Fan-In (Many to One)

```python
[task_b, task_c, task_d] >> task_e

# B, C, D must ALL finish → then E runs
```

```
B ──┐
C ──┼──► E
D ──┘
```

### 7.4 Diamond Pattern

```python
start >> [extract_users, extract_orders]
[extract_users, extract_orders] >> transform
transform >> load >> end
```

```
           ┌─► extract_users ──┐
  start ───┤                   ├──► transform ──► load ──► end
           └─► extract_orders ─┘
```

### 7.5 Alternative Syntax

```python
# These are equivalent:
task_a >> task_b                      # Bitshift operator (most common)
task_a.set_downstream(task_b)         # Method syntax
task_b.set_upstream(task_a)           # Reverse method syntax
```

---

## 8. Scheduling

### 8.1 Schedule Presets

| Preset | Cron Equivalent | Meaning |
|--------|----------------|---------|
| `@once` | — | Run exactly once |
| `@hourly` | `0 * * * *` | Every hour |
| `@daily` | `0 0 * * *` | Every day at midnight |
| `@weekly` | `0 0 * * 0` | Every Sunday at midnight |
| `@monthly` | `0 0 1 * *` | 1st of every month |
| `@yearly` | `0 0 1 1 *` | January 1st every year |
| `None` | — | Manual trigger only |

### 8.2 Cron Expressions

```
┌──────── minute (0-59)
│ ┌────── hour (0-23)
│ │ ┌──── day of month (1-31)
│ │ │ ┌── month (1-12)
│ │ │ │ ┌ day of week (0-6, Sun=0)
│ │ │ │ │
* * * * *
```

**Examples:**

```python
# Every 15 minutes
schedule_interval="*/15 * * * *"

# Every day at 6:30 AM
schedule_interval="30 6 * * *"

# Every Monday and Friday at 9 AM
schedule_interval="0 9 * * 1,5"

# Every 1st and 15th of month at noon
schedule_interval="0 12 1,15 * *"
```

### 8.3 Understanding Execution Date

> [!WARNING]
> This confuses most beginners! Airflow's execution date is the **start** of the interval, NOT when the task actually runs.

```
Schedule: @daily
Start date: Jan 1

Jan 1 data period: Jan 1 00:00 → Jan 2 00:00
  └── Actually RUNS on: Jan 2 00:00 (after the period ends)
  └── execution_date = Jan 1 (start of the period)

Jan 2 data period: Jan 2 00:00 → Jan 3 00:00
  └── Actually RUNS on: Jan 3 00:00
  └── execution_date = Jan 2
```

**Why?** Because you typically want to process **yesterday's data** at midnight. The data for Jan 1 isn't complete until Jan 2.

### 8.4 Catchup

```python
# catchup=True (default): Runs for ALL past dates since start_date
# If start_date=Jan 1 and today=Jan 10, it creates 10 DAG runs!

# catchup=False: Only runs from NOW onwards
with DAG(
    start_date=datetime(2026, 1, 1),
    catchup=False,      # ← Usually what you want
) as dag:
```

---

## 9. XComs — Passing Data Between Tasks

**XCom** (Cross-Communication) lets tasks share small pieces of data.

```python
# Task 1: Push data to XCom
def extract(**context):
    data = {"users": 150, "orders": 340}
    context["ti"].xcom_push(key="stats", value=data)
    # OR simply return it (auto-pushed with key "return_value")
    return data

# Task 2: Pull data from XCom
def report(**context):
    stats = context["ti"].xcom_pull(task_ids="extract_task", key="stats")
    print(f"Users: {stats['users']}, Orders: {stats['orders']}")

extract_task = PythonOperator(
    task_id="extract_task",
    python_callable=extract,
)

report_task = PythonOperator(
    task_id="report_task",
    python_callable=report,
)

extract_task >> report_task
```

> [!CAUTION]
> XComs are stored in the **metadata database**. Only use them for small data (IDs, counts, file paths, status messages). **Never** pass large DataFrames or files through XCom.

---

## 10. Connections & Hooks

### 10.1 Connections

Connections store **credentials** for external systems (databases, APIs, cloud services).

```bash
# Add via CLI
airflow connections add my_postgres \
    --conn-type postgres \
    --conn-host localhost \
    --conn-port 5432 \
    --conn-login myuser \
    --conn-password mypass \
    --conn-schema mydb

# Add via UI
# Admin → Connections → + Add
```

### 10.2 Hooks

Hooks are Python classes that **use Connections** to interact with external systems.

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

def query_database():
    hook = PostgresHook(postgres_conn_id="my_postgres")
    records = hook.get_records("SELECT count(*) FROM users")
    print(f"Total users: {records[0][0]}")

task = PythonOperator(
    task_id="query_db",
    python_callable=query_database,
)
```

```
Connection (credentials)  →  Hook (interface)  →  Operator (task)

"my_postgres"             →  PostgresHook      →  PostgresOperator
  host: localhost              .get_records()       sql="SELECT ..."
  port: 5432                   .run()
  login: myuser
  password: mypass
```

---

## 11. Variables

Variables are **key-value pairs** stored in Airflow for configuration.

```bash
# Set via CLI
airflow variables set environment "production"
airflow variables set api_url "https://api.example.com"

# Set via UI: Admin → Variables → + Add
```

```python
# Use in a DAG
from airflow.models import Variable

env = Variable.get("environment")                          # "production"
config = Variable.get("my_config", deserialize_json=True)  # Parse JSON
default = Variable.get("missing_key", default_var="fallback")
```

> [!TIP]
> Use Variables for settings that change between environments (dev/staging/prod) without modifying DAG code.

---

## 12. Airflow UI Walkthrough

### Main Pages

| Page | What You See | How to Access |
|------|-------------|---------------|
| **DAGs** | List of all DAGs with status | Home page |
| **Grid View** | Task status across runs (matrix) | Click DAG name → Grid |
| **Graph View** | Visual dependency diagram | Click DAG name → Graph |
| **Calendar** | DAG run history on a calendar | Click DAG name → Calendar |
| **Code** | DAG source code | Click DAG name → Code |
| **Audit Log** | Who did what and when | Click DAG name → Audit Log |

### Task States (Colors)

| Color | State | Meaning |
|-------|-------|---------|
| 🟢 Dark Green | `success` | Task completed successfully |
| 🟡 Yellow | `running` | Task is currently executing |
| 🔴 Red | `failed` | Task failed |
| 🟠 Orange | `up_for_retry` | Task failed, waiting to retry |
| 🔵 Light Blue | `queued` | Task is waiting to be picked up |
| ⚪ Light Gray | `no_status` | Task hasn't run yet |
| 🟣 Purple | `skipped` | Task was skipped (branching) |
| 🩷 Pink | `upstream_failed` | A parent task failed |

### Common UI Actions

```
Toggle DAG ON/OFF     → Left toggle switch on DAGs page
Trigger DAG           → ▶️ Play button on the right
View task logs        → Click task square → "Log" button
Mark task success     → Click task → "Mark Success"
Clear task (re-run)   → Click task → "Clear"
View XCom values      → Click task → "XCom"
```

---

## 13. Jinja Templating

Airflow uses **Jinja templates** to inject runtime values into your tasks.

### Common Template Variables

| Variable | Example Value | Description |
|----------|--------------|-------------|
| `{{ ds }}` | `2026-01-15` | Execution date (YYYY-MM-DD) |
| `{{ ds_nodash }}` | `20260115` | Execution date without dashes |
| `{{ ts }}` | `2026-01-15T00:00:00+00:00` | Full timestamp |
| `{{ execution_date }}` | datetime object | Python datetime |
| `{{ prev_ds }}` | `2026-01-14` | Previous execution date |
| `{{ next_ds }}` | `2026-01-16` | Next execution date |
| `{{ dag.dag_id }}` | `my_pipeline` | DAG name |
| `{{ task.task_id }}` | `extract_data` | Task name |
| `{{ params.my_param }}` | user-defined | Custom parameters |

### Usage Examples

```python
# In BashOperator
BashOperator(
    task_id="process",
    bash_command="python etl.py --date {{ ds }} --env {{ params.env }}",
    params={"env": "production"},
)

# In SQL
PostgresOperator(
    task_id="clean_old_data",
    sql="DELETE FROM logs WHERE log_date < '{{ ds }}'",
)

# In Databricks
DatabricksRunNowOperator(
    task_id="run_job",
    notebook_params={"run_date": "{{ ds }}"},
)
```

---

## 14. Task Flow API (Modern Way)

Since Airflow 2.0, there's a cleaner way to write DAGs using the **`@task` decorator**:

### Traditional Way

```python
def extract_fn():
    return {"users": 100}

def transform_fn(**context):
    data = context["ti"].xcom_pull(task_ids="extract")
    data["users_doubled"] = data["users"] * 2
    return data

extract = PythonOperator(task_id="extract", python_callable=extract_fn)
transform = PythonOperator(task_id="transform", python_callable=transform_fn)
extract >> transform
```

### Modern Way (TaskFlow API) ✨

```python
from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="modern_etl",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
)
def my_etl_pipeline():

    @task
    def extract():
        return {"users": 100}

    @task
    def transform(data: dict):
        data["users_doubled"] = data["users"] * 2
        return data

    @task
    def load(data: dict):
        print(f"Loading {data['users_doubled']} users")

    # Dependencies are automatic from function calls!
    raw_data = extract()
    transformed = transform(raw_data)
    load(transformed)

# Instantiate the DAG
my_etl_pipeline()
```

> [!NOTE]
> The TaskFlow API automatically handles **XCom** — return values are passed as function arguments. No manual `xcom_push`/`xcom_pull` needed!

---

## 15. Best Practices

### ✅ Do

```python
# 1. Use catchup=False unless you need historical runs
DAG(catchup=False)

# 2. Set meaningful retries
default_args = {"retries": 3, "retry_delay": timedelta(minutes=5)}

# 3. Use tags for organization
DAG(tags=["etl", "production", "team-data"])

# 4. Keep DAG files simple — import heavy logic from modules
# dags/my_dag.py    → DAG definition only
# plugins/etl.py    → Business logic

# 5. Use Variables for environment-specific config
env = Variable.get("environment")

# 6. Use Connections for ALL credentials
# Never hardcode passwords in DAG files!

# 7. Set task timeouts
BashOperator(
    task_id="my_task",
    execution_timeout=timedelta(hours=2),
)
```

### ❌ Don't

```python
# 1. Don't do heavy processing at DAG parse time
# BAD:
data = requests.get("https://api.example.com/config").json()  # Runs every 30s!

# GOOD:
def fetch_config():
    return requests.get("https://api.example.com/config").json()
task = PythonOperator(python_callable=fetch_config)

# 2. Don't pass large data through XCom
# BAD:  return huge_dataframe
# GOOD: Save to S3/GCS, pass the file path

# 3. Don't hardcode credentials
# BAD:  password = "my_secret_123"
# GOOD: Use Airflow Connections

# 4. Don't use dynamic start_date
# BAD:  start_date=datetime.now()
# GOOD: start_date=datetime(2026, 1, 1)
```

---

## 16. Common Commands Cheat Sheet

### DAG Management

```bash
# List all DAGs
airflow dags list

# Show tasks in a DAG
airflow tasks list my_dag

# Trigger a DAG manually
airflow dags trigger my_dag

# Trigger with parameters
airflow dags trigger my_dag --conf '{"key": "value"}'

# Pause / Unpause a DAG
airflow dags pause my_dag
airflow dags unpause my_dag

# List recent DAG runs
airflow dags list-runs -d my_dag
```

### Task Management

```bash
# Test a single task (without recording in DB)
airflow tasks test my_dag my_task 2026-01-01

# Run a task (records in DB)
airflow tasks run my_dag my_task 2026-01-01

# View task logs
airflow tasks logs my_dag my_task 2026-01-01

# Clear a task (to re-run it)
airflow tasks clear my_dag -t my_task -s 2026-01-01 -e 2026-01-02
```

### Connections & Variables

```bash
# List connections
airflow connections list

# Add a connection
airflow connections add my_conn --conn-type http --conn-host api.example.com

# Delete a connection
airflow connections delete my_conn

# Set / Get variables
airflow variables set my_key my_value
airflow variables get my_key

# Import variables from JSON
airflow variables import variables.json
```

### Administration

```bash
# Initialize / migrate the database
airflow db migrate

# Create a user
airflow users create --username admin --role Admin --password admin \
    --firstname Admin --lastname User --email admin@example.com

# List users
airflow users list

# Start services
airflow webserver --port 8080
airflow scheduler
airflow triggerer   # For deferrable operators
```

---

## 17. Glossary

| Term | Definition |
|------|-----------|
| **DAG** | Directed Acyclic Graph — your workflow definition |
| **Task** | A single step/unit of work in a DAG |
| **Task Instance** | One execution of a task for a specific date |
| **DAG Run** | One execution of the entire DAG |
| **Operator** | A class that defines what a task does |
| **Sensor** | A special operator that waits for a condition |
| **Hook** | A Python class to connect to external systems |
| **Connection** | Stored credentials for external systems |
| **Variable** | Key-value config stored in Airflow |
| **XCom** | Mechanism for tasks to share small data |
| **Executor** | Component that determines how tasks are run |
| **Scheduler** | Process that triggers DAGs and assigns tasks |
| **Worker** | Process that actually executes tasks |
| **Pool** | Limits concurrent task execution |
| **SLA** | Service Level Agreement — deadline for task completion |
| **Backfill** | Running DAGs for past dates |
| **Catchup** | Auto-running missed DAG runs since start_date |
| **Trigger Rule** | Condition for when a task should run |
| **Deferrable** | Task that frees the worker while waiting |
| **Provider** | Plugin package for external system integration |

---

## What's Next?

Now that you understand the basics, try these exercises:

1. ✏️ **Create a simple DAG** with 3 tasks using `BashOperator`
2. ✏️ **Add branching** with `BranchPythonOperator`
3. ✏️ **Pass data** between tasks using XCom
4. ✏️ **Connect to Databricks** using what we set up in this project
5. ✏️ **Add a Sensor** that waits for a file before processing

### Useful Resources

- 📖 [Official Airflow Docs](https://airflow.apache.org/docs/)
- 📖 [Airflow Tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/index.html)
- 📖 [Provider Packages](https://airflow.apache.org/docs/apache-airflow-providers/packages-ref.html)
- 📖 [Astronomer Guides](https://docs.astronomer.io/learn)

---

*Created as part of the [Airflow + Databricks Asset Bundle project](/mnt/c/WSL/codebase/airflow-databricks-bundle/README.md)*
