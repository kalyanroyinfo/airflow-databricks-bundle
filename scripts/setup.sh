#!/usr/bin/env bash
# =============================================================================
# setup.sh — One-shot setup for Airflow + Databricks Asset Bundle project
# =============================================================================
# Usage:
#   chmod +x scripts/setup.sh
#   ./scripts/setup.sh
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "============================================"
echo " Airflow + Databricks Bundle Setup"
echo "============================================"
echo ""
echo "Project directory: ${PROJECT_DIR}"
echo ""

# ─── Step 1: Create virtual environment ───────────────────────

if [ ! -d "${PROJECT_DIR}/.venv" ]; then
    echo "📦 Creating Python virtual environment..."
    python3 -m venv "${PROJECT_DIR}/.venv"
else
    echo "✅ Virtual environment already exists."
fi

source "${PROJECT_DIR}/.venv/bin/activate"
pip install --upgrade pip --quiet

# ─── Step 2: Install dependencies ─────────────────────────────

echo ""
echo "📦 Installing Apache Airflow + Databricks provider..."

AIRFLOW_VERSION="2.10.4"
PYTHON_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}" --quiet
pip install apache-airflow-providers-databricks --quiet

echo "✅ Dependencies installed."

# ─── Step 3: Set up Airflow home ──────────────────────────────

export AIRFLOW_HOME="${PROJECT_DIR}/airflow_home"
mkdir -p "${AIRFLOW_HOME}"

echo ""
echo "🏠 AIRFLOW_HOME = ${AIRFLOW_HOME}"

# ─── Step 4: Initialize Airflow DB ────────────────────────────

echo ""
echo "🗄️  Initializing Airflow database..."
airflow db migrate 2>&1 | tail -1

# ─── Step 5: Configure dags_folder ────────────────────────────

AIRFLOW_CFG="${AIRFLOW_HOME}/airflow.cfg"
if [ -f "${AIRFLOW_CFG}" ]; then
    # Update dags_folder to point to our dags directory
    sed -i "s|^dags_folder = .*|dags_folder = ${PROJECT_DIR}/dags|" "${AIRFLOW_CFG}"
    echo "✅ dags_folder configured → ${PROJECT_DIR}/dags"
fi

# ─── Step 6: Create admin user ────────────────────────────────

echo ""
echo "👤 Creating Airflow admin user..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@example.com \
    --role Admin \
    --password admin \
    2>/dev/null || echo "   (User already exists)"

# ─── Step 7: Check Databricks CLI ─────────────────────────────

echo ""
if command -v databricks &> /dev/null; then
    echo "✅ Databricks CLI found: $(databricks --version)"
else
    echo "⚠️  Databricks CLI not found. Install it:"
    echo "   curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh"
fi

# ─── Done ──────────────────────────────────────────────────────

echo ""
echo "============================================"
echo " ✅ Setup Complete!"
echo "============================================"
echo ""
echo " Next steps:"
echo ""
echo "   1. Activate the venv:"
echo "      source .venv/bin/activate"
echo ""
echo "   2. Configure Databricks CLI:"
echo "      databricks configure"
echo ""
echo "   3. Edit databricks.yml with your workspace URL"
echo ""
echo "   4. Deploy the bundle:"
echo "      databricks bundle deploy -t dev"
echo ""
echo "   5. Set up the Airflow connection:"
echo "      airflow connections add databricks_default \\"
echo "          --conn-type databricks \\"
echo "          --conn-host 'https://<workspace>.cloud.databricks.com' \\"
echo "          --conn-password '<your-pat-token>'"
echo ""
echo "   6. Start Airflow:"
echo "      ./scripts/start_airflow.sh"
echo ""
echo "   7. Open http://localhost:8080 (admin / admin)"
echo ""
