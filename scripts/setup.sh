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

AIRFLOW_VERSION="2.10.4"
REQUIRED_PYTHON_MINOR=8   # Minimum Python 3.8

echo "============================================"
echo " Airflow + Databricks Bundle Setup"
echo "============================================"
echo ""
echo "Project directory: ${PROJECT_DIR}"
echo ""

# ─── Step 0: Ensure Python 3.8+ is available ──────────────────

find_python() {
    # Try python3.12 down to python3.8, then fall back to python3
    for ver in python3.12 python3.11 python3.10 python3.9 python3.8; do
        if command -v "$ver" &>/dev/null; then
            echo "$ver"
            return 0
        fi
    done
    # Check default python3
    if command -v python3 &>/dev/null; then
        local minor
        minor=$(python3 -c "import sys; print(sys.version_info.minor)")
        if [ "$minor" -ge "$REQUIRED_PYTHON_MINOR" ]; then
            echo "python3"
            return 0
        fi
    fi
    return 1
}

PYTHON_BIN=""
if PYTHON_BIN=$(find_python); then
    PYTHON_VER=$($PYTHON_BIN --version 2>&1)
    echo "✅ Found suitable Python: ${PYTHON_BIN} (${PYTHON_VER})"
else
    echo "⚠️  No Python 3.8+ found. Attempting to install Python 3.9..."
    echo ""

    # Use sudo only if not root
    SUDO=""
    if [ "$(id -u)" -ne 0 ]; then
        SUDO="sudo"
    fi

    # Detect package manager and install
    if command -v dnf &>/dev/null; then
        echo "   → Detected dnf (RHEL/CentOS/Fedora)"
        $SUDO dnf module enable python39 -y
        $SUDO dnf install python39 python39-pip python39-devel -y
        PYTHON_BIN="python3.9"
    elif command -v yum &>/dev/null; then
        echo "   → Detected yum (RHEL/CentOS)"
        $SUDO yum install python39 python39-pip python39-devel -y
        PYTHON_BIN="python3.9"
    elif command -v apt-get &>/dev/null; then
        echo "   → Detected apt (Debian/Ubuntu)"
        $SUDO apt-get update
        $SUDO apt-get install -y python3.9 python3.9-venv python3.9-dev python3-pip
        PYTHON_BIN="python3.9"
    else
        echo ""
        echo "❌ Could not auto-install Python 3.8+."
        echo "   Please install Python 3.9+ manually and re-run this script."
        echo ""
        echo "   CentOS/RHEL:  sudo dnf module enable python39 && sudo dnf install python39"
        echo "   Ubuntu/Debian: sudo apt install python3.9 python3.9-venv"
        echo "   macOS:         brew install python@3.9"
        exit 1
    fi

    if ! command -v "$PYTHON_BIN" &>/dev/null; then
        echo "❌ Installation failed. Please install Python 3.9+ manually."
        exit 1
    fi

    echo "✅ Installed $($PYTHON_BIN --version 2>&1)"
fi

echo "   Using: $(command -v $PYTHON_BIN)"
echo ""

# ─── Step 1: Create virtual environment ───────────────────────

if [ ! -d "${PROJECT_DIR}/.venv" ]; then
    echo "📦 Creating Python virtual environment with ${PYTHON_BIN}..."
    $PYTHON_BIN -m venv "${PROJECT_DIR}/.venv"
else
    echo "✅ Virtual environment already exists."
fi

source "${PROJECT_DIR}/.venv/bin/activate"
pip install --upgrade pip --quiet

# Verify Python version inside venv
VENV_PYTHON_VER=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
echo "   venv Python version: ${VENV_PYTHON_VER}"

# ─── Step 2: Install dependencies ─────────────────────────────

echo ""
echo "📦 Installing Apache Airflow ${AIRFLOW_VERSION} + Databricks provider..."

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${VENV_PYTHON_VER}.txt"

echo "   Constraints: ${CONSTRAINT_URL}"

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
    # Disable example DAGs
    sed -i "s|^load_examples = .*|load_examples = False|" "${AIRFLOW_CFG}"
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
    echo "✅ Databricks CLI found: $(databricks --version 2>&1 | head -1)"
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
