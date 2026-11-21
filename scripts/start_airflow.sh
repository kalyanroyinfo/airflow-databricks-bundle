#!/usr/bin/env bash
# =============================================================================
# start_airflow.sh — Start Airflow scheduler + webserver
# =============================================================================
# Usage:
#   chmod +x scripts/start_airflow.sh
#   ./scripts/start_airflow.sh
#
# Stop:
#   ./scripts/start_airflow.sh stop
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

export AIRFLOW_HOME="${PROJECT_DIR}/airflow_home"
export AIRFLOW__CORE__DAGS_FOLDER="${PROJECT_DIR}/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES="False"

# Activate venv if it exists
if [ -f "${PROJECT_DIR}/.venv/bin/activate" ]; then
    source "${PROJECT_DIR}/.venv/bin/activate"
fi

# ─── Stop command ─────────────────────────────────────────────

if [ "${1:-}" = "stop" ]; then
    echo "🛑 Stopping Airflow..."
    pkill -f "airflow scheduler" 2>/dev/null || true
    pkill -f "airflow webserver" 2>/dev/null || true
    echo "✅ Airflow stopped."
    exit 0
fi

# ─── Preflight checks ────────────────────────────────────────

if [ ! -d "${AIRFLOW_HOME}" ]; then
    echo "❌ AIRFLOW_HOME not found. Run ./scripts/setup.sh first."
    exit 1
fi

echo "============================================"
echo " Starting Apache Airflow"
echo "============================================"
echo ""
echo " AIRFLOW_HOME:  ${AIRFLOW_HOME}"
echo " DAGs folder:   ${AIRFLOW__CORE__DAGS_FOLDER}"
echo ""

# ─── Start scheduler (background) ────────────────────────────

echo "🚀 Starting Airflow scheduler..."
airflow scheduler \
    --daemon \
    --pid "${AIRFLOW_HOME}/scheduler.pid" \
    --stdout "${AIRFLOW_HOME}/logs/scheduler.log" \
    --stderr "${AIRFLOW_HOME}/logs/scheduler.err.log"

# ─── Start webserver (background) ────────────────────────────

echo "🌐 Starting Airflow webserver on port 8080..."
airflow webserver \
    --port 8080 \
    --daemon \
    --pid "${AIRFLOW_HOME}/webserver.pid" \
    --stdout "${AIRFLOW_HOME}/logs/webserver.log" \
    --stderr "${AIRFLOW_HOME}/logs/webserver.err.log"

# ─── Wait and verify ─────────────────────────────────────────

sleep 3

if pgrep -f "airflow scheduler" > /dev/null && pgrep -f "airflow webserver" > /dev/null; then
    echo ""
    echo "============================================"
    echo " ✅ Airflow is running!"
    echo "============================================"
    echo ""
    echo " 🌐 UI:       http://localhost:8080"
    echo " 👤 Login:    admin / admin"
    echo ""
    echo " To stop:     ./scripts/start_airflow.sh stop"
    echo ""
else
    echo ""
    echo "❌ Something went wrong. Check logs:"
    echo "   ${AIRFLOW_HOME}/logs/scheduler.err.log"
    echo "   ${AIRFLOW_HOME}/logs/webserver.err.log"
    exit 1
fi
