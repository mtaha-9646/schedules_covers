#!/usr/bin/env bash
set -euo pipefail

# Roots to repository
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${ROOT}"

# --- ADD THIS LINE HERE ---
# Set the specific path to your WSGI file
PYTHON_ANYWHERE_WSGI="/var/www/coveralreef_pythonanywhere_com_wsgi.py"
# --------------------------

echo "Starting deploy at $(date -u)"

# Pull latest from tracked branch; adjust if you want a specific remote/branch
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "Fetching latest code"
  git fetch --all --prune
  git pull --ff-only
else
  echo "Not in a git repository; skipping git pull"
fi

# Activate virtualenv if available (PythonAnywhere default)
if [ -f "${ROOT}/env/bin/activate" ]; then
  echo "Activating virtualenv"
  # shellcheck source=/dev/null
  . "${ROOT}/env/bin/activate"
elif [ -f "${ROOT}/env/Scripts/activate" ]; then
  echo "Activating Windows virtualenv"
  # shellcheck source=/dev/null
  . "${ROOT}/env/Scripts/activate"
fi

# Install dependencies (skip if requirements missing)
if [ -f "${ROOT}/requirements.txt" ]; then
  echo "Installing requirements"
  pip install -r "${ROOT}/requirements.txt"
else
  echo "No requirements.txt found; skipping install"
fi

# Touch PythonAnywhere WSGI file to reload the web app
if [ -n "${PYTHON_ANYWHERE_WSGI:-}" ]; then
  echo "Reloading PythonAnywhere web app by touching ${PYTHON_ANYWHERE_WSGI}"
  touch "${PYTHON_ANYWHERE_WSGI}"
else
  echo "PYTHON_ANYWHERE_WSGI not set; skipping reload trigger"
fi

echo "Deploy finished at $(date -u)"
