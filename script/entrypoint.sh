#!/usr/bin/env bash
set -euo pipefail
REQ=/opt/airflow/requirements.txt
if [ -f "$REQ" ]; then
  python -m pip install --no-cache-dir -r "$REQ"
fi
exec "$@"
