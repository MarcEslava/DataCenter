#!/usr/bin/env bash
set -euo pipefail

# Project-local venv under the mounted /work directory
VENV_DIR="/work/.venv"

if [ ! -d "$VENV_DIR" ]; then
  python -m venv "$VENV_DIR"
fi

# shellcheck disable=SC1090
source "$VENV_DIR/bin/activate"

python -m pip install --upgrade pip setuptools wheel >/dev/null

# Install only if requirements exist in your mounted project
if [ -f "/work/requirements.txt" ]; then
  echo "[python-master] Installing requirements from /work/requirements.txt ..."
  python -m pip install -r /work/requirements.txt
fi

export PATH="$VENV_DIR/bin:$PATH"

exec "$@"
