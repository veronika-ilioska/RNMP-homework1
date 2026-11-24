#!/usr/bin/env bash
# Course runner adapted for PyCharm's .venv on Windows and Linux.
set -euo pipefail
cd "$(dirname "$0")"

if command -v uv >/dev/null 2>&1; then
  exec uv run --quiet "$@"
fi

if [[ -z "${PYTHON_BIN:-}" ]]; then
  if [[ ! -x .venv/bin/python && ! -f .venv/Scripts/python.exe ]]; then
    if command -v python3 >/dev/null 2>&1; then python3 -m venv .venv; else python -m venv .venv; fi
  fi
  if [[ -x .venv/bin/python ]]; then PYTHON_BIN=.venv/bin/python; else PYTHON_BIN=.venv/Scripts/python.exe; fi
fi
if ! "$PYTHON_BIN" -c 'import kafka' >/dev/null 2>&1; then
  "$PYTHON_BIN" -m pip install -q -r requirements.txt
fi
exec "$PYTHON_BIN" "$@"
