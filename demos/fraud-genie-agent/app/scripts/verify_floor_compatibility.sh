#!/usr/bin/env bash
set -euo pipefail

APP_ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
STREAMLIT_FLOOR_VERSION=1.32.0
FLOOR_VENV=$(mktemp -d /tmp/mulegraph-streamlit-floor.XXXXXX)

cleanup() {
  rm -rf -- "$FLOOR_VENV"
}
trap cleanup EXIT

python -m venv "$FLOOR_VENV"
"$FLOOR_VENV/bin/python" -m pip install --quiet --upgrade pip
"$FLOOR_VENV/bin/python" -m pip install \
  --quiet \
  --requirement "$APP_ROOT/requirements.txt" \
  "streamlit==$STREAMLIT_FLOOR_VERSION" \
  pytest

"$FLOOR_VENV/bin/python" - <<'PY'
import inspect
import streamlit

expected = "1.32.0"
assert streamlit.__version__ == expected, (streamlit.__version__, expected)
assert "delta_arrow" not in inspect.signature(streamlit.metric).parameters
assert "default" not in inspect.signature(streamlit.tabs).parameters
assert "key" not in inspect.signature(streamlit.tabs).parameters
assert "disabled" in inspect.signature(streamlit.chat_input).parameters
assert callable(streamlit.status)
print(f"Verified Streamlit floor API: {streamlit.__version__}")
PY

cd "$APP_ROOT"
PYTHONDONTWRITEBYTECODE=1 "$FLOOR_VENV/bin/python" -m pytest -q
