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
from streamlit.testing.v1 import AppTest

expected = "1.32.0"
assert streamlit.__version__ == expected, (streamlit.__version__, expected)
assert "delta_arrow" not in inspect.signature(streamlit.metric).parameters
assert "default" not in inspect.signature(streamlit.tabs).parameters
assert "key" not in inspect.signature(streamlit.tabs).parameters
assert "disabled" in inspect.signature(streamlit.chat_input).parameters
assert callable(streamlit.status)
assert "hide_index" in inspect.signature(streamlit.dataframe).parameters
assert "use_container_width" in inspect.signature(streamlit.dataframe).parameters
assert "disabled" in inspect.signature(streamlit.button).parameters
assert "page_icon" in inspect.signature(streamlit.set_page_config).parameters
assert "horizontal" in inspect.signature(streamlit.radio).parameters
assert "options" in inspect.signature(streamlit.radio).parameters
assert "key" in inspect.signature(streamlit.radio).parameters
assert "label_visibility" in inspect.signature(streamlit.radio).parameters
assert "border" in inspect.signature(streamlit.container).parameters
assert "gap" in inspect.signature(streamlit.columns).parameters
assert "use_container_width" in inspect.signature(streamlit.graphviz_chart).parameters
assert "x" in inspect.signature(streamlit.bar_chart).parameters
assert "y" in inspect.signature(streamlit.bar_chart).parameters
assert "use_container_width" in inspect.signature(streamlit.bar_chart).parameters
assert list(inspect.signature(streamlit.divider).parameters) == []
assert list(inspect.signature(streamlit.expander).parameters) == ["label", "expanded"]

status_app = AppTest.from_string(
    """
import inspect
import streamlit as st

status_container = st.status("Floor compatibility check")
st.session_state.status_has_update = hasattr(status_container, "update")
st.session_state.status_update_parameters = list(
    inspect.signature(status_container.update).parameters
)
"""
).run()
assert not status_app.exception
assert status_app.session_state["status_has_update"]
assert {"label", "state", "expanded"} <= set(
    status_app.session_state["status_update_parameters"]
)
print(f"Verified Streamlit floor API: {streamlit.__version__}")
PY

cd "$APP_ROOT"
PYTHONDONTWRITEBYTECODE=1 "$FLOOR_VENV/bin/python" -m pytest -q
