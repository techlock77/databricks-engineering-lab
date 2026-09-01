#!/usr/bin/env bash
set -euo pipefail

APP_ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
STREAMLIT_FLOOR_VERSION=1.32.0
SDK_FLOOR_VERSION=0.66.0
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
  "databricks-sdk==$SDK_FLOOR_VERSION" \
  pytest

"$FLOOR_VENV/bin/python" - <<'PY'
import inspect
import importlib.metadata
import streamlit
from databricks.sdk.errors import OperationFailed
from databricks.sdk.service import dashboards
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
assert "key" in inspect.signature(streamlit.toggle).parameters
assert "disabled" in inspect.signature(streamlit.toggle).parameters
assert "key" in inspect.signature(streamlit.selectbox).parameters
assert "on_change" in inspect.signature(streamlit.selectbox).parameters
assert "page_icon" in inspect.signature(streamlit.set_page_config).parameters
assert "horizontal" in inspect.signature(streamlit.radio).parameters
assert "options" in inspect.signature(streamlit.radio).parameters
assert "key" in inspect.signature(streamlit.radio).parameters
assert "label_visibility" in inspect.signature(streamlit.radio).parameters
assert "border" in inspect.signature(streamlit.container).parameters
# container(key=...) arrived after the declared floor; app.py signature-gates it.
assert "key" not in inspect.signature(streamlit.container).parameters
assert "gap" in inspect.signature(streamlit.columns).parameters
assert "unsafe_allow_html" in inspect.signature(streamlit.markdown).parameters
assert "use_container_width" in inspect.signature(streamlit.graphviz_chart).parameters
assert "x" in inspect.signature(streamlit.bar_chart).parameters
assert "y" in inspect.signature(streamlit.bar_chart).parameters
assert "use_container_width" in inspect.signature(streamlit.bar_chart).parameters
assert list(inspect.signature(streamlit.divider).parameters) == []
assert list(inspect.signature(streamlit.expander).parameters) == ["label", "expanded"]

sdk_expected = "0.66.0"
assert importlib.metadata.version("databricks-sdk") == sdk_expected
assert OperationFailed.__module__ == "databricks.sdk.errors.sdk"
assert list(inspect.signature(dashboards.GenieAPI.get_message).parameters) == [
    "self", "space_id", "conversation_id", "message_id"
]
assert "error" in dashboards.GenieMessage.__annotations__
assert set(dashboards.MessageError.__annotations__) == {"error", "type"}
assert dashboards.MessageErrorType.TABLES_MISSING_EXCEPTION.value == "TABLES_MISSING_EXCEPTION"

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
print(f"Verified Databricks SDK floor API: {sdk_expected}")
PY

cd "$APP_ROOT"
PYTHONDONTWRITEBYTECODE=1 "$FLOOR_VENV/bin/python" -m pytest -q
