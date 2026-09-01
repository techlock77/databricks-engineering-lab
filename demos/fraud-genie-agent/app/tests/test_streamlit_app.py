import ast
from pathlib import Path
import re
import sys

from streamlit.testing.v1 import AppTest


APP_PATH = Path(__file__).parents[1] / "src" / "app" / "app.py"
DEMO_SCRIPT_PATH = Path(__file__).parents[1] / "docs" / "CONTEST_DEMO_SCRIPT.md"


def _app_script(genie_effect=None):
    genie_patch = ""
    if genie_effect == "paused":
        genie_patch = """
import streamlit as st
def test_genie_query(question, context):
    # Freeze execution at the same point as a slow SDK call so AppTest can
    # inspect the deltas already rendered to the browser.
    st.stop()
genie = patch("src.genie.interface.genie_query", side_effect=test_genie_query)
"""
    elif genie_effect == "error":
        genie_patch = """
genie = patch("src.genie.interface.genie_query", side_effect=RuntimeError("Genie unavailable"))
"""
    else:
        genie_patch = "genie = patch(\"src.genie.interface.genie_query\")"

    return f"""
from unittest.mock import patch
import runpy
from src.pipeline.orchestrator import run_pipeline
{genie_patch}
gold = run_pipeline(seed=42).gold
with patch("src.data_access.load_gold_tables", return_value=gold), genie:
    runpy.run_path({str(APP_PATH)!r}, run_name="__main__")
"""


def _documented_questions():
    source = DEMO_SCRIPT_PATH.read_text(encoding="utf-8")
    section = source.split(
        "## Concrete questions Genie can answer today (grounded, not hypothetical)", 1
    )[1].split("## Safety notes", 1)[0]
    return re.findall(r"^- (.+)$", section, flags=re.MULTILINE)


def test_path_bootstrap_works_without_file(monkeypatch):
    app_root = APP_PATH.parents[2]
    monkeypatch.chdir(app_root)
    source = APP_PATH.read_text(encoding="utf-8")
    bootstrap_source = source.split("import streamlit as st", maxsplit=1)[0]
    namespace = {"__builtins__": __builtins__}

    original_path = sys.path.copy()
    try:
        exec(compile(bootstrap_source, "manual-notebook-cell", "exec"), namespace)
        assert namespace["_ROOT"] == app_root.resolve()
        assert str(app_root.resolve()) in sys.path
    finally:
        sys.path[:] = original_path


def test_app_uses_cross_version_dataframe_width_api():
    source = APP_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    string_widths = [
        keyword.value.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        for keyword in node.keywords
        if keyword.arg == "width"
        and isinstance(keyword.value, ast.Constant)
        and isinstance(keyword.value.value, str)
    ]

    assert string_widths == []
    dataframe_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "dataframe"
    ]
    assert len(dataframe_calls) == 3
    assert all(
        any(keyword.arg == "use_container_width" for keyword in call.keywords)
        for call in dataframe_calls
    )


def test_page_config_sets_investigation_icon():
    tree = ast.parse(APP_PATH.read_text(encoding="utf-8"))
    page_config_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "set_page_config"
    ]

    assert len(page_config_calls) == 1
    page_icon = next(
        keyword.value
        for keyword in page_config_calls[0].keywords
        if keyword.arg == "page_icon"
    )
    assert isinstance(page_icon, ast.Constant)
    assert page_icon.value == "🔎"


def test_evidence_tab_and_other_dataframes_render_with_real_streamlit():
    app = AppTest.from_string(_app_script()).run(timeout=10)

    assert not app.exception
    assert len(app.dataframe) == 3


def test_new_tab_structure_kpis_and_documented_genie_questions_render_once():
    app = AppTest.from_string(_app_script()).run(timeout=10)

    assert not app.exception
    assert [tab.label for tab in app.tabs] == [
        "Overview",
        "Investigation",
        "Network",
        "Ask Genie",
    ]
    assert [metric.label for metric in app.metric] == [
        "Risk band",
        "Linked exposure",
        "Connected accounts",
        "Potential victims",
        "Shared devices",
        "External destinations",
    ]
    risk_metric = app.metric[0]
    assert risk_metric.value == "HIGH"
    assert risk_metric.delta == "Flagged for review"
    suggested_buttons = [
        app.button(key=f"genie_suggestion_{index}").label
        for index in range(len(_documented_questions()))
    ]
    assert suggested_buttons == _documented_questions()


def test_genie_waiting_state_keeps_page_and_question_visible_and_disables_duplicates():
    app = AppTest.from_string(_app_script("paused")).run(timeout=10)
    question = _documented_questions()[0]

    app.button(key="genie_suggestion_0").click().run(timeout=10)

    assert not app.exception
    assert [tab.label for tab in app.tabs] == [
        "Overview",
        "Investigation",
        "Network",
        "Ask Genie",
    ]
    assert len(app.metric) == 6
    assert question in [item.value for item in app.markdown]
    assert [status.label for status in app.status] == ["Genie is investigating..."]
    assert app.chat_input[0].disabled
    assert all(
        app.button(key=f"genie_suggestion_{index}").disabled
        for index in range(len(_documented_questions()))
    )


def test_investigate_with_genie_submits_the_approved_question():
    app = AppTest.from_string(_app_script("paused")).run(timeout=10)

    investigate_button = next(
        button for button in app.button if button.label == "Investigate with Genie"
    )
    investigate_button.click().run(timeout=10)

    assert "Why was this account flagged?" in [item.value for item in app.markdown]
    assert [status.label for status in app.status] == ["Genie is investigating..."]


def test_genie_error_becomes_assistant_message_instead_of_crashing():
    app = AppTest.from_string(_app_script("error")).run(timeout=10)

    app.button(key="genie_suggestion_0").click().run(timeout=10)

    assert not app.exception
    rendered_text = [item.value for item in app.markdown]
    assert _documented_questions()[0] in rendered_text
    assert any("Genie could not complete this question" in text for text in rendered_text)
    assert not app.session_state["is_querying"]
