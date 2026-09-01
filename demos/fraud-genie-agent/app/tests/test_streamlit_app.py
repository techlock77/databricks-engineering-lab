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
    elif genie_effect == "success":
        genie_patch = """
from src.genie.interface import Citation, GenieResponse
genie = patch(
    "src.genie.interface.genie_query",
    return_value=GenieResponse(
        question="Why was `ACC_M_COLLECTOR` flagged?",
        answer="The account shows concentrated fan-in followed by rapid fan-out.",
        citations=[Citation("gold_evidence", "EVID_001", "Five inbound sources")],
        freshness_note="Evidence as of 2026-06-01 (current).",
        evidence_policy="permissive",
    ),
)
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
    assert len(dataframe_calls) == 4
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


def test_money_flow_dataframe_and_monthly_bar_chart_render_with_real_streamlit():
    app = AppTest.from_string(_app_script()).run(timeout=10)

    assert not app.exception
    assert len(app.dataframe) == 4
    transfers = next(
        dataframe.value
        for dataframe in app.dataframe
        if "txn_id" in dataframe.value.columns
    )
    assert {"month", "amount"} <= set(transfers.columns)
    charts = [
        element
        for element in app._tree
        if element.type in {"vega_lite_chart", "arrow_vega_lite_chart"}
    ]
    assert len(charts) == 1
    assert '"field": "month"' in charts[0].proto.spec
    assert '"field": "amount"' in charts[0].proto.spec


def test_new_tab_structure_kpis_and_documented_genie_questions_render_once():
    app = AppTest.from_string(_app_script()).run(timeout=10)

    assert not app.exception
    assert [tab.label for tab in app.tabs] == [
        "Overview",
        "Investigation",
        "Money Flow",
        "Network",
        "Genie",
        "Reports",
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
    assert risk_metric.delta == "🔴 Flagged for review"
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
        "Money Flow",
        "Network",
        "Genie",
        "Reports",
    ]
    assert len(app.metric) == 6
    assert f"**Question:** {question}" in [item.value for item in app.markdown]
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

    assert "**Question:** Why was this account flagged?" in [
        item.value for item in app.markdown
    ]
    assert [status.label for status in app.status] == ["Genie is investigating..."]


def test_genie_error_becomes_assistant_message_instead_of_crashing():
    app = AppTest.from_string(_app_script("error")).run(timeout=10)

    app.button(key="genie_suggestion_0").click().run(timeout=10)

    assert not app.exception
    rendered_text = [item.value for item in app.markdown]
    assert f"**Question:** {_documented_questions()[0]}" in rendered_text
    assert any("Genie could not complete this question" in text for text in rendered_text)
    assert not app.session_state["is_querying"]


def test_network_graph_and_structured_reports_render():
    app = AppTest.from_string(_app_script()).run(timeout=10)

    assert not app.exception
    graphs = [element for element in app._tree if element.type == "graphviz_chart"]
    assert len(graphs) == 1
    assert "ACC_M_COLLECTOR" in graphs[0].proto.spec
    assert "device_and_fund_flow" in graphs[0].proto.spec
    rendered_text = [item.value for item in app.markdown]
    assert "**Case:** CASE_ACC_M_COLLECTOR" in rendered_text
    assert "**Case Summary**" in rendered_text
    assert "**Evidence**" in rendered_text
    download_buttons = [
        element for element in app._tree if element.type == "download_button"
    ]
    assert [button.proto.label for button in download_buttons] == ["Download case file"]


def test_genie_response_renders_insight_card_evidence_and_freshness():
    app = AppTest.from_string(_app_script("success")).run(timeout=10)

    app.button(key="genie_suggestion_0").click().run(timeout=10)

    assert not app.exception
    rendered_text = [item.value for item in app.markdown]
    assert f"**Question:** {_documented_questions()[0]}" in rendered_text
    assert any("concentrated fan-in" in text for text in rendered_text)
    evidence_expander = next(
        expander for expander in app.expander if expander.label == "View evidence"
    )
    assert evidence_expander.proto.expanded
    assert any("gold_evidence#EVID_001" in text for text in rendered_text)
    assert any("Evidence as of 2026-06-01" in caption.value for caption in app.caption)


def test_policy_toggle_updates_network_graph_scope():
    app = AppTest.from_string(_app_script()).run(timeout=10)
    permissive_graph = next(
        element for element in app._tree if element.type == "graphviz_chart"
    )
    assert "ACC_M_LOOKALIKE" in permissive_graph.proto.spec

    app.radio[0].set_value("strict").run(timeout=10)

    strict_graph = next(element for element in app._tree if element.type == "graphviz_chart")
    assert "ACC_M_LOOKALIKE" not in strict_graph.proto.spec
