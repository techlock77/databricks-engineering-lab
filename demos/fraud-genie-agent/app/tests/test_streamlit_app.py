import ast
from pathlib import Path
import re
import sys

from streamlit.testing.v1 import AppTest

from src.pipeline.orchestrator import run_pipeline


APP_PATH = Path(__file__).parents[1] / "src" / "app" / "app.py"
DEMO_SCRIPT_PATH = Path(__file__).parents[1] / "docs" / "CONTEST_DEMO_SCRIPT.md"


def _app_script(
    genie_effect=None,
    stale=False,
    featured_risk_band=None,
    flagged_account_limit=None,
):
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
genie = patch(
    "src.genie.interface.genie_query",
    side_effect=RuntimeError(
        "Genie query failed (TABLES_MISSING_EXCEPTION): table not found"
    ),
)
"""
    elif genie_effect == "success":
        genie_patch = """
from src.genie.interface import Citation, GenieResponse
def successful_genie_query(question, context):
    st.session_state.query_invocations = st.session_state.get("query_invocations", 0) + 1
    return GenieResponse(
        question="Why was `ACC_M_COLLECTOR` flagged?",
        answer="The account shows concentrated fan-in followed by rapid fan-out.",
        citations=[Citation("gold_evidence", "EVID_001", "Five inbound sources")],
        freshness_note="Evidence as of 2026-06-01 (current).",
        evidence_policy="permissive",
    )
genie = patch(
    "src.genie.interface.genie_query",
    side_effect=successful_genie_query,
)
"""
    else:
        genie_patch = "genie = patch(\"src.genie.interface.genie_query\")"

    context_patch = "context = patch(\"src.genie.interface.GenieContext\", side_effect=AssertionError(\"GenieContext constructed\"))" if genie_effect == "disabled" else """
def capture_context(**kwargs):
    st.session_state.context_seed_account = kwargs["seed_account"]
    return object()
context = patch("src.genie.interface.GenieContext", side_effect=capture_context)
"""
    if genie_effect == "disabled":
        genie_patch = "genie = patch(\"src.genie.interface.genie_query\", side_effect=AssertionError(\"genie_query called\"))"
    return f"""
from unittest.mock import patch
import runpy
import streamlit as st
from src.pipeline.orchestrator import run_pipeline
{genie_patch}
{context_patch}
gold = run_pipeline(seed=42).gold
{"gold['freshness'].loc[:, 'is_stale'] = True" if stale else ""}
{"st.cache_resource.clear()" if stale or featured_risk_band or flagged_account_limit is not None else ""}
{"flagged = gold['accounts'][gold['accounts']['is_flagged_mule_network'].astype(bool)].sort_values('case_total_exposure_permissive', ascending=False); gold['accounts'].loc[gold['accounts']['account_id'] == flagged.iloc[0]['account_id'], 'risk_band'] = " + repr(featured_risk_band) if featured_risk_band else ""}
{"flagged = gold['accounts'][gold['accounts']['is_flagged_mule_network'].astype(bool)].sort_values('case_total_exposure_permissive', ascending=False); keep = set(flagged.head(" + str(flagged_account_limit) + ")['account_id']); gold['accounts'].loc[~gold['accounts']['account_id'].isin(keep), 'is_flagged_mule_network'] = False" if flagged_account_limit is not None else ""}
with patch("src.data_access.load_gold_tables", return_value=gold), genie as genie_mock, context as context_mock:
    runpy.run_path({str(APP_PATH)!r}, run_name="__main__")
st.session_state.genie_mock_calls = genie_mock.call_count
st.session_state.context_mock_calls = context_mock.call_count
"""


def _documented_questions():
    source = DEMO_SCRIPT_PATH.read_text(encoding="utf-8")
    section = source.split(
        "## Concrete questions Genie can answer today (grounded, not hypothetical)", 1
    )[1].split("## Safety notes", 1)[0]
    return re.findall(r"^- (.+)$", section, flags=re.MULTILINE)


def _open_flagged_case(app):
    if app.session_state["top_level_view"] == "home":
        app.button(key="nav_workspace").click().run(timeout=10)
    app.button(key="open_case_ACC_M_COLLECTOR").click().run(timeout=10)
    assert app.session_state["selected_account"] == "ACC_M_COLLECTOR"
    assert app.session_state["top_level_view"] == "workspace"
    return app


def _open_workspace(app):
    app.button(key="nav_workspace").click().run(timeout=10)
    assert app.session_state["top_level_view"] == "workspace"
    return app


def _priority_accounts(gold):
    flagged = gold["accounts"][gold["accounts"]["is_flagged_mule_network"].astype(bool)].copy()
    risk_order = {"critical": 4, "high": 3, "medium": 2, "low": 1}
    flagged["_risk_order"] = (
        flagged["risk_band"].astype(str).str.lower().map(risk_order).fillna(0)
    )
    return flagged.sort_values(
        ["_risk_order", "case_total_exposure_permissive"], ascending=False
    )["account_id"].astype(str).tolist()


def _select_section(app, section):
    app.radio(key="active_section").set_value(section).run(timeout=10)
    assert app.session_state["active_section"] == section
    return app


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
    assert len(dataframe_calls) == 5
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
    _open_flagged_case(app)
    _select_section(app, "Money Flow")

    assert not app.exception
    assert len(app.dataframe) == 1
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


def test_timeline_tab_renders_policy_scoped_transfers_in_date_order():
    app = AppTest.from_string(_app_script()).run(timeout=10)
    _open_flagged_case(app)
    _select_section(app, "Timeline")

    assert not app.exception
    timeline = next(
        dataframe.value
        for dataframe in app.dataframe
        if "txn_date" in dataframe.value.columns
    )
    assert {"txn_date", "txn_id", "source_account", "dest_account", "amount"} <= set(
        timeline.columns
    )
    assert timeline["txn_date"].tolist() == sorted(timeline["txn_date"].tolist())


def test_alert_queue_opens_flagged_account_and_section_navigation_renders():
    app = AppTest.from_string(_app_script()).run(timeout=10)

    assert not app.exception
    app.button(key="nav_workspace").click().run(timeout=10)
    assert [radio.options for radio in app.radio] == [[
        "Overview",
        "Investigation",
        "Money Flow",
        "Network",
        "Timeline",
        "Reports",
    ]]
    assert any("Alert Queue" in item.value for item in app.subheader)
    assert any("8 active alerts" in caption.value for caption in app.caption)
    assert app.button(key="open_case_ACC_M_COLLECTOR").label == "Open case →"

    _open_flagged_case(app)

    assert app.session_state["active_section"] == "Overview"
    assert any("Case: ACC_M_COLLECTOR" in item.value for item in app.subheader)
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
    assert risk_metric.delta == "◆ Flagged for review"
    suggested_buttons = [
        app.button(key=f"genie_suggestion_{index}").label
        for index in range(len(_documented_questions()))
    ]
    assert suggested_buttons == _documented_questions()


def test_landing_hero_is_live_computed_and_disappears_after_opening_case():
    gold = run_pipeline(seed=42).gold
    app = AppTest.from_string(_app_script()).run(timeout=10)
    rendered = [item.value for item in app.markdown]
    expected_stat = (
        f'{len(gold["case_summary"])} scenario cases · {len(gold["accounts"])} accounts · '
        f'{gold["freshness"].iloc[0]["last_refreshed_ts"]} refreshed'
    )
    assert any("Find the network before the money moves" in value for value in rendered)
    assert any(expected_stat in value for value in rendered)
    assert not any('id="ask-genie"' in value for value in rendered)

    app.button(key="open_highest_priority_case").click().run(timeout=10)
    assert not any(
        "Find the network before the money moves" in item.value for item in app.markdown
    )


def test_home_is_default_and_workspace_navigation_is_cleanly_gated():
    app = AppTest.from_string(_app_script()).run(timeout=10)
    assert app.session_state["top_level_view"] == "home"
    assert any(item.value == '<span class="nav-active-home"></span>' for item in app.markdown)
    assert not any(
        item.value == '<span class="nav-active-workspace"></span>' for item in app.markdown
    )
    assert any("Find the network before the money moves" in item.value for item in app.markdown)
    assert any(button.key and button.key.startswith("scenario_chip_") for button in app.button)
    assert not app.radio
    assert [selectbox.key for selectbox in app.selectbox] == ["home_scenario_selector"]
    assert not any("Alert Queue" in item.value for item in app.subheader)
    assert not any("Genie is on this case" in item.value for item in app.subheader)

    _open_workspace(app)
    assert not any(item.value == '<span class="nav-active-home"></span>' for item in app.markdown)
    assert any(
        item.value == '<span class="nav-active-workspace"></span>' for item in app.markdown
    )
    assert any("Alert Queue" in item.value for item in app.subheader)
    assert not any("Find the network before the money moves" in item.value for item in app.markdown)


def test_home_and_workspace_navigation_resume_selected_case():
    app = AppTest.from_string(_app_script()).run(timeout=10)
    app.button(key="open_highest_priority_case").click().run(timeout=10)
    selected_account = app.session_state["selected_account"]
    assert selected_account

    app.button(key="nav_home").click().run(timeout=10)
    assert app.session_state["top_level_view"] == "home"
    assert app.session_state["selected_account"] == selected_account
    assert any("Find the network before the money moves" in item.value for item in app.markdown)

    _open_workspace(app)
    assert app.session_state["selected_account"] == selected_account
    assert any(f"Case: {selected_account}" in item.value for item in app.subheader)


def test_featured_case_copy_uses_real_case_values_and_approved_question():
    gold = run_pipeline(seed=42).gold
    top_account = _priority_accounts(gold)[0]
    top_case = gold["case_summary"][
        gold["case_summary"]["seed_account"].astype(str) == top_account
    ].iloc[0]
    app = AppTest.from_string(_app_script()).run(timeout=10)
    rendered = [item.value for item in app.markdown]
    assert any("Priority · HIGH" in value for value in rendered)
    assert f"**Scenario**\n\n### {top_case.scenario_label}" in rendered
    assert f"**Seed account**\n\n{top_account}" in rendered
    assert f"**Pattern**\n\n{top_case.scenario_label}" in rendered
    assert any("What transfers contribute to the linked exposure?" in value for value in rendered)
    assert app.button(key="open_featured_case").label == "Investigate with Genie →"


def test_featured_priority_badge_is_sourced_from_account_risk_band():
    app = AppTest.from_string(
        _app_script(featured_risk_band="critical")
    ).run(timeout=10)
    assert any("Priority · CRITICAL" in item.value for item in app.markdown)


def test_all_hero_actions_open_the_same_top_priority_queue_account():
    gold = run_pipeline(seed=42).gold
    expected = _priority_accounts(gold)[0]
    for key in ("open_highest_priority_case", "open_featured_case"):
        app = AppTest.from_string(_app_script()).run(timeout=10)
        app.button(key=key).click().run(timeout=10)
        assert app.session_state["selected_account"] == expected
        assert app.session_state["top_level_view"] == "workspace"
        assert any(f"Case: {expected}" in header.value for header in app.subheader)
        assert [metric.label for metric in app.metric] == [
            "Risk band",
            "Linked exposure",
            "Connected accounts",
            "Potential victims",
            "Shared devices",
            "External destinations",
        ]
        assert any("Genie is on this case" in header.value for header in app.subheader)

    genie_app = AppTest.from_string(_app_script("success")).run(timeout=10)
    genie_app.button(key="open_featured_case").click().run(timeout=10)
    genie_app.button(key="genie_suggestion_0").click().run(timeout=10)
    assert genie_app.session_state["context_seed_account"] == expected


def test_scenario_chips_use_one_palette_accent_without_rainbow_semantics():
    app = AppTest.from_string(_app_script()).run(timeout=10)
    css = next(item.value for item in app.markdown if "stMetric" in item.value)
    scenario_css = css.split('.st-key-scenario_row', 1)[1].split('@media', 1)[0]
    assert "nth-child" not in scenario_css
    assert "border: 2px solid #14B8A6" in scenario_css
    for rainbow_color in (
        "#ef4444", "#f97316", "#eab308", "#22c55e", "#06b6d4",
        "#3b82f6", "#8b5cf6", "#ec4899",
    ):
        assert rainbow_color not in scenario_css


def test_scenario_selector_lists_all_cases_and_highlights_next_two():
    gold = run_pipeline(seed=42).gold
    expected = {
        str(row.scenario_label): str(row.seed_account)
        for row in gold["case_summary"].itertuples(index=False)
    }
    app = AppTest.from_string(_app_script()).run(timeout=10)
    assert len(expected) == 9
    selector = app.selectbox(key="home_scenario_selector")
    assert selector.options == list(expected)
    selected_label = list(expected)[3]
    selector.set_value(selected_label).run(timeout=10)
    assert app.session_state["selected_account"] == expected[selected_label]
    assert app.session_state["top_level_view"] == "workspace"

    ranked = _priority_accounts(gold)
    app = AppTest.from_string(_app_script()).run(timeout=10)
    chips = [button for button in app.button if button.key and button.key.startswith("scenario_chip_")]
    assert [button.key for button in chips] == [
        f"scenario_chip_{account}" for account in ranked[1:3]
    ]


def test_scenario_row_handles_no_secondary_flagged_accounts():
    app = AppTest.from_string(
        _app_script(flagged_account_limit=1)
    ).run(timeout=10)
    assert not app.exception
    assert app.selectbox(key="home_scenario_selector").options
    assert not [
        button
        for button in app.button
        if button.key and button.key.startswith("scenario_chip_")
    ]


def test_four_process_cards_render_exact_approved_copy():
    app = AppTest.from_string(_app_script()).run(timeout=10)
    rendered = [item.value for item in app.markdown]
    expected = [
        "**01 -- SELECT THE SIGNAL**",
        "**02 -- INVESTIGATE WITH GENIE**",
        "**03 -- FOLLOW THE MONEY**",
        "**04 -- ASSESS THE IMPACT**",
    ]
    assert all(text in rendered for text in expected)
    writes = [item.value for item in app.markdown]
    descriptions = [
        "Choose a suspicious seed account.",
        "Understand why the activity is unusual.",
        "Discover connected accounts and transaction paths.",
        "Identify potential victims, exposure, and evidence requiring review.",
    ]
    assert all(text in writes for text in descriptions)
    assert any(
        "Every number -- strict or permissive -- comes from the same eight Gold tables"
        in caption.value
        for caption in app.caption
    )


def test_home_kpi_strip_uses_loaded_gold_totals():
    gold = run_pipeline(seed=42).gold
    app = AppTest.from_string(_app_script()).run(timeout=10)
    metrics = {metric.label: metric.value for metric in app.metric}
    assert metrics == {
        "Cases": str(len(gold["case_summary"])),
        "Accounts": str(len(gold["accounts"])),
        "Connections": str(len(gold["_network_edges_raw"])),
        "Exposure / evidence": (
            f"${gold['case_summary']['total_exposure_permissive'].sum():,.2f} · "
            f"{len(gold['evidence'])} items"
        ),
        "Last updated": str(gold["freshness"].iloc[0]["last_refreshed_ts"]),
    }


def test_journey_stage_uses_marker_and_has_selector_for_session_state():
    app = AppTest.from_string(_app_script()).run(timeout=10)
    markers = [item.value for item in app.markdown]
    assert markers.count('<span class="journey-stage-active"></span>') == 1
    assert markers.index('<span class="journey-stage-active"></span>') < markers.index(
        "**01 -- SELECT THE SIGNAL**"
    )
    css = next(item.value for item in app.markdown if "stMetric" in item.value)
    assert ':has(.journey-stage-active)' in css
    assert ".st-key-journey_cards" in css

    app.button(key="open_featured_case").click().run(timeout=10)
    app.button(key="nav_home").click().run(timeout=10)
    markers = [item.value for item in app.markdown]
    assert markers.index('<span class="journey-stage-active"></span>') < markers.index(
        "**02 -- INVESTIGATE WITH GENIE**"
    )


def test_stale_freshness_uses_warning_and_muted_amber_token():
    app = AppTest.from_string(_app_script(stale=True)).run(timeout=10)
    _open_flagged_case(app)
    assert len(app.warning) == 1
    assert "-- STALE." in app.warning[0].value
    assert any(
        item.value == '<span class="freshness-stale-marker"></span>'
        for item in app.markdown
    )
    css = next(item.value for item in app.markdown if "stMetric" in item.value)
    assert "border-color: #C79A43" in css


def test_genie_waiting_state_keeps_page_and_question_visible_and_disables_duplicates():
    app = AppTest.from_string(_app_script("paused")).run(timeout=10)
    _open_workspace(app)
    question = _documented_questions()[0]

    app.button(key="genie_suggestion_0").click().run(timeout=10)

    assert not app.exception
    assert app.session_state["active_section"] == "Overview"
    assert f"**Question:** {question}" in [item.value for item in app.markdown]
    assert [status.label for status in app.status] == ["Genie is investigating..."]
    assert app.chat_input[0].disabled
    assert not any(button.key and button.key.startswith("genie_suggestion_") for button in app.button)


def test_investigate_with_genie_submits_the_approved_question():
    app = AppTest.from_string(_app_script("paused")).run(timeout=10)
    _open_flagged_case(app)

    investigate_button = next(
        button for button in app.button if button.label == "Investigate with Genie"
    )
    investigate_button.click().run(timeout=10)

    assert app.session_state["active_section"] == "Investigation"
    assert any("evidence_id" in dataframe.value.columns for dataframe in app.dataframe)
    assert "**Question:** Why was this account flagged?" in [
        item.value for item in app.markdown
    ]
    assert [status.label for status in app.status] == ["Genie is investigating..."]


def test_genie_error_becomes_assistant_message_instead_of_crashing():
    app = AppTest.from_string(_app_script("error")).run(timeout=10)
    _open_workspace(app)

    app.button(key="genie_suggestion_0").click().run(timeout=10)

    assert not app.exception
    rendered_text = [item.value for item in app.markdown]
    assert f"**Question:** {_documented_questions()[0]}" in rendered_text
    expected_fallback = (
        "Genie could not complete this question. You can continue the "
        "investigation with the KPI strip, Investigation evidence, and "
        "Network connected accounts while the service recovers."
    )
    assert expected_fallback in rendered_text
    details = next(expander for expander in app.expander if expander.label == "Technical details")
    assert not details.proto.expanded
    assert (
        "RuntimeError: Genie query failed (TABLES_MISSING_EXCEPTION): table not found"
        in rendered_text
    )
    assert not app.session_state["is_querying"]


def test_network_graph_and_structured_reports_render():
    app = AppTest.from_string(_app_script()).run(timeout=10)
    _open_flagged_case(app)
    _select_section(app, "Network")

    assert not app.exception
    graphs = [element for element in app._tree if element.type == "graphviz_chart"]
    assert len(graphs) == 1
    assert "ACC_M_COLLECTOR" in graphs[0].proto.spec
    assert "device_and_fund_flow" in graphs[0].proto.spec
    _select_section(app, "Reports")
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
    _open_workspace(app)

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
    _open_flagged_case(app)
    _select_section(app, "Network")
    permissive_graph = next(
        element for element in app._tree if element.type == "graphviz_chart"
    )
    assert "ACC_M_LOOKALIKE" in permissive_graph.proto.spec

    _select_section(app, "Investigation")
    app.radio(key="evidence_policy").set_value("strict").run(timeout=10)
    _select_section(app, "Network")

    strict_graph = next(element for element in app._tree if element.type == "graphviz_chart")
    assert "ACC_M_LOOKALIKE" not in strict_graph.proto.spec


def test_overview_action_buttons_render_the_selected_section_content():
    app = AppTest.from_string(_app_script()).run(timeout=10)
    _open_flagged_case(app)

    next(button for button in app.button if button.label == "Trace Funds").click().run(timeout=10)
    assert app.session_state["active_section"] == "Money Flow"
    assert any("Monthly amount totals" in item.value for item in app.subheader)
    assert any("txn_id" in dataframe.value.columns for dataframe in app.dataframe)

    _select_section(app, "Overview")
    next(
        button for button in app.button if button.label == "View Connected Accounts"
    ).click().run(timeout=10)
    assert app.session_state["active_section"] == "Network"
    assert any("Relationship graph" in item.value for item in app.subheader)
    assert any(element.type == "graphviz_chart" for element in app._tree)


def test_genie_panel_is_persistent_on_every_section():
    app = AppTest.from_string(_app_script()).run(timeout=10)
    _open_flagged_case(app)

    for section in ["Overview", "Investigation", "Money Flow", "Network", "Reports"]:
        _select_section(app, section)
        headers = [item.value for item in app.subheader]
        assert any("Genie is on this case" in header for header in headers)
        assert app.button(key="genie_suggestion_0").label == _documented_questions()[0]


def test_theme_toggle_changes_literal_palette_and_documents_limitation():
    app = AppTest.from_string(_app_script()).run(timeout=10)
    dark_css = next(item.value for item in app.markdown if "stMetric" in item.value)
    assert "#132238" in dark_css and "#E6EDF3" in dark_css
    app.toggle(key="light_mode").set_value(True).run(timeout=10)
    light_css = next(item.value for item in app.markdown if "stMetric" in item.value)
    assert "#F8FAFC" in light_css and "#172033" in light_css
    assert dark_css != light_css
    theme_toggle = app.toggle(key="light_mode")
    assert "Native Streamlit chrome" in theme_toggle.help
    assert "skinned regions -- nav bar, metrics, dataframes" in theme_toggle.help


def test_full_stylesheet_uses_palette_tokens_and_nav_has_mobile_reflow():
    source = APP_PATH.read_text(encoding="utf-8")
    stylesheet = re.search(
        r'"""(?P<css>\s*<style>.*?</style>\s*)"""\.replace',
        source,
        flags=re.DOTALL,
    )
    assert stylesheet
    assert not re.search(r"#[0-9a-fA-F]{3,8}", stylesheet.group("css"))

    nav_css = source.split(".top-nav-brand", 1)[1].split(
        '[data-testid="stColumn"]:has(.hero-link-button-marker)', 1
    )[0]
    assert {"__TEXT__", "__BORDER__", "__SURFACE__", "__SHADOW__"} <= set(
        re.findall(r"__[A-Z_]+__", nav_css)
    )
    assert '<span class="nav-active-home"></span>' in source
    assert '<span class="nav-active-workspace"></span>' in source
    assert '[data-testid="stColumn"]:has(.nav-active-home) button' in nav_css
    assert '[data-testid="stColumn"]:has(.nav-active-workspace) button' in nav_css
    assert "background: __SURFACE__; color: __TEXT__;" in nav_css
    assert "background: __BORDER__; color: __TEXT__;" in nav_css
    assert "align-items: center; gap: 1rem;" in nav_css
    assert "height: 2.75rem; min-height: 2.75rem" in nav_css
    assert "display: flex; align-items: center; justify-content: center;" in nav_css
    assert ".stElementContainer:has(.nav-active-home)" in nav_css
    assert ".stElementContainer:has(.nav-active-workspace)" in nav_css
    assert "width: fit-content; margin-left: auto;" in nav_css
    brand_size = re.search(r"\.top-nav-brand \{[^}]*font-size: ([0-9.]+)rem", nav_css)
    assert brand_size and float(brand_size.group(1)) >= 1.25
    assert "font-weight: 800" in nav_css
    assert ".top-nav-floor-marker" in source
    assert "flex: 1 1 240px" in source


def test_light_mode_css_does_not_override_native_header_color():
    app = AppTest.from_string(_app_script()).run(timeout=10)
    app.toggle(key="light_mode").set_value(True).run(timeout=10)
    css = next(item.value for item in app.markdown if "stMetric" in item.value)
    assert "h2, h3" not in css
    assert "h2" not in css
    assert '[data-testid="stVerticalBlock"]:has(.ask-genie-subheader-marker) h3' in css


def test_kpi_strip_has_narrowly_scoped_reflow_css():
    app = AppTest.from_string(_app_script()).run(timeout=10)
    css = next(item.value for item in app.markdown if "stMetric" in item.value)
    assert ".st-key-kpi_strip [data-testid=\"stHorizontalBlock\"]" in css
    assert "flex: 1 1 160px" in css
    assert "@media (max-width: 768px)" in css


def test_account_selector_resets_chat_and_changes_actual_kpis():
    app = AppTest.from_string(_app_script()).run(timeout=10)
    _open_workspace(app)
    selector = app.selectbox(key="account_selector")
    simple = next(option for option in selector.options if "ACC_M_COLLECTOR" in option)
    selector.set_value(simple).run(timeout=10)
    first_exposure = next(metric.value for metric in app.metric if metric.label == "Linked exposure")
    app.session_state["chat_history"] = [{"role": "assistant", "content": "stale case"}]
    app.session_state["next_suggestions"] = ["stale suggestion"]
    selector = app.selectbox(key="account_selector")
    large = next(option for option in selector.options if "ACC_LARGE_COLLECTOR" in option)
    selector.set_value(large).run(timeout=10)
    second_exposure = next(metric.value for metric in app.metric if metric.label == "Linked exposure")
    assert app.session_state["selected_account"] == "ACC_LARGE_COLLECTOR"
    assert app.session_state["chat_history"] == []
    assert app.session_state["next_suggestions"] == []
    assert first_exposure != second_exposure


def test_non_flagged_selector_label_is_not_duplicated():
    app = AppTest.from_string(_app_script()).run(timeout=10)
    _open_workspace(app)
    control = next(
        option for option in app.selectbox(key="account_selector").options
        if "ACC_C_HUB" in option
    )
    assert control == "Normal account (not flagged) — ACC_C_HUB"
    assert control.count("(not flagged)") == 1


def test_success_replaces_initial_questions_with_hand_authored_followups():
    app = AppTest.from_string(_app_script("success")).run(timeout=10)
    _open_workspace(app)
    initial = [app.button(key=f"genie_suggestion_{i}").label for i in range(9)]
    app.button(key="genie_suggestion_0").click().run(timeout=10)
    followups = [app.button(key=f"genie_suggestion_{i}").label for i in range(3)]
    assert len(followups) == 3
    assert followups != initial[:3]
    assert followups == [_documented_questions()[4], _documented_questions()[1], _documented_questions()[8]]


def test_disabled_genie_never_constructs_context_or_calls_query():
    app = AppTest.from_string(_app_script("disabled")).run(timeout=10)
    _open_workspace(app)
    app.toggle(key="genie_disabled").set_value(True).run(timeout=10)
    assert any(
        "Genie is currently disabled for this demo" in caption.value
        for caption in app.caption
    )
    app.button(key="genie_suggestion_0").click().run(timeout=10)
    assert not app.exception
    assert app.session_state["genie_mock_calls"] == 0
    assert app.session_state["context_mock_calls"] == 0
    assert any("conversational tracing is unavailable" in value.value for value in app.markdown)


def test_restoring_genie_resumes_successful_querying():
    app = AppTest.from_string(_app_script("success")).run(timeout=10)
    _open_workspace(app)
    app.toggle(key="genie_disabled").set_value(True).run(timeout=10)
    app.button(key="genie_suggestion_0").click().run(timeout=10)
    assert app.session_state["genie_mock_calls"] == 0
    app.toggle(key="genie_disabled").set_value(False).run(timeout=10)
    app.button(key="genie_suggestion_0").click().run(timeout=10)
    assert app.session_state["query_invocations"] == 1
    assert any("concentrated fan-in" in item.value for item in app.markdown)
