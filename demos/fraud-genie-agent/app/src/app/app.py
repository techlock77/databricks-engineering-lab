"""MuleGraph Investigator -- Streamlit app.

All filtering/view logic lives in src.pipeline.views (plain functions, no
Streamlit calls) so it is unit-testable without booting the app. This
module only wires those functions to widgets.
"""

from __future__ import annotations

import inspect
import sys
from pathlib import Path


def _resolve_project_root() -> Path:
    """Locate the app root for script and notebook-style execution."""
    candidates = []
    script_file = globals().get("__file__")
    if script_file:
        candidates.append(Path(script_file).resolve().parents[2])
    candidates.append(Path.cwd().resolve())

    attempted = []
    for candidate in candidates:
        if candidate in attempted:
            continue
        attempted.append(candidate)
        if (candidate / "src" / "app" / "app.py").is_file():
            return candidate

    locations = ", ".join(str(path) for path in attempted)
    raise RuntimeError(
        "Unable to locate the MuleGraph app root. Expected to find "
        f"src/app/app.py under one of: {locations}. When executing manually, "
        "change the working directory to demos/fraud-genie-agent/app first; "
        "for the deployed app, launch with `streamlit run src/app/app.py`."
    )


_ROOT = _resolve_project_root()
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import streamlit as st

from src.genie.export import build_case_export
from src.genie.interface import GenieContext, genie_query
from src.data_access import load_gold_tables
from src.pipeline import policy, views

st.set_page_config(page_title="MuleGraph Investigator", page_icon="🔎", layout="wide")


GENIE_QUESTIONS = [
    "Why was `ACC_M_COLLECTOR` flagged?",
    "How many inbound sources / outbound destinations does it have?",
    "Compare permissive vs. strict exposure, connected accounts, shared devices.",
    "Which evidence disappears under the strict policy?",
    "Which accounts are connected under the selected policy?",
    "What transfers contribute to the linked exposure?",
    "Why are control-cohort accounts not flagged?",
    "How fresh is this data?",
    "What would we have missed investigating only the original transaction?",
]
GENIE_INVESTIGATION_QUESTION = "Why was this account flagged?"
SECTION_NAMES = ["Overview", "Investigation", "Money Flow", "Network", "Reports"]
ALL_ALERTS = "◀ All alerts"
FOLLOW_UP_QUESTIONS = {
    GENIE_QUESTIONS[0]: [GENIE_QUESTIONS[4], GENIE_QUESTIONS[1], GENIE_QUESTIONS[8]],
    GENIE_QUESTIONS[1]: [GENIE_QUESTIONS[5], GENIE_QUESTIONS[4], GENIE_QUESTIONS[2]],
    GENIE_QUESTIONS[2]: [GENIE_QUESTIONS[3], GENIE_QUESTIONS[4], GENIE_QUESTIONS[5]],
    GENIE_INVESTIGATION_QUESTION: [GENIE_QUESTIONS[4], GENIE_QUESTIONS[1], GENIE_QUESTIONS[8]],
}


@st.cache_resource(show_spinner="Pulling the latest case data from the Gold tables...")
def _load_gold_tables():
    return load_gold_tables()


def _init_session_state() -> None:
    if "evidence_policy" not in st.session_state:
        st.session_state.evidence_policy = policy.DEFAULT_POLICY
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []
    if "is_querying" not in st.session_state:
        st.session_state.is_querying = False
    if "pending_question" not in st.session_state:
        st.session_state.pending_question = None
    if "active_section" not in st.session_state:
        st.session_state.active_section = "Overview"
    if "selected_account" not in st.session_state:
        st.session_state.selected_account = None
    if "suggestions_visible" not in st.session_state:
        st.session_state.suggestions_visible = True
    if "next_suggestions" not in st.session_state:
        st.session_state.next_suggestions = []
    if "genie_disabled" not in st.session_state:
        st.session_state.genie_disabled = False
    if "light_mode" not in st.session_state:
        st.session_state.light_mode = False
    if "top_level_view" not in st.session_state:
        st.session_state.top_level_view = "home"


def _reset_evidence_policy() -> None:
    st.session_state.evidence_policy = policy.DEFAULT_POLICY


def _select_section(section_name: str) -> None:
    st.session_state.active_section = section_name


def _select_top_level_view(view_name: str) -> None:
    st.session_state.top_level_view = view_name


def _open_case(account_id: str) -> None:
    st.session_state.top_level_view = "workspace"
    if st.session_state.selected_account != account_id:
        _reset_investigation(account_id)
    st.session_state.account_selector = next(
        (label for label, value in st.session_state.account_option_ids.items() if value == account_id),
        account_id,
    )
    st.session_state.active_section = "Overview"


def _reset_investigation(account_id: str | None) -> None:
    st.session_state.selected_account = account_id
    st.session_state.chat_history = []
    st.session_state.pending_question = None
    st.session_state.is_querying = False
    st.session_state.suggestions_visible = True
    st.session_state.next_suggestions = []


def _select_account() -> None:
    value = st.session_state.account_option_ids[st.session_state.account_selector]
    _reset_investigation(value)
    st.session_state.active_section = "Overview"


def _queue_genie_question(question: str) -> None:
    if st.session_state.is_querying:
        return
    st.session_state.chat_history.append({"role": "user", "content": question})
    st.session_state.pending_question = question
    st.session_state.is_querying = True
    st.session_state.suggestions_visible = False


def _investigate_with_genie() -> None:
    st.session_state.active_section = "Investigation"
    _queue_genie_question(GENIE_INVESTIGATION_QUESTION)


def render_freshness_banner(gold: dict) -> None:
    row = gold["freshness"].iloc[0]
    staleness = "STALE" if row["is_stale"] else "current"
    st.info(
        f"Evidence refreshed as of {row['last_refreshed_ts']} "
        f"(freshness contract: {row['freshness_contract_hours']}h) -- {staleness}."
    )


def render_case_header(gold: dict, seed_account: str) -> None:
    accounts = gold["accounts"]
    row = accounts[accounts["account_id"] == seed_account].iloc[0]
    band = row["risk_band"]
    flagged = bool(row["is_flagged_mule_network"])
    # `framing` is derived directly from the same flag that produced `band`
    # (see policy.risk_band_for), so the two can never say different things.
    framing = "Flagged for investigator review" if flagged else "Not flagged"
    st.title("MuleGraph Investigator")
    st.subheader(f"Case: {seed_account}")
    st.caption(f"Risk band: {band} -- {framing}")


def render_evidence_tab(gold: dict, seed_account: str, evidence_policy: str) -> None:
    st.caption(f"Evidence policy: {evidence_policy}")
    evidence = views.case_evidence(gold, seed_account, evidence_policy)
    if evidence.empty:
        st.info("No evidence meets the selected policy for this case.")
    st.dataframe(evidence, use_container_width=True, hide_index=True)


def render_kpi_strip(gold: dict, seed_account: str, metrics: dict) -> None:
    account = gold["accounts"][gold["accounts"]["account_id"] == seed_account].iloc[0]
    flagged = bool(account["is_flagged_mule_network"])
    container_kwargs = {"key": "kpi_strip"} if "key" in inspect.signature(st.container).parameters else {}
    with st.container(**container_kwargs):
        st.markdown('<span class="kpi-strip-floor-marker"></span>', unsafe_allow_html=True)
        columns = st.columns(6)
    metric_parameters = inspect.signature(st.metric).parameters
    risk_metric_kwargs = {
        "delta": "🔴 Flagged for review" if flagged else "🟢 Not flagged",
        # Named delta colors were added after the declared Streamlit floor.
        "delta_color": "off",
    }
    if "delta_arrow" in metric_parameters:
        risk_metric_kwargs["delta_arrow"] = "off"
    columns[0].metric(
        "Risk band",
        str(account["risk_band"]).upper(),
        **risk_metric_kwargs,
    )
    columns[1].metric("Linked exposure", f"${metrics['total_exposure']:,.2f}")
    columns[2].metric("Connected accounts", metrics["other_connected_accounts_count"])
    columns[3].metric("Potential victims", metrics["potential_victims_count"])
    columns[4].metric("Shared devices", metrics["shared_device_count"])
    columns[5].metric("External destinations", metrics["destinations_count"])


def render_connected_accounts_tab(gold: dict, seed_account: str, evidence_policy: str) -> None:
    st.caption(f"Evidence policy: {evidence_policy}")
    table = views.connected_accounts_table(gold, seed_account, evidence_policy)
    st.dataframe(table, use_container_width=True, hide_index=True)


def render_control_cohort_tab(gold: dict) -> None:
    st.write(
        "Legitimate-remittance control cohort: the same fan-in/fan-out shape as the "
        "mule network above, protected from being flagged by the recurring-corridor "
        "override (long account tenure and a long-running monthly corridor)."
    )
    st.dataframe(gold["control_cohort"], use_container_width=True, hide_index=True)


def render_control_comparison(comparison: dict) -> None:
    st.caption(
        "Case vs. control median: "
        f"{comparison['case_tenure_days']} vs. "
        f"{comparison['control_median_tenure_days']} tenure days; "
        f"{comparison['case_outbound_months']} vs. "
        f"{comparison['control_median_outbound_months']} outbound months."
    )


def render_money_flow_tab(gold: dict, seed_account: str, evidence_policy: str) -> None:
    st.caption(f"Evidence policy: {evidence_policy}")
    transfers = views.case_transfers(gold, seed_account, evidence_policy)
    st.dataframe(transfers, use_container_width=True, hide_index=True)
    monthly_totals = (
        transfers.groupby("month", as_index=False)["amount"].sum().sort_values("month")
    )
    st.subheader("Monthly amount totals")
    st.bar_chart(
        monthly_totals,
        x="month",
        y="amount",
        use_container_width=True,
    )


def _graphviz_dot(edges) -> str:
    lines = ["graph mulegraph {", '  graph [bgcolor="transparent"];']
    for row in edges.itertuples(index=False):
        amount = f"${row.amount:,.2f}" if row.amount else "$0.00"
        label = f"{row.edge_type} | {amount}"
        lines.append(f'  "{row.account_a}" -- "{row.account_b}" [label="{label}"];')
    lines.append("}")
    return "\n".join(lines)


def render_network_graph(gold: dict, seed_account: str, evidence_policy: str) -> None:
    network = views.compute_network(gold, seed_account, evidence_policy)
    st.subheader("Relationship graph")
    st.graphviz_chart(_graphviz_dot(network.edges), use_container_width=True)


def _render_chat_history() -> None:
    for turn in st.session_state.chat_history:
        with st.chat_message(turn["role"]):
            if turn["role"] == "user":
                st.markdown(f"**Question:** {turn['content']}")
            else:
                st.write(turn["content"])
            citations = turn.get("citations", [])
            if citations:
                with st.expander("View evidence", expanded=True):
                    for citation in citations:
                        st.write(
                            f"{citation.source_table}#{citation.source_row_id}: "
                            f"{citation.text}"
                        )
            if turn.get("freshness"):
                st.caption(turn["freshness"])
            if turn.get("error_detail"):
                with st.expander("Technical details", expanded=False):
                    st.write(turn["error_detail"])


def render_genie_chat(gold: dict, seed_account: str) -> None:
    st.subheader("Ask Genie")
    st.caption("Try a grounded question about this case:")
    if st.session_state.suggestions_visible:
        suggestions = st.session_state.next_suggestions or GENIE_QUESTIONS
        for index, suggested_question in enumerate(suggestions):
            st.button(suggested_question, key=f"genie_suggestion_{index}",
                      on_click=_queue_genie_question, args=(suggested_question,),
                      use_container_width=True)

    _render_chat_history()

    question = st.chat_input(
        "Ask a question about this case", disabled=st.session_state.is_querying
    )
    if question:
        _queue_genie_question(question)
        st.rerun()

    pending_question = st.session_state.pending_question
    if pending_question:
        if st.session_state.genie_disabled:
            st.session_state.chat_history.append({"role": "assistant", "content": (
                "Genie conversational tracing is unavailable while the dependency is disabled. "
                "The KPI strip, Investigation, Money Flow, Network, and Reports remain live.")})
            st.session_state.pending_question = None
            st.session_state.is_querying = False
            st.session_state.suggestions_visible = True
            st.session_state.next_suggestions = []
            st.rerun()
        with st.chat_message("assistant"):
            status = st.status("Genie is investigating...", expanded=True)

        context = GenieContext(
            gold=gold, seed_account=seed_account, evidence_policy=st.session_state.evidence_policy
        )
        try:
            response = genie_query(pending_question, context)
            st.session_state.chat_history.append(
                {
                    "role": "assistant",
                    "content": response.answer,
                    "citations": response.citations,
                    "freshness": response.freshness_note,
                }
            )
            status.update(label="Genie investigation complete", state="complete", expanded=False)
            st.session_state.next_suggestions = FOLLOW_UP_QUESTIONS.get(
                pending_question, GENIE_QUESTIONS
            )
            st.session_state.suggestions_visible = True
        except Exception as exc:
            error_detail = f"{type(exc).__name__}: {exc}"
            print(f"Genie query failed: {exc!r}", file=sys.stderr)
            st.session_state.chat_history.append(
                {
                    "role": "assistant",
                    "content": (
                        "Genie could not complete this question. You can continue the "
                        "investigation with the KPI strip, Investigation evidence, and "
                        "Network connected accounts while the service recovers."
                    ),
                    "error_detail": error_detail,
                }
            )
            status.update(label="Genie is temporarily unavailable", state="error", expanded=True)
            st.session_state.suggestions_visible = True
            st.session_state.next_suggestions = []
        finally:
            st.session_state.pending_question = None
            st.session_state.is_querying = False
        st.rerun()


def _sorted_flagged_accounts(gold: dict):
    """Return the alert queue in its single canonical priority order."""
    flagged = gold["accounts"][gold["accounts"]["is_flagged_mule_network"].astype(bool)].copy()
    risk_order = {"critical": 4, "high": 3, "medium": 2, "low": 1}
    flagged["_risk_order"] = flagged["risk_band"].astype(str).str.lower().map(risk_order).fillna(0)
    exposure_column = next(
        (
            name
            for name in (
                "case_total_exposure_permissive",
                "total_exposure",
                "linked_exposure",
                "exposure",
            )
            if name in flagged
        ),
        None,
    )
    sort_columns = ["_risk_order"] + ([exposure_column] if exposure_column else [])
    return flagged.sort_values(sort_columns, ascending=False)


def _case_summary_row(gold: dict, account_id: str):
    return gold["case_summary"][
        gold["case_summary"]["seed_account"].astype(str) == account_id
    ].iloc[0]


def render_landing_hero(gold: dict, flagged) -> None:
    top_account = str(flagged.iloc[0]["account_id"])
    top_case = _case_summary_row(gold, top_account)
    container_kwargs = (
        {"key": "hero_card"} if "key" in inspect.signature(st.container).parameters else {}
    )
    with st.container(**container_kwargs):
        st.markdown('<span class="hero-card-floor-marker"></span>', unsafe_allow_html=True)
        introduction, featured = st.columns([3, 2], gap="large")
        with introduction:
            st.markdown(
                '<p class="hero-eyebrow">FRAUD DETECTION · MULEGRAPH</p>'
                '<h1 class="hero-headline">Find the network before the money moves</h1>'
                '<p class="hero-subheadline">Turn a prioritized alert into a grounded, '
                'network-wide investigation before suspicious funds disappear.</p>',
                unsafe_allow_html=True,
            )
            refreshed = gold["freshness"].iloc[0]["last_refreshed_ts"]
            st.markdown(
                f'<p class="hero-stat">{len(gold["case_summary"])} scenario cases · '
                f'{len(gold["accounts"])} accounts · {refreshed} refreshed</p>',
                unsafe_allow_html=True,
            )
            primary, secondary = st.columns(2)
            primary.button(
                "Open highest-priority case →",
                key="open_highest_priority_case",
                on_click=_open_case,
                args=(top_account,),
                use_container_width=True,
            )
            secondary.markdown(
                '<span class="hero-link-button-marker"></span>',
                unsafe_allow_html=True,
            )
            secondary.button(
                "Ask Genie a question →",
                key="ask_genie_top_case",
                on_click=_open_case,
                args=(top_account,),
                use_container_width=True,
            )
        with featured:
            with st.container(border=True):
                st.markdown("**INVESTIGATION CASE**")
                st.markdown(f"### {top_case.scenario_label}")
                st.markdown(f"Suspicious Account: {top_account}")
                st.markdown(f"Potential Pattern: {top_case.scenario_label}")
                st.markdown("**INVESTIGATOR'S QUESTION**")
                st.markdown(GENIE_QUESTIONS[5])
                st.button(
                    "Investigate with Genie →",
                    key="open_featured_case",
                    on_click=_open_case,
                    args=(top_account,),
                    use_container_width=True,
                )


def render_scenario_row(gold: dict) -> None:
    container_kwargs = (
        {"key": "scenario_row"}
        if "key" in inspect.signature(st.container).parameters
        else {}
    )
    with st.container(**container_kwargs):
        st.markdown('<span class="scenario-row-floor-marker"></span>', unsafe_allow_html=True)
        columns = st.columns(len(gold["case_summary"]))
        for column, row in zip(columns, gold["case_summary"].itertuples(index=False)):
            column.button(
                str(row.scenario_label),
                key=f"scenario_chip_{row.scenario_type}",
                on_click=_open_case,
                args=(str(row.seed_account),),
                use_container_width=True,
            )


def render_process_cards() -> None:
    cards = [
        (
            "01 -- SELECT THE SIGNAL",
            "Choose a suspicious seed account.",
        ),
        (
            "02 -- INVESTIGATE WITH GENIE",
            "Understand why the activity is unusual.",
        ),
        (
            "03 -- FOLLOW THE MONEY",
            "Discover connected accounts and transaction paths.",
        ),
        (
            "04 -- ASSESS THE IMPACT",
            "Identify potential victims, exposure, and evidence requiring review.",
        ),
    ]
    columns = st.columns(4)
    for column, (title, description) in zip(columns, cards):
        with column.container(border=True):
            st.markdown(f"**{title}**")
            st.write(description)
    st.caption(
        "Every number -- strict or permissive -- comes from the same eight Gold tables, "
        "so the UI, the export, and Genie can never quietly disagree."
    )


def render_alert_queue(gold: dict, flagged=None) -> None:
    flagged = _sorted_flagged_accounts(gold) if flagged is None else flagged

    alert_count = len(flagged)
    st.subheader("Alert Queue")
    st.caption(f"{alert_count} active alert{'s' if alert_count != 1 else ''}")
    for row in flagged.itertuples(index=False):
        account_id = str(row.account_id)
        with st.container(border=True):
            details, action = st.columns([3, 1])
            details.markdown(f"**{account_id}**")
            details.caption(f"Risk band: {str(row.risk_band).upper()}")
            action.button(
                "Open case →",
                key=f"open_case_{account_id}",
                on_click=_open_case,
                args=(account_id,),
                use_container_width=True,
            )


def render_export_button(bundle, evidence_policy: str) -> None:
    st.caption(
        f"Export ready: {bundle.item_count} citation-backed item(s) under the "
        f"{evidence_policy} policy."
    )
    export_text = "\n".join(f"[{item.source_table}#{item.source_row_id}] {item.text}" for item in bundle.items)
    st.download_button(
        "Download case file",
        data=export_text,
        file_name=f"{bundle.case_id}_{evidence_policy}.txt",
    )


def render_reports_tab(
    gold: dict, seed_account: str, evidence_policy: str, metrics: dict
) -> None:
    bundle = build_case_export(gold, seed_account, evidence_policy)
    st.subheader("Case file")
    st.write(f"**Case:** {bundle.case_id}")
    st.caption(
        f"Seed account: {bundle.seed_account} · Evidence policy: {bundle.evidence_policy}"
    )
    st.subheader("KPI recap")
    st.write(
        f"Linked exposure: ${metrics['total_exposure']:,.2f} · "
        f"Connected accounts: {metrics['other_connected_accounts_count']} · "
        f"Potential victims: {metrics['potential_victims_count']} · "
        f"Shared devices: {metrics['shared_device_count']}"
    )
    st.subheader("Export evidence")
    item_types = sorted({item.item_type for item in bundle.items})
    for item_type in item_types:
        st.write(f"**{item_type.replace('_', ' ').title()}**")
        for item in (item for item in bundle.items if item.item_type == item_type):
            st.write(f"{item.source_table}#{item.source_row_id}: {item.text}")
    render_export_button(bundle, evidence_policy)


def main() -> None:
    _init_session_state()
    nav_kwargs = {"key": "top_nav"} if "key" in inspect.signature(st.container).parameters else {}
    with st.container(**nav_kwargs):
        st.markdown('<span class="top-nav-floor-marker"></span>', unsafe_allow_html=True)
        brand, destinations, badge = st.columns([2, 3, 2])
        brand.markdown(
            '<div class="top-nav-brand">🔎 MuleGraph Investigator'
            '<span>Follow the money. Uncover the network.</span></div>',
            unsafe_allow_html=True,
        )
        home, workspace = destinations.columns(2)
        if st.session_state.top_level_view == "home":
            home.markdown('<span class="nav-active-home"></span>', unsafe_allow_html=True)
        home.button("Home", key="nav_home", on_click=_select_top_level_view,
                    args=("home",), use_container_width=True)
        if st.session_state.top_level_view == "workspace":
            workspace.markdown(
                '<span class="nav-active-workspace"></span>', unsafe_allow_html=True
            )
        workspace.button("Investigation Workspace", key="nav_workspace",
                         on_click=_select_top_level_view, args=("workspace",),
                         use_container_width=True)
        badge.markdown(
            '<span class="top-nav-badge">⚡ Powered by Databricks Genie</span>',
            unsafe_allow_html=True,
        )
    st.toggle("Light mode", key="light_mode")
    st.caption(
        "Light mode flips only the app's skinned regions -- nav bar, metrics, dataframes, "
        "expander borders/headers, radio pills, and chat messages. "
        "Native Streamlit chrome remains governed by config.toml base=\"dark\" and will not fully flip."
    )
    dark = {"metric_a": "#132238", "metric_b": "#0B1220", "surface": "#132238",
            "text": "#E6EDF3", "border": "#14B8A6", "shadow": "#000000"}
    light = {"metric_a": "#F8FAFC", "metric_b": "#E2E8F0", "surface": "#FFFFFF",
             "text": "#172033", "border": "#0F766E", "shadow": "#64748B"}
    palette = light if st.session_state.light_mode else dark
    st.markdown(
        """
        <style>
        [data-testid="stMetric"] {
            background: linear-gradient(145deg, __METRIC_A__, __METRIC_B__);
            border: 1px solid __BORDER__; color: __TEXT__;
            border-radius: 0.75rem;
            padding: 0.9rem 1rem;
        }
        [data-testid="stDataFrame"], [data-testid="stExpander"], .stChatMessage {
            box-shadow: 0 8px 24px __SHADOW__;
            border: 1px solid __BORDER__; background: __SURFACE__; color: __TEXT__;
            border-radius: 0.75rem;
        }
        [data-testid="stRadio"] > label { display: none; }
        [data-testid="stRadio"] [role="radiogroup"] {
            gap: 0.35rem;
            margin: 0.75rem 0;
        }
        [data-testid="stRadio"] [role="radio"] {
            position: absolute;
            opacity: 0;
            pointer-events: none;
        }
        [data-testid="stRadio"] [role="radio"] + div {
            border: 1px solid __BORDER__;
            border-radius: 999px;
            padding: 0.45rem 0.9rem;
            background: __SURFACE__; color: __TEXT__;
        }
        [data-testid="stRadio"] [role="radio"][aria-checked="true"] + div {
            border-color: __BORDER__; background: __METRIC_B__; color: __TEXT__;
        }
        .stChatMessage {
            padding: 0.25rem;
        }
        .st-key-hero_card, [data-testid="stVerticalBlock"]:has(.hero-card-floor-marker) {
            border: 1px solid __BORDER__; background: __SURFACE__; color: __TEXT__;
            border-radius: 0.75rem; box-shadow: 0 8px 24px __SHADOW__;
            padding: 1.25rem;
        }
        .hero-eyebrow { color: __BORDER__; font-size: 0.75rem; font-weight: 700;
            letter-spacing: 0.08em; margin: 0 0 0.65rem; }
        .hero-headline { color: __TEXT__; font-size: clamp(2.25rem, 5vw, 4.5rem);
            font-weight: 800; letter-spacing: -0.04em; line-height: 0.98; margin: 0; }
        .hero-subheadline { color: __TEXT__; opacity: 0.72; font-size: 1.05rem;
            margin: 1rem 0 0.65rem; max-width: 44rem; }
        .hero-stat { color: __TEXT__; opacity: 0.78; font-size: 0.85rem; }
        .top-nav-brand { color: __TEXT__; font-weight: 700; }
        .top-nav-brand span { display: block; color: __TEXT__; opacity: 0.72;
            font-size: 0.8rem; font-weight: 400; }
        .top-nav-badge { display: inline-block; border: 1px solid __BORDER__;
            border-radius: 999px; padding: 0.45rem 0.8rem; background: __SURFACE__;
            color: __TEXT__; box-shadow: 0 8px 24px __SHADOW__; opacity: 0.78; }
        .st-key-top_nav button,
        [data-testid="stVerticalBlock"]:has(.top-nav-floor-marker) button {
            border: 1px solid __BORDER__; border-radius: 999px;
            padding: 0.45rem 0.9rem; background: __SURFACE__; color: __TEXT__;
        }
        [data-testid="stColumn"]:has(.nav-active-home) button,
        [data-testid="stColumn"]:has(.nav-active-workspace) button {
            background: __BORDER__; color: __TEXT__; box-shadow: 0 8px 24px __SHADOW__;
        }
        [data-testid="stColumn"]:has(.hero-link-button-marker) button {
            border: 1px solid __BORDER__; border-radius: 0.5rem;
            color: __TEXT__; background: transparent;
        }
        .st-key-scenario_row [data-testid="stHorizontalBlock"],
        [data-testid="stVerticalBlock"]:has(.scenario-row-floor-marker) > [data-testid="stHorizontalBlock"] {
            gap: 0.35rem;
        }
        .st-key-scenario_row [data-testid="column"] button,
        [data-testid="stVerticalBlock"]:has(.scenario-row-floor-marker) > [data-testid="stHorizontalBlock"] [data-testid="column"] button {
            min-height: 4.25rem; border: 2px solid __BORDER__; font-size: 0.72rem;
        }
        @media (max-width: 768px) {
            .st-key-top_nav [data-testid="stHorizontalBlock"],
            [data-testid="stVerticalBlock"]:has(.top-nav-floor-marker) > [data-testid="stHorizontalBlock"] { flex-wrap: wrap; }
            .st-key-top_nav [data-testid="stHorizontalBlock"] > [data-testid="column"],
            [data-testid="stVerticalBlock"]:has(.top-nav-floor-marker) > [data-testid="stHorizontalBlock"] > [data-testid="column"] { flex: 1 1 240px; }
            .st-key-kpi_strip [data-testid="stHorizontalBlock"] { flex-wrap: wrap; }
            .st-key-kpi_strip [data-testid="column"] { flex: 1 1 160px; }
            [data-testid="stVerticalBlock"]:has(.kpi-strip-floor-marker) > [data-testid="stHorizontalBlock"] { flex-wrap: wrap; }
            [data-testid="stVerticalBlock"]:has(.kpi-strip-floor-marker) > [data-testid="stHorizontalBlock"] > [data-testid="column"] { flex: 1 1 160px; }
        }
        </style>
        """.replace("__METRIC_A__", palette["metric_a"])
        .replace("__METRIC_B__", palette["metric_b"])
        .replace("__SURFACE__", palette["surface"])
        .replace("__TEXT__", palette["text"])
        .replace("__BORDER__", palette["border"])
        .replace("__SHADOW__", palette["shadow"]),
        unsafe_allow_html=True,
    )
    gold = _load_gold_tables()
    case_rows = gold["case_summary"]
    labels = {row.seed_account: row.scenario_label for row in case_rows.itertuples(index=False)}
    flagged_by_account = gold["accounts"].set_index("account_id")["is_flagged_mule_network"]
    account_option_ids = {ALL_ALERTS: None}
    for account_id in case_rows["seed_account"].astype(str):
        label = f"{labels[account_id]} — {account_id}"
        if not bool(flagged_by_account.loc[account_id]) and not labels[
            account_id
        ].lower().endswith("(not flagged)"):
            label += " (not flagged)"
        account_option_ids[label] = account_id
    st.session_state.account_option_ids = account_option_ids
    options = list(account_option_ids)
    if "account_selector" not in st.session_state:
        st.session_state.account_selector = next(
            (label for label, value in account_option_ids.items() if value == st.session_state.selected_account),
            ALL_ALERTS,
        )
    if st.session_state.top_level_view == "home":
        flagged = _sorted_flagged_accounts(gold)
        render_landing_hero(gold, flagged)
        render_scenario_row(gold)
        render_process_cards()
        return

    st.selectbox(
        "Investigation account",
        options=options,
        key="account_selector",
        on_change=_select_account,
    )
    selected_account = st.session_state.selected_account
    seed_account = selected_account or gold["_seed_account"]
    evidence_policy = st.session_state.evidence_policy
    st.radio(
        "Investigation section",
        options=SECTION_NAMES,
        key="active_section",
        horizontal=True,
        label_visibility="collapsed",
    )

    section_column, genie_column = st.columns([2, 1], gap="large")
    with section_column:
        if selected_account is None:
            render_alert_queue(gold)
        else:
            render_case_header(gold, seed_account)
            metrics = views.blast_radius_metrics(gold, seed_account, evidence_policy)
            control_comparison = views.control_cohort_comparison(gold, seed_account)
            render_kpi_strip(gold, seed_account, metrics)
            render_control_comparison(control_comparison)
            st.divider()
            render_freshness_banner(gold)
            st.divider()

            if st.session_state.active_section == "Overview":
                st.subheader("Investigation actions")
                action_trace, action_network, action_genie = st.columns(3)
                action_trace.button(
                    "Trace Funds",
                    on_click=_select_section,
                    args=("Money Flow",),
                    use_container_width=True,
                )
                action_network.button(
                    "View Connected Accounts",
                    on_click=_select_section,
                    args=("Network",),
                    use_container_width=True,
                )
                action_genie.button(
                    "Investigate with Genie",
                    on_click=_investigate_with_genie,
                    disabled=st.session_state.is_querying,
                    use_container_width=True,
                )

            if st.session_state.active_section == "Investigation":
                col_policy, col_reset = st.columns([3, 1])
                with col_policy:
                    st.radio(
                        "Evidence policy",
                        options=list(policy.VALID_POLICIES),
                        key="evidence_policy",
                        horizontal=True,
                        help=(
                            "Strict: only fund-flow-corroborated evidence. "
                            "Permissive: also includes weak device-only evidence."
                        ),
                    )
                with col_reset:
                    st.write("")
                    st.button("Reset to default policy", on_click=_reset_evidence_policy)
                st.divider()
                render_evidence_tab(gold, seed_account, evidence_policy)
                with st.expander("Legitimate control cohort (audit)", expanded=False):
                    render_control_cohort_tab(gold)

            if st.session_state.active_section == "Money Flow":
                st.info("Trace Funds selected — review the policy-scoped transfers below.")
                render_money_flow_tab(gold, seed_account, evidence_policy)

            if st.session_state.active_section == "Network":
                st.info("Connected Accounts selected — review the linked network below.")
                render_network_graph(gold, seed_account, evidence_policy)
                render_connected_accounts_tab(gold, seed_account, evidence_policy)

            if st.session_state.active_section == "Reports":
                render_reports_tab(gold, seed_account, evidence_policy, metrics)

    with genie_column:
        st.toggle("🔌 Disable Genie -- Demonstrate Dependency", key="genie_disabled")
        if st.session_state.genie_disabled:
            st.caption(
                "Genie is currently disabled for this demo -- questions will return "
                "a static explanation instead of a live answer."
            )
        st.markdown('<span id="ask-genie"></span>', unsafe_allow_html=True)
        st.subheader("🔎 Genie is on this case")
        render_genie_chat(gold, seed_account)


main()
