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
TAB_NAMES = ["Overview", "Investigation", "Money Flow", "Network", "Genie", "Reports"]


@st.cache_resource
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
    if "target_tab" not in st.session_state:
        st.session_state.target_tab = None


def _reset_evidence_policy() -> None:
    st.session_state.evidence_policy = policy.DEFAULT_POLICY


def _target_tab(tab_name: str) -> None:
    st.session_state.target_tab = tab_name


def _queue_genie_question(question: str) -> None:
    if st.session_state.is_querying:
        return
    st.session_state.chat_history.append({"role": "user", "content": question})
    st.session_state.pending_question = question
    st.session_state.is_querying = True
    st.session_state.target_tab = "Genie"


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


def render_evidence_tab(gold: dict, evidence_policy: str) -> None:
    st.caption(f"Evidence policy: {evidence_policy}")
    evidence = views.filter_evidence(gold["evidence"], evidence_policy)
    st.dataframe(evidence, use_container_width=True, hide_index=True)


def render_kpi_strip(gold: dict, seed_account: str, metrics: dict) -> None:
    account = gold["accounts"][gold["accounts"]["account_id"] == seed_account].iloc[0]
    flagged = bool(account["is_flagged_mule_network"])
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


def render_genie_chat(gold: dict, seed_account: str) -> None:
    st.subheader("Ask Genie")
    st.caption("Try a grounded question about this case:")
    for index, suggested_question in enumerate(GENIE_QUESTIONS):
        st.button(
            suggested_question,
            key=f"genie_suggestion_{index}",
            disabled=st.session_state.is_querying,
            on_click=_queue_genie_question,
            args=(suggested_question,),
            use_container_width=True,
        )

    _render_chat_history()

    question = st.chat_input(
        "Ask a question about this case", disabled=st.session_state.is_querying
    )
    if question:
        _queue_genie_question(question)
        st.rerun()

    pending_question = st.session_state.pending_question
    if pending_question:
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
        except Exception:
            st.session_state.chat_history.append(
                {
                    "role": "assistant",
                    "content": (
                        "Genie could not complete this question. You can continue the "
                        "investigation with the KPI strip, Investigation evidence, and "
                        "Network connected accounts while the service recovers."
                    ),
                }
            )
            status.update(label="Genie is temporarily unavailable", state="error", expanded=True)
        finally:
            st.session_state.pending_question = None
            st.session_state.is_querying = False
            st.session_state.target_tab = "Genie"
        st.rerun()


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
    st.markdown(
        """
        <style>
        [data-testid="stMetric"] {
            background: linear-gradient(145deg, rgba(19, 34, 56, 0.96), rgba(11, 18, 32, 0.96));
            border: 1px solid rgba(20, 184, 166, 0.28);
            border-radius: 0.75rem;
            padding: 0.9rem 1rem;
        }
        [data-testid="stDataFrame"], [data-testid="stExpander"], .stChatMessage {
            box-shadow: 0 8px 24px rgba(0, 0, 0, 0.22);
            border: 1px solid rgba(230, 237, 243, 0.10);
            border-radius: 0.75rem;
        }
        h2, h3 {
            color: rgba(230, 237, 243, 0.82);
            letter-spacing: 0.025em;
        }
        [data-testid="stTabs"] { margin-top: 0.75rem; }
        .stChatMessage {
            padding: 0.25rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    gold = _load_gold_tables()
    seed_account = gold["_seed_account"]
    render_case_header(gold, seed_account)
    evidence_policy = st.session_state.evidence_policy
    metrics = views.blast_radius_metrics(gold, seed_account, evidence_policy)
    control_comparison = views.control_cohort_comparison(gold, seed_account)
    render_kpi_strip(gold, seed_account, metrics)
    render_control_comparison(control_comparison)
    st.divider()
    render_freshness_banner(gold)
    st.divider()

    requested_tab = st.session_state.target_tab
    st.session_state.target_tab = None
    tabs_parameters = inspect.signature(st.tabs).parameters
    tabs_kwargs = {"key": "main_tabs"} if "key" in tabs_parameters else {}
    if requested_tab in TAB_NAMES and "default" in tabs_parameters:
        tabs_kwargs["default"] = requested_tab
    (
        tab_overview,
        tab_investigation,
        tab_money_flow,
        tab_network,
        tab_genie,
        tab_reports,
    ) = st.tabs(TAB_NAMES, **tabs_kwargs)

    with tab_overview:
        st.subheader("Investigation actions")
        action_trace, action_network, action_genie = st.columns(3)
        action_trace.button(
            "Trace Funds", on_click=_target_tab, args=("Money Flow",), use_container_width=True
        )
        action_network.button(
            "View Connected Accounts",
            on_click=_target_tab,
            args=("Network",),
            use_container_width=True,
        )
        action_genie.button(
            "Investigate with Genie",
            on_click=_queue_genie_question,
            args=(GENIE_INVESTIGATION_QUESTION,),
            disabled=st.session_state.is_querying,
            use_container_width=True,
        )

    with tab_investigation:
        if requested_tab == "Investigation":
            st.info("Trace Funds selected — review the evidence and policy controls below.")
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
        render_evidence_tab(gold, evidence_policy)
        with st.expander("Legitimate control cohort (audit)", expanded=False):
            render_control_cohort_tab(gold)

    with tab_money_flow:
        if requested_tab == "Money Flow":
            st.info("Trace Funds selected — review the policy-scoped transfers below.")
        render_money_flow_tab(gold, seed_account, evidence_policy)

    with tab_network:
        if requested_tab == "Network":
            st.info("Connected Accounts selected — review the linked network below.")
        render_network_graph(gold, seed_account, evidence_policy)
        render_connected_accounts_tab(gold, seed_account, evidence_policy)

    with tab_genie:
        render_genie_chat(gold, seed_account)

    with tab_reports:
        render_reports_tab(gold, seed_account, evidence_policy, metrics)


main()
