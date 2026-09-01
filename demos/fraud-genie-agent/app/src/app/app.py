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


def _reset_evidence_policy() -> None:
    st.session_state.evidence_policy = policy.DEFAULT_POLICY


def _select_section(section_name: str) -> None:
    st.session_state.active_section = section_name


def _open_case(account_id: str) -> None:
    st.session_state.selected_account = account_id
    st.session_state.active_section = "Overview"


def _queue_genie_question(question: str) -> None:
    if st.session_state.is_querying:
        return
    st.session_state.chat_history.append({"role": "user", "content": question})
    st.session_state.pending_question = question
    st.session_state.is_querying = True


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
        st.rerun()


def render_alert_queue(gold: dict) -> None:
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
    flagged = flagged.sort_values(sort_columns, ascending=False)

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
            border: 1px solid rgba(20, 184, 166, 0.24);
            border-radius: 999px;
            padding: 0.45rem 0.9rem;
            background: rgba(19, 34, 56, 0.72);
        }
        [data-testid="stRadio"] [role="radio"][aria-checked="true"] + div {
            border-color: rgba(20, 184, 166, 0.9);
            background: rgba(20, 184, 166, 0.20);
            color: #E6EDF3;
        }
        .stChatMessage {
            padding: 0.25rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    gold = _load_gold_tables()
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
                render_evidence_tab(gold, evidence_policy)
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
        st.subheader("🔎 Genie is on this case")
        render_genie_chat(gold, seed_account)


main()
