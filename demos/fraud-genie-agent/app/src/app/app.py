"""MuleGraph Investigator -- Streamlit app.

All filtering/view logic lives in src.pipeline.views (plain functions, no
Streamlit calls) so it is unit-testable without booting the app. This
module only wires those functions to widgets.
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import streamlit as st

from src.genie.export import build_case_export
from src.genie.interface import GenieContext, genie_query
from src.data_access import load_gold_tables
from src.pipeline import policy, views

st.set_page_config(page_title="MuleGraph Investigator", layout="wide")


@st.cache_resource
def _load_gold_tables():
    return load_gold_tables()


def _init_session_state() -> None:
    if "evidence_policy" not in st.session_state:
        st.session_state.evidence_policy = policy.DEFAULT_POLICY
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []


def _reset_evidence_policy() -> None:
    st.session_state.evidence_policy = policy.DEFAULT_POLICY


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
    st.dataframe(evidence, width="stretch", hide_index=True)


def render_blast_radius_tab(gold: dict, seed_account: str, evidence_policy: str) -> None:
    metrics = views.blast_radius_metrics(gold, seed_account, evidence_policy)
    c1, c2, c3 = st.columns(3)
    c1.metric("Other connected accounts", metrics["other_connected_accounts_count"])
    c2.metric("Shared devices", metrics["shared_device_count"])
    c3.metric("Linked exposure", f"${metrics['total_exposure']:,.2f}")
    c4, c5 = st.columns(2)
    c4.metric("Potential victims", metrics["potential_victims_count"])
    c5.metric("External destinations", metrics["destinations_count"])


def render_connected_accounts_tab(gold: dict, seed_account: str, evidence_policy: str) -> None:
    st.caption(f"Evidence policy: {evidence_policy}")
    table = views.connected_accounts_table(gold, seed_account, evidence_policy)
    st.dataframe(table, width="stretch", hide_index=True)


def render_control_cohort_tab(gold: dict) -> None:
    st.write(
        "Legitimate-remittance control cohort: the same fan-in/fan-out shape as the "
        "mule network above, protected from being flagged by the recurring-corridor "
        "override (long account tenure and a long-running monthly corridor)."
    )
    st.dataframe(gold["control_cohort"], width="stretch", hide_index=True)


def render_genie_chat(gold: dict, seed_account: str) -> None:
    st.subheader("Ask Genie")
    for turn in st.session_state.chat_history:
        with st.chat_message(turn["role"]):
            st.write(turn["content"])
            for citation in turn.get("citations", []):
                st.caption(f"Source: {citation.source_table}#{citation.source_row_id} -- {citation.text}")
            if turn.get("freshness"):
                st.caption(turn["freshness"])

    question = st.chat_input("Ask a question about this case")
    if question:
        st.session_state.chat_history.append({"role": "user", "content": question})
        context = GenieContext(
            gold=gold, seed_account=seed_account, evidence_policy=st.session_state.evidence_policy
        )
        response = genie_query(question, context)
        st.session_state.chat_history.append(
            {
                "role": "assistant",
                "content": response.answer,
                "citations": response.citations,
                "freshness": response.freshness_note,
            }
        )
        st.rerun()


def render_export_button(gold: dict, seed_account: str, evidence_policy: str) -> None:
    bundle = build_case_export(gold, seed_account, evidence_policy)
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


def main() -> None:
    _init_session_state()
    gold = _load_gold_tables()
    seed_account = gold["_seed_account"]

    render_freshness_banner(gold)
    render_case_header(gold, seed_account)

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
        # `on_click` runs before the script reruns and the radio widget below
        # is re-instantiated, so it's safe to write to its session_state key
        # here -- assigning it directly in the main body (after the widget
        # already exists) raises a StreamlitAPIException.
        st.button("Reset to default policy", on_click=_reset_evidence_policy)

    evidence_policy = st.session_state.evidence_policy

    tab_evidence, tab_blast, tab_connected, tab_control = st.tabs(
        ["Evidence", "Blast Radius", "Connected Accounts", "Control Cohort"]
    )
    with tab_evidence:
        render_evidence_tab(gold, evidence_policy)
    with tab_blast:
        render_blast_radius_tab(gold, seed_account, evidence_policy)
    with tab_connected:
        render_connected_accounts_tab(gold, seed_account, evidence_policy)
    with tab_control:
        render_control_cohort_tab(gold)

    render_export_button(gold, seed_account, evidence_policy)
    render_genie_chat(gold, seed_account)


main()
