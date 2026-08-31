"""Explicit local test fallback over the Gold tables.

This module is never selected by the deployed app. Tests may inject
``answer_question`` into ``genie_query`` explicitly. Every number in every
answer is read from a Gold table or from network.build_case_network's
output for the context's evidence policy -- nothing is invented.
"""

from __future__ import annotations

import pandas as pd

from src.genie.interface import Citation, GenieContext, GenieResponse
from src.pipeline import network, policy, views

QUESTIONS: list[tuple[str, str, tuple[str, ...]]] = [
    (
        "why_flagged",
        "Why was this account flagged?",
        ("why", "flagged", "flag"),
    ),
    (
        "what_changed",
        "What changed since the last review?",
        ("changed", "new since", "update"),
    ),
    (
        "who_connected",
        "Who is connected to this account?",
        ("who is connected", "connected accounts", "who's connected"),
    ),
    (
        "follow_money",
        "Where did the funds go?",
        ("follow the money", "where did the funds", "fund flow", "where did the money"),
    ),
    (
        "pattern_match",
        "What fraud pattern does this match?",
        ("pattern", "fraud pattern", "what kind of fraud"),
    ),
    (
        "potential_victims",
        "Who are the potential victims?",
        ("victim", "victims"),
    ),
    (
        "why_missed",
        "Why might old approaches miss this?",
        ("old approaches", "miss this", "dashboard", "rule engine"),
    ),
    (
        "blast_radius",
        "What is the blast radius of this case?",
        ("blast radius", "how big is this"),
    ),
    (
        "closing_question",
        "What would we have missed if we investigated only the original transaction?",
        ("only the original transaction", "isolated transaction", "what would we have missed"),
    ),
]

QUESTION_TEXT_BY_ID = {qid: text for qid, text, _ in QUESTIONS}


def match_question_id(question: str) -> str | None:
    q = question.strip().lower()
    best_id, best_score = None, 0
    for qid, text, keywords in QUESTIONS:
        score = 0
        if q == text.lower():
            return qid
        for kw in keywords + (text.lower(),):
            if kw in q:
                score += len(kw)
        if score > best_score:
            best_id, best_score = qid, score
    return best_id if best_score > 0 else None


def _freshness_note(gold: dict[str, pd.DataFrame]) -> str:
    freshness = gold["freshness"]
    row = freshness.iloc[0]
    staleness = "stale" if row["is_stale"] else "current"
    return (
        f"Evidence as of {row['last_refreshed_ts']} "
        f"(freshness contract: {row['freshness_contract_hours']}h, {staleness})."
    )


def _network_for(context: GenieContext) -> network.NetworkResult:
    return views.compute_network(context.gold, context.seed_account, context.evidence_policy)


def _account_row(gold: dict[str, pd.DataFrame], account_id: str) -> pd.Series:
    accounts = gold["accounts"]
    return accounts[accounts["account_id"] == account_id].iloc[0]


def _evidence_in_scope(context: GenieContext) -> pd.DataFrame:
    return views.filter_evidence(context.gold["evidence"], context.evidence_policy)


def _case_summary_row(gold: dict[str, pd.DataFrame]) -> pd.Series:
    return gold["case_summary"].iloc[0]


def _case_citation(gold: dict[str, pd.DataFrame]) -> Citation:
    row = _case_summary_row(gold)
    return Citation(
        source_table="gold_case_summary",
        source_row_id=row["case_id"],
        text=(
            f"{row['case_id']}: permissive exposure ${row['total_exposure_permissive']:,.2f} "
            f"across {row['other_connected_accounts_permissive']} other connected accounts."
        ),
    )


def _evidence_citations(evidence_df: pd.DataFrame, limit: int = 3) -> list[Citation]:
    out = []
    for row in evidence_df.head(limit).itertuples(index=False):
        out.append(
            Citation(
                source_table="gold_evidence",
                source_row_id=row.evidence_id,
                text=row.description,
            )
        )
    return out


def answer_question(question: str, context: GenieContext) -> GenieResponse:
    qid = match_question_id(question)
    net = _network_for(context)
    acc = _account_row(context.gold, context.seed_account)
    evidence_scope = _evidence_in_scope(context)
    freshness = _freshness_note(context.gold)

    if qid == "why_flagged":
        answer = (
            f"{context.seed_account} was flagged because it receives recurring transfers from "
            f"{acc['distinct_source_count']} distinct source accounts and sends recurring "
            f"transfers to {acc['distinct_destination_count']} distinct destination accounts "
            f"across {acc['distinct_outbound_months']} months, moving "
            f"${acc['total_outbound_amount']:,.2f} outbound -- a fan-in/fan-out collector "
            f"pattern. {acc['detection_reason']}"
        )
        citations = [_case_citation(context.gold)] + _evidence_citations(evidence_scope, limit=2)

    elif qid == "what_changed":
        evidence = context.gold["evidence"]
        takeover = evidence[evidence["evidence_type"] == policy.EVIDENCE_TYPE_ACCOUNT_TAKEOVER]
        if not takeover.empty:
            row = takeover.iloc[0]
            answer = (
                f"The earliest event in this case's evidence trail is an account-takeover "
                f"provenance record: {row['description']}"
            )
            citations = [
                Citation(source_table="gold_evidence", source_row_id=row["evidence_id"], text=row["description"])
            ]
        else:
            answer = "No account-takeover provenance evidence is present for this case."
            citations = []

    elif qid == "who_connected":
        others = net.other_connected_accounts
        preview = ", ".join(others[:10])
        answer = (
            f"Under the {context.evidence_policy} evidence policy, {net.other_connected_accounts_count} "
            f"other accounts are connected to {context.seed_account}: {preview}. "
            f"{net.shared_device_count} shared device(s) link these accounts."
        )
        citations = [_case_citation(context.gold)]

    elif qid == "follow_money":
        edges = net.edges
        accounts = context.gold["accounts"].set_index("account_id")["account_role"]
        inbound = edges[
            edges.apply(
                lambda r: accounts.get(r["account_a"]) == "fan_in_source"
                or accounts.get(r["account_b"]) == "fan_in_source",
                axis=1,
            )
        ]["amount"].sum()
        outbound = edges[
            edges.apply(
                lambda r: accounts.get(r["account_a"]) == "fan_out_destination"
                or accounts.get(r["account_b"]) == "fan_out_destination",
                axis=1,
            )
        ]["amount"].sum()
        answer = (
            f"${inbound:,.2f} flowed in from fan-in source accounts and ${outbound:,.2f} flowed "
            f"out to fan-out destination accounts, for ${net.total_exposure:,.2f} in total linked "
            f"movement under the {context.evidence_policy} policy."
        )
        citations = [_case_citation(context.gold)]

    elif qid == "pattern_match":
        answer = (
            f"This matches a fan-in/fan-out mule-collector pattern: {acc['distinct_source_count']} "
            f"sources feeding {context.seed_account}, which disperses to "
            f"{acc['distinct_destination_count']} distinct destinations recurring across "
            f"{acc['distinct_outbound_months']} months, corroborated by shared-device evidence."
        )
        citations = _evidence_citations(evidence_scope, limit=2)

    elif qid == "potential_victims":
        case_row = _case_summary_row(context.gold)
        accounts = context.gold["accounts"].set_index("account_id")["account_role"]
        victims = [a for a in net.other_connected_accounts if accounts.get(a) == "fan_in_source"]
        answer = (
            f"{case_row['potential_victims_count']} potential victim account(s) feed "
            f"{context.seed_account} directly: {', '.join(victims)}."
        )
        citations = [_case_citation(context.gold)]

    elif qid == "why_missed":
        answer = (
            f"A single-transaction review sees one transfer in isolation. It would not surface "
            f"the {net.shared_device_count} shared device(s) linking otherwise-unrelated accounts, "
            f"nor the {net.other_connected_accounts_count}-account network revealed only by "
            f"traversing fund flow and device evidence together across "
            f"{acc['distinct_outbound_months']} months of recurring activity."
        )
        citations = [_case_citation(context.gold)]

    elif qid == "blast_radius":
        case_row = _case_summary_row(context.gold)
        answer = (
            f"Under the {context.evidence_policy} evidence policy: {net.other_connected_accounts_count} "
            f"other connected accounts, {net.shared_device_count} shared device(s), "
            f"${net.total_exposure:,.2f} in linked exposure, {case_row['potential_victims_count']} "
            f"potential victims, and {case_row['destinations_count']} external destinations."
        )
        citations = [_case_citation(context.gold)]

    elif qid == "closing_question":
        answer = (
            f"Investigating only the original transaction on {context.seed_account} in isolation "
            f"would have missed {net.other_connected_accounts_count} other connected accounts, "
            f"{net.shared_device_count} shared device(s), and ${net.total_exposure:,.2f} in linked "
            f"fund movement -- evidence that only emerges from tracing connected accounts, not "
            f"from reviewing one transaction on its own."
        )
        citations = [_case_citation(context.gold)]

    else:
        options = "; ".join(text for _, text, _ in QUESTIONS)
        answer = (
            "I can answer investigation questions grounded in this case's Gold tables. Try one "
            f"of: {options}"
        )
        citations = []

    return GenieResponse(
        question=question,
        answer=answer,
        citations=citations,
        freshness_note=freshness,
        evidence_policy=context.evidence_policy,
    )
