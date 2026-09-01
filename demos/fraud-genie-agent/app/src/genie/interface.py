"""Databricks Genie Conversations API adapter used by the Streamlit UI."""

from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any, Callable

import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import OperationFailed


@dataclass(frozen=True)
class Citation:
    source_table: str
    source_row_id: str
    text: str


@dataclass(frozen=True)
class GenieContext:
    gold: dict[str, pd.DataFrame]
    seed_account: str
    evidence_policy: str


@dataclass(frozen=True)
class GenieResponse:
    question: str
    answer: str
    citations: list[Citation]
    freshness_note: str
    evidence_policy: str


def _freshness_note(context: GenieContext) -> str:
    row = context.gold["freshness"].iloc[0]
    staleness = "stale" if bool(row["is_stale"]) else "current"
    return (
        f"Evidence as of {row['last_refreshed_ts']} "
        f"(freshness contract: {row['freshness_contract_hours']}h, {staleness})."
    )


def _attachment_citations(attachments: list[Any]) -> list[Citation]:
    citations = []
    for attachment in attachments:
        query = getattr(attachment, "query", None)
        if query is None:
            continue
        attachment_id = (
            getattr(attachment, "attachment_id", None)
            or getattr(query, "id", None)
            or getattr(query, "statement_id", None)
            or "query"
        )
        text = getattr(query, "description", None) or getattr(query, "query", None) or "Genie query"
        citations.append(Citation("Databricks Genie", str(attachment_id), str(text)))
    return citations


def genie_query(
    question: str,
    context: GenieContext,
    client: Any | None = None,
    responder: Callable[[str, GenieContext], GenieResponse] | None = None,
) -> GenieResponse:
    """Ask the configured Genie Space; ``responder`` is explicit test-only injection."""
    if responder is not None:
        return responder(question, context)

    space_id = os.getenv("DATABRICKS_GENIE_SPACE_ID", "").strip()
    if not space_id:
        raise RuntimeError(
            "DATABRICKS_GENIE_SPACE_ID is required. Attach a Genie Space resource "
            "to the Databricks App or set the environment variable explicitly."
        )

    workspace = client or WorkspaceClient()
    evidence_view = f"evidence_{context.evidence_policy}_v"
    network_view = f"network_edges_{context.evidence_policy}_v"
    prompt = (
        f"Investigation context: seed account {context.seed_account}; evidence policy "
        f"{context.evidence_policy}. For this question, query only `{evidence_view}` and "
        f"`{network_view}` for evidence and network questions. Do not query the raw "
        "`evidence` or `network_edges` tables, which mix both policies. "
        "Answer using only the persisted MuleGraph Gold tables and these policy-scoped views. "
        f"Question: {question}"
    )
    message_waiter = workspace.genie.start_conversation(space_id=space_id, content=prompt)
    try:
        message = message_waiter.result()
    except OperationFailed as operation_failure:
        try:
            failed_message = workspace.genie.get_message(
                space_id=space_id,
                conversation_id=message_waiter.conversation_id,
                message_id=message_waiter.message_id,
            )
        except Exception:
            raise operation_failure from None

        message_error = getattr(failed_message, "error", None)
        error_type = getattr(message_error, "type", None)
        error_detail = getattr(message_error, "error", None)
        if error_type is None and not error_detail:
            raise operation_failure from None

        error_type = getattr(error_type, "value", error_type) or "UNKNOWN"
        error_detail = error_detail or "No error detail was returned."
        raise RuntimeError(f"Genie query failed ({error_type}): {error_detail}") from operation_failure
    attachments = list(getattr(message, "attachments", None) or [])
    text_parts = [
        str(attachment.text.content)
        for attachment in attachments
        if getattr(attachment, "text", None) is not None
        and getattr(attachment.text, "content", None)
    ]
    answer = "\n\n".join(text_parts) or str(getattr(message, "content", "") or "").strip()
    if not answer:
        raise RuntimeError("Databricks Genie completed without returning an answer.")

    return GenieResponse(
        question=question,
        answer=answer,
        citations=_attachment_citations(attachments),
        freshness_note=_freshness_note(context),
        evidence_policy=context.evidence_policy,
    )
