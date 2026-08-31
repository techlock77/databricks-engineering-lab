"""Databricks Genie Conversations API adapter used by the Streamlit UI."""

from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any, Callable

import pandas as pd
from databricks.sdk import WorkspaceClient


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
    prompt = (
        f"Investigation context: seed account {context.seed_account}; evidence policy "
        f"{context.evidence_policy}. Answer using only the persisted MuleGraph Gold tables. "
        f"Question: {question}"
    )
    message = workspace.genie.start_conversation_and_wait(space_id=space_id, content=prompt)
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
