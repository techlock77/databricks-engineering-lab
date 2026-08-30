"""Genie seam: the dataclasses and entrypoint the UI talks to.

`genie_query` is the one function the Streamlit app calls. Today it
delegates to the rule-based responder in responder.py; swapping in real
Databricks Genie later means changing this function's body to call the
Genie Conversations API and translate the result into the same
`GenieResponse` shape -- the UI never has to change.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


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


def genie_query(question: str, context: GenieContext) -> GenieResponse:
    from src.genie import responder

    return responder.answer_question(question, context)
