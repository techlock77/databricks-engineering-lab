from types import SimpleNamespace

import pandas as pd
import pytest
from databricks.sdk.errors import OperationFailed
from databricks.sdk.service.dashboards import GenieMessage, MessageError, MessageErrorType

from src.genie.interface import Citation, GenieContext, GenieResponse, genie_query


def _context():
    return GenieContext(
        gold={
            "freshness": pd.DataFrame(
                [{"last_refreshed_ts": "2026-01-01T00:00:00", "freshness_contract_hours": 24, "is_stale": False}]
            )
        },
        seed_account="MULE_1",
        evidence_policy="strict",
    )


def test_genie_query_calls_real_sdk_surface_and_maps_contract(monkeypatch):
    monkeypatch.setenv("DATABRICKS_GENIE_SPACE_ID", "space-1")
    attachment = SimpleNamespace(
        attachment_id="attachment-1",
        text=SimpleNamespace(content="The persisted evidence shows a fan-out pattern."),
        query=SimpleNamespace(description="Evidence query", query="SELECT * FROM evidence", id="query-1", statement_id="s1"),
    )

    class GenieApi:
        def __init__(self):
            self.calls = []

        def start_conversation(self, space_id, content):
            self.calls.append((space_id, content))
            return SimpleNamespace(
                result=lambda: SimpleNamespace(content="", attachments=[attachment])
            )

    genie = GenieApi()
    response = genie_query("Why flagged?", _context(), client=SimpleNamespace(genie=genie))

    assert genie.calls[0][0] == "space-1"
    assert "MULE_1" in genie.calls[0][1]
    assert "strict" in genie.calls[0][1]
    assert "`evidence_strict_v`" in genie.calls[0][1]
    assert "`network_edges_strict_v`" in genie.calls[0][1]
    assert "Do not query the raw `evidence` or `network_edges` tables" in genie.calls[0][1]
    assert response == GenieResponse(
        question="Why flagged?",
        answer="The persisted evidence shows a fan-out pattern.",
        citations=[Citation("Databricks Genie", "attachment-1", "Evidence query")],
        freshness_note="Evidence as of 2026-01-01T00:00:00 (freshness contract: 24h, current).",
        evidence_policy="strict",
    )


def test_genie_query_surfaces_failed_message_error(monkeypatch):
    monkeypatch.setenv("DATABRICKS_GENIE_SPACE_ID", "space-1")
    operation_failure = OperationFailed(
        "failed to reach COMPLETED, got MessageStatus.FAILED"
    )

    class MessageWaiter:
        conversation_id = "conversation-1"
        message_id = "message-1"

        def result(self):
            raise operation_failure

    genie = SimpleNamespace(
        start_conversation=lambda **kwargs: MessageWaiter(),
        get_message=lambda **kwargs: GenieMessage(
            id="message-1",
            space_id="space-1",
            conversation_id="conversation-1",
            content="",
            message_id="message-1",
            error=MessageError(
                type=MessageErrorType.TABLES_MISSING_EXCEPTION,
                error="table not found",
            )
        ),
    )

    with pytest.raises(RuntimeError) as exc_info:
        genie_query("Why?", _context(), client=SimpleNamespace(genie=genie))

    assert str(exc_info.value) == (
        "Genie query failed (TABLES_MISSING_EXCEPTION): table not found"
    )
    assert "failed to reach COMPLETED" not in str(exc_info.value)


def test_genie_query_preserves_operation_failure_when_refetch_fails(monkeypatch):
    monkeypatch.setenv("DATABRICKS_GENIE_SPACE_ID", "space-1")
    operation_failure = OperationFailed(
        "failed to reach COMPLETED, got MessageStatus.FAILED"
    )

    class MessageWaiter:
        conversation_id = "conversation-1"
        message_id = "message-1"

        def result(self):
            raise operation_failure

    def fail_refetch(**kwargs):
        raise ConnectionError("diagnostic refetch failed")

    genie = SimpleNamespace(
        start_conversation=lambda **kwargs: MessageWaiter(),
        get_message=fail_refetch,
    )

    with pytest.raises(OperationFailed) as exc_info:
        genie_query("Why?", _context(), client=SimpleNamespace(genie=genie))

    assert exc_info.value is operation_failure
    assert str(exc_info.value) == "failed to reach COMPLETED, got MessageStatus.FAILED"


def test_genie_query_requires_space_id(monkeypatch):
    monkeypatch.delenv("DATABRICKS_GENIE_SPACE_ID", raising=False)
    with pytest.raises(RuntimeError, match="DATABRICKS_GENIE_SPACE_ID is required"):
        genie_query("Why?", _context(), client=SimpleNamespace())


def test_local_responder_requires_explicit_injection(monkeypatch):
    monkeypatch.delenv("DATABRICKS_GENIE_SPACE_ID", raising=False)
    expected = GenieResponse("q", "local", [], "fresh", "strict")
    response = genie_query("q", _context(), responder=lambda question, context: expected)
    assert response is expected
