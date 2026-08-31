"""Read MuleGraph's persisted Gold tables through Databricks SQL."""

from __future__ import annotations

import os
import re
import time
from typing import Any

import pandas as pd
from databricks.sdk import WorkspaceClient

GOLD_TABLE_NAMES = (
    "accounts",
    "evidence",
    "network_edges",
    "transfers",
    "case_summary",
    "control_cohort",
    "freshness",
    "export_citations",
)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TERMINAL_STATES = {"SUCCEEDED", "FAILED", "CANCELED", "CLOSED"}


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(
            f"{name} is required. Attach the corresponding resource to the "
            "Databricks App or set the environment variable explicitly."
        )
    return value


def _quote_identifier(value: str, setting: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"{setting} must be a simple Unity Catalog identifier: {value!r}")
    return f"`{value}`"


def _state_name(response: Any) -> str:
    state = getattr(getattr(response, "status", None), "state", None)
    return str(getattr(state, "value", state) or "").upper()


def _wait_for_statement(statement_api: Any, response: Any, timeout_seconds: int = 60) -> Any:
    deadline = time.monotonic() + timeout_seconds
    while _state_name(response) not in _TERMINAL_STATES:
        if not getattr(response, "statement_id", None):
            raise RuntimeError("Databricks SQL did not return a statement ID.")
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Databricks SQL statement {response.statement_id} timed out.")
        time.sleep(0.25)
        response = statement_api.get_statement(response.statement_id)
    if _state_name(response) != "SUCCEEDED":
        error = getattr(getattr(response, "status", None), "error", None)
        detail = getattr(error, "message", None) or str(error or "unknown error")
        raise RuntimeError(f"Databricks SQL statement failed: {detail}")
    return response


def _column_kind(column: Any) -> str:
    type_name = getattr(column, "type_name", None)
    return str(getattr(type_name, "value", type_name) or "").upper()


def _coerce_value(value: Any, kind: str) -> Any:
    if value is None:
        return None
    if kind == "BOOLEAN":
        return value if isinstance(value, bool) else str(value).lower() == "true"
    if kind in {"BYTE", "SHORT", "INT", "INTEGER", "LONG"}:
        return int(value)
    if kind in {"FLOAT", "DOUBLE", "DECIMAL"}:
        return float(value)
    return value


def statement_response_to_dataframe(statement_api: Any, response: Any) -> pd.DataFrame:
    """Convert an INLINE statement response, including all chunks, to pandas."""
    manifest = getattr(response, "manifest", None)
    schema = getattr(manifest, "schema", None)
    columns = list(getattr(schema, "columns", None) or [])
    names = [column.name for column in columns]
    kinds = [_column_kind(column) for column in columns]

    result = getattr(response, "result", None)
    rows = list(getattr(result, "data_array", None) or [])
    next_chunk = getattr(result, "next_chunk_index", None)
    while next_chunk is not None:
        result = statement_api.get_statement_result_chunk_n(response.statement_id, next_chunk)
        rows.extend(getattr(result, "data_array", None) or [])
        next_chunk = getattr(result, "next_chunk_index", None)

    converted = [[_coerce_value(value, kinds[i]) for i, value in enumerate(row)] for row in rows]
    return pd.DataFrame(converted, columns=names)


def load_gold_tables(client: Any | None = None) -> dict[str, pd.DataFrame]:
    """Fetch all eight Gold tables and add the two app-only compatibility keys."""
    catalog = os.getenv("DATABRICKS_CATALOG", "mulegraph")
    schema = os.getenv("DATABRICKS_SCHEMA", "investigations")
    warehouse_id = _required_env("DATABRICKS_WAREHOUSE_ID")
    catalog_sql = _quote_identifier(catalog, "DATABRICKS_CATALOG")
    schema_sql = _quote_identifier(schema, "DATABRICKS_SCHEMA")
    workspace = client or WorkspaceClient()
    statement_api = workspace.statement_execution

    tables: dict[str, pd.DataFrame] = {}
    for table_name in GOLD_TABLE_NAMES:
        response = statement_api.execute_statement(
            statement=f"SELECT * FROM {catalog_sql}.{schema_sql}.`{table_name}`",
            warehouse_id=warehouse_id,
            wait_timeout="50s",
        )
        response = _wait_for_statement(statement_api, response)
        tables[table_name] = statement_response_to_dataframe(statement_api, response)

    if tables["case_summary"].empty:
        raise RuntimeError("The persisted case_summary table is empty; run the data-generation notebook first.")
    tables["_network_edges_raw"] = tables["network_edges"].drop(columns=["edge_id"], errors="ignore")
    tables["_seed_account"] = str(tables["case_summary"].iloc[0]["seed_account"])
    return tables
