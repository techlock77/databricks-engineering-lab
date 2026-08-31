from types import SimpleNamespace

import pandas as pd
import pytest

from src.data_access import GOLD_TABLE_NAMES, load_gold_tables, statement_response_to_dataframe


def _column(name, kind):
    return SimpleNamespace(name=name, type_name=SimpleNamespace(value=kind))


def _response(columns, rows, statement_id="statement-1", next_chunk_index=None):
    return SimpleNamespace(
        statement_id=statement_id,
        status=SimpleNamespace(state=SimpleNamespace(value="SUCCEEDED")),
        manifest=SimpleNamespace(schema=SimpleNamespace(columns=columns)),
        result=SimpleNamespace(data_array=rows, next_chunk_index=next_chunk_index),
    )


def test_statement_response_to_dataframe_reads_chunks_and_types():
    columns = [_column("account_id", "STRING"), _column("amount", "DOUBLE"), _column("flag", "BOOLEAN")]
    response = _response(columns, [["A1", "12.5", "true"]], next_chunk_index=1)
    statement_api = SimpleNamespace(
        get_statement_result_chunk_n=lambda statement_id, chunk_index: SimpleNamespace(
            data_array=[["A2", "7.0", "false"]], next_chunk_index=None
        )
    )

    frame = statement_response_to_dataframe(statement_api, response)

    assert frame.to_dict("records") == [
        {"account_id": "A1", "amount": 12.5, "flag": True},
        {"account_id": "A2", "amount": 7.0, "flag": False},
    ]


def test_load_gold_tables_queries_all_persisted_tables(monkeypatch):
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "warehouse-1")
    calls = []
    columns_by_table = {
        "case_summary": [_column("seed_account", "STRING")],
        "network_edges": [_column("edge_id", "STRING"), _column("account_a", "STRING")],
    }

    class StatementApi:
        def execute_statement(self, statement, warehouse_id, wait_timeout):
            calls.append((statement, warehouse_id, wait_timeout))
            table = statement.rsplit(".", 1)[-1].strip("`")
            columns = columns_by_table.get(table, [_column("value", "STRING")])
            if table == "case_summary":
                rows = [["COLLECTOR_1"]]
            elif table == "network_edges":
                rows = [["EDGE_1", "A1"]]
            else:
                rows = [[table]]
            return _response(columns, rows, statement_id=table)

    client = SimpleNamespace(statement_execution=StatementApi())
    gold = load_gold_tables(client=client)

    assert len(calls) == len(GOLD_TABLE_NAMES)
    assert all(call[1:] == ("warehouse-1", "50s") for call in calls)
    assert gold["_seed_account"] == "COLLECTOR_1"
    assert list(gold["_network_edges_raw"].columns) == ["account_a"]
    assert isinstance(gold["accounts"], pd.DataFrame)


def test_load_gold_tables_requires_warehouse(monkeypatch):
    monkeypatch.delenv("DATABRICKS_WAREHOUSE_ID", raising=False)
    with pytest.raises(RuntimeError, match="DATABRICKS_WAREHOUSE_ID is required"):
        load_gold_tables(client=SimpleNamespace())
