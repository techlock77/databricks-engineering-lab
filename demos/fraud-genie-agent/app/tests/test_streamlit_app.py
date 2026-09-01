import ast
from pathlib import Path

from streamlit.testing.v1 import AppTest


APP_PATH = Path(__file__).parents[1] / "src" / "app" / "app.py"


def test_app_uses_cross_version_dataframe_width_api():
    source = APP_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    string_widths = [
        keyword.value.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        for keyword in node.keywords
        if keyword.arg == "width"
        and isinstance(keyword.value, ast.Constant)
        and isinstance(keyword.value.value, str)
    ]

    assert string_widths == []
    assert source.count("use_container_width=True") == 3


def test_evidence_tab_and_other_dataframes_render_with_real_streamlit():
    script = """
from unittest.mock import patch
from src.pipeline.orchestrator import run_pipeline

gold = run_pipeline(seed=42).gold
with patch("src.data_access.load_gold_tables", return_value=gold):
    import src.app.app
"""

    app = AppTest.from_string(script).run(timeout=10)

    assert not app.exception
    assert len(app.dataframe) == 3
