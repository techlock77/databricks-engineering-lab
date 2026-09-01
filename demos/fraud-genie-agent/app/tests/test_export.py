import pytest

from src.genie.export import build_case_export
from src.pipeline import policy, views
from src.pipeline.orchestrator import run_pipeline


@pytest.fixture(scope="module")
def gold():
    return run_pipeline(seed=42).gold


@pytest.mark.parametrize(
    "evidence_policy", [policy.POLICY_STRICT, policy.POLICY_PERMISSIVE]
)
def test_case_export_is_scoped_to_each_of_the_nine_cases(gold, evidence_policy):
    assert len(gold["case_summary"]) == 9

    for case_row in gold["case_summary"].itertuples(index=False):
        bundle = build_case_export(gold, case_row.seed_account, evidence_policy)
        evidence_items = [
            item for item in bundle.items if item.item_type == "evidence"
        ]
        expected_evidence = views.case_evidence(
            gold, case_row.seed_account, evidence_policy
        )
        network_accounts = set(
            views.compute_network(
                gold, case_row.seed_account, evidence_policy
            ).accounts_in_network
        )
        exported_rows = gold["evidence"].set_index("evidence_id").loc[
            [item.source_row_id for item in evidence_items]
        ]

        assert bundle.case_id == case_row.case_id
        assert bundle.seed_account == case_row.seed_account
        assert {item.source_row_id for item in evidence_items} == set(expected_evidence.evidence_id)
        assert len(evidence_items) == len(expected_evidence)
        assert all(
            row.account_id in network_accounts
            or row.related_account_id in network_accounts
            for row in exported_rows.itertuples()
        )


def test_case_export_uses_selected_non_first_summary_and_persisted_citations(gold):
    selected = gold["case_summary"].iloc[1]
    first = gold["case_summary"].iloc[0]
    bundle = build_case_export(gold, selected["seed_account"], policy.POLICY_PERMISSIVE)
    summary = next(item for item in bundle.items if item.item_type == "case_summary")

    assert selected["case_id"] in summary.text
    assert first["case_id"] not in summary.text
    assert summary.source_row_id == selected["case_id"]

    citation_text = gold["export_citations"].set_index("source_row_id")["citation_text"]
    for item in (item for item in bundle.items if item.item_type == "evidence"):
        assert item.text == citation_text.loc[item.source_row_id]


def test_case_export_rejects_unknown_seed_account(gold):
    with pytest.raises(ValueError, match="No case summary found for seed account 'ACC_UNKNOWN'"):
        build_case_export(gold, "ACC_UNKNOWN", policy.POLICY_STRICT)
