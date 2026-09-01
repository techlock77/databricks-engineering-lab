import pandas as pd

from src.pipeline import policy, views


def test_blast_radius_metrics_uses_the_requested_seed_case_summary():
    gold = {
        "_network_edges_raw": pd.DataFrame(
            [
                {
                    "account_a": "ACC_UNRELATED_A",
                    "account_b": "ACC_UNRELATED_B",
                    "edge_type": "fund_flow",
                    "device_id": None,
                    "amount": 1.0,
                    "strict_included": True,
                    "permissive_included": True,
                }
            ]
        ),
        "case_summary": pd.DataFrame(
            [
                {
                    "seed_account": "ACC_FIRST",
                    "potential_victims_count": 101,
                    "destinations_count": 201,
                },
                {
                    "seed_account": "ACC_SELECTED",
                    "potential_victims_count": 7,
                    "destinations_count": 9,
                },
            ]
        ),
    }

    metrics = views.blast_radius_metrics(
        gold, "ACC_SELECTED", policy.DEFAULT_POLICY
    )

    assert metrics["potential_victims_count"] == 7
    assert metrics["destinations_count"] == 9
