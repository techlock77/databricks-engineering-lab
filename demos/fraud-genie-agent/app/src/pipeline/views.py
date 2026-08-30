"""Plain (non-Streamlit) view/filtering functions shared by the app UI and
by export.py, so the app's displayed rows and the export's item count are
guaranteed to match -- both call these same functions rather than each
recomputing their own filter.
"""

from __future__ import annotations

import pandas as pd

from src.pipeline import network, policy


def filter_evidence(evidence_df: pd.DataFrame, evidence_policy: str) -> pd.DataFrame:
    in_scope = evidence_df["evidence_type"].apply(
        lambda t: policy.evidence_included_under_policy(t, evidence_policy)
    )
    return evidence_df[in_scope].reset_index(drop=True)


def compute_network(
    gold: dict[str, pd.DataFrame], seed_account: str, evidence_policy: str
) -> network.NetworkResult:
    edges_df = gold["_network_edges_raw"]
    return network.build_case_network(seed_account, edges_df, evidence_policy)


def connected_accounts_table(
    gold: dict[str, pd.DataFrame], seed_account: str, evidence_policy: str
) -> pd.DataFrame:
    """Accounts connected to the seed account under the given policy,
    EXCLUDING the seed account itself."""
    net = compute_network(gold, seed_account, evidence_policy)
    accounts = gold["accounts"]
    out = accounts[accounts["account_id"].isin(net.other_connected_accounts)][
        ["account_id", "cohort", "account_role", "tenure_days", "risk_band"]
    ].reset_index(drop=True)
    return out


def blast_radius_metrics(
    gold: dict[str, pd.DataFrame], seed_account: str, evidence_policy: str
) -> dict:
    net = compute_network(gold, seed_account, evidence_policy)
    case_row = gold["case_summary"].iloc[0]
    return {
        "other_connected_accounts_count": net.other_connected_accounts_count,
        "shared_device_count": net.shared_device_count,
        "total_exposure": net.total_exposure,
        "potential_victims_count": int(case_row["potential_victims_count"]),
        "destinations_count": int(case_row["destinations_count"]),
    }
