"""Gold layer: builds the 8 investigation-ready tables Genie and the app
read from.

Headline numbers (total exposure, connected-account counts, shared-device
counts) are computed exactly once per evidence policy, via
network.build_case_network, and that same result object is fanned out into
every Gold table that reports it (gold_case_summary AND the seed account's
row in gold_accounts). No table recomputes these numbers independently --
that is what makes the cross-table consistency guarantee real rather than
coincidental.
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from src.data_generator import generator
from src.pipeline import detection, network, policy

GOLD_TABLE_NAMES = [
    "accounts",
    "evidence",
    "network_edges",
    "transfers",
    "case_summary",
    "control_cohort",
    "freshness",
    "export_citations",
]


def _edge_amount_for_pair(edges_df: pd.DataFrame, account_id: str, hub_account_id: str) -> float:
    pair = tuple(sorted([account_id, hub_account_id]))
    match = edges_df[
        (edges_df["account_a"] == pair[0]) & (edges_df["account_b"] == pair[1])
    ]
    if match.empty:
        return 0.0
    return float(match.iloc[0]["amount"])


def build_gold_accounts(
    detection_df: pd.DataFrame,
    case_networks: list[tuple[str, network.NetworkResult, network.NetworkResult]],
) -> pd.DataFrame:
    """Build the accounts Gold table with case-level headline numbers.

    Parameters
    ----------
    detection_df : pd.DataFrame
        Detection results for all accounts.
    case_networks : list of (seed_account, net_permissive, net_strict) tuples
        Network results for each case's seed account.
    """
    out = detection_df.copy()
    out["risk_band"] = out["is_flagged_mule_network"].apply(policy.risk_band_for)

    headline_cols = [
        "case_total_exposure_permissive",
        "case_total_exposure_strict",
        "case_other_connected_accounts_permissive",
        "case_other_connected_accounts_strict",
    ]
    for col in headline_cols:
        out[col] = pd.NA

    for seed_account, net_permissive, net_strict in case_networks:
        seed_mask = out["account_id"] == seed_account
        out.loc[seed_mask, "case_total_exposure_permissive"] = net_permissive.total_exposure
        out.loc[seed_mask, "case_total_exposure_strict"] = net_strict.total_exposure
        out.loc[seed_mask, "case_other_connected_accounts_permissive"] = (
            net_permissive.other_connected_accounts_count
        )
        out.loc[seed_mask, "case_other_connected_accounts_strict"] = (
            net_strict.other_connected_accounts_count
        )
    return out


def build_gold_evidence(silver: dict[str, pd.DataFrame], edges_df: pd.DataFrame) -> pd.DataFrame:
    accounts = silver["accounts"].set_index("account_id")
    device_links = silver["device_links"]
    sessions = silver["sessions"]

    rows = []
    for i, row in enumerate(device_links.itertuples(index=False), start=1):
        amount = _edge_amount_for_pair(edges_df, row.account_id, row.hub_account_id)
        cohort = accounts.loc[row.account_id, "cohort"] if row.account_id in accounts.index else None
        rail = "device+fund_flow" if row.evidence_type == policy.EVIDENCE_TYPE_DEVICE_AND_FUND_FLOW else "device"
        rows.append(
            {
                "evidence_id": f"EV_{i:04d}",
                "account_id": row.account_id,
                "related_account_id": row.hub_account_id,
                "device_id": row.device_id,
                "evidence_type": row.evidence_type,
                "confidence": policy.EVIDENCE_CONFIDENCE[row.evidence_type],
                "rail": rail,
                "cohort": cohort,
                "fund_flow_amount": amount,
                "description": (
                    f"{row.account_id} shares device {row.device_id} with "
                    f"{row.hub_account_id}"
                    + (
                        f"; corroborated by ${amount:,.2f} in fund flow between the two accounts."
                        if row.evidence_type == policy.EVIDENCE_TYPE_DEVICE_AND_FUND_FLOW
                        else "; no fund-flow relationship found between the two accounts."
                    )
                ),
            }
        )

    for j, row in enumerate(sessions.itertuples(index=False), start=1):
        cohort = accounts.loc[row.account_id, "cohort"] if row.account_id in accounts.index else None
        rows.append(
            {
                "evidence_id": f"EV_TAKEOVER_{j:04d}",
                "account_id": row.account_id,
                "related_account_id": None,
                "device_id": row.device_id,
                "evidence_type": policy.EVIDENCE_TYPE_ACCOUNT_TAKEOVER,
                "confidence": policy.EVIDENCE_CONFIDENCE[policy.EVIDENCE_TYPE_ACCOUNT_TAKEOVER],
                "rail": "device",
                "cohort": cohort,
                "fund_flow_amount": 0.0,
                "description": row.note,
            }
        )

    return pd.DataFrame(rows)


def build_gold_network_edges(edges_df: pd.DataFrame) -> pd.DataFrame:
    out = edges_df.copy()
    out.insert(0, "edge_id", [f"EDGE_{i:04d}" for i in range(1, len(out) + 1)])
    return out


def build_gold_transfers(silver: dict[str, pd.DataFrame]) -> pd.DataFrame:
    transfers = silver["transfers"]
    accounts = silver["accounts"][["account_id", "cohort"]]

    out = transfers.merge(
        accounts.rename(columns={"account_id": "source_account", "cohort": "source_cohort"}),
        on="source_account",
        how="left",
    ).merge(
        accounts.rename(columns={"account_id": "dest_account", "cohort": "dest_cohort"}),
        on="dest_account",
        how="left",
    )
    return out


def build_gold_case_summary(
    seed_account: str,
    net_permissive: network.NetworkResult,
    net_strict: network.NetworkResult,
    accounts: pd.DataFrame,
) -> pd.DataFrame:
    indexed = accounts.set_index("account_id")
    roles = indexed["account_role"]
    seed = indexed.loc[seed_account]

    def count_role(account_list: list[str], role: str) -> int:
        return sum(1 for a in account_list if roles.get(a) == role)

    row = {
        "case_id": f"CASE_{seed_account}",
        "seed_account": seed_account,
        "scenario_type": seed["scenario_type"],
        "scenario_label": seed["scenario_label"],
        "total_exposure_permissive": net_permissive.total_exposure,
        "total_exposure_strict": net_strict.total_exposure,
        "other_connected_accounts_permissive": net_permissive.other_connected_accounts_count,
        "other_connected_accounts_strict": net_strict.other_connected_accounts_count,
        "shared_devices_permissive": net_permissive.shared_device_count,
        "shared_devices_strict": net_strict.shared_device_count,
        "potential_victims_count": count_role(net_permissive.other_connected_accounts, "fan_in_source"),
        "destinations_count": count_role(net_permissive.other_connected_accounts, "fan_out_destination"),
    }
    return pd.DataFrame([row])


def build_gold_control_cohort(detection_df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "account_id",
        "cohort",
        "account_role",
        "tenure_days",
        "distinct_outbound_months",
        "distinct_source_count",
        "distinct_destination_count",
        "total_outbound_amount",
        "raw_fan_pattern_flag",
        "override_applied",
        "is_flagged_mule_network",
        "detection_reason",
    ]
    out = detection_df[detection_df["cohort"] == generator.COHORT_CONTROL][cols].reset_index(drop=True)
    return out


def build_gold_freshness(as_of: datetime) -> pd.DataFrame:
    rows = [
        {
            "table_name": f"gold_{name}",
            "last_refreshed_ts": as_of.isoformat(),
            "freshness_contract_hours": policy.FRESHNESS_CONTRACT_HOURS,
            "is_stale": False,
        }
        for name in GOLD_TABLE_NAMES
    ]
    return pd.DataFrame(rows)


def build_gold_export_citations(evidence_df: pd.DataFrame, case_summary_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for i, row in enumerate(evidence_df.itertuples(index=False), start=1):
        rows.append(
            {
                "citation_id": f"CIT_{i:04d}",
                "source_table": "gold_evidence",
                "source_row_id": row.evidence_id,
                "account_id": row.account_id,
                "citation_text": row.description,
            }
        )
    for row in case_summary_df.itertuples(index=False):
        rows.append(
            {
                "citation_id": f"CIT_CASE_{row.case_id}",
                "source_table": "gold_case_summary",
                "source_row_id": row.case_id,
                "account_id": row.seed_account,
                "citation_text": (
                    f"Case {row.case_id}: permissive exposure ${row.total_exposure_permissive:,.2f} "
                    f"across {row.other_connected_accounts_permissive} other connected accounts."
                ),
            }
        )
    return pd.DataFrame(rows)


def build_gold(
    bronze: dict[str, pd.DataFrame],
    silver: dict[str, pd.DataFrame],
    seed_account: str | None = None,
    seed_accounts: list[str] | None = None,
) -> dict[str, pd.DataFrame]:
    """Build all Gold tables.

    Parameters
    ----------
    bronze : dict[str, pd.DataFrame]
        Bronze layer tables.
    silver : dict[str, pd.DataFrame]
        Silver layer tables.
    seed_account : str, optional
        Single seed account for backward compatibility. If provided alone,
        only this account's case is included in case_summary.
    seed_accounts : list[str], optional
        List of seed accounts for multi-case support. If provided, each
        account gets its own case_summary row with independently computed
        metrics. Takes precedence over seed_account if both are provided.

    Returns
    -------
    dict[str, pd.DataFrame]
        All 8 Gold tables plus internal helper tables.
    """
    if seed_accounts is None:
        if seed_account is None:
            seed_account = generator.MULE_COLLECTOR
        seed_accounts = [seed_account]

    detection_df = detection.run_detection(silver)
    edges_df = network.build_network_edges(silver)

    case_networks: list[tuple[str, network.NetworkResult, network.NetworkResult]] = []
    case_summary_rows: list[pd.DataFrame] = []

    for sa in seed_accounts:
        net_permissive = network.build_case_network(sa, edges_df, policy.POLICY_PERMISSIVE)
        net_strict = network.build_case_network(sa, edges_df, policy.POLICY_STRICT)
        case_networks.append((sa, net_permissive, net_strict))
        case_summary_rows.append(
            build_gold_case_summary(sa, net_permissive, net_strict, silver["accounts"])
        )

    gold_accounts = build_gold_accounts(detection_df, case_networks)
    gold_evidence = build_gold_evidence(silver, edges_df)
    gold_network_edges = build_gold_network_edges(edges_df)
    gold_transfers = build_gold_transfers(silver)
    gold_case_summary = pd.concat(case_summary_rows, ignore_index=True)
    gold_control_cohort = build_gold_control_cohort(detection_df)
    gold_freshness = build_gold_freshness(datetime.combine(policy.REFERENCE_DATE, datetime.min.time()))
    gold_export_citations = build_gold_export_citations(gold_evidence, gold_case_summary)

    return {
        "accounts": gold_accounts,
        "evidence": gold_evidence,
        "network_edges": gold_network_edges,
        "transfers": gold_transfers,
        "case_summary": gold_case_summary,
        "control_cohort": gold_control_cohort,
        "freshness": gold_freshness,
        "export_citations": gold_export_citations,
        # not one of the 8 official Gold tables, but exposed so the app and
        # tests can re-walk the network under either policy without
        # rebuilding edges from scratch.
        "_network_edges_raw": edges_df,
        "_seed_account": seed_accounts[0],
    }
