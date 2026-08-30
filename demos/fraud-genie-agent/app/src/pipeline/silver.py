"""Silver layer: normalization and derived columns over the bronze tables.

No classification decisions happen here -- that all lives in policy.py /
detection.py. Silver only derives facts (tenure, month buckets, per-account
in/out aggregates) that detection needs as inputs.
"""

from __future__ import annotations

import pandas as pd

from src.pipeline import policy


def normalize_accounts(accounts: pd.DataFrame) -> pd.DataFrame:
    out = accounts.copy()
    out["tenure_days"] = out["open_date"].apply(lambda d: policy.tenure_days(d))
    return out


def normalize_transfers(transfers: pd.DataFrame) -> pd.DataFrame:
    out = transfers.copy()
    out["month"] = out["txn_date"].apply(lambda d: f"{d.year:04d}-{d.month:02d}")
    return out


def compute_account_stats(transfers_silver: pd.DataFrame) -> pd.DataFrame:
    """Per-account fan-in/fan-out aggregates used directly by detection.py.

    - distinct_source_count: number of distinct accounts sending TO this account
    - distinct_destination_count: number of distinct accounts this account sends TO
    - distinct_outbound_months: number of distinct months this account sent money out
    - total_outbound_amount / total_inbound_amount
    """
    inbound = (
        transfers_silver.groupby("dest_account")
        .agg(
            distinct_source_count=("source_account", "nunique"),
            total_inbound_amount=("amount", "sum"),
        )
        .reset_index()
        .rename(columns={"dest_account": "account_id"})
    )

    outbound = (
        transfers_silver.groupby("source_account")
        .agg(
            distinct_destination_count=("dest_account", "nunique"),
            distinct_outbound_months=("month", "nunique"),
            total_outbound_amount=("amount", "sum"),
        )
        .reset_index()
        .rename(columns={"source_account": "account_id"})
    )

    stats = pd.merge(inbound, outbound, on="account_id", how="outer")
    numeric_cols = [
        "distinct_source_count",
        "total_inbound_amount",
        "distinct_destination_count",
        "distinct_outbound_months",
        "total_outbound_amount",
    ]
    for col in numeric_cols:
        stats[col] = stats[col].fillna(0)
    stats["distinct_source_count"] = stats["distinct_source_count"].astype(int)
    stats["distinct_destination_count"] = stats["distinct_destination_count"].astype(int)
    stats["distinct_outbound_months"] = stats["distinct_outbound_months"].astype(int)
    return stats


def normalize_device_links(device_links: pd.DataFrame, transfers_silver: pd.DataFrame) -> pd.DataFrame:
    """Attach evidence_type to each device link by checking, independently
    of the generator, whether the linked account actually has a fund-flow
    relationship with the hub account it shares the device with. This is
    computed here (not hardcoded by the data generator) so device_only vs
    device_and_fund_flow reflects real derived fund-flow evidence."""
    if device_links.empty:
        out = device_links.copy()
        out["evidence_type"] = pd.Series(dtype="object")
        return out

    pair_has_flow = set(
        zip(transfers_silver["source_account"], transfers_silver["dest_account"])
    ) | set(zip(transfers_silver["dest_account"], transfers_silver["source_account"]))

    out = device_links.copy()
    out["evidence_type"] = out.apply(
        lambda row: (
            policy.EVIDENCE_TYPE_DEVICE_AND_FUND_FLOW
            if (row["account_id"], row["hub_account_id"]) in pair_has_flow
            else policy.EVIDENCE_TYPE_DEVICE_ONLY
        ),
        axis=1,
    )
    return out


def build_silver(bronze: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    accounts = normalize_accounts(bronze["accounts"])
    transfers = normalize_transfers(bronze["transfers"])
    account_stats = compute_account_stats(transfers)
    device_links = normalize_device_links(bronze["device_links"], transfers)

    return {
        "accounts": accounts,
        "transfers": transfers,
        "account_stats": account_stats,
        "device_links": device_links,
        "devices": bronze["devices"].copy(),
        "sessions": bronze["sessions"].copy(),
    }
