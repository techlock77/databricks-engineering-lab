"""Fan-in / fan-out / collector pattern detection, plus the
recurring-corridor override that protects the legitimate control cohort.

All classification logic itself lives in policy.py -- this module's job is
just to assemble the per-account inputs (from silver) and call
policy.classify_account for every account, so the override "does real
work" against real computed stats rather than being a special case for a
known cohort label.
"""

from __future__ import annotations

import pandas as pd

from src.pipeline import policy


def run_detection(silver: dict[str, pd.DataFrame]) -> pd.DataFrame:
    accounts = silver["accounts"]
    stats = silver["account_stats"]

    merged = pd.merge(accounts, stats, on="account_id", how="left")
    for col in [
        "distinct_source_count",
        "distinct_destination_count",
        "distinct_outbound_months",
        "total_outbound_amount",
        "total_inbound_amount",
    ]:
        merged[col] = merged[col].fillna(0)
        if col != "total_outbound_amount" and col != "total_inbound_amount":
            merged[col] = merged[col].astype(int)

    results = []
    for row in merged.itertuples(index=False):
        result = policy.classify_account(
            account_id=row.account_id,
            distinct_source_count=row.distinct_source_count,
            distinct_destination_count=row.distinct_destination_count,
            distinct_outbound_months=row.distinct_outbound_months,
            total_outbound_amount=row.total_outbound_amount,
            account_tenure_days=row.tenure_days,
        )
        results.append(
            {
                "account_id": result.account_id,
                "raw_fan_pattern_flag": result.raw_flag,
                "override_applied": result.override_applied,
                "is_flagged_mule_network": result.is_flagged_mule_network,
                "detection_reason": result.reason,
            }
        )

    detection_df = pd.DataFrame(results)
    return pd.merge(merged, detection_df, on="account_id", how="left")
