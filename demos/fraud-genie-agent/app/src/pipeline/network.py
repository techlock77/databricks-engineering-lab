"""Case-network construction: builds all evidence-backed edges once, then
does a BFS from a seed account parameterized by an evidence-policy argument
(strict vs permissive), so the same edge set can be re-walked under either
policy without recomputation drift.
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field

import pandas as pd

from src.pipeline import policy


def build_network_edges(silver: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """One row per account-pair relationship, typed by the strongest
    evidence available for that pair (device_and_fund_flow > device_only >
    plain fund_flow)."""
    transfers = silver["transfers"]
    device_links = silver["device_links"]

    def pair_key(a: str, b: str) -> tuple[str, str]:
        return tuple(sorted([a, b]))

    pair_amount: dict[tuple[str, str], float] = defaultdict(float)
    for row in transfers.itertuples(index=False):
        pair_amount[pair_key(row.source_account, row.dest_account)] += row.amount

    edges: list[dict] = []
    covered_pairs: set[tuple[str, str]] = set()

    for row in device_links.itertuples(index=False):
        pair = pair_key(row.account_id, row.hub_account_id)
        amount = float(pair_amount.get(pair, 0.0))
        edges.append(
            {
                "account_a": pair[0],
                "account_b": pair[1],
                "edge_type": row.evidence_type,
                "device_id": row.device_id,
                "amount": amount,
            }
        )
        covered_pairs.add(pair)

    for pair, amount in pair_amount.items():
        if pair in covered_pairs:
            continue
        edges.append(
            {
                "account_a": pair[0],
                "account_b": pair[1],
                "edge_type": "fund_flow",
                "device_id": None,
                "amount": float(amount),
            }
        )

    edges_df = pd.DataFrame(
        edges,
        columns=["account_a", "account_b", "edge_type", "device_id", "amount"],
    )
    edges_df["strict_included"] = edges_df["edge_type"].apply(
        lambda t: policy.edge_included_under_policy(t, policy.POLICY_STRICT)
    )
    edges_df["permissive_included"] = edges_df["edge_type"].apply(
        lambda t: policy.edge_included_under_policy(t, policy.POLICY_PERMISSIVE)
    )
    return edges_df


@dataclass
class NetworkResult:
    seed_account: str
    evidence_policy: str
    accounts_in_network: list[str]        # includes the seed account
    other_connected_accounts: list[str]   # excludes the seed account
    shared_device_ids: list[str]
    total_exposure: float
    edges: pd.DataFrame = field(repr=False)

    @property
    def other_connected_accounts_count(self) -> int:
        return len(self.other_connected_accounts)

    @property
    def shared_device_count(self) -> int:
        return len(self.shared_device_ids)


def build_case_network(
    seed_account: str, edges_df: pd.DataFrame, evidence_policy: str
) -> NetworkResult:
    if evidence_policy not in policy.VALID_POLICIES:
        raise ValueError(f"unknown evidence policy: {evidence_policy!r}")

    included_col = (
        "strict_included" if evidence_policy == policy.POLICY_STRICT else "permissive_included"
    )
    active = edges_df[edges_df[included_col]]

    adjacency: dict[str, list[int]] = defaultdict(list)
    for idx, row in active.iterrows():
        adjacency[row["account_a"]].append(idx)
        adjacency[row["account_b"]].append(idx)

    visited = {seed_account}
    queue = deque([seed_account])
    while queue:
        current = queue.popleft()
        for edge_idx in adjacency.get(current, []):
            row = active.loc[edge_idx]
            neighbor = row["account_b"] if row["account_a"] == current else row["account_a"]
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append(neighbor)

    other_accounts = sorted(visited - {seed_account})
    accounts_in_network = [seed_account] + other_accounts

    network_edges = active[
        active["account_a"].isin(visited) & active["account_b"].isin(visited)
    ].copy()
    shared_device_ids = sorted(network_edges["device_id"].dropna().unique().tolist())
    total_exposure = float(network_edges["amount"].sum())

    return NetworkResult(
        seed_account=seed_account,
        evidence_policy=evidence_policy,
        accounts_in_network=accounts_in_network,
        other_connected_accounts=other_accounts,
        shared_device_ids=shared_device_ids,
        total_exposure=total_exposure,
        edges=network_edges,
    )
