"""Generate deterministic, independently selectable MuleGraph cases.

The curated dataset contains nine intentionally different investigation
stories plus a small baseline-noise cohort. Every value is derived from a
single ``random.Random`` instance and the fixed reference date in ``policy``,
so a seed produces byte-identical tables on every run.
"""

from __future__ import annotations

import random
import warnings
from datetime import date, timedelta

import pandas as pd

from src.pipeline import policy

MULE_COLLECTOR = "ACC_M_COLLECTOR"
MULE_LOOKALIKE = "ACC_M_LOOKALIKE"
MULE_SOURCES = [f"ACC_M_SRC_{i}" for i in range(1, 6)]
MULE_DESTINATIONS = [f"ACC_M_DEST_{i}" for i in range(1, 4)]
CONTROL_HUB = "ACC_C_HUB"
CONTROL_SOURCES = [f"ACC_C_SRC_{i}" for i in range(1, 6)]
CONTROL_DESTINATIONS = [f"ACC_C_DEST_{i}" for i in range(1, 4)]
DEVICE_MULE_SHARED = "DEV_M_001"
DEVICE_MULE_LOOKALIKE = "DEV_M_002"
DEVICE_CONTROL_SHARED = "DEV_C_001"
TAKEOVER_SESSION_ID = "SESS_M_001"
BASELINE_ACCOUNT_COUNT = 20
COHORT_MULE = "mule_network"
COHORT_CONTROL = "control_remittance"
COHORT_BASELINE = "baseline"
SCENARIOS = (
    ("simple_transfer", "Simple suspicious transfer", MULE_COLLECTOR),
    ("rapid_pass_through", "Rapid pass-through money mule", "ACC_RAPID_COLLECTOR"),
    ("victim_funnel", "Multiple victims funneling into one account", "ACC_FUNNEL_COLLECTOR"),
    ("multi_hop", "Multi-hop fund movement", "ACC_CHAIN_COLLECTOR"),
    ("shared_device_cluster", "Shared device/account relationships", "ACC_DEVICE_COLLECTOR"),
    ("normal_control", "Normal account (not flagged)", CONTROL_HUB),
    ("threshold_boundary", "Exact detection-threshold boundary", "ACC_BOUNDARY_COLLECTOR"),
    ("insufficient_evidence", "Insufficient strict evidence", "ACC_THIN_COLLECTOR"),
    ("large_network", "Larger connected mule network", "ACC_LARGE_COLLECTOR"),
)
SCENARIO_SEED_ACCOUNTS = [scenario[2] for scenario in SCENARIOS]


def _month_dates(rng: random.Random, start_year: int, start_month: int, count: int) -> list[date]:
    """Monthly dates with seeded day jitter; no case is pinned to the 10th."""
    dates = []
    for offset in range(count):
        absolute_month = start_year * 12 + start_month - 1 + offset
        dates.append(
            date(
                absolute_month // 12,
                absolute_month % 12 + 1,
                rng.randint(1, 28),
            )
        )
    return dates


def generate_dataset(
    seed: int = policy.DEFAULT_SEED,
) -> dict[str, pd.DataFrame]:
    """Build the five Bronze tables for all curated investigation scenarios."""
    rng = random.Random(seed)
    accounts: list[dict] = []
    devices: list[dict] = []
    device_links: list[dict] = []
    transfers: list[dict] = []
    sessions: list[dict] = []
    transaction_counter = 0

    def add_account(
        account_id: str,
        role: str,
        scenario_type: str,
        scenario_label: str,
        open_date: date = date(2026, 1, 1),
        cohort: str = COHORT_MULE,
    ) -> None:
        accounts.append(
            dict(
                account_id=account_id,
                cohort=cohort,
                account_role=role,
                open_date=open_date,
                display_name=account_id.replace("ACC_", "").replace("_", " ").title(),
                scenario_type=scenario_type,
                scenario_label=scenario_label,
            )
        )

    def add_transfer(
        source_account: str,
        destination_account: str,
        amount: float,
        transaction_date: date,
        channel: str = "ach",
    ) -> None:
        nonlocal transaction_counter
        transaction_counter += 1
        transfers.append(
            dict(
                txn_id=f"TXN_{transaction_counter:05d}",
                source_account=source_account,
                dest_account=destination_account,
                amount=round(amount, 2),
                txn_date=transaction_date,
                channel=channel,
            )
        )

    def add_fan_case(
        scenario_type: str,
        scenario_label: str,
        collector: str,
        source_count: int = 4,
        destination_count: int = 3,
        month_count: int = 3,
        inbound_amount: float = 1800,
        outbound_amount: float = 2400,
        account_prefix: str | None = None,
        device_mode: str | None = "flow",
        collector_open_date: date = date(2026, 1, 1),
        cohort: str = COHORT_MULE,
    ) -> tuple[list[str], list[str]]:
        """Add a reusable fan-in/fan-out case with optional device evidence."""
        account_prefix = account_prefix or collector.removesuffix("_COLLECTOR")
        sources = [f"{account_prefix}_SRC_{i}" for i in range(1, source_count + 1)]
        destinations = [f"{account_prefix}_DEST_{i}" for i in range(1, destination_count + 1)]
        add_account(
            collector,
            "collector",
            scenario_type,
            scenario_label,
            collector_open_date,
            cohort,
        )
        for source in sources:
            add_account(
                source,
                "fan_in_source",
                scenario_type,
                scenario_label,
                date(2025, 1, 1),
                cohort,
            )
        for destination in destinations:
            add_account(
                destination,
                "fan_out_destination",
                scenario_type,
                scenario_label,
                date(2025, 1, 1),
                cohort,
            )
        transaction_dates = _month_dates(rng, 2026, 3, month_count)
        for source in sources:
            for transaction_date in transaction_dates:
                add_transfer(
                    source,
                    collector,
                    inbound_amount + rng.uniform(-50, 50),
                    transaction_date,
                )
        for destination in destinations:
            for transaction_date in transaction_dates:
                add_transfer(
                    collector,
                    destination,
                    outbound_amount + rng.uniform(-50, 50),
                    transaction_date,
                    "wire",
                )
        if device_mode:
            device_id = f"DEV_{scenario_type.upper()}"
            devices.append(dict(device_id=device_id, first_seen_date=date(2026, 2, 1)))
            related_accounts = (
                sources
                if device_mode == "flow"
                else [f"{account_prefix}_DEVICE_ONLY_{i}" for i in range(1, 6)]
            )
            for related_account in related_accounts:
                if device_mode != "flow":
                    add_account(
                        related_account,
                        "device_only_lookalike",
                        scenario_type,
                        scenario_label,
                    )
                device_links.append(
                    dict(
                        link_id=f"LINK_{device_id}_{related_account}",
                        device_id=device_id,
                        account_id=related_account,
                        hub_account_id=collector,
                        linked_date=date(2026, 2, 2),
                    )
                )
        return sources, destinations

    # 1. A compact, unambiguous collector case, augmented with takeover
    # provenance and one device-only lookalike for policy comparison.
    simple_sources, _ = add_fan_case(
        *SCENARIOS[0][:2], MULE_COLLECTOR, source_count=5, account_prefix="ACC_M"
    )
    sessions.append(
        dict(
            session_id=TAKEOVER_SESSION_ID,
            device_id="DEV_SIMPLE_TRANSFER",
            account_id=simple_sources[0],
            compromise_type="credential_stuffing",
            session_date=date(2026, 2, 15),
            note="Compromised session preceded fund movement.",
        )
    )
    add_account(MULE_LOOKALIKE, "device_only_lookalike", *SCENARIOS[0][:2])
    devices.append(dict(device_id=DEVICE_MULE_LOOKALIKE, first_seen_date=date(2026, 2, 1)))
    device_links.append(
        dict(
            link_id="LINK_M_LOOKALIKE",
            device_id=DEVICE_MULE_LOOKALIKE,
            account_id=MULE_LOOKALIKE,
            hub_account_id=MULE_COLLECTOR,
            linked_date=date(2026, 2, 2),
        )
    )
    # 2. Every monthly receipt arrives on days 1-4 and leaves on day 6, making
    # rapid pass-through visible at the transaction-date level.
    rapid_type, rapid_label, rapid_collector = SCENARIOS[1]
    add_account(rapid_collector, "collector", rapid_type, rapid_label)
    for source_number in range(1, 5):
        source = f"ACC_RAPID_SRC_{source_number}"
        add_account(source, "fan_in_source", rapid_type, rapid_label)
        for month in (3, 4, 5):
            add_transfer(source, rapid_collector, 1000, date(2026, month, source_number))
    for destination_number in range(1, 4):
        destination = f"ACC_RAPID_DEST_{destination_number}"
        add_account(destination, "fan_out_destination", rapid_type, rapid_label)
        for month in (3, 4, 5):
            add_transfer(rapid_collector, destination, 2400, date(2026, month, 6), "wire")

    # 3. Twelve distinct sources model many victims funneling into one account.
    add_fan_case(*SCENARIOS[2][:2], SCENARIOS[2][2], source_count=12, inbound_amount=900)

    # 4. Extend a qualifying collector topology through three intermediaries
    # and a final destination to prove the graph is not hub-only.
    _, chain_destinations = add_fan_case(*SCENARIOS[3][:2], SCENARIOS[3][2], device_mode=None)
    previous_node = chain_destinations[0]
    for hop_number in range(1, 4):
        hop_account = f"ACC_CHAIN_HOP_{hop_number}"
        add_account(hop_account, "intermediate", *SCENARIOS[3][:2])
        add_transfer(
            previous_node,
            hop_account,
            6000,
            date(2026, 5, 20 + hop_number),
            "wire",
        )
        previous_node = hop_account
    final_destination = "ACC_CHAIN_FINAL"
    add_account(final_destination, "final_destination", *SCENARIOS[3][:2])
    add_transfer(previous_node, final_destination, 5800, date(2026, 5, 25), "wire")

    # 5. Device-only lookalikes amplify the strict/permissive policy difference
    # while the qualifying fund-flow core remains unchanged.
    add_fan_case(*SCENARIOS[4][:2], SCENARIOS[4][2], device_mode="only")

    # 6. A long-tenured, eight-month remittance corridor deliberately meets the
    # raw shape but is protected by the legitimate-corridor override.
    add_fan_case(
        *SCENARIOS[5][:2],
        CONTROL_HUB,
        source_count=5,
        month_count=8,
        inbound_amount=1800,
        outbound_amount=2500,
        account_prefix="ACC_C",
        collector_open_date=date(2022, 1, 1),
        cohort=COHORT_CONTROL,
    )

    # 7. Counts and total outbound flow sit exactly on every inclusive policy
    # threshold, demonstrating deterministic boundary behavior.
    boundary_type, boundary_label, boundary_collector = SCENARIOS[6]
    add_account(boundary_collector, "collector", boundary_type, boundary_label)
    for source_number in range(1, policy.FAN_IN_MIN_SOURCES + 1):
        source = f"ACC_BOUNDARY_SRC_{source_number}"
        add_account(source, "fan_in_source", boundary_type, boundary_label)
        add_transfer(source, boundary_collector, 100, date(2026, 3, source_number))
    for destination_number, amount in enumerate((6666.67, 6666.67, 6666.66), 1):
        destination = f"ACC_BOUNDARY_DEST_{destination_number}"
        add_account(destination, "fan_out_destination", boundary_type, boundary_label)
        add_transfer(
            boundary_collector,
            destination,
            amount,
            date(2026, destination_number + 2, destination_number),
            "wire",
        )

    # 8. Device relationships have no fund-flow corroboration, yielding an
    # empty strict evidence view but visible permissive evidence.
    add_fan_case(*SCENARIOS[7][:2], SCENARIOS[7][2], device_mode="only")

    # 9. Eighteen sources and ten destinations provide a materially larger
    # connected component for graph and Money Flow scaling demonstrations.
    add_fan_case(
        *SCENARIOS[8][:2],
        SCENARIOS[8][2],
        source_count=18,
        destination_count=10,
        inbound_amount=1200,
        outbound_amount=2600,
    )

    # Ordinary unrelated transfers provide a false-positive baseline.
    baseline_accounts = [f"ACC_B_{i}" for i in range(1, BASELINE_ACCOUNT_COUNT + 1)]
    for baseline_account in baseline_accounts:
        add_account(
            baseline_account,
            "baseline",
            "baseline_noise",
            "Ordinary baseline account",
            date(2023, 1, 1),
            COHORT_BASELINE,
        )
    for baseline_account in baseline_accounts:
        add_transfer(
            baseline_account,
            rng.choice([account for account in baseline_accounts if account != baseline_account]),
            rng.uniform(50, 350),
            policy.REFERENCE_DATE - timedelta(days=rng.randint(1, 300)),
        )
    return {
        "accounts": pd.DataFrame(accounts),
        "devices": pd.DataFrame(devices),
        "device_links": pd.DataFrame(device_links),
        "transfers": pd.DataFrame(transfers),
        "sessions": pd.DataFrame(sessions),
    }


def generate_dataset_scaled(
    seed: int = policy.DEFAULT_SEED, scale_factor: int = 1
) -> dict[str, pd.DataFrame]:
    """Return the curated dataset through the legacy scaling call signature.

    The curated nine-scenario dataset supersedes scale-factor-based repetition.
    ``scale_factor`` is retained only for backward call compatibility; values
    other than 1 do not alter the dataset and emit a visible warning.
    """
    if scale_factor < 1:
        raise ValueError("scale_factor must be >= 1")
    if scale_factor != 1:
        warnings.warn(
            "scale_factor is deprecated and no longer changes dataset size; "
            "the curated nine-scenario dataset is always generated",
            FutureWarning,
            stacklevel=2,
        )
    return generate_dataset(seed)
