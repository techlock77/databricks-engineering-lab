"""Deterministic synthetic data generator for MuleGraph Investigator.

Produces four cohorts in one dataset, for a fixed seed:

  (a) a mule network        -- real fan-in (5 sources -> 1 collector) AND
                                fan-out (collector -> 3 distinct destinations,
                                recurring across 3 months), plus one
                                device-only "lookalike" account that shares a
                                device with the collector but never
                                transacts with it.
  (b) a control cohort       -- a legitimate remittance hub with the SAME
                                fan-in/fan-out shape and the SAME large
                                per-transfer amounts, but long account tenure
                                (900+ days) and an 8-month recurring corridor.
  (c) account-takeover       -- a compromised-session-to-device record folded
      provenance                in as evidence for one of the mule accounts.
  (d) baseline noise         -- ordinary small, one-off transactions between
                                unrelated accounts.

Every entity, date, amount and ID is derived from a single seeded
`random.Random` instance, so the same seed always produces byte-identical
output. Reference date (deterministic "today" for tenure math) lives in
policy.REFERENCE_DATE, never datetime.now().
"""

from __future__ import annotations

import random
from datetime import date, timedelta

import pandas as pd

from src.pipeline import policy

# ---------------------------------------------------------------------------
# Static IDs (kept explicit / non-randomized so downstream code and tests can
# reference specific accounts by name).
# ---------------------------------------------------------------------------

MULE_COLLECTOR = "ACC_M_COLLECTOR"
MULE_SOURCES = [f"ACC_M_SRC_{i}" for i in range(1, 6)]  # 5 fan-in sources
MULE_DESTINATIONS = [f"ACC_M_DEST_{j}" for j in range(1, 4)]  # 3 fan-out destinations
MULE_LOOKALIKE = "ACC_M_LOOKALIKE"

CONTROL_HUB = "ACC_C_HUB"
CONTROL_SOURCES = [f"ACC_C_SRC_{i}" for i in range(1, 6)]
CONTROL_DESTINATIONS = [f"ACC_C_DEST_{j}" for j in range(1, 4)]

DEVICE_MULE_SHARED = "DEV_M_001"       # collector <-> fan-in sources (fund-flow corroborated)
DEVICE_MULE_LOOKALIKE = "DEV_M_002"    # collector <-> lookalike (device-only)
DEVICE_CONTROL_SHARED = "DEV_C_001"    # hub <-> control sources (fund-flow corroborated)

TAKEOVER_SESSION_ID = "SESS_M_001"

BASELINE_ACCOUNT_COUNT = 20

COHORT_MULE = "mule_network"
COHORT_CONTROL = "control_remittance"
COHORT_BASELINE = "baseline"


def _mule_ids(case_index: int) -> tuple:
    """Generate unique IDs for a mule network case. Case 0 uses the original
    static IDs for backward compatibility with scale_factor=1."""
    if case_index == 0:
        return (
            MULE_COLLECTOR,
            MULE_SOURCES,
            MULE_DESTINATIONS,
            MULE_LOOKALIKE,
            DEVICE_MULE_SHARED,
            DEVICE_MULE_LOOKALIKE,
            TAKEOVER_SESSION_ID,
        )
    suffix = f"_{case_index:02d}"
    collector = f"ACC_M{suffix}_COLLECTOR"
    sources = [f"ACC_M{suffix}_SRC_{i}" for i in range(1, 6)]
    destinations = [f"ACC_M{suffix}_DEST_{j}" for j in range(1, 4)]
    lookalike = f"ACC_M{suffix}_LOOKALIKE"
    dev_shared = f"DEV_M{suffix}_001"
    dev_lookalike = f"DEV_M{suffix}_002"
    session_id = f"SESS_M{suffix}_001"
    return collector, sources, destinations, lookalike, dev_shared, dev_lookalike, session_id


def _control_ids(case_index: int) -> tuple:
    """Generate unique IDs for a control cohort case. Case 0 uses the original
    static IDs for backward compatibility with scale_factor=1."""
    if case_index == 0:
        return CONTROL_HUB, CONTROL_SOURCES, CONTROL_DESTINATIONS, DEVICE_CONTROL_SHARED
    suffix = f"_{case_index:02d}"
    hub = f"ACC_C{suffix}_HUB"
    sources = [f"ACC_C{suffix}_SRC_{i}" for i in range(1, 6)]
    destinations = [f"ACC_C{suffix}_DEST_{j}" for j in range(1, 4)]
    device = f"DEV_C{suffix}_001"
    return hub, sources, destinations, device


def _months(start_year: int, start_month: int, count: int) -> list[date]:
    """Return `count` dates, one per month, on the 10th of each month."""
    out = []
    y, m = start_year, start_month
    for _ in range(count):
        out.append(date(y, m, 10))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return out


def _jitter_amount(rng: random.Random, base: float, spread: float) -> float:
    return round(base + rng.uniform(-spread, spread), 2)


def generate_dataset(seed: int = policy.DEFAULT_SEED) -> dict[str, pd.DataFrame]:
    """Generate the full synthetic bronze dataset for the given seed.

    Returns a dict of DataFrames: accounts, devices, device_links, transfers,
    sessions.
    """
    rng = random.Random(seed)

    accounts: list[dict] = []
    devices: list[dict] = []
    device_links: list[dict] = []
    transfers: list[dict] = []
    sessions: list[dict] = []

    txn_counter = 0

    def next_txn_id() -> str:
        nonlocal txn_counter
        txn_counter += 1
        return f"TXN_{txn_counter:05d}"

    # -----------------------------------------------------------------
    # (a) Mule network
    # -----------------------------------------------------------------
    accounts.append(
        {
            "account_id": MULE_COLLECTOR,
            "cohort": COHORT_MULE,
            "account_role": "collector",
            "open_date": date(2026, 4, 1),
            "display_name": "Collector Account",
        }
    )
    for src in MULE_SOURCES:
        accounts.append(
            {
                "account_id": src,
                "cohort": COHORT_MULE,
                "account_role": "fan_in_source",
                "open_date": date(2026, 1, 15) - timedelta(days=rng.randint(0, 120)),
                "display_name": f"Source {src.split('_')[-1]}",
            }
        )
    for dest in MULE_DESTINATIONS:
        accounts.append(
            {
                "account_id": dest,
                "cohort": COHORT_MULE,
                "account_role": "fan_out_destination",
                "open_date": date(2026, 2, 1) - timedelta(days=rng.randint(0, 90)),
                "display_name": f"Destination {dest.split('_')[-1]}",
            }
        )
    accounts.append(
        {
            "account_id": MULE_LOOKALIKE,
            "cohort": COHORT_MULE,
            "account_role": "device_only_lookalike",
            "open_date": date(2026, 5, 1),
            "display_name": "Device-Linked Lookalike",
        }
    )

    mule_months = _months(2026, 3, 3)  # Mar, Apr, May 2026 -> 3 distinct months

    # Fan-in: each of 5 sources sends to the collector every month.
    for src in MULE_SOURCES:
        for m in mule_months:
            transfers.append(
                {
                    "txn_id": next_txn_id(),
                    "source_account": src,
                    "dest_account": MULE_COLLECTOR,
                    "amount": _jitter_amount(rng, 2500.0, 200.0),
                    "txn_date": m,
                    "channel": "ach",
                }
            )

    # Fan-out: collector sends to all 3 distinct destinations every month
    # (recurring corridor, not a single repeated destination).
    for dest in MULE_DESTINATIONS:
        for m in mule_months:
            transfers.append(
                {
                    "txn_id": next_txn_id(),
                    "source_account": MULE_COLLECTOR,
                    "dest_account": dest,
                    "amount": _jitter_amount(rng, 3000.0, 250.0),
                    "txn_date": m,
                    "channel": "wire",
                }
            )

    # Shared device: collector <-> all 5 fan-in sources (fund-flow corroborated).
    devices.append(
        {"device_id": DEVICE_MULE_SHARED, "first_seen_date": date(2026, 2, 20)}
    )
    for src in MULE_SOURCES:
        device_links.append(
            {
                "link_id": f"LINK_{DEVICE_MULE_SHARED}_{src}",
                "device_id": DEVICE_MULE_SHARED,
                "account_id": src,
                "hub_account_id": MULE_COLLECTOR,
                "linked_date": date(2026, 2, 25),
            }
        )

    # Shared device: collector <-> lookalike (device-only, no transfers at all).
    devices.append(
        {"device_id": DEVICE_MULE_LOOKALIKE, "first_seen_date": date(2026, 5, 2)}
    )
    device_links.append(
        {
            "link_id": f"LINK_{DEVICE_MULE_LOOKALIKE}_{MULE_LOOKALIKE}",
            "device_id": DEVICE_MULE_LOOKALIKE,
            "account_id": MULE_LOOKALIKE,
            "hub_account_id": MULE_COLLECTOR,
            "linked_date": date(2026, 5, 3),
        }
    )

    # (c) Account-takeover provenance: how DEVICE_MULE_SHARED first became
    # linked to the network, via a compromised session on ACC_M_SRC_1.
    sessions.append(
        {
            "session_id": TAKEOVER_SESSION_ID,
            "device_id": DEVICE_MULE_SHARED,
            "account_id": MULE_SOURCES[0],
            "compromise_type": "credential_stuffing",
            "session_date": date(2026, 2, 15),
            "note": (
                "Session flagged for credential-stuffing indicators; device "
                f"{DEVICE_MULE_SHARED} was first linked to the network through "
                "this compromised session, prior to any fund movement."
            ),
        }
    )

    # -----------------------------------------------------------------
    # (b) Legitimate-remittance control cohort
    # -----------------------------------------------------------------
    accounts.append(
        {
            "account_id": CONTROL_HUB,
            "cohort": COHORT_CONTROL,
            "account_role": "hub",
            "open_date": date(2022, 1, 1),
            "display_name": "Remittance Hub",
        }
    )
    for src in CONTROL_SOURCES:
        accounts.append(
            {
                "account_id": src,
                "cohort": COHORT_CONTROL,
                "account_role": "source",
                "open_date": date(2021, 6, 1) - timedelta(days=rng.randint(0, 400)),
                "display_name": f"Remittance Sender {src.split('_')[-1]}",
            }
        )
    for dest in CONTROL_DESTINATIONS:
        accounts.append(
            {
                "account_id": dest,
                "cohort": COHORT_CONTROL,
                "account_role": "destination",
                "open_date": date(2021, 3, 1) - timedelta(days=rng.randint(0, 400)),
                "display_name": f"Remittance Corridor {dest.split('_')[-1]}",
            }
        )

    control_months = _months(2025, 10, 8)  # Oct 2025 -> May 2026, 8 distinct months

    for src in CONTROL_SOURCES:
        for m in control_months:
            transfers.append(
                {
                    "txn_id": next_txn_id(),
                    "source_account": src,
                    "dest_account": CONTROL_HUB,
                    "amount": _jitter_amount(rng, 2000.0, 150.0),
                    "txn_date": m,
                    "channel": "ach",
                }
            )
    for dest in CONTROL_DESTINATIONS:
        for m in control_months:
            transfers.append(
                {
                    "txn_id": next_txn_id(),
                    "source_account": CONTROL_HUB,
                    "dest_account": dest,
                    "amount": _jitter_amount(rng, 2500.0, 200.0),
                    "txn_date": m,
                    "channel": "wire",
                }
            )

    # Control cohort ALSO has device evidence corroborating its fan-in, on
    # the same footing as the mule network -- the override must protect it
    # despite full device+fund-flow corroboration, not because the evidence
    # is weaker.
    devices.append(
        {"device_id": DEVICE_CONTROL_SHARED, "first_seen_date": date(2021, 5, 1)}
    )
    for src in CONTROL_SOURCES:
        device_links.append(
            {
                "link_id": f"LINK_{DEVICE_CONTROL_SHARED}_{src}",
                "device_id": DEVICE_CONTROL_SHARED,
                "account_id": src,
                "hub_account_id": CONTROL_HUB,
                "linked_date": date(2021, 5, 5),
            }
        )

    # -----------------------------------------------------------------
    # (d) Baseline noise
    # -----------------------------------------------------------------
    baseline_ids = [f"ACC_B_{i}" for i in range(1, BASELINE_ACCOUNT_COUNT + 1)]
    for bid in baseline_ids:
        accounts.append(
            {
                "account_id": bid,
                "cohort": COHORT_BASELINE,
                "account_role": "baseline",
                "open_date": date(2024, 1, 1) - timedelta(days=rng.randint(0, 1000)),
                "display_name": f"Customer {bid.split('_')[-1]}",
            }
        )
    for bid in baseline_ids:
        n_txns = rng.randint(1, 3)
        for _ in range(n_txns):
            counterparty = rng.choice([b for b in baseline_ids if b != bid])
            txn_day_offset = rng.randint(0, 360)
            transfers.append(
                {
                    "txn_id": next_txn_id(),
                    "source_account": bid,
                    "dest_account": counterparty,
                    "amount": _jitter_amount(rng, 200.0, 150.0),
                    "txn_date": policy.REFERENCE_DATE - timedelta(days=txn_day_offset),
                    "channel": rng.choice(["ach", "wire", "card"]),
                }
            )

    return {
        "accounts": pd.DataFrame(accounts),
        "devices": pd.DataFrame(devices),
        "device_links": pd.DataFrame(device_links),
        "transfers": pd.DataFrame(transfers),
        "sessions": pd.DataFrame(sessions),
    }


def generate_dataset_scaled(
    seed: int = policy.DEFAULT_SEED,
    scale_factor: int = 1,
) -> dict[str, pd.DataFrame]:
    """Generate scaled synthetic dataset with multiple independent cases.

    At scale_factor=1 (default), produces exactly the same output as
    generate_dataset(seed) -- byte-identical, all existing tests pass.

    At scale_factor > 1, produces:
      - `scale_factor` independent mule-network cases (distinct seeds,
        distinct networks, distinct amounts/dates shifted in time)
      - `scale_factor` independent control-cohort hub accounts
      - A larger pool of baseline/noise accounts and transactions spanning
        a wider date range

    Parameters
    ----------
    seed : int
        Base random seed for reproducibility.
    scale_factor : int
        Number of independent cases to generate. Must be >= 1.

    Returns
    -------
    dict[str, pd.DataFrame]
        Bronze tables: accounts, devices, device_links, transfers, sessions.
    """
    if scale_factor < 1:
        raise ValueError("scale_factor must be >= 1")

    if scale_factor == 1:
        return generate_dataset(seed=seed)

    rng = random.Random(seed)

    accounts: list[dict] = []
    devices: list[dict] = []
    device_links: list[dict] = []
    transfers: list[dict] = []
    sessions: list[dict] = []

    txn_counter = 0

    def next_txn_id() -> str:
        nonlocal txn_counter
        txn_counter += 1
        return f"TXN_{txn_counter:05d}"

    for case_idx in range(scale_factor):
        (
            collector,
            mule_sources,
            mule_destinations,
            lookalike,
            dev_shared,
            dev_lookalike,
            session_id,
        ) = _mule_ids(case_idx)

        month_offset = case_idx * 2
        year_add, month_start = divmod(2 + month_offset, 12)
        if month_start == 0:
            month_start = 12
            year_add -= 1
        mule_months = _months(2026 + year_add, month_start + 1, 3)

        accounts.append(
            {
                "account_id": collector,
                "cohort": COHORT_MULE,
                "account_role": "collector",
                "open_date": date(2026, 4, 1) + timedelta(days=case_idx * 30),
                "display_name": f"Collector Account (Case {case_idx + 1})",
            }
        )
        for src in mule_sources:
            accounts.append(
                {
                    "account_id": src,
                    "cohort": COHORT_MULE,
                    "account_role": "fan_in_source",
                    "open_date": date(2026, 1, 15) - timedelta(days=rng.randint(0, 120)),
                    "display_name": f"Source {src.split('_')[-1]}",
                }
            )
        for dest in mule_destinations:
            accounts.append(
                {
                    "account_id": dest,
                    "cohort": COHORT_MULE,
                    "account_role": "fan_out_destination",
                    "open_date": date(2026, 2, 1) - timedelta(days=rng.randint(0, 90)),
                    "display_name": f"Destination {dest.split('_')[-1]}",
                }
            )
        accounts.append(
            {
                "account_id": lookalike,
                "cohort": COHORT_MULE,
                "account_role": "device_only_lookalike",
                "open_date": date(2026, 5, 1) + timedelta(days=case_idx * 15),
                "display_name": f"Device-Linked Lookalike (Case {case_idx + 1})",
            }
        )

        base_fan_in_amount = 2500.0 + case_idx * 500
        for src in mule_sources:
            for m in mule_months:
                transfers.append(
                    {
                        "txn_id": next_txn_id(),
                        "source_account": src,
                        "dest_account": collector,
                        "amount": _jitter_amount(rng, base_fan_in_amount, 200.0),
                        "txn_date": m,
                        "channel": "ach",
                    }
                )

        base_fan_out_amount = 3000.0 + case_idx * 600
        for dest in mule_destinations:
            for m in mule_months:
                transfers.append(
                    {
                        "txn_id": next_txn_id(),
                        "source_account": collector,
                        "dest_account": dest,
                        "amount": _jitter_amount(rng, base_fan_out_amount, 250.0),
                        "txn_date": m,
                        "channel": "wire",
                    }
                )

        devices.append(
            {
                "device_id": dev_shared,
                "first_seen_date": date(2026, 2, 20) + timedelta(days=case_idx * 7),
            }
        )
        for src in mule_sources:
            device_links.append(
                {
                    "link_id": f"LINK_{dev_shared}_{src}",
                    "device_id": dev_shared,
                    "account_id": src,
                    "hub_account_id": collector,
                    "linked_date": date(2026, 2, 25) + timedelta(days=case_idx * 7),
                }
            )

        devices.append(
            {
                "device_id": dev_lookalike,
                "first_seen_date": date(2026, 5, 2) + timedelta(days=case_idx * 10),
            }
        )
        device_links.append(
            {
                "link_id": f"LINK_{dev_lookalike}_{lookalike}",
                "device_id": dev_lookalike,
                "account_id": lookalike,
                "hub_account_id": collector,
                "linked_date": date(2026, 5, 3) + timedelta(days=case_idx * 10),
            }
        )

        sessions.append(
            {
                "session_id": session_id,
                "device_id": dev_shared,
                "account_id": mule_sources[0],
                "compromise_type": "credential_stuffing",
                "session_date": date(2026, 2, 15) + timedelta(days=case_idx * 5),
                "note": (
                    f"Session flagged for credential-stuffing indicators; device "
                    f"{dev_shared} was first linked to the network through "
                    "this compromised session, prior to any fund movement."
                ),
            }
        )

    for case_idx in range(scale_factor):
        hub, ctrl_sources, ctrl_destinations, ctrl_device = _control_ids(case_idx)

        ctrl_start_month = 10 - case_idx
        if ctrl_start_month < 1:
            ctrl_start_month += 12
        ctrl_months = _months(2025 if ctrl_start_month >= 7 else 2025, ctrl_start_month, 8)

        accounts.append(
            {
                "account_id": hub,
                "cohort": COHORT_CONTROL,
                "account_role": "hub",
                "open_date": date(2022, 1, 1) - timedelta(days=case_idx * 90),
                "display_name": f"Remittance Hub {case_idx + 1}",
            }
        )
        for src in ctrl_sources:
            accounts.append(
                {
                    "account_id": src,
                    "cohort": COHORT_CONTROL,
                    "account_role": "source",
                    "open_date": date(2021, 6, 1) - timedelta(days=rng.randint(0, 400)),
                    "display_name": f"Remittance Sender {src.split('_')[-1]}",
                }
            )
        for dest in ctrl_destinations:
            accounts.append(
                {
                    "account_id": dest,
                    "cohort": COHORT_CONTROL,
                    "account_role": "destination",
                    "open_date": date(2021, 3, 1) - timedelta(days=rng.randint(0, 400)),
                    "display_name": f"Remittance Corridor {dest.split('_')[-1]}",
                }
            )

        for src in ctrl_sources:
            for m in ctrl_months:
                transfers.append(
                    {
                        "txn_id": next_txn_id(),
                        "source_account": src,
                        "dest_account": hub,
                        "amount": _jitter_amount(rng, 2000.0, 150.0),
                        "txn_date": m,
                        "channel": "ach",
                    }
                )
        for dest in ctrl_destinations:
            for m in ctrl_months:
                transfers.append(
                    {
                        "txn_id": next_txn_id(),
                        "source_account": hub,
                        "dest_account": dest,
                        "amount": _jitter_amount(rng, 2500.0, 200.0),
                        "txn_date": m,
                        "channel": "wire",
                    }
                )

        devices.append(
            {
                "device_id": ctrl_device,
                "first_seen_date": date(2021, 5, 1) - timedelta(days=case_idx * 60),
            }
        )
        for src in ctrl_sources:
            device_links.append(
                {
                    "link_id": f"LINK_{ctrl_device}_{src}",
                    "device_id": ctrl_device,
                    "account_id": src,
                    "hub_account_id": hub,
                    "linked_date": date(2021, 5, 5) - timedelta(days=case_idx * 60),
                }
            )

    scaled_baseline_count = BASELINE_ACCOUNT_COUNT * scale_factor
    baseline_ids = [f"ACC_B_{i}" for i in range(1, scaled_baseline_count + 1)]
    date_range_days = 360 + (scale_factor - 1) * 180

    for bid in baseline_ids:
        accounts.append(
            {
                "account_id": bid,
                "cohort": COHORT_BASELINE,
                "account_role": "baseline",
                "open_date": date(2024, 1, 1) - timedelta(days=rng.randint(0, 1000)),
                "display_name": f"Customer {bid.split('_')[-1]}",
            }
        )
    for bid in baseline_ids:
        n_txns = rng.randint(1, 3)
        for _ in range(n_txns):
            counterparty = rng.choice([b for b in baseline_ids if b != bid])
            txn_day_offset = rng.randint(0, date_range_days)
            transfers.append(
                {
                    "txn_id": next_txn_id(),
                    "source_account": bid,
                    "dest_account": counterparty,
                    "amount": _jitter_amount(rng, 200.0, 150.0),
                    "txn_date": policy.REFERENCE_DATE - timedelta(days=txn_day_offset),
                    "channel": rng.choice(["ach", "wire", "card"]),
                }
            )

    return {
        "accounts": pd.DataFrame(accounts),
        "devices": pd.DataFrame(devices),
        "device_links": pd.DataFrame(device_links),
        "transfers": pd.DataFrame(transfers),
        "sessions": pd.DataFrame(sessions),
    }
