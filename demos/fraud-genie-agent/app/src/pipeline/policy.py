"""Shared constants and classification logic for MuleGraph Investigator.

Every Gold table and every UI panel that needs to decide "is this a mule
network", "is this evidence row in scope under the current policy", or
"what is the headline exposure/account count" must go through the
functions in this module. No other module may reimplement these rules.
That is what guarantees two Gold tables can never independently compute a
conflicting headline number.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

# ---------------------------------------------------------------------------
# Global constants
# ---------------------------------------------------------------------------

DEFAULT_SEED = 42

# Fixed analysis anchor date. Deliberately NOT datetime.now() -- the whole
# pipeline (tenure, freshness, "as of" language) must be reproducible byte
# for byte on any machine, on any day, for a fixed seed.
REFERENCE_DATE = date(2026, 6, 1)

# --- Detection thresholds (raw pattern signal, before any override) -------
FAN_IN_MIN_SOURCES = 4
FAN_OUT_MIN_DESTINATIONS = 3
FAN_OUT_MIN_MONTHS = 3
TOTAL_FLOW_THRESHOLD = 20_000.0

# --- Recurring-corridor override (protects the legitimate control cohort) -
# An account that raw-trips the pattern above is NOT flagged if it also has
# long tenure AND a long-running, consistent monthly corridor. Money-mule
# collectors are set up recently and burn out in a few months; legitimate
# remittance corridors run for years.
CONTROL_TENURE_DAYS_THRESHOLD = 900
CONTROL_RECURRENCE_MONTHS_THRESHOLD = 6

# --- Evidence typing --------------------------------------------------------
EVIDENCE_TYPE_DEVICE_ONLY = "device_only"
EVIDENCE_TYPE_DEVICE_AND_FUND_FLOW = "device_and_fund_flow"
EVIDENCE_TYPE_ACCOUNT_TAKEOVER = "account_takeover_provenance"

CONFIDENCE_CERTAIN = "certain"
CONFIDENCE_INFERRED = "inferred"

EVIDENCE_CONFIDENCE = {
    EVIDENCE_TYPE_DEVICE_ONLY: CONFIDENCE_INFERRED,
    EVIDENCE_TYPE_DEVICE_AND_FUND_FLOW: CONFIDENCE_CERTAIN,
    EVIDENCE_TYPE_ACCOUNT_TAKEOVER: CONFIDENCE_CERTAIN,
}

# --- Evidence policy ---------------------------------------------------------
POLICY_STRICT = "strict"
POLICY_PERMISSIVE = "permissive"
DEFAULT_POLICY = POLICY_PERMISSIVE
VALID_POLICIES = (POLICY_STRICT, POLICY_PERMISSIVE)

# Evidence types included under the strict (fund-flow-corroborated only)
# policy. Permissive includes every evidence type, including weak
# device-only links.
STRICT_EVIDENCE_TYPES = frozenset(
    {EVIDENCE_TYPE_DEVICE_AND_FUND_FLOW, EVIDENCE_TYPE_ACCOUNT_TAKEOVER}
)

# --- Freshness ---------------------------------------------------------------
FRESHNESS_CONTRACT_HOURS = 24


def evidence_included_under_policy(evidence_type: str, policy: str) -> bool:
    """Whether a single evidence row is in scope under `policy`."""
    if policy not in VALID_POLICIES:
        raise ValueError(f"unknown evidence policy: {policy!r}")
    if policy == POLICY_PERMISSIVE:
        return True
    return evidence_type in STRICT_EVIDENCE_TYPES


def edge_included_under_policy(edge_type: str, policy: str) -> bool:
    """Whether a network edge is traversable under `policy`.

    Edge types mirror evidence types plus a plain "fund_flow" edge (a
    transfer relationship with no accompanying device evidence at all,
    which is always fund-flow-corroborated by definition).
    """
    if policy not in VALID_POLICIES:
        raise ValueError(f"unknown evidence policy: {policy!r}")
    if policy == POLICY_PERMISSIVE:
        return True
    return edge_type in STRICT_EVIDENCE_TYPES or edge_type == "fund_flow"


def tenure_days(open_date: date, reference_date: date = REFERENCE_DATE) -> int:
    return (reference_date - open_date).days


def is_recurring_corridor(distinct_months: int, distinct_destinations: int) -> bool:
    """A fan-out shape that recurs across multiple months to 3+ distinct
    destinations -- the shape shared by both real mule networks and the
    legitimate remittance control cohort."""
    return (
        distinct_months >= FAN_OUT_MIN_MONTHS
        and distinct_destinations >= FAN_OUT_MIN_DESTINATIONS
    )


def raw_pattern_flag(
    distinct_source_count: int,
    distinct_destination_count: int,
    distinct_outbound_months: int,
    total_outbound_amount: float,
) -> bool:
    """Raw fan-in/fan-out/collector signal, BEFORE the recurring-corridor
    override is applied. This is intentionally shape-only: it does not know
    about tenure or long-run recurrence, so it fires identically for a real
    mule collector and for a legitimate high-volume remittance hub."""
    return (
        distinct_source_count >= FAN_IN_MIN_SOURCES
        and is_recurring_corridor(distinct_outbound_months, distinct_destination_count)
        and total_outbound_amount >= TOTAL_FLOW_THRESHOLD
    )


def is_protected_by_recurring_corridor(
    account_tenure_days: int, distinct_outbound_months: int
) -> bool:
    """The override: long tenure + long-running monthly recurrence marks an
    account as a legitimate recurring corridor, immune from the raw
    fan-in/fan-out flag. This must be evaluated on the SAME account the raw
    flag was computed for -- it does not just check "is this account in the
    control cohort", it re-derives the exemption from tenure/recurrence
    facts alone, exactly like a real classifier would have to."""
    return (
        account_tenure_days >= CONTROL_TENURE_DAYS_THRESHOLD
        and distinct_outbound_months >= CONTROL_RECURRENCE_MONTHS_THRESHOLD
    )


@dataclass(frozen=True)
class DetectionResult:
    account_id: str
    raw_flag: bool
    override_applied: bool
    is_flagged_mule_network: bool
    reason: str


def classify_account(
    account_id: str,
    distinct_source_count: int,
    distinct_destination_count: int,
    distinct_outbound_months: int,
    total_outbound_amount: float,
    account_tenure_days: int,
) -> DetectionResult:
    """Single source of truth for "is this account a flagged mule-network
    collector". Both detection.py and gold.py call this rather than
    reimplementing the rule."""
    raw = raw_pattern_flag(
        distinct_source_count,
        distinct_destination_count,
        distinct_outbound_months,
        total_outbound_amount,
    )
    if not raw:
        return DetectionResult(
            account_id=account_id,
            raw_flag=False,
            override_applied=False,
            is_flagged_mule_network=False,
            reason="does not meet raw fan-in/fan-out/collector thresholds",
        )

    protected = is_protected_by_recurring_corridor(
        account_tenure_days, distinct_outbound_months
    )
    if protected:
        return DetectionResult(
            account_id=account_id,
            raw_flag=True,
            override_applied=True,
            is_flagged_mule_network=False,
            reason=(
                "meets raw fan-in/fan-out/collector thresholds but is protected "
                "by the recurring-corridor override "
                f"(tenure={account_tenure_days}d >= {CONTROL_TENURE_DAYS_THRESHOLD}d, "
                f"recurrence={distinct_outbound_months}mo >= "
                f"{CONTROL_RECURRENCE_MONTHS_THRESHOLD}mo)"
            ),
        )

    return DetectionResult(
        account_id=account_id,
        raw_flag=True,
        override_applied=False,
        is_flagged_mule_network=True,
        reason="meets raw fan-in/fan-out/collector thresholds; not a protected recurring corridor",
    )


def risk_band_for(is_flagged_mule_network: bool) -> str:
    return "high" if is_flagged_mule_network else "low"
