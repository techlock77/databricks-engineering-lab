from src.data_generator import generator
from src.pipeline import policy, views
from src.pipeline.orchestrator import run_pipeline


def _case_tables():
    gold = run_pipeline(seed=42).gold
    seeds = gold["case_summary"].set_index("scenario_type")["seed_account"]
    accounts = gold["accounts"].set_index("account_id")
    return gold, seeds, accounts


def test_all_nine_scenarios_are_independently_classified():
    gold, seeds, accounts = _case_tables()
    assert set(seeds.index) == {item[0] for item in generator.SCENARIOS}
    for scenario, seed in seeds.items():
        expected = scenario != "normal_control"
        assert bool(accounts.loc[seed, "is_flagged_mule_network"]) is expected


def test_normal_control_is_honestly_unflagged_by_override():
    _, seeds, accounts = _case_tables()
    row = accounts.loc[seeds["normal_control"]]
    assert bool(row["raw_fan_pattern_flag"])
    assert bool(row["override_applied"])
    assert not bool(row["is_flagged_mule_network"])


def test_threshold_boundary_hits_every_inclusive_threshold_exactly():
    _, seeds, accounts = _case_tables()
    row = accounts.loc[seeds["threshold_boundary"]]
    assert row["distinct_source_count"] == policy.FAN_IN_MIN_SOURCES
    assert row["distinct_destination_count"] == policy.FAN_OUT_MIN_DESTINATIONS
    assert row["distinct_outbound_months"] == policy.FAN_OUT_MIN_MONTHS
    assert row["total_outbound_amount"] == policy.TOTAL_FLOW_THRESHOLD
    assert bool(row["is_flagged_mule_network"])


def test_rapid_case_has_day_level_dates_and_short_final_pass_through():
    result = run_pipeline(seed=42)
    transfers = result.bronze["transfers"]
    case = transfers[(transfers.source_account.str.startswith("ACC_RAPID")) |
                     (transfers.dest_account.str.startswith("ACC_RAPID"))]
    assert case.txn_date.map(lambda value: value.day).nunique() > 1
    may_in = case[(case.dest_account == "ACC_RAPID_COLLECTOR")].txn_date.max()
    may_out = case[(case.source_account == "ACC_RAPID_COLLECTOR")].txn_date.max()
    assert (may_out - may_in).days <= 5


def test_multi_hop_is_a_real_chain_and_large_case_scales():
    gold, seeds, _ = _case_tables()
    chain = views.compute_network(gold, seeds["multi_hop"], policy.POLICY_STRICT)
    assert {"ACC_CHAIN_HOP_1", "ACC_CHAIN_HOP_2", "ACC_CHAIN_HOP_3", "ACC_CHAIN_FINAL"} <= set(chain.accounts_in_network)
    assert ((chain.edges.account_a.str.contains("HOP")) | (chain.edges.account_b.str.contains("HOP"))).sum() >= 4
    large = views.compute_network(gold, seeds["large_network"], policy.POLICY_STRICT)
    assert large.other_connected_accounts_count >= 28
    assert len(large.edges) >= 28


def test_insufficient_evidence_has_empty_strict_evidence_but_permissive_device_rows():
    gold, seeds, _ = _case_tables()
    seed = seeds["insufficient_evidence"]
    assert views.case_evidence(gold, seed, policy.POLICY_STRICT).empty
    permissive = views.case_evidence(gold, seed, policy.POLICY_PERMISSIVE)
    assert len(permissive) == 5
    assert set(permissive.evidence_type) == {policy.EVIDENCE_TYPE_DEVICE_ONLY}


def test_scenario_contract_reaches_accounts_and_case_summary():
    gold, _, _ = _case_tables()
    assert {"scenario_type", "scenario_label"} <= set(gold["accounts"].columns)
    assert {"scenario_type", "scenario_label"} <= set(gold["case_summary"].columns)
    assert gold["case_summary"].scenario_type.nunique() == 9
