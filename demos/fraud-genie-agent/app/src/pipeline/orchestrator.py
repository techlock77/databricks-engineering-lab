"""Single pipeline entrypoint used by both the Streamlit app and the tests,
so there is exactly one way bronze -> silver -> gold gets assembled."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd

from src.data_generator import generator
from src.pipeline import gold, policy, silver


@dataclass
class PipelineResult:
    seed: int
    bronze: dict[str, pd.DataFrame]
    silver: dict[str, pd.DataFrame]
    gold: dict[str, pd.DataFrame]
    scale_factor: int = 1


def run_pipeline(
    seed: int = policy.DEFAULT_SEED,
    scale_factor: int = 1,
    seed_account: Optional[str] = None,
) -> PipelineResult:
    """Run the full bronze -> silver -> gold pipeline.

    Parameters
    ----------
    seed : int
        Random seed for reproducibility.
    scale_factor : int
        Deprecated compatibility argument. The curated nine-scenario dataset
        supersedes repetition-based scaling, so values other than 1 produce
        the same cases and emit a warning from ``generate_dataset_scaled``.
    seed_account : str, optional
        Optional single seed account. When omitted, all nine curated scenario
        seed accounts are included.

    Returns
    -------
    PipelineResult
        Contains bronze, silver, and gold DataFrames.
    """
    if scale_factor == 1:
        bronze = generator.generate_dataset(seed=seed)
    else:
        bronze = generator.generate_dataset_scaled(seed=seed, scale_factor=scale_factor)

    silver_tables = silver.build_silver(bronze)

    seed_accounts = [seed_account] if seed_account else generator.SCENARIO_SEED_ACCOUNTS
    gold_tables = gold.build_gold(bronze, silver_tables, seed_accounts=seed_accounts)

    return PipelineResult(
        seed=seed,
        bronze=bronze,
        silver=silver_tables,
        gold=gold_tables,
        scale_factor=scale_factor,
    )
