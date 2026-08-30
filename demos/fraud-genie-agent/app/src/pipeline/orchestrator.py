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
        Number of independent cases to generate. At scale_factor=1 (default),
        produces the original single-case dataset. At scale_factor > 1,
        produces multiple independent mule networks, control cohorts, and
        baseline accounts.
    seed_account : str, optional
        The seed account for case-network analysis. At scale_factor=1, defaults
        to MULE_COLLECTOR. Ignored at scale_factor > 1 (all collectors are
        automatically included).

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

    if scale_factor == 1:
        effective_seed_account = seed_account or generator.MULE_COLLECTOR
        gold_tables = gold.build_gold(bronze, silver_tables, seed_account=effective_seed_account)
    else:
        accounts_df = silver_tables["accounts"]
        collector_accounts = accounts_df[
            accounts_df["account_role"] == "collector"
        ]["account_id"].tolist()
        gold_tables = gold.build_gold(bronze, silver_tables, seed_accounts=collector_accounts)

    return PipelineResult(
        seed=seed,
        bronze=bronze,
        silver=silver_tables,
        gold=gold_tables,
        scale_factor=scale_factor,
    )
