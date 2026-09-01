"""Citation-backed case-file export.

The export's item count is derived from the exact same
`views.case_evidence` call the Evidence panel uses to decide what to
display, plus the case-summary row. There is no second, independent count
computed anywhere -- that is what keeps the export count and the UI's
displayed row count from drifting apart.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from src.pipeline import views


@dataclass(frozen=True)
class ExportItem:
    item_type: str
    source_table: str
    source_row_id: str
    text: str


@dataclass(frozen=True)
class ExportBundle:
    case_id: str
    seed_account: str
    evidence_policy: str
    items: list[ExportItem]

    @property
    def item_count(self) -> int:
        return len(self.items)


def build_case_export(
    gold: dict[str, pd.DataFrame], seed_account: str, evidence_policy: str
) -> ExportBundle:
    evidence_in_scope = views.case_evidence(gold, seed_account, evidence_policy)
    case_rows = gold["case_summary"][
        gold["case_summary"]["seed_account"].astype(str) == str(seed_account)
    ]
    if case_rows.empty:
        raise ValueError(f"No case summary found for seed account {seed_account!r}")
    case_row = case_rows.iloc[0]

    citations = gold.get("export_citations", pd.DataFrame())
    citation_text_by_evidence_id = {
        str(row.source_row_id): row.citation_text
        for row in citations.itertuples(index=False)
        if row.source_table == "gold_evidence"
    }

    items = [
        ExportItem(
            item_type="evidence",
            source_table="gold_evidence",
            source_row_id=row.evidence_id,
            text=citation_text_by_evidence_id.get(str(row.evidence_id), row.description),
        )
        for row in evidence_in_scope.itertuples(index=False)
    ]
    items.append(
        ExportItem(
            item_type="case_summary",
            source_table="gold_case_summary",
            source_row_id=case_row["case_id"],
            text=(
                f"{case_row['case_id']}: {evidence_policy} exposure "
                f"${(case_row['total_exposure_strict'] if evidence_policy == 'strict' else case_row['total_exposure_permissive']):,.2f}."
            ),
        )
    )

    return ExportBundle(
        case_id=case_row["case_id"],
        seed_account=seed_account,
        evidence_policy=evidence_policy,
        items=items,
    )
