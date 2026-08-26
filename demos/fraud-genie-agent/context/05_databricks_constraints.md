# Databricks Prototype Constraints

## Design Goal

Build a working prototype that can be demonstrated in Databricks Free Edition.

Keep the implementation intentionally lightweight.

## Preferred Architecture

Synthetic source data
→ Bronze Delta tables
→ Silver normalized / enriched tables
→ Gold investigation-ready tables
→ Genie
→ Databricks App

## Suggested Gold-Level Data Products

Depending on the winning use case:

- account_risk_summary
- transaction_behavior_summary
- connected_account_summary
- device_relationship_summary
- potential_victim_summary
- fraud_pattern_summary
- incident_blast_radius
- investigation_timeline

## Genie Design Principles

- Give Genie curated, understandable tables.
- Prefer explicit business-friendly column names.
- Add descriptions / semantic guidance where supported.
- Avoid exposing raw technical complexity unless necessary.
- Precompute difficult relationship summaries when that improves reliability.
- Make Genie responsible for investigation and explanation, not punishment.

## Prototype Data

Prefer synthetic but realistic data.

Synthetic data should include:

- normal customer behavior;
- a small number of seeded fraud scenarios;
- connected accounts;
- timestamps;
- devices;
- counterparties;
- transaction channels;
- fraud-pattern labels for validation only.

Do not expose "ground truth" fraud labels directly to Genie in the main demo unless explicitly needed for evaluation.

## Scope Control

The prototype should favor:

- 5–10 well-designed tables;
- one strong fraud scenario;
- one polished app;
- 8–12 excellent Genie questions;

over a large but shallow system.
