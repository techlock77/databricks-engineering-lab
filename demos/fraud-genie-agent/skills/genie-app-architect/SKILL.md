# Skill: Genie App Architect

## Purpose

Transform a selected fraud problem into a small, credible Databricks Genie application.

## Use When

Activate after finalists exist, and especially after the winner is chosen.

## Architecture Method

### Step 1 — Define the User

Examples:

- fraud investigator;
- fraud operations manager;
- financial crime analyst;
- payment-risk analyst.

### Step 2 — Define the Investigation Questions

Identify 8–12 questions the user should be able to ask.

Questions should move from:

what
→ why
→ who else
→ when
→ how much
→ what pattern
→ what should be reviewed next

### Step 3 — Define Data Entities

Typical entities:

- customer;
- account;
- transaction;
- device;
- merchant;
- counterparty;
- login / authentication event;
- alert;
- investigation case.

### Step 4 — Create Medallion Model

Bronze:
- source-like raw events.

Silver:
- normalized transactions;
- identity / account links;
- device relationships;
- behavioral features;
- time-window summaries.

Gold:
- investigation-ready summaries;
- connected-account patterns;
- blast-radius metrics;
- evidence timelines;
- victim-impact summaries.

### Step 5 — Define Genie Scope

Specify:

- tables Genie sees;
- business definitions;
- expected joins / relationships;
- sample questions;
- terms requiring clear definitions.

### Step 6 — Define the Databricks App

Recommended UI:

1. Incident Overview
2. Investigation Timeline
3. Connected Entities
4. Blast Radius
5. Ask Genie

Do not overbuild the UI.

### Step 7 — Prove Genie Centrality

For each major app feature answer:

> What can Genie do here that a static dashboard cannot do efficiently?

If the answer is weak, redesign the experience.

## Required Output

- architecture diagram in text / Mermaid;
- source-to-gold table design;
- Genie-facing table list;
- app page design;
- 8–12 Genie questions;
- one primary fraud scenario;
- Free Edition scope assumptions.
