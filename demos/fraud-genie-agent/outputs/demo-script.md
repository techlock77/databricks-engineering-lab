# Live Demo Script — MuleGraph Investigator

**Validated against the live app (commit `9169331`, branch `mulegraph-app-genie-fix`).**
Reviewed by two independent lenses before this was written: Claude confirmed the
business story and target-user fit against the real fraud/policy code; Codex
re-ran the actual pipeline fresh and confirmed every number and table below,
plus verified nothing has drifted since the prior check. No number or feature
below is invented — everything traces to the current code and a fresh run
(`seed=42`, `scale_factor=1`).

Target length: **2–3 minutes**.

---

## Problem / Opportunity

*(Layman)* Money-mule networks move stolen or scammed funds through chains of
accounts before anyone can trace them. Fraud teams usually see one flagged
transaction at a time — by the time a human manually traces the connections
by hand, the money is often gone.

*(Technical)* MuleGraph Investigator turns a single flagged account into a
fully-traced case: shared devices, fund flow, and connected accounts,
surfaced through conversation instead of manual SQL or spreadsheet work — and
built with a guardrail so it never falsely implicates a legitimate customer
whose account happens to move money the same way a mule network does.

## Target Users

A **front-line fraud investigator** at the case-review decision point — the
person who opens one flagged account and has to decide, with evidence, what
happens next. The app is scoped to exactly that moment: one case at a time
(one seed account, one evidence policy), with every answer citation-backed so
it can support a real investigative decision, not just a dashboard glance.
Nothing in this app takes automated action on an account — it only prepares
the evidence for a person to act on.

## Architecture and Data Flow

*(Layman)* The data lives in Databricks, not inside the app — the app just
reads it, and so does Genie, independently.

*(Technical)* Bronze synthetic events → normalized Silver entities/relationships
→ **8 persisted Gold Delta tables** in Unity Catalog (`accounts`, `evidence`,
`network_edges`, `transfers`, `case_summary`, `control_cohort`, `freshness`,
`export_citations`) → **4 policy-scoped views** (`evidence_strict_v` /
`evidence_permissive_v`, `network_edges_strict_v` / `network_edges_permissive_v`)
that separate fund-flow-corroborated evidence from weaker device-only links.

From there, two genuinely **independent read paths** hit the same tables:
1. The Streamlit app reads the 8 Gold tables directly through a Databricks SQL
   warehouse (`src/data_access.py`) to render the case header, Evidence panel,
   Blast Radius card, Connected Accounts tab, and Control Cohort tab.
2. A real **Databricks Genie Space**, configured over those same 8 tables plus
   the 4 views, answers natural-language questions in the "Ask Genie" chat
   panel — Genie does not read the app's data, and the app's tabs don't depend
   on Genie; they're two views of the same source of truth.

Nothing is generated at runtime by the app itself — the data is set up once
(SQL script + a data-generation notebook), then persisted.

## What Users Can Ask Genie

Grounded in the actual schema, not a hypothetical list — these are the
questions the deployed Genie Space can genuinely answer today:

- *"Why was this account flagged?"*
- *"How many inbound sources and outbound destinations does it have?"*
- *"Compare exposure, connected accounts, and shared devices under strict vs.
  permissive evidence."*
- *"Which evidence disappears under the strict policy?"*
- *"Which accounts are connected under the selected policy?"*
- *"Which transfers contribute to the exposure?"*
- *"Why were the control-cohort accounts not flagged?"*
- *"How fresh is this data?"*
- **Closing question:** *"What would we have missed if we investigated only
  the original transaction?"*

## How Genie Powers the Core Experience

*(Layman)* The centerpiece of the demo is a single toggle: strict vs.
permissive evidence. Flip it, and the case visibly changes in front of you —
not just in a chat answer, but in the Blast Radius card, the Evidence panel,
and the Connected Accounts tab, all at once.

*(Technical, current real numbers — seed account `ACC_M_COLLECTOR`, re-verified
this run)*:

| Metric | Permissive | Strict |
|---|---:|---:|
| Other connected accounts | 9 | 8 |
| Shared devices | 2 | 1 |
| Total exposure | $63,637.69 | $63,637.69 |
| Potential victims | 5 | 5 |
| External destinations | 3 | 3 |

Tightening the policy drops one weakly-linked account and one weak device
connection — but exposure holds *exactly* steady, because every dollar of it
was already fund-flow-corroborated. Genie is explicitly instructed to answer
evidence/network questions from the policy-scoped views (not the raw tables),
so its live answers stay consistent with what's on screen under whichever
policy is currently selected. Ask Genie *"why were the control-cohort accounts
not flagged?"* to show the customer-protection guardrail live, not just as a
claim — a legitimate, high-volume remittance corridor with the *same*
fan-in/fan-out shape as the mule network, protected because of its long tenure
and long-running recurring pattern, re-derived from real behavior each time,
not a hardcoded exemption list.

## What We Learned Building and Testing It

Two concrete things the cross-vendor review process actually caught, not
generic "we tested it":

1. **The guardrail had to prove itself behaviorally, not by lookup.** The
   recurring-corridor override that protects legitimate customers is
   re-derived per account from tenure and recurrence facts alone — never by
   checking cohort membership directly. That distinction matters: a version
   that special-cased the known control-cohort accounts would have hidden a
   real false-negative risk (a genuine mule account with faked tenure
   metadata slipping through), which only a real behavioral test catches.
2. **The "Genie" originally in this app was a local stand-in, and the raw
   tables let policy leak.** An earlier build's "Ask Genie" panel was a local
   rule-based responder over in-memory data — replaced with a real Databricks
   Genie Space reading real persisted tables. Cross-review then caught that
   pointing Genie at the raw `evidence`/`network_edges` tables let it answer
   from the *wrong* policy population regardless of the UI toggle — closed by
   adding the 4 policy-scoped views and requiring every relevant prompt to
   name them explicitly, so the chat answer and the on-screen cards can never
   silently contradict each other.

---

*Note (open item, not yet resolved): `outputs/final-selection.md` is currently
an empty section skeleton in this repo, not filled canonical content — this
demo script was validated directly against the running code instead. Worth
regenerating separately.*
