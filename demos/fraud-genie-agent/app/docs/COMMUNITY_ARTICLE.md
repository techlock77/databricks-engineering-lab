# MuleGraph Investigator: From One Alert to the Hidden Network

*How a Databricks App turns a single flagged account into a fully-traced fraud
network — with Genie doing the actual investigating, not just the talking.*

[SCREENSHOT: Home page hero section — "Find the network before the money moves"]

---

## 1. The Problem

Banks see thousands of "money mule" accounts every year — accounts that quietly
collect stolen or scammed money from multiple victims and forward it onward
before anyone notices. A single flagged transaction almost never tells the
whole story. By the time a human investigator manually traces the connections
by hand — pulling transaction logs, cross-referencing device IDs, building a
spreadsheet of who-paid-whom — the money is often already gone.

MuleGraph Investigator starts from exactly that moment: **one flagged
account**. Its seed case is `ACC_M_COLLECTOR`, a "simple suspicious transfer"
scenario in which a single collector account receives recurring transfers
from 5 distinct source accounts and forwards funds to 3 distinct destination
accounts, month after month, crossing a total-outbound-flow threshold that a
rules engine would catch — but a rules engine stops there. It flags the
account's aggregated pattern, not a single transaction. It does not tell you
who else is involved, whether the same device shows up elsewhere, or whether
the money you're looking at is the tip of a much larger network.

> **The pattern is deliberately shape-based, not label-based.** Detection
> looks for fan-in from 4+ sources, a recurring fan-out to 3+ destinations
> across 3+ months, and total outbound flow over $20,000 — the same shape a
> legitimate high-volume remittance business can produce. That collision is
> the whole reason the app exists: catching the pattern is easy; not
> punishing an innocent customer for it is the hard part.

The synthetic dataset behind the app encodes nine independently selectable
investigation scenarios — not one canned demo case, but nine real, differently
shaped stories: a simple collector, a rapid pass-through mule, a
multi-victim funnel, a multi-hop chain, a shared-device cluster, a legitimate
control cohort that must *not* be flagged, an exact-threshold boundary case,
a case with weak (device-only) evidence, and a larger network scenario with
28 other connected accounts (a 29-account network including the seed).

## 2. Who It's For

MuleGraph Investigator is built for a **front-line fraud investigator** — the
person who opens one flagged account and has to decide, with evidence, what
happens next. The app's own copy makes this explicit: the Home page's hero
card frames the highest-priority case as an *"INVESTIGATION CASE"* with a
suspicious account, a potential pattern, and an *"INVESTIGATOR'S QUESTION"*
displayed alongside it — opening that case is what the hero's buttons do, not
queuing the question into the chat. Once inside the Investigation Workspace,
an *"Alert Queue"* of active, prioritized cases with an *"Open case →"*
button appears whenever no case is currently selected.

The workflow is scoped to exactly one investigator's decision point: one case
at a time, one seed account, one evidence policy selected at a time — every
answer citation-backed so it can support a real investigative decision, not
just decorate a dashboard. Nothing in the app takes automated action against
an account. It only prepares the evidence — the fund flow, the shared
devices, the connected accounts, the potential victims — for a person to
review and act on.

## 3. How MuleGraph Investigator Works

*(Layman)* You land on a home page that already tells you where to start: the
highest-priority alert, front and center. Click into it, and instead of one
transaction, you see a whole case — a risk score, dollar exposure, how many
other accounts are tied to it, and a chat panel where you can just ask Genie
what happened.

*(Technical)* The navigation is a two-level structure: **Home** and
**Investigation Workspace**, toggled from the top nav bar. Home renders a
hero card for the single highest-priority case (sorted by risk band, then
exposure), a row of scenario chips for all nine cases, and four process cards
(*01 — Select the Signal*, *02 — Investigate with Genie*, *03 — Follow the
Money*, *04 — Assess the Impact*) that describe the investigation journey.
Home does not show the Alert Queue itself — that list lives inside the
Investigation Workspace, and only renders there when no case is currently
selected.

[SCREENSHOT: Home page — scenario chip row and process cards]

Opening a case (from the hero card, a scenario chip, the Alert Queue, or the
account selector) switches to the Investigation Workspace, which shows:

- A **case header** — account ID, risk band, "Flagged for investigator
  review" / "Not flagged" framing.
- A **KPI strip** — six metrics: Risk band, Linked exposure, Connected
  accounts, Potential victims, Shared devices, External destinations.
- Five **section tabs** — **Overview**, **Investigation**, **Money Flow**,
  **Network**, **Reports** (implemented under the hood as a segmented
  `st.radio` control, not `st.tabs`) — plus a persistent **Ask Genie** panel
  alongside them.

[SCREENSHOT: Investigation Workspace — KPI strip and section tabs]

The **Overview** tab offers three action buttons — *Trace Funds*, *View
Connected Accounts*, *Investigate with Genie* — that jump straight into the
relevant tab or chat question. **Investigation** exposes the strict/permissive
evidence-policy toggle plus a collapsible legitimate control-cohort audit
view. **Money Flow** lists the policy-scoped transfers and a monthly bar
chart. **Network** renders a live relationship graph and a connected-accounts
table. **Reports** builds a citation-backed case file with a downloadable
export.

## 4. Application Architecture & Data Flow

Nothing about the running app is generated at request time. The synthetic
data is produced once, offline, by a deterministic generator and pipeline,
and only the resulting Gold layer is written to Unity Catalog — everything
downstream, the Streamlit UI and the Genie Space, reads from that same
persisted source of truth through two independent paths.

```
┌────────────────────────────────────────────────────────────────┐
│  scripts/generate_synthetic_data.py   (Databricks notebook)       │
│  outer driver — run once per data refresh                          │
└────────────────────────────────┬───────────────────────────────────┘
                                  │ calls
                                  ▼
┌────────────────────────────────────────────────────────────────┐
│  src/pipeline/orchestrator.py :: run_pipeline(seed, scale_factor)  │
│  single bronze → silver → gold entrypoint, shared by the notebook  │
│  above and the test suite                                          │
└────────────────────────────────┬───────────────────────────────────┘
                                  │
                                  ▼
   1. generator.generate_dataset(seed=42)
      → Bronze: accounts, devices, device_links, transfers, sessions
        (in-memory pandas only — never persisted)
                                  │
                                  ▼
   2. silver.build_silver(bronze)
      → Silver: normalized entities/relationships
        (in-memory pandas only — never persisted)
                                  │
                                  ▼
   3. gold.build_gold(bronze, silver, seed_accounts=...)
      → detection.py + policy.py + network.py
      → 8 Gold pandas DataFrames (in-memory)
                                  │
                                  ▼  scripts/generate_synthetic_data.py writes
                                     each result.gold[...] DataFrame to Delta
                                     (spark.createDataFrame(...).saveAsTable(...))
┌───────────────────────────────────────────────────────────┐
│  Unity Catalog  ·  mulegraph.investigations                │
│  8 persisted Gold Delta tables (the only persisted layer): │
│   accounts · evidence · network_edges · transfers ·         │
│   case_summary · control_cohort · freshness ·                │
│   export_citations                                          │
│  4 policy-scoped Genie views, created ahead of time by       │
│  scripts/sql/01_setup_catalog_and_schema.sql:                 │
│   evidence_strict_v · evidence_permissive_v ·                │
│   network_edges_strict_v · network_edges_permissive_v        │
└───────────┬───────────────────────────────┬─────────────────┘
            │                               │
   SQL Warehouse read                Genie Space (Conversations API)
   src/data_access.py                src/genie/interface.py
            │                               │
            ▼                               ▼
┌───────────────────────────┐   ┌───────────────────────────┐
│  Streamlit UI               │   │  "Ask Genie" chat panel    │
│  (Databricks App)           │◄──┤  natural-language Q&A      │
│  KPI strip, tabs, graph,    │   │  grounded in the same       │
│  export                     │   │  Gold tables + views        │
└──────────────┬───────────────┘   └──────────────┬─────────────┘
               │                                   │
               └──────────────┬────────────────────┘
                               ▼
                    the investigator
```

`scripts/generate_synthetic_data.py` is a Databricks notebook and the outer
driver: it imports `run_pipeline` from `src.pipeline.orchestrator`, calls
`run_pipeline(seed, scale_factor)`, then loops over `GOLD_TABLE_NAMES` and
writes each of the 8 resulting Gold DataFrames to a Unity Catalog Delta
table. Bronze and Silver exist only as in-memory pandas DataFrames for the
duration of that one pipeline run — they are never written anywhere. The 4
policy-scoped Genie views aren't created by that notebook at all; they're
created ahead of time, alongside the 8 Gold table definitions, by
`scripts/sql/01_setup_catalog_and_schema.sql`.

The app's own inline comment captures the design intent behind that shared
source:

> "Every number — strict or permissive — comes from the same eight Gold
> tables, so the UI, the export, and Genie can never quietly disagree."

That's an intent the architecture is built to hold, not a guarantee enforced
by one code path: the Streamlit UI recomputes each case's network locally
from the Gold tables (`views.compute_network`), Genie is *prompted* — not
code-enforced — to query only the matching policy-scoped view for the
selected policy, and the shared-source design is what keeps those two paths,
plus the export, pointed at the same eight tables in practice. `data_access.py`
never generates anything: it fails loudly (`RuntimeError`) if `case_summary`
comes back empty, rather than silently falling back to synthetic data at
runtime. `genie/interface.py` never falls back to a local responder either —
the deployed path calls the real Genie Conversations API or raises.

## 5. Complete Technology Stack

| Technology | Role in MuleGraph Investigator |
|---|---|
| **Unity Catalog** | Governs the `mulegraph.investigations` catalog/schema; grants the app's service principal scoped `SELECT` on exactly the Gold tables and views it needs. |
| **Delta Lake tables** | The 8 persisted Gold tables (`accounts`, `evidence`, `network_edges`, `transfers`, `case_summary`, `control_cohort`, `freshness`, `export_citations`) — the single source of truth for both the app and Genie, and the only layer of the pipeline that's actually persisted. |
| **4 policy-scoped views** | `evidence_strict_v` / `evidence_permissive_v`, `network_edges_strict_v` / `network_edges_permissive_v`, created by `scripts/sql/01_setup_catalog_and_schema.sql` — keep Genie from ever answering from the wrong evidence-policy population. |
| **Databricks SQL Warehouse** | Executes the `SELECT * FROM ...` statements in `src/data_access.py` via the Statement Execution API; also the compute Genie itself queries against. |
| **Genie Space (Conversations API)** | Answers natural-language investigation questions, scoped to the 8 tables + 4 views, via `workspace.genie.start_conversation(...).result()`. |
| **Databricks Apps** | Hosts the Streamlit process; injects `DATABRICKS_WAREHOUSE_ID` and `DATABRICKS_GENIE_SPACE_ID` as resource-bound env vars — no manual tokens. |
| **Databricks Asset Bundles** | `databricks.yml` declares the app, its SQL-warehouse resource, and its Genie Space resource as bundle-managed infrastructure. |
| **Streamlit `>=1.32,<2.0`** | The UI framework — `st.metric`, `st.dataframe`, `st.graphviz_chart`, `st.bar_chart`, `st.chat_message`/`st.chat_input`, `st.status`, `st.radio` for the section switcher. |
| **`databricks-sdk` `>=0.66,<1.0`** | `WorkspaceClient` for both the SQL Statement Execution API and the Genie Conversations API; also surfaces structured Genie error types (e.g. `TABLES_MISSING_EXCEPTION`) via `databricks.sdk.errors.OperationFailed`. |
| **pandas `>=2.0,<3.0`** | In-memory shape for Bronze and Silver end-to-end — neither layer is ever persisted — and for Gold up until the notebook writes it to Delta; the running app then reads Gold back as pandas DataFrames through `data_access.py`. |
| **Graphviz (via `st.graphviz_chart`)** | Renders the live relationship graph on the Network tab from policy-scoped edges. |
| **Databricks Free Edition** | The deployment target documented end-to-end in `docs/DEPLOYMENT_GUIDE.md` — no CLI, Git, or personal access token required. |

## 6. Genie at the Core

Genie in MuleGraph Investigator is not a chatbot bolted on top of a finished
dashboard — it's a real call to the Databricks Genie Conversations API, and
the app is written so it visibly *needs* that call to succeed.

`src/genie/interface.py`'s `genie_query()` builds a prompt that names the
seed account and evidence policy, instructs Genie to query only the
policy-scoped view for that policy (never the raw `evidence` /
`network_edges` tables, which mix both), then calls:

```python
message_waiter = workspace.genie.start_conversation(space_id=space_id, content=prompt)
message = message_waiter.result()
```

If `message_waiter.result()` raises `databricks.sdk.errors.OperationFailed`
specifically, the code doesn't swallow it — it calls
`workspace.genie.get_message(...)` to pull the structured failure (error
`type`, e.g. `TABLES_MISSING_EXCEPTION`, plus the human-readable detail) and
re-raises a `RuntimeError` carrying that detail. That enrichment call is
deliberately narrow: it only fires for `OperationFailed`, and if
`get_message()` itself raises, or comes back with no structured `error.type`
or `error.error` detail to report, `genie_query()` falls back to re-raising
the original `OperationFailed` rather than masking it behind a worse error.
The Streamlit layer catches whatever comes out of that (enriched or
original), shows a friendly message in the chat, and puts the real
`{ExceptionType}: {message}` text behind a collapsed "Technical details"
expander — so an investigator sees a clean answer and an engineer debugging
the demo doesn't have to guess.

The one explicit escape hatch is intentionally test-only:
`genie_query(question, context, responder=...)` lets unit tests inject a
local stand-in. The deployed app never passes `responder` — there is no
silent local fallback in production.

**The clearest proof of Genie's centrality is a feature built to demonstrate
its absence.** The Investigation Workspace's Genie panel carries a toggle:

> 🔌 **Disable Genie — Demonstrate Dependency**

Flip it, and the app is explicit about what breaks:

> "Genie conversational tracing is unavailable while the dependency is
> disabled. The KPI strip, Investigation, Money Flow, Network, and Reports
> remain live."

That is the honest boundary of what Genie does versus what the rest of the
app does. The KPI strip, the policy-scoped Evidence tab, the Money Flow
chart, the Network graph, and the Reports export all keep working from the
same persisted Gold tables — because they read `data_access.py` directly. Only
the analytical *conversation* — "why was this flagged," "what would we have
missed," "why isn't the control cohort flagged" — goes away. That's the app
proving, in one toggle, exactly which part of the experience Genie owns.

[SCREENSHOT: "Disable Genie — Demonstrate Dependency" toggle, showing the static fallback message]

## 7. What Users Can Ask Genie

These are the real suggested questions shown in the Ask Genie panel today
(`GENIE_QUESTIONS` in `src/app/app.py`), each grounded in the persisted Gold
tables and the policy-scoped views:

1. *Why was `ACC_M_COLLECTOR` flagged?*
2. *How many inbound sources / outbound destinations does it have?*
3. *Compare permissive vs. strict exposure, connected accounts, shared devices.*
4. *Which evidence disappears under the strict policy?*
5. *Which accounts are connected under the selected policy?*
6. *What transfers contribute to the linked exposure?*
7. *Why are control-cohort accounts not flagged?*
8. *How fresh is this data?*
9. *What would we have missed investigating only the original transaction?*

The panel also chains follow-up suggestions after each answer, but the shape
is a fixed mapping, not a policy-aware one. `FOLLOW_UP_QUESTIONS` in
`src/app/app.py` curates a 3-question follow-up set for exactly four
prompts — questions 1, 2, and 3 above, plus the "Investigate with Genie"
quick action's own question, *"Why was this account flagged?"* — and each of
those four points to the same three follow-ups: "which accounts are
connected," "how many sources/destinations," and the closing "what would we
have missed" question. Every other question — including anything typed into
the free-text chat box — falls back to redisplaying the full original
nine-question list above.

## 8. Application Experience

**Home.** A hero card frames the highest-priority case as an investigation
brief — suspicious account, potential pattern, an "INVESTIGATOR'S QUESTION"
shown alongside it — with two calls to action: *Open highest-priority case →*
and *Ask Genie a question →*. Both buttons do the same thing: they open that
case in the Investigation Workspace. Neither one queues the displayed
question into the chat input, so the chat still starts empty, waiting for
whatever the investigator actually asks. Below the hero, a row of scenario
chips lets you jump directly into any of the nine cases, and four process
cards narrate the journey: *Select the Signal → Investigate with Genie →
Follow the Money → Assess the Impact.* Home never shows the Alert Queue
itself — that list lives inside the Investigation Workspace.

[SCREENSHOT: Home hero card with featured investigation case]

**Investigation Workspace.** Selecting a case (from the hero card, a
scenario chip, the Alert Queue, or the account selector) opens the full
workspace: case header, KPI strip, and the five section tabs alongside the
persistent Ask Genie panel. If no case is selected yet, this same view shows
the Alert Queue instead — active, prioritized cases with an *"Open case →"*
button for each.

**Follow the Money — the Money Flow tab.** Policy-scoped transfers touching
the case network, plus a monthly bar chart of transfer amounts — the visual
answer to "where did the funds go."

[SCREENSHOT: Money Flow tab — monthly transfer bar chart]

**Connected Network — the Network tab.** A live Graphviz relationship graph
(`st.graphviz_chart`) drawn directly from the policy-scoped edges, each edge
labeled with its type and dollar amount, plus a connected-accounts table
below it (account ID, cohort, role, tenure, risk band).

[SCREENSHOT: Network tab — relationship graph and connected accounts table]

**Impact and Insights.** The KPI strip (Linked exposure, Connected accounts,
Potential victims, Shared devices, External destinations) is the constant
header across every tab, and the Reports tab turns the same numbers into a
citation-backed case file — every line traceable back to a specific Gold
table row, correctly scoped to the selected case's own network (the export
now shares `views.case_evidence` with the Evidence panel, and selects its
`case_summary` row by matching the actual seed account rather than defaulting
to the first row) — with a one-click case-file export.

[SCREENSHOT: Reports tab — case file export with citations]

## 9. Demo Guide

A concise 2–3 minute walkthrough, using a fresh, verified pipeline run
(`seed=42`, `scale_factor=1`) for `ACC_M_COLLECTOR`:

**0:00 — Open the app, land on Home.** Point at the hero card, then at the
row of scenario chips below it. At this seed, the *actual* highest-priority
case by risk band and exposure is `ACC_LARGE_COLLECTOR` ($142,501.57) — the
*"Open highest-priority case →"* button opens that one, not
`ACC_M_COLLECTOR`. For this walkthrough, click the **"Simple suspicious
transfer"** scenario chip (or select `ACC_M_COLLECTOR` from the account
selector once inside the workspace) to open `ACC_M_COLLECTOR` directly.

**0:20 — The case header.** "`ACC_M_COLLECTOR` — high risk band, flagged for
investigator review." One account, quietly receiving money from several
people and sending it out to several places, over and over, for months.

**0:40 — Ask Genie, live.** Type: *"Why was this account flagged?"* Genie
answers from the `accounts` Gold table and `evidence_permissive_v`,
grounded in the real detection reason (fan-in from multiple sources, a
recurring fan-out to multiple destinations, crossing the total-outbound-flow
threshold).

**1:10 — Follow the money.** Switch to the **Network** and **Money Flow**
tabs, or ask *"Where did the funds go?"*. Show the numbers on screen:

| Metric (permissive policy) | Value |
|---|---:|
| Other connected accounts | **9** |
| Shared devices | **2** |
| Linked exposure | **$48,465.78** |
| Potential victims | **5** |
| External destinations | **3** |

One flagged account turns out to be the center of a 10-account network —
itself plus 9 other connected accounts — tied together by 2 shared devices
and over $48,000 in linked transfers.

**1:40 — The reveal: toggle the evidence policy, live.** Switch **Investigation
> Evidence policy** from *permissive* to *strict*, on screen. Watch the KPI
strip, Evidence panel, and Network tab update together:

| Metric | Permissive | Strict |
|---|---:|---:|
| Other connected accounts | 9 | **8** |
| Shared devices | 2 | **1** |
| Linked exposure | $48,465.78 | **$48,465.78** |

Tightening the evidence standard drops one weakly-linked account and one weak
device connection — but the dollar exposure **does not move**. Strict isn't
"fund-flow only," though: `STRICT_EVIDENCE_TYPES` also keeps
`device_and_fund_flow` and `account_takeover_provenance` evidence, and strict
network traversal still admits plain `fund_flow` edges with no device
evidence at all. What strict actually excludes is *weak, device-only* links —
and in this case, none of the linked exposure sat on one of those, which is
why the dollar figure holds steady while the account and device counts drop.

**2:10 — The guardrail.** Ask Genie: *"Why are control-cohort accounts not
flagged?"* — show the legitimate remittance corridor (Investigation tab's
control-cohort audit expander), same fan-in/fan-out shape, protected by
900+ days of tenure and an 8-month recurring corridor.

**2:30 — Close on the strongest question.** Ask, live: *"What would we have
missed if we investigated only the original transaction?"* Land the point: a
single-transaction review would never have surfaced the shared devices, the
other 8 connected accounts, or the $48,465.78 in linked exposure — that only
comes from tracing the network.

## 10. What We Learned

**A guardrail has to prove itself behaviorally, not by lookup.** The
recurring-corridor override that protects legitimate customers is re-derived
per account from tenure and recurrence facts alone (`policy.py`'s
`is_protected_by_recurring_corridor`) — never by checking cohort membership
directly. A version that special-cased known control-cohort account IDs would
have hidden a real false-negative risk: a genuine mule account with faked
tenure metadata slipping through undetected. Only a behavioral rule, applied
uniformly, catches that.

**A Genie Space doesn't auto-resync its schema.** The deployment guide's own
troubleshooting note is blunt about it: after changing the Gold table schema
or rerunning the data-generation notebook with different scenario counts,
Genie Spaces snapshot table/column metadata at attach time and do *not*
automatically detect schema changes on an already-attached source. The fix is
manual — reopen the Space, go to **Configure data**, and re-select/re-save
the affected tables and all 4 views before retesting.

**Floor-compatibility bugs hide in optional keyword arguments.** The app
pins a Streamlit floor (`>=1.32,<2.0`) and checks it in CI
(`scripts/verify_floor_compatibility.sh`), because parameters like
`st.metric`'s `delta_arrow` and `st.container`'s `key=` were added to
Streamlit *after* that floor version — the app signature-gates them at
runtime (`if "delta_arrow" in inspect.signature(st.metric).parameters`)
rather than assuming they exist. A real regression along these lines (a
`st.dataframe` width-parameter incompatibility, fixed in commit
`d3fd270`) is exactly why that floor-verification script exists at all —
caught by running the actual pinned floor versions in a clean virtualenv, not
just the latest installed ones.

**SDK error surfacing needed an explicit second call — with a fallback of
its own.** The Databricks SDK's `OperationFailed` exception from a failed
Genie conversation doesn't carry the structured failure reason on its own —
so, specifically when `message_waiter.result()` raises `OperationFailed`,
`genie_query()` calls `workspace.genie.get_message(...)` separately to
retrieve the message's `error.type` (e.g. `TABLES_MISSING_EXCEPTION`) and
`error.error` detail, then wraps both into the `RuntimeError` the UI
displays. If that second call itself fails, or returns nothing structured to
report, `genie_query()` re-raises the original `OperationFailed` rather than
hiding it behind a worse error. Without the enrichment call succeeding, a
failed Genie query in production would usually show only a generic
"OperationFailed" with no actionable detail.

**A toggle is a more convincing proof than a slide.** Rather than *claiming*
Genie is essential, the app ships a literal "🔌 Disable Genie — Demonstrate
Dependency" switch that turns off the live Conversations API call and shows,
in real time, exactly what stops working (the analytical Ask Genie answers)
and exactly what keeps working (the KPI strip, Investigation evidence, Money
Flow, Network, and Reports, which all read the Gold tables directly). That
distinction — between the persisted-data experience and the
conversational-reasoning experience — is the clearest way to show a judge, or
an investigator's manager, what Genie is actually contributing.

---

An alert tells us something may be wrong. MuleGraph Investigator uses Genie to help investigators understand why, follow the money, uncover connected activity, and understand the potential impact.
