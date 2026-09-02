# MuleGraph Investigator — 2-Minute Contest Demo Script

**Fully revalidated against the current app** (branch `mulegraph-app-genie-fix`) — not an edit of an
older script. This replaces all prior versions of this file, which described a version of the app that
predates the 9-scenario expansion, the Home / Investigation Workspace split, the KPI strips, journey
cards, the Genie-purple accent, the Genie error-detail surfacing fix, and the case-export scoping fix.

**Grounding:** every number, question, screen, and feature below was independently re-verified twice —
once by re-reading the current source (`src/app/app.py`, `src/genie/interface.py`, `src/pipeline/gold.py`
and friends) and running the real pipeline fresh, and again by an independent, different-vendor
cross-review that re-ran the pipeline itself and re-checked every claim against the live code. Nothing
here is invented or carried over from a stale draft.

**v3 note:** this revision is a pure narration pass, requested after human review that the opening felt
flat and the narration didn't sell the app's features. Every WHAT TO SHOW cell, every number, every
screen/click sequence, every Genie question, the required closing question, the required closing line,
and the section structure/timing are unchanged from the validated `docs/CONTEST_DEMO_SCRIPT.md`
(commit `392099e`) — only the WHAT TO SAY prose was rewritten for a stronger narrative arc.

## Flagship case: `ACC_M_COLLECTOR` ("Simple suspicious transfer")

Of the 9 seeded scenarios, this one — not the larger `ACC_LARGE_COLLECTOR` case — is used throughout,
for three concrete, code-verified reasons:

1. The app's first suggested Genie question is hardcoded to name `ACC_M_COLLECTOR` by literal text.
   Opening any other case would show that button asking about the wrong account on screen.
2. `ACC_M_COLLECTOR` is the only case that visibly demonstrates the strict-vs-permissive evidence
   policy difference — `ACC_LARGE_COLLECTOR`'s numbers are identical under both policies.
3. Its 9-account network renders as a clean, readable graph at demo resolution; `ACC_LARGE_COLLECTOR`'s
   28-node graph does not, in the time available.

**Real, current numbers for this case (permissive policy, the default):** Linked exposure **$48,465.78**
· Connected accounts **9** (8 under strict) · Shared devices **2** (1 under strict) · Potential victims
**5** · External destinations **3** · Evidence rows **7** (6 under strict — the one dropped row is
`ACC_M_LOOKALIKE`'s device-only link, which has no fund-flow corroboration) · Case export **8**
citation-backed items under permissive.

Platform totals shown on Home: **9 scenario cases · 138 accounts · 129 network edges · 52 evidence rows
· $584,433.67 combined exposure.**

---

## Timed script (0:00 – 2:00)

Evidence policy stays **permissive** throughout except for a deliberate strict detour in Section 3,
which is explicitly switched back to permissive before Section 4 — so every number shown on screen
always matches the policy actually selected at that moment.

### 1. Strong Opening — Real-World Problem *(0:00 – 0:15)*

| Time | WHAT TO SHOW | WHAT TO SAY |
|---|---|---|
| 0:00–0:08 | Home screen: hero headline "Find the network before the money moves," stat line ("9 scenario cases · 138 accounts · 2026-06-01T00:00:00 refreshed"), Home KPI strip (Cases 9 · Accounts 138 · Connections 129 · Exposure/evidence $584,433.67 · 52 items). | "An alert just fired on one account, and that's all it tells you — something here is wrong. Not who's really behind it. Not where the money already went. Not whether this is one bad account, or the surface of an entire mule network still moving beneath it." |
| 0:08–0:15 | Pan on the featured case card (Priority · HIGH badge, scenario name, seed account) and the "Investigate with Genie →" button. | "That's the moment MuleGraph Investigator exists for — built on Databricks — turning one flagged account into a full, evidence-backed investigation in minutes." |

### 2. Track A — Solve the Problem *(0:15 – 0:35)*

| Time | WHAT TO SHOW | WHAT TO SAY |
|---|---|---|
| 0:15–0:20 | Home → "Browse all scenarios" dropdown → select **"Simple suspicious transfer"** (the exact Home option text — no account-id suffix). App opens the Investigation Workspace, Overview section, on `ACC_M_COLLECTOR`. | "Step one: select the signal. This is a real flagged case, not a mockup — Simple suspicious transfer, account `ACC_M_COLLECTOR`, one of nine live investigation scenarios sitting in Unity Catalog right now." |
| 0:20–0:26 | Overview: case header ("Case: ACC_M_COLLECTOR," "Risk band: HIGH — Flagged for investigator review"), KPI strip (Linked exposure $48,465.78 · Connected accounts 9 · Potential victims 5 · Shared devices 2 · External destinations 3), journey-stage card 02 highlighted. | "And there it is — before we've typed a single question: forty-eight thousand, four hundred sixty-five dollars already linked to this account, nine connected accounts, five potential victims." |
| 0:26–0:35 | Click "Investigate with Genie" action button → view jumps to Investigation section, Genie panel begins answering. | "Numbers tell you what happened. To find out why — and what might still be moving — we hand this investigation to Genie, live." |

### 3. Genie at the Core *(0:35 – 1:25, ~50s — the hero section)*

| Time | WHAT TO SHOW | WHAT TO SAY |
|---|---|---|
| 0:35–0:43 | "🔎 Genie is on this case" panel answering **"Why was this account flagged?"** (fired by the CTA), citations panel open. | "Watch this — no dropdown, no pre-built filter. Just a plain-English question, answered by real Genie running conversational SQL over governed data in Unity Catalog. This is the investigation engine, not a chatbot bolted onto a dashboard." |
| 0:43–0:52 | Click the follow-up **"Which accounts are connected under the selected policy?"**. Genie's answer lands; briefly switch to Network to show the 9-spoke graph (5 device+fund-flow, 1 device-only to `ACC_M_LOOKALIKE`, 3 fund-flow). | "One answer only earns the next question. Genie points us to the wider network — nine connected accounts, two shared devices — and the graph we pull up next is grounded in that exact same governed data. Same investigation, same source of truth, two views." |
| 0:52–1:01 | Switch the evidence-policy radio from Permissive to Strict. Evidence table drops 7→6 rows (the device-only `ACC_M_LOOKALIKE` row disappears); KPI strip drops 9→8 connected, 2→1 shared devices. | "Now we raise the bar ourselves. Flip the evidence policy to strict, and watch the case tighten in real time: the one device link with no money behind it disappears. Connected accounts, nine to eight — live, not staged. That's policy-scoped reasoning over governed views, not static text." |
| 1:01–1:12 | Toggle **"🔌 Disable Genie — Demonstrate Dependency"** ON. Submit any question — the panel returns: *"Genie conversational tracing is unavailable while the dependency is disabled. The KPI strip, Investigation, Money Flow, Network, and Reports remain live."* Toggle back OFF. | "Now the moment that proves this is load-bearing, not decoration. Cut Genie off, and the KPI strip, the evidence, the network graph don't even flinch — they're reading straight from Delta tables. But the conversational thread, the part connecting one fact to the next, goes dark instantly. Flip it back on, and the investigation picks up exactly where it left off." |
| 1:12–1:25 | Genie re-enabled. **Manually type** (not a suggested button) the required closing question: **"What would we have missed if we investigated only the original transaction?"** Show it submitted and Genie's grounded answer landing. | "Last question — typed by hand, on purpose, because it's the one every investigator should ask before closing a case: 'What would we have missed if we investigated only the original transaction?'" |

### 4. App Experience *(1:25 – 1:45, ~20s)*

| Time | WHAT TO SHOW | WHAT TO SAY |
|---|---|---|
| 1:25–1:29 | Switch the evidence-policy radio back to Permissive. Evidence returns to 7 rows; KPI strip returns to 9 connected / 2 devices. | "We ease the bar back down — permissive, our default, for the rest of the walkthrough." |
| 1:29–1:34 | Money Flow: transfers table (24 rows) + monthly bar chart. | "Every dollar leaves a trail — all twenty-four transfers behind this case, rolled up month by month." |
| 1:34–1:39 | Network: 9-edge permissive graph, then the connected-accounts table underneath. | "This is the same network Genie just walked us through — accounts, devices, fund flow — pulled straight from the same governed tables." |
| 1:39–1:45 | Reports: case file header (`CASE_ACC_M_COLLECTOR`), KPI recap, evidence list, "Download case file" button — caption reads exactly "Export ready: 8 citation-backed item(s) under the permissive policy." | "And it all closes into one case file — eight citation-backed items, each traceable back to the row it came from — running on Databricks Apps over Unity Catalog." |

### 5. Closing *(1:45 – 2:00)*

| Time | WHAT TO SHOW | WHAT TO SAY |
|---|---|---|
| 1:45–1:52 | Home hero card, full screen, holding on the headline and the "Investigate with Genie →" button. | (hold — let the moment breathe before the final line) |
| 1:52–2:00 | Static end card: MuleGraph Investigator logo mark / title. | **"An alert tells us something may be wrong. MuleGraph Investigator uses Databricks Genie to help investigators understand why, follow the money, uncover the hidden network, and understand who may be impacted."** |

---

## Before recording — checklist

- **Rehearse against the real, deployed Genie Space once.** The exact sentences Genie speaks are not
  scriptable — they come from a live model over live Delta tables. The numbers above (9→8 connected,
  2→1 devices, $48,465.78 exposure) should inform its answers, but nothing guarantees Genie enumerates
  every entity verbatim, matches the graph exactly, or names Gold tables by row id — the real citation
  renderer labels sources generically as "Databricks Genie," not `gold_case_summary`/`gold_evidence`.
  Describe citations on camera as "a citations panel grounding the answer," not by specific table names.
- **Watch Genie's response latency** — the "Genie is investigating..." status has no fixed timeout in
  the UI. If a real call runs long, hold on the loading state briefly or trim a beat elsewhere to stay
  inside 2:00; the schedule above already has tight margins after the two Section 3/4 additions.
- **Home's "Browse all scenarios" dropdown option is exactly `Simple suspicious transfer`** — no
  account-id suffix. The longer `Simple suspicious transfer — ACC_M_COLLECTOR` label only appears in the
  separate Investigation Workspace account selector, not on Home.
- The "Disable Genie" toggle's real label uses two ASCII hyphens (`--`), not an em dash, if overlaying
  on-screen text quoting it.
- Do one live visual pass of Home → scenario dropdown → Overview → Investigation → policy toggle →
  Disable Genie toggle → Network → Reports before recording, to confirm layout/CSS hasn't drifted since
  this script was written.
