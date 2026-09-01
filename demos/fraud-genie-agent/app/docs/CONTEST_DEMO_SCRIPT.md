# Contest Demo Script — MuleGraph Investigator

**Updated for the current architecture.** The app now reads persisted Delta tables
from a real Databricks SQL warehouse and asks a real Databricks Genie Space —
it is *not* the in-memory / rule-based-responder version described in earlier
drafts of this script. Read the "Before you demo" box below before you present.

> **Before you demo — do this or the numbers below won't match what you see:**
> Run `scripts/generate_synthetic_data.py` with **`scale_factor = 1`, `seed = 42`**.
> The app always shows whichever `case_summary` row loads first, and Unity Catalog
> does not guarantee row order when more than one case exists — at the deployment
> guide's *default* of `scale_factor = 3`, you could see a different account and
> different numbers every time you reload the app. `scale_factor = 1` removes the
> ambiguity: exactly one case exists, so the numbers below are guaranteed.
>
> Because Genie now answers from a live, generative model (not the old fixed
> keyword-matched responder), its exact wording will vary run to run. Everything
> it says is still grounded in the same eight Gold tables and four policy views —
> ask the questions below with confidence, but don't read Genie's answers as a
> fixed script; paraphrase live and lean on the numbers, which *are* fixed.

Total time: ~2:30–3:00.

---

## 1. Problem (20s)

*(Layman)* Banks see thousands of "money mule" accounts every year — accounts
that quietly collect stolen or scammed money from multiple victims and forward
it onward before anyone notices. A single flagged transaction almost never
tells the whole story.

*(Technical)* MuleGraph Investigator is a Databricks App that lets an
investigator start from **one flagged account** and use Genie to trace shared
devices and fund flow outward — turning an isolated transaction alert into a
full case file, live, in conversation.

## 2. Suspicious Activity (20s)

Open the app. Point at the case header:

> Account **`ACC_M_COLLECTOR`** is flagged — **high risk band, flagged for
> investigator review.**

*(Layman)* This one account has been quietly receiving money from several
different people and sending it out to several different places, over and
over, for months. That pattern alone is why it got flagged.

## 3. Ask Genie (30s)

Open the **Ask Genie** panel and ask, live:

> **"Why was this account flagged?"**

Genie answers from the `accounts` Gold table and the `evidence_permissive_v`
view — grounded in the real detection reason (fan-in from multiple sources,
recurring fan-out to multiple destinations, crossing the flow threshold). The
exact wording will vary; the facts it cites won't.

*(Technical, if a Databricks engineer is in the room)* Genie is scoped to
query the app's 8 persisted Gold Delta tables plus 4 policy-scoped views
(`evidence_strict_v` / `evidence_permissive_v`, `network_edges_strict_v` /
`network_edges_permissive_v`) — there is no separate "demo mode"; this is the
same data the app's own UI reads.

## 4. Follow the Money (40s)

Ask Genie:

> **"Where did the funds go?"** / **"Who is connected to this account?"**

Then switch to the **Connected Accounts** and **Blast Radius** tabs to show
the same facts, visually:

| Metric (permissive policy) | Value |
|---|---:|
| Other connected accounts | **9** |
| Shared devices | **2** |
| Linked exposure | **$63,637.69** |
| Potential victims | **5** |
| External destinations | **3** |

*(Layman)* One flagged account turns out to be the center of a nine-account
network, tied together by two shared devices and over $63,000 in linked
transfers.

## 5. Reveal the Pattern — the cohort-redefinition moment (40s)

This is the app's centerpiece. Toggle **Evidence policy** from *permissive* to
*strict* live, on screen.

*(Technical)* Strict mode keeps only fund-flow-corroborated evidence
(`device_and_fund_flow` and `account_takeover_provenance`) and drops weak,
device-only links. Watch the Blast Radius card, Evidence panel, and Connected
Accounts tab all update together:

| Metric | Permissive | Strict |
|---|---:|---:|
| Other connected accounts | 9 | **8** |
| Shared devices | 2 | **1** |
| Linked exposure | $63,637.69 | **$63,637.69** |

*(Layman)* Tightening the evidence standard drops one weakly-linked account
and one weak device connection — but notice the dollar exposure **doesn't
move at all**. That's not a coincidence: every dollar in this case is already
backed by real fund flow, not just a shared device. The investigator can now
say exactly which parts of the case are rock-solid and which are worth a
second look, on demand, without re-running anything.

## 6. Technical Value (25s)

*(Technical)* Every number on screen — permissive or strict — comes from the
**same eight Gold tables**, computed once via a single shared policy module
(`policy.py`), so the UI, the export, and Genie can never quietly disagree.
The legitimate-remittance **control cohort** (Control Cohort tab) has the
*exact same* fan-in/fan-out shape as the mule network but is protected by a
recurring-corridor override (900+ days of tenure, an 8-month recurring
corridor) — ask Genie **"Why are control-cohort accounts not flagged?"** to
show this guardrail live, not just as a claim in a slide.

## 7. Customer / Social Impact (20s)

*(Layman)* This isn't about catching a computer's guess. It gives a human
investigator the evidence to make the call — and just as importantly, it
protects innocent customers whose accounts move money the same way legitimate
businesses do. Nothing here takes automated action against an account; it
only prioritizes it for a person's review.

## Close — the strongest question (15s)

Ask Genie, live, as the final beat:

> **"What would we have missed if we investigated only the original
> transaction?"**

Let Genie answer from the real network data. Land the point yourself if
needed: a single-transaction review would never have surfaced the shared
devices, the other 8 connected accounts, or the $63,637.69 in linked exposure
— that only comes from tracing the network, which is exactly what this app
does in one conversation.

---

## Technical reveal (if judges ask "how does this actually work?")

- **Data:** Bronze→Silver→Gold pipeline (`src/pipeline/`), materialized as 8
  persisted Delta tables + 4 policy-scoped views in Unity Catalog — not
  generated in-app.
- **Genie:** a real Databricks Genie Space over those tables/views
  (`src/genie/interface.py` calls `WorkspaceClient.genie.start_conversation_and_wait`).
  No mock, no local responder in the deployed path.
- **App:** Databricks App (Streamlit), reads via a SQL warehouse resource
  (`src/data_access.py`), no data generated at runtime.

## Concrete questions Genie can answer today (grounded, not hypothetical)

- Why was `ACC_M_COLLECTOR` flagged?
- How many inbound sources / outbound destinations does it have?
- Compare permissive vs. strict exposure, connected accounts, shared devices.
- Which evidence disappears under the strict policy?
- Which accounts are connected under the selected policy?
- What transfers contribute to the linked exposure?
- Why are control-cohort accounts not flagged?
- How fresh is this data?
- What would we have missed investigating only the original transaction?

## Safety notes

- Prefer the app's own buttons/tabs to demonstrate mechanics precisely; use
  the questions above (or close paraphrases) for the live Genie chat so
  answers stay reliably grounded.
- Genie is generative now — don't promise the audience exact wording from a
  prior run. Promise the *facts* (they're fixed by the data); let the
  phrasing be live.
- If Genie is slow or the warehouse is cold-starting, the Blast Radius /
  Connected Accounts / Evidence tabs still work instantly from the same
  persisted tables — fall back to those if needed and keep talking.
