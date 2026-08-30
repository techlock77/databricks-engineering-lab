# Contest Demo Script — MuleGraph Investigator (~2:45)

Grounded in the delivered app's deterministic seed-42 pipeline. Every number below comes from
the application behavior in this delivery.

## Opening (20s)

> "Fraud investigators today catch one flagged account at a time — but fraud doesn't happen one
> account at a time. It happens in networks: shared devices, layered transfers, accounts that
> look unrelated until you actually trace them. MuleGraph Investigator lets an investigator open
> one flagged account and conversationally trace the whole network behind it — in Databricks,
> on Free Edition, no live workspace data required for the demo."

## The case (20s)

> "Here's `ACC_M_COLLECTOR` — flagged high-risk. It's receiving money from 5 different source
> accounts and sending it out to 3 different destinations, recurring across 3 months. That
> fan-in/fan-out shape, on its own, moved $26,494.74 through this one account."

*(Point at the Evidence tab, then the Blast Radius tab.)*

> "Under our default, permissive evidence policy: 9 other connected accounts, 2 shared devices,
> $63,637.69 in total linked exposure."

## The demo moment — cohort redefinition (45s)

> "Here's the proof-of-concept moment. One of those 9 accounts — `ACC_M_LOOKALIKE` — is only
> linked by a shared device. No money ever moved between it and this case. Watch what happens
> when I ask the investigation to only trust fund-flow-corroborated evidence."

*(Toggle the Evidence policy radio to `strict`.)*

> "9 connected accounts drops to 8. 2 shared devices drops to 1. And the exposure number — the
> actual dollars at risk — doesn't move. $63,637.69, exactly the same. That's the point: tightening
> the evidence standard removed a weak claim, not real money-flow evidence. This isn't a filter
> on a static chart — it's a live re-investigation, and it works the same way across every tab:
> Evidence, Blast Radius, and Connected Accounts all update together."

*(Click Reset to default policy.)*

> "And reset brings it right back — nothing here is destructive."

## Why it matters for the innocent, not just the guilty (25s)

> "Here's the Control Cohort tab — 9 legitimate remittance accounts with the *same* fan-in/fan-out
> shape, moving comparable money. They are never flagged, because the system distinguishes them
> by tenure and long-running recurrence, not just by pattern shape. That distinction is the whole
> point: a fraud tool that can't tell a mule network from a legitimate immigrant remittance
> corridor causes real harm to real customers. This one can."

## Ask Genie (30s)

> "Now let's ask it directly."

*(Type into the chat: "What would we have missed if we investigated only the original transaction?")*

> "It answers using only the case's actual Gold-table numbers — connected accounts, shared
> devices, dollars — with a citation and a freshness note right in the chat, so nothing here is
> a black box."

## Close (15s)

> "One flagged account, traced into a network, with a conversational way to test how confident
> that network really is — and a guardrail that protects innocent customers built in from day
> one, not bolted on after. That's MuleGraph Investigator."

## If judges ask "how does it actually work?" (technical reveal)

- Synthetic Bronze→Silver→Gold pipeline (pandas, in-memory, deterministic for seed 42) — no
  live workspace data needed for this demo.
- `Ask Genie` is currently a rule-based responder over the Gold tables behind a clean
  `genie_query()` seam — built to be swapped for a real Databricks Genie Space without touching
  the UI once deployed against live data.
- Every number shown anywhere in the app — dashboard metrics, chat answers, the exported case
  file — reads from the *same* computed network object per evidence policy, so nothing can
  silently disagree across the UI.

## Safety notes for live demo

- Use the provided buttons/toggle and the approximately nine scripted question phrasings listed
  in the monorepo's `demos/fraud-genie-agent/outputs/demo-script.md`. The responder is
  keyword-matched, not generative, so free-text questions outside that set will not get a
  grounded answer.
- If asked to repeat the demo moment, toggling strict → permissive → strict again is safe and
  deterministic; the underlying dataset never changes.
