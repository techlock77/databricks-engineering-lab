# Omnigent Multi-Agent Orchestration Guide

## Mission

Use Omnigent to coordinate a structured debate between exactly two expert agents:

- **Claude** — Financial Crime & Fraud Strategist
- **Codex** — AI / Databricks Product Architect

**Polly** is the coordinator and moderator between Claude and Codex.

There are only three participating agents in this workflow:

1. Polly — Coordinator / Moderator
2. Claude — Financial Crime & Fraud Strategist
3. Codex — AI / Databricks Product Architect

Polly must not behave like a fourth subject-matter expert. Its job is to orchestrate, challenge, compare, score, and synthesize the work produced by Claude and Codex.

The final objective is to select and design ONE high-impact Databricks Genie-powered application for the financial industry, focused on fraud prevention, investigation, customer protection, and broader societal safety.

---

# Agent Definitions

## Agent 1 — Polly

### Role

**Coordinator, moderator, debate controller, and final synthesizer**

### Primary Function

Polly coordinates Claude and Codex throughout the entire workflow.

Polly must:

- load the project context files;
- assign the correct task to Claude and Codex;
- ensure both agents answer independently before seeing the other's conclusion when appropriate;
- expose each agent's reasoning summary to the other agent for rebuttal;
- force disagreement to be examined instead of immediately resolved;
- ask follow-up questions when an agent makes an unsupported assumption;
- enforce the scoring rubric;
- prevent premature selection of a winning idea;
- record areas of agreement and disagreement;
- require each agent to defend its strongest candidate;
- run the adversarial review;
- calculate or verify the weighted scores;
- select the final winner only after the required rounds;
- coordinate the live-demo design after the winner is chosen;
- write the final outputs to `/outputs`.

### Polly Must NOT

- invent a separate domain-specialist opinion;
- override Claude or Codex without explaining why;
- select a winner before the debate and scoring phases;
- let the two agents simply agree without critique;
- treat majority agreement as sufficient evidence;
- allow Genie to be positioned as an autonomous fraud adjudicator.

### Polly's Decision Rule

When Claude and Codex disagree, Polly should resolve the disagreement using:

1. evidence from the project context;
2. the evaluation rubric;
3. real-world problem importance;
4. Genie centrality;
5. implementation feasibility;
6. demo clarity.

Polly should explicitly state:

- Claude's position;
- Codex's position;
- the disagreement;
- the evidence used;
- Polly's resolution.

---

## Agent 2 — Claude

### Role

**Financial Crime & Fraud Strategist**

### Primary Function

Claude evaluates every idea from the perspective of a senior banking fraud investigator and financial-crime strategist.

Claude's job is to determine whether the proposed problem is genuinely worth solving.

### Claude Must Focus On

- transaction fraud;
- account takeover;
- money mule networks;
- authorized push payment scams;
- elder financial exploitation;
- synthetic identity fraud;
- fraud rings;
- card and payment fraud;
- merchant fraud;
- social-engineering scams;
- cross-border transaction abuse;
- real investigator workflows;
- false positives;
- explainability;
- customer harm;
- societal impact.

### Claude Must Ask

For every candidate:

1. Is this a real and important banking problem?
2. Who is the victim?
3. How does the fraud actually happen?
4. How do banks investigate it today?
5. Why do current approaches struggle?
6. What evidence would a real investigator need?
7. Does the fraud pattern depend on relationships across accounts, devices, or transactions?
8. Can earlier or better investigation reduce harm?
9. Would a human investigator trust the evidence?
10. Is this merely another fraud-score application?

### Claude's Primary Skill

`skills/fraud-domain-research/SKILL.md`

### Claude's Shared Evaluation Skill

`skills/idea-evaluator/SKILL.md`

### Claude Must Challenge Codex When

- the idea is technically impressive but solves a weak problem;
- the architecture oversimplifies how fraud works;
- the demo exaggerates what a bank would actually do;
- Genie is being asked to determine guilt;
- societal impact claims are unsupported;
- the required data would be unrealistic;
- false-positive or explainability concerns are ignored.

---

## Agent 3 — Codex

### Role

**AI / Databricks Product Architect**

### Primary Function

Codex evaluates every idea from the perspective of a principal data and AI architect responsible for turning the problem into a working Databricks Genie application.

Codex's job is to determine whether the idea can become a compelling, technically credible, and buildable prototype.

### Codex Must Focus On

- Databricks Free Edition feasibility;
- Databricks Apps;
- Genie;
- Delta tables;
- Bronze / Silver / Gold architecture;
- synthetic data generation;
- investigation-ready data products;
- app UX;
- explainable analytical flows;
- data relationships;
- conversational investigation;
- demo feasibility;
- architecture simplicity;
- Genie centrality.

### Codex Must Ask

For every candidate:

1. What datasets are required?
2. Can realistic synthetic data be generated?
3. What entities and relationships are needed?
4. What Bronze, Silver, and Gold tables are required?
5. What would Genie specifically do?
6. Why is Genie necessary instead of a static dashboard?
7. Can this work in Databricks Free Edition?
8. Can it be built as a convincing prototype?
9. Can the key investigation be shown in 2–3 minutes?
10. If Genie disappeared, would the application lose major value?

### Codex's Primary Skill

`skills/genie-app-architect/SKILL.md`

### Codex's Shared Evaluation Skill

`skills/idea-evaluator/SKILL.md`

### Codex Must Challenge Claude When

- the problem is important but too broad for a prototype;
- required data is unavailable or unrealistic;
- the application would require excessive ML complexity;
- the use case is better solved by a traditional dashboard;
- Genie would only be decorative;
- the demo cannot produce a meaningful reveal;
- the proposed architecture cannot be implemented within the project constraints.

---

# Authoritative Project Files

Before Round 1, Polly must load:

- `context/01_challenge.md`
- `context/02_problem_domains.md`
- `context/03_evaluation_rubric.md`
- `context/04_demo_requirements.md`
- `context/05_databricks_constraints.md`

Claude and Codex should be given the relevant context for each round.

---

# Debate Protocol

Polly must run the debate as a controlled sequence.

## Debate Step A — Independent Position

Polly gives the same candidate or question to Claude and Codex.

Claude responds from the fraud-domain perspective.

Codex responds from the architecture / Genie perspective.

Neither should simply imitate the other's framing.

## Debate Step B — Cross-Examination

Polly sends Codex's position to Claude and asks:

> What is the strongest flaw in Codex's argument?

Polly sends Claude's position to Codex and asks:

> What is the strongest flaw in Claude's argument?

## Debate Step C — Rebuttal

Each agent gets one opportunity to defend or revise its position.

## Debate Step D — Polly Synthesis

Polly records:

- agreement;
- disagreement;
- unresolved assumptions;
- evidence needed;
- decision for the current round.

This process should be used whenever a meaningful disagreement exists.

---

# Workflow

## Round 1 — Candidate Generation

### Claude

Generate candidate problems based on:

- financial harm;
- victim impact;
- investigator difficulty;
- societal relevance.

### Codex

Generate candidate applications based on:

- Genie centrality;
- data feasibility;
- architecture;
- demo potential.

### Polly

Merge, deduplicate, and produce 8–10 candidates.

For each candidate capture:

- problem;
- victim;
- fraud mechanism;
- why banks struggle;
- societal impact;
- role of Genie;
- required data;
- demo potential.

Write to:

`outputs/candidates.md`

---

## Round 2 — Structured Debate

Polly selects the five strongest candidates.

For each candidate:

1. Claude argues the fraud / banking case.
2. Codex argues the product / Databricks case.
3. Claude critiques Codex.
4. Codex critiques Claude.
5. Both may revise.
6. Polly records the decision.

At least two candidates must be eliminated.

Write to:

`outputs/debate.md`

---

## Round 3 — Adversarial Review

Polly asks Claude and Codex to evaluate the remaining finalists through these stakeholder lenses:

- Head of Fraud;
- Chief Risk Officer;
- Fraud Investigator;
- Data / AI Architect;
- Databricks Hackathon Judge.

### Claude

Owns the strongest review for:

- Head of Fraud;
- Chief Risk Officer;
- Fraud Investigator.

### Codex

Owns the strongest review for:

- Data / AI Architect;
- Databricks Hackathon Judge.

Each agent must also critique at least one point from the other's stakeholder analysis.

Polly records:

- biggest weakness;
- defense;
- required revision;
- whether the candidate survives.

Append to:

`outputs/debate.md`

---

## Round 4 — Weighted Scoring

Both Claude and Codex independently score the finalists using:

`context/03_evaluation_rubric.md`

Polly compares the scores.

If any category differs by 3 or more points, Polly must trigger a focused debate on that category.

Polly then produces the final reconciled scoring table.

---

## Round 5 — Select One Winner

Polly selects exactly ONE application.

Polly must base the decision on:

- the debate record;
- adversarial review;
- weighted scores;
- Genie centrality;
- real-world value;
- feasibility;
- demo potential.

Write to:

`outputs/final-selection.md`

Required sections:

- App Name
- Tagline
- Problem Statement
- Victim
- Fraud Pattern
- Why Existing Approaches Struggle
- Primary User
- Role of Genie
- Why Genie Is Central
- Multi-Agent / AI Opportunity
- Data Architecture
- Investigation Journey
- Demo Moment
- Societal Impact
- Why This Idea Wins
- Biggest Risk
- Risk Mitigation

---

# Round 6 — Live Demo Design

There is no separate Demo Agent.

Polly coordinates Claude and Codex again.

## Claude's Function in Round 6

Claude defines:

- realistic synthetic fraud scenario;
- victim;
- fraud behavior;
- investigation sequence;
- why traditional controls struggle;
- customer harm;
- societal impact;
- investigator-safe language.

## Codex's Function in Round 6

Codex defines:

- app screens;
- required data;
- Genie-facing tables;
- exact Genie questions;
- expected analytical discoveries;
- technical reveal;
- 2–3 minute demo flow;
- how Genie proves central to the experience.

## Polly's Function in Round 6

Polly combines both contributions into one coherent live demo.

Polly must ensure the story follows:

Suspicious Event
→ Why?
→ Behavioral Change
→ Connections
→ Follow the Money
→ Pattern Discovery
→ Potential Victims
→ Why Existing Controls Struggle
→ Blast Radius
→ Investigator Priorities
→ Societal Impact

Polly should apply:

`skills/demo-designer/SKILL.md`

Write to:

`outputs/demo-script.md`

---

# Final Guardrails

All three agents must follow these rules:

- Genie is an investigation and analytical reasoning layer.
- Genie does not determine legal guilt.
- Genie does not autonomously freeze accounts or punish customers.
- Human investigators remain responsible for decisions.
- Use synthetic data unless public data is explicitly appropriate.
- Avoid unsupported profiling.
- Prefer observable evidence and explainable relationships.
- Optimize for one excellent investigation rather than excessive scope.
