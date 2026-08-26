# Fraud Genie — Omnigent Multi-Agent Context Pack

This package is designed for an Omnigent workflow using exactly three agents:

1. **Polly** — Coordinator / Moderator
2. **Claude** — Financial Crime & Fraud Strategist
3. **Codex** — AI / Databricks Product Architect

Claude and Codex debate the problem and proposed solution.

Polly coordinates the debate, forces cross-examination, applies the evaluation rubric, resolves disagreements, and synthesizes the final result.

There is no fourth specialist agent.

## Start Here

Give Omnigent this instruction:

> Read `AGENTS.md` and follow the complete workflow. Polly is the coordinator between Claude and Codex. Claude owns the financial-crime and societal-impact perspective. Codex owns the Databricks, Genie, architecture, and implementation perspective. Polly must make Claude and Codex debate, critique, rebut, and score the candidates before selecting one winner. Use `/context` as authoritative requirements and `/skills` for specialized procedures. Write all results to `/outputs`.

## Agent-to-Skill Mapping

### Polly

Uses:

- `skills/idea-evaluator/SKILL.md`
- `skills/demo-designer/SKILL.md`

Function:

- orchestration;
- debate control;
- evaluation;
- synthesis;
- final selection;
- live-demo coordination.

### Claude

Uses:

- `skills/fraud-domain-research/SKILL.md`
- `skills/idea-evaluator/SKILL.md`

Function:

- fraud-domain expertise;
- victim analysis;
- banking relevance;
- societal impact;
- investigation realism;
- challenge technical ideas that do not solve meaningful problems.

### Codex

Uses:

- `skills/genie-app-architect/SKILL.md`
- `skills/idea-evaluator/SKILL.md`

Function:

- Databricks architecture;
- Genie centrality;
- Free Edition feasibility;
- data modeling;
- implementation scope;
- app and demo design;
- challenge valuable ideas that cannot become a strong working prototype.

## Folder Structure

```text
fraud-genie-agent/
├── AGENTS.md
├── README.md
├── context/
├── skills/
└── outputs/
```

## Debate Model

```text
                 POLLY
          Coordinator / Judge
                /   \
               /     \
              ▼       ▼
          CLAUDE ⇄ CODEX
        Fraud/Banking  Databricks/AI
              \       /
               \     /
                ▼   ▼
             REBUTTAL
                │
                ▼
          POLLY SYNTHESIS
                │
                ▼
           FINAL WINNER
```

## Important Positioning

The final application should use Genie to help investigators explore evidence, relationships, timelines, and impact.

It should not position Genie as an autonomous system that determines guilt or takes punitive action without human review.
