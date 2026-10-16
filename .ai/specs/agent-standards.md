# Quick Agent and Space Standards

Conventions for agentic catalog content (`agentic/`): Amazon Quick chat agents and the Spaces they use, deployed with `cid-cmd create-agent` / `update-agent`. Follow them when creating or reviewing an agent, a Space or a persona. Section 7 is a review checklist. For how the `cid-cmd` agent commands work, see [quick-agents/design.md](quick-agents/design.md).

---

## 1. Layout: a Space owns the knowledge, an agent owns the behavior

```
agentic/
  catalog.yaml                     # lists every agent and Space file (agents first, then Spaces)
  agents/<name>/<name>.yaml        # the agent (kind-wrapped under agents:)
  agents/<name>/persona.yaml       # the 5-field persona
  spaces/<space-id>.yaml           # the Space (kind-wrapped under spaces:)
```

- **Space** (`spaces:`): `name`, `description` and `dependsOn` with `dashboards`, `datasets` and `knowledgeBases`. This is the knowledge an agent can query.
- **Agent** (`agents:`): `name`, `agentId`, `summary`, `description`, `dependsOn.spaces` (exactly one Space id), `dependsOn.actionConnectors`, `personaFile`, `starterPrompts`, `welcomeMessage`, `lifecycle`.
- Never put `dashboards`, `datasets` or `knowledgeBases` on an agent: they are ignored with a warning, and a catalog test fails.
- Action connectors stay on the agent, so sharing a Space never shares the actions another agent can take.
- One file per Space under `agentic/spaces/`, also for a Space used by a single agent: Spaces are shared resources (agents now, flows later).
- Both the agent and its Space must be listed in `agentic/catalog.yaml`. The default catalog URL points to `main`, so changes reach customers on their next run.

## 2. Dependencies

- Dashboards are referenced by **dashboard id** (as in `cid-cmd deploy --dashboard-id`, for example `cudos-v5`), never by catalog key.
- One `dashboards` list; there is no required/optional split. Missing dashboards only produce a warning; the agent works with what is deployed. With none deployed, `create-agent` stops before creating anything.
- Datasets are referenced by dataset catalog key; at runtime they are matched to deployed datasets by `DataSetId`, then key, then name.
- Link only datasets that carry business data the agent should query. Leave out helper datasets (lookup tables, tag filter helpers, data-freshness and evaluation-count tables, wide joined duplicates).
- Amazon Quick allows at most **20 resources of each type per Space**. A catalog test enforces it.
- CID dashboards deployed from their own resources file (for example `cid-crcd`) are not in `dashboards/catalog.yaml`: register them and their datasets in `EXTERNAL_DASHBOARDS` in `cid/test/python/test_agent_catalog.py`.

## 3. Agent manifest

- `agentId`: `cid-<topic>-advisor` or `cid-<topic>-analyst`; it is the deployed id and cannot change later without a new agent.
- `name`: 1-50 characters. `summary`: one line, 70 characters or fewer, quoted (`summary: "..."`, because `: ` breaks YAML), shown in the picker and `list-agents`.
- `description`: two or three sentences naming the dashboards and the purpose.
- `starterPrompts`: at most 3, each 100 characters or fewer, phrased as real user questions the Space can answer.
- `welcomeMessage`: 300 characters or fewer.
- `lifecycle: PUBLISHED`. Agents have no category.

## 4. Persona

All five fields are required (5 to 350,000 characters each): `identity`, `customInstructions`, `tone`, `outputStyle`, `responseLength`. Structure `customInstructions` like the shipped agents:

1. **Communication Style**: tone, response format, length.
2. **Knowledge in this Space**: one section per dashboard: which tab and dataset answers what, with the key rules (for example never sum overlapping CORA savings; summarize a support case from all its communications).
3. **How to answer**: multi-dashboard approach, dashboard integration pairs, which source for which question, prioritization or triage order, questions about the dashboards themselves.
4. **Enhanced Response Framework**: when data is available, when combining dashboards (with two or three concrete examples), when data is unavailable.
5. **Success Metrics Focus**.
6. **Boundaries**: ground every figure in the data, state the time window, never change resources, decision support only, and refer other domains to the other CID agents by name.

Rules:

- Keep field-level meanings out of the persona; they belong in each dataset's semantic metadata (see `summary_view`).
- Ask the agent to **name the exact dashboard and tab** (and visual when useful) where the user can verify a figure. Do not ask for dashboard quicklinks: Quick does not render them reliably.
- Use the dashboard and tab names as they appear in the dashboards; check the dashboard definition or the visual reference before naming a tab or visual.
- Prefer prescriptive guidance ("for X use dashboard Y, tab Z") over generic advice.
- Keep it consistent with the other personas: if you move a topic between agents, update the referrals in the others.

## 5. The launch library

| Agent | Space | Role |
|---|---|---|
| `operations` (CID Operations Advisor) | `cid-operations-space` | All-in-one over most dashboards, for customers who deploy one agent |
| `finops` | `cid-finops-space` | Cost and optimization |
| `security` | `cid-security-space` | Findings, compliance, detection coverage |
| `resilience` | `cid-resilience-space` | Single points of failure, RTO/RPO, fault tolerance |
| `support` | `cid-support-space` | Support case summaries and patterns |
| `health` | `cid-health-space` | AWS Health event prioritization |

The Operations Advisor follows the CID generative AI guide (<https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/generative-ai.html>); the specialists go deeper in one domain.

## 6. Testing

```bash
pip install -e '.[test]'
pytest cid/test/python/
cid-cmd list-agents --catalog dashboards/catalog.yaml,agentic/catalog.yaml
cid-cmd create-agent --agent-id <name> --catalog dashboards/catalog.yaml,agentic/catalog.yaml
```

`--catalog` replaces the default catalog list, so list the dashboards catalog too.

## 7. Review checklist

- [ ] Agent and Space files listed in `agentic/catalog.yaml`
- [ ] Knowledge declared on the Space only; the agent references exactly one Space
- [ ] Dashboard ids (not catalog keys); every dashboard id and dataset key exists in the catalogs or in `EXTERNAL_DASHBOARDS`
- [ ] No helper datasets; at most 20 resources of each type per Space
- [ ] Quoted one-line `summary` (<= 70 chars); starter prompts, welcome message and name within limits
- [ ] Persona follows the section structure, names exact dashboards and tabs, keeps field meanings in dataset metadata, and refers other domains to the other agents
- [ ] Docs updated: launch library table in `docs/cid-cmd.md`
- [ ] `pytest cid/test/python/` passes
