# Contributing a Quick Agent — Zero-Python Guide

Adding a new agent to the `cid-cmd` catalog is a **content-only** contribution:
**no Python change is required**. Agents are catalog content, defined and shipped the
same way dashboards are: a YAML resource file at the repo root, listed in the agents'
own catalog, `agentic/catalog.yaml`. If your pull request touches any `.py` file, it is
not an agent contribution and will be reviewed under the regular code process instead.

## Agents and Spaces

An agent is two things:

- a **Space** — the knowledge: the dashboards, datasets and knowledge bases the agent
  can use. It lives in `agentic/spaces/<space-id>.yaml`.
- the **agent** — the behavior: persona, starter prompts, welcome message and action
  connectors. It references its Space by id and lives in `agentic/agents/<name>/`.

```
agentic/spaces/<space-id>.yaml  # the Space and its knowledge (kind-wrapped under spaces:)
agentic/agents/<name>/
├── <name>.yaml         # the agent manifest (kind-wrapped under agents:)
└── persona.yaml        # the 5-field persona
```

Several agents (and, later, flows) can use the same Space: they then share exactly that
Space's knowledge, and users need access to one Space instead of one per agent. Action
connectors stay on the agent, so sharing a Space never shares the actions another agent
can take.

List both files in `agentic/catalog.yaml`:

```yaml
  - Url: agents/<name>/<name>.yaml
  - Url: spaces/<space-id>.yaml        # skip if the agent reuses an existing Space
```

That is all the wiring there is — the file flows through the same resource pipeline
dashboards use. `personaFile` is referenced by relative path and resolved against the
manifest's location, so it works both from a local checkout and over HTTPS.

### `agentic/spaces/<space-id>.yaml` — the Space

A worked example mirroring the shipped `cid-finops-space`:

```yaml
spaces:
  cid-finops-space:                        # space id, [0-9a-zA-Z-_=.+]
    name: CID FinOps Space
    description: >-
      Cost and optimization knowledge from the Cloud Intelligence Dashboards.
    dependsOn:
      dashboards:                          # dashboard ids (as in cid-cmd deploy --dashboard-id);
        - cudos-v5                         #   missing ones only produce a warning
        - cora
        - graviton-savings
      datasets:                            # dataset catalog keys, attached as DATA_SET
        - summary_view                     #   knowledge when present; never deployed
        - cora_view
      knowledgeBases: []                   # optional pre-existing knowledge-base ARNs
```

### `<name>.yaml` — the agent manifest

The file wraps the agent under a top-level `agents:` kind key, exactly the way dashboard
files wrap under `dashboards:`. A worked example mirroring the shipped `finops` agent:

```yaml
agents:
  finops:                                  # library id
    category: Foundational                 # Foundational | Advanced | Additional | Deprecated | Other
    name: CID FinOps Advisor               # display name; Agent Name 1-50 chars
    agentId: cid-finops-advisor            # deployed agent id, [0-9a-zA-Z-_.+]
    description: >-
      Cost optimization and anomaly advisor over the CUDOS, Cost Intelligence,
      and KPI dashboards.
    dependsOn:
      spaces: [cid-finops-space]           # exactly one Space id from spaces/
      actionConnectors: []                 # optional action-connector ARNs
    personaFile: persona.yaml              # sibling relative path
    starterPrompts:                        # 0-3 strings, each 100 chars or fewer
      - "What are my top cost optimization opportunities right now?"
      - "Were any cost anomalies detected this month?"
    welcomeMessage: >-                     # 300 chars or fewer
      Hi, I'm the CID FinOps Advisor. Ask me about your cost data.
    lifecycle: PUBLISHED                   # PREVIEW | PUBLISHED
    versions: { version: "v1.0.0" }        # informational only
```

Rules to keep your PR green:

- Knowledge (dashboards, datasets, knowledge bases) is declared on the Space, never on
  the agent; an agent references exactly one Space (a smoke test enforces both).
- Space dashboards are referenced by their `dashboardId` (the id you pass to
  `cid-cmd deploy --dashboard-id`, e.g. `cudos-v5`), not by their catalog key. Every
  dashboard id and every `datasets` key must exist in the core catalog, or be added as an
  ordinary catalog entry in the same PR (referential completeness — a smoke test enforces
  this).
- List every dashboard the agent can use under the Space's `dashboards`. `create-agent`
  never deploys dashboards: missing ones produce a warning pointing to the deployment
  guidance, and the agent works with the ones that are deployed.
- Caps are validated before any API call: at most 3 starter prompts of 100 chars each,
  welcome message 300 chars, agent name 1-50 chars, at most 10 spaces and 10 action
  connectors.

### `persona.yaml` — the five persona fields

All five fields are required, each 5 to 350,000 characters:

```yaml
identity: >-
  You are the CID FinOps Advisor, a cost optimization specialist working over
  the Cloud Intelligence Dashboards.
customInstructions: |
  # Mission
  Turn the cost and usage data in the attached Space into prioritized,
  dollar-quantified optimization recommendations.
  ...
tone: >-
  Direct and numbers-first.
outputStyle: >-
  Dollar-impact-first summaries with supporting tables or bullet lists.
responseLength: >-
  Concise by default; detailed breakdowns for trend analyses.
```

The `customInstructions` body is where the agent's real behavior lives — the shipped
agents under `agents/` are good templates (mission, analysis workflow, which dashboard to
consult for what, response structure, and guardrails).

## Content-only PR template

Copy this into your pull request description:

```markdown
## New agent: <name>

**Content-only contribution — no Python source files changed.**

- [ ] Agent folder `agentic/agents/<name>/` with `<name>.yaml` (kind-wrapped under `agents:`)
      + `persona.yaml`
- [ ] Space `agentic/spaces/<space-id>.yaml` with the dashboards/datasets (or an existing Space reused)
- [ ] `agentic/catalog.yaml` lists `agents/<name>/<name>.yaml` (and `spaces/<space-id>.yaml` for a new Space)
- [ ] All 5 persona fields present and non-empty
- [ ] Every Space dashboard id and dataset key exists in the core catalog (or is added as
      an ordinary catalog entry in this PR)
- [ ] Starter prompts <= 3, each <= 100 chars; welcome message <= 300 chars; name <= 50 chars
- [ ] `pip install -e '.[test]' && pytest cid/test/python/` passes locally
      (the launch-content tests validate referential completeness and loading)

### What the agent does
<one paragraph>

### Dashboards it needs
<dashboard ids>
```

## Review

Content-only agent PRs that follow the template above go through the repo's normal
review process. Because no Python review is involved, review focuses on catalog
referential completeness, persona quality, and naming. PRs that also change Python
source follow the regular code-review process.

## Testing your agent locally

The agent commands load the agents catalog at runtime (the default URL points at this
repository's `main` branch; dashboard commands never load it), so point them at your local
checkout with `--catalog` (list the dashboards catalog too: `--catalog` replaces the default
catalog list) while iterating:

```bash
pip install -e '.[test]'
pytest cid/test/python/          # includes catalog-loading and referential-completeness tests
cid-cmd list-agents --catalog dashboards/catalog.yaml,agentic/catalog.yaml    # your agent appears under its category
cid-cmd create-agent --agent-id <name> --catalog dashboards/catalog.yaml,agentic/catalog.yaml   # against an account with the dashboards deployed
```

You can also iterate without touching the repo at all by shipping the same YAML as an
external resource file and passing `--resources <file-or-url>` — entries merge into the
loaded resources with their origin tagged.
