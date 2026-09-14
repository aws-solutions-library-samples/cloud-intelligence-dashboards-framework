# Contributing a Quick Agent — Zero-Python Guide

Adding a new agent to the `cid-cmd` catalog is a **content-only** contribution:
**no Python change is required**. Agents are catalog content, defined and shipped the
same way dashboards are: a YAML resource file at the repo root, listed in the agents'
own catalog, `agents/catalog.yaml`. If your pull request touches any `.py` file, it is
not an agent contribution and will be reviewed under the regular code process instead.

## The agent folder

An agent lives in a per-agent folder at the repo root, next to `dashboards/`:

```
agents/<name>/
├── <name>.yaml         # required — the agent manifest (kind-wrapped, like dashboards)
└── persona.yaml        # required — the 5-field persona
```

Then add one line to `agents/catalog.yaml`:

```yaml
  - Url: <name>/<name>.yaml
```

That is all the wiring there is — the file flows through the same resource pipeline
dashboards use. `personaFile` is referenced by relative path and resolved against the
manifest's location, so it works both from a local checkout and over HTTPS.

### `<name>.yaml` — the manifest

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
      spaces: [cid-dashboards-space]       # shared space id(s) from spaces/
      dashboards: [CUDOSv5, CID, KPI]      # REQUIRED dashboard catalog keys (foundational/additional)
      optionalDashboards: [Trends]         # OPTIONAL dashboard keys (advanced, Data Collection-backed)
      datasets:                            # optional dataset catalog keys, attached as
        - daily-anomaly-detection          #   DATA_SET knowledge when present; never deployed
        - monthly-anomaly-detection
      knowledgeBases: []                   # optional pre-existing knowledge-base ARNs
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

- Every key under `dependsOn.dashboards` / `optionalDashboards` / `datasets` must exist in
  the core catalog, or be added as an ordinary catalog entry in the same PR (referential
  completeness — a smoke test enforces this).
- Foundational/additional dashboards go under `dashboards` (Required); advanced,
  Data Collection-backed dashboards go under `optionalDashboards` (Optional). `create-agent`
  never deploys either kind — it warns and points to the deployment guidance.
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

### Spaces: shared vs agent-owned

- **Shared Space** — used by several agents; one kind-wrapped file per Space under
  `spaces/<space-id>.yaml`, listed in `agents/catalog.yaml`
  (as `../spaces/<space-id>.yaml`):

  ```yaml
  spaces:
    cid-dashboards-space:                  # space id
      name: CID Dashboards Space
      description: >-
        Comprehensive knowledge base for the Cloud Intelligence Dashboards.
      dependsOn:
        dashboards: [CUDOSv5, CID, KPI]    # Required dashboard keys
        optionalDashboards: [Trends]       # Optional (Data Collection-backed) keys
        datasets: []
  ```

- **Agent-owned Space** — specific to one agent; since resource files are kind-agnostic,
  define it in the agent's own manifest file under a `spaces:` key alongside `agents:`,
  and reference its id from `dependsOn.spaces`. No extra file or catalog line needed.

Dashboard dependencies may be declared on the agent, on its Space, or both; the
required/optional split applies in every location and `create-agent` merges them.

## Content-only PR template

Copy this into your pull request description:

```markdown
## New agent: <name>

**Content-only contribution — no Python source files changed.**

- [ ] Agent folder `agents/<name>/` with `<name>.yaml` (kind-wrapped under `agents:`)
      + `persona.yaml`
- [ ] `agents/catalog.yaml` lists `<name>/<name>.yaml`
- [ ] All 5 persona fields present and non-empty
- [ ] Every `dependsOn` key exists in the core catalog (or is added as an ordinary
      catalog entry in this PR)
- [ ] Required vs Optional dashboard split follows foundational-vs-Data-Collection lines
- [ ] Starter prompts <= 3, each <= 100 chars; welcome message <= 300 chars; name <= 50 chars
- [ ] `pip install -e '.[test]' && pytest cid/test/python/` passes locally
      (the launch-content tests validate referential completeness and loading)

### What the agent does
<one paragraph>

### Dashboards it needs
Required: <keys>  |  Optional: <keys>
```

## Review

Content-only agent PRs that follow the template above go through the repo's normal
review process. Because no Python review is involved, review focuses on catalog
referential completeness, persona quality, and naming. PRs that also change Python
source follow the regular code-review process.

## Testing your agent locally

Agents are loaded from the catalog at runtime (the default catalog URL points at this
repository's `main` branch), so point `cid-cmd` at your local checkout while iterating:

```bash
pip install -e '.[test]'
pytest cid/test/python/          # includes catalog-loading and referential-completeness tests
cid-cmd list-agents --catalog agents/catalog.yaml    # your agent appears under its category
cid-cmd create-agent --agent-id <name> --catalog agents/catalog.yaml   # against an account with the dashboards deployed
```

You can also iterate without touching the repo at all by shipping the same YAML as an
external resource file and passing `--resources <file-or-url>` — entries merge into the
loaded resources with their origin tagged.
