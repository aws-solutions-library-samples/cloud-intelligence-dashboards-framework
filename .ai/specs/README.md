# AI steering for the Cloud Intelligence Dashboards Framework

This folder is the shared, tool-agnostic source of AI steering for this repository. Kiro loads this index through `.kiro/steering/ai-specs.md`, and Claude Code through `.claude/rules/ai-specs.md`. Other assistants should be pointed here too.

Before you create or change anything, read the files below that apply to your task.

## Standards (apply to all work of that type)

| File | Read when |
|---|---|
| [dashboard-standards.md](dashboard-standards.md) | Creating or reviewing dashboard resource files (`dashboards/**/*.yaml`): views, datasets, `dependsOn`, placeholders, account names (`account_map`), prerequisite checks, metadata |
| [agent-standards.md](agent-standards.md) | Creating or reviewing Quick agents, Spaces or personas (`agentic/**`): layout, dependencies, manifest fields, persona structure, Space limits |
| [release-readiness.md](release-readiness.md) | Preparing or reviewing a dashboard release: account-specific or hardcoded content, spelling, duplicate fields, version consistency, changelog and RSS |
| [usage-type-groups.md](usage-type-groups.md) | Creating, reviewing or updating `* Usage Type Group` / `Sub Group` calculated fields: checking coverage against the AWS Price List bulk files, pricing-page naming, match order, dependents |

## Feature specs (read only when working on that feature)

| Folder | Feature |
|---|---|
| [advanced-account-mapper/](advanced-account-mapper/) | `cid-cmd map` advanced account mapper |
| [focus-consolidation-dynamic-view/](focus-consolidation-dynamic-view/) | FOCUS consolidation dynamic view |
| [quick-agents/](quick-agents/) | `cid-cmd` agent commands (`create-agent`, `update-agent`, `list-agents`, `delete-agent`): architecture and design decisions |

## Adding steering

- **Standards** that apply broadly go in a top-level `.md` file here. Add a row to the Standards table.
- **Feature specs** go in a subfolder named after the feature (`requirements.md`, `design.md`, `tasks.md`). Add a row to the Feature specs table.
- Don't add tool-specific copies. Kiro and Claude Code both read this index, so one entry here is enough.
- Personal or experimental steering goes in `.ai/local/`, which isn't committed.
