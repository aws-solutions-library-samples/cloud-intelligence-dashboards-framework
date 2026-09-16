Adds a new **Bedrock Invocation Usage** QuickSight dashboard for visualizing Amazon Bedrock model invocation activity across AWS accounts, tracking per-account and per-model token consumption, request patterns, and cost attribution.

### Files added

- `dashboards/amazon-bedrock-invocation-logs/bedrock-invocation-logs-dashboard.yaml` — dashboard definition with a single SPICE dataset built on top of the `bedrock_invocations_view` Athena view, the `CREATE OR REPLACE VIEW` SQL that flattens the nested `bedrock_invocations` token fields, and a daily refresh schedule. Sheets:
  - **Usage Overview & Trends** — KPI highlights for invocation count and token usage, daily trend charts, breakdowns by model, user, and account, and operational details.
  - **Raw Data Explorer** — Bedrock Invocation Records Detail and Total Token Usage by Request.

### Files updated

- `dashboards/catalog.yaml` — registered the new dashboard.
- `changes/CHANGELOG-amazon-bedrock-invocation-logs.md` — added v1.0.0 changelog entry.
- `changes/cloud-intelligence-dashboards.rss` — added the `[New Dashboard]` feed item.

### Data fields

Surfaces per-invocation token metrics (`usage_input_tokens`, `usage_output_tokens`, `usage_total_tokens`, `usage_cache_read_tokens`, `usage_cache_write_tokens`), `stop_reason` analysis, and prompt/response previews (truncated to 1,000 characters) for spot-checking quality. The view filters to the trailing 6 months of invocations and exposes `year`/`month`/`day` partitions along with `accountid`, `region`, `modelid`, and `identity_arn`.

### Dependency

Requires the companion `bedrock` data collection module from the [cid-framework](https://github.com/awslabs/cid-framework) repo to populate the underlying `bedrock_invocations` Athena table. The `bedrock_invocations_view` view is created automatically by the Data Collection Stack.
