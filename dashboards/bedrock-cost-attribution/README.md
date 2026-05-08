# Bedrock Cost Attribution Dashboard

A Cloud Intelligence Dashboard for tracking and attributing Amazon Bedrock costs across accounts, models, and teams.

## Overview

As generative AI adoption grows, organizations need visibility into which teams, models, and use cases drive Bedrock spend. This dashboard provides:

- **Cost by Model** — Compare spend across Claude, Titan, Llama, and other foundation models
- **Cost by Account/Team** — Attribute costs to teams using account-level or tag-based grouping
- **Daily Trend** — Track spend patterns and detect anomalies
- **Token Analysis** — Break down input vs output token consumption
- **Usage Categories** — Separate model invocation, provisioned throughput, agents, knowledge bases, and guardrails

## Prerequisites

- CUR 2.0 (Data Exports) configured with Bedrock line items
- Cloud Intelligence Dashboards framework deployed (`cid-cmd`)
- Amazon Bedrock usage in at least one account

## Deployment

```bash
cid-cmd deploy --dashboard-id bedrock-cost-attribution
```

## Dashboard Visuals

| Visual | Description |
|--------|-------------|
| Total Bedrock Spend (KPI) | Aggregate cost for the selected period |
| Total Tokens Processed (KPI) | Sum of all input + output tokens |
| Cost by Model (Bar) | Horizontal bar chart ranked by spend per model |
| Cost by Account (Bar) | Per-account attribution for chargeback |
| Daily Spend Trend (Line) | Day-over-day cost with anomaly detection |
| Cost by Category (Pie) | Invocation vs Provisioned vs Agents vs KB |
| Token Direction (Bar) | Input tokens vs Output tokens |
| Top Consumers (Table) | Account × Model × Region breakdown |

## Data Source

The dashboard queries CUR 2.0 data filtered by:
- `product_product_name LIKE '%Bedrock%'`
- Product codes: `AmazonBedrock`, `AmazonBedrockFoundationModels`, `AmazonBedrockService`, `AmazonBedrockAgentCore`

## Author

Nithin Chandran R — Technical Account Manager, AWS
