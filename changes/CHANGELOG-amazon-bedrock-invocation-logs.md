# What's new in Bedrock Invocation Usage Dashboard

## Bedrock Invocation Usage Dashboard v1.0.0
* Initial release
* Usage Overview & Trends: KPI highlights for invocation count and token usage, daily trend charts, breakdown by model, user, and account, and operational details
* Raw Data Explorer: Bedrock Invocation Records Detail and Total Token Usage by Request
* Per-account and per-model invocation count and token usage breakdown
* Input, output, total, cache read, and cache write token tracking
* Stop reason analysis for understanding model response patterns
* Prompt and response previews (truncated to 1,000 characters) for spot-checking quality
* Regional breakdown of invocation activity
* Shared filter controls for date range, AWS account, model ID, and region across all tabs
* Single SPICE dataset (`bedrock_invocations_view`) with daily refresh schedule
* Source data collected via the `bedrock` module in the CID Data Collection framework
