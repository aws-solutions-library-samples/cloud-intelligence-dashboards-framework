# What's new in Kiro User Activity Dashboard

## Kiro User Activity Dashboard v1.0.0
* Initial release
* Executive Summary: KPI tiles for Active Users, Total Messages, Total Credits Used, and Overage Credits; daily active users by client type; donuts for credits by subscription tier and messages by client type; daily credits consumed trend
* User Engagement: Top 20 users by message count, per-user daily activity detail with message/conversation/credit breakdown
* Credit & Overage Tracking: Daily credits used vs overage, Average Plan Utilization % KPI, per-user overage detail table with plan credits, overage cap, plan utilization %, and overage utilization %
* Client Type Breakdown: Daily messages by client type (IDE / CLI / Plugin), daily metrics by client type
* Subscription tier credit allocation built in (Free: 50, Pro: 1000, Pro+: 2000, Power: 10000)
* Calculated columns: `report_date` (parsed date), `user_id_clean` (quotes stripped), `plan_credits`, `plan_utilization_pct`, `overage_utilization_pct`
* Single SPICE dataset with daily refresh schedule
* Source data collected via `kiro-user-activity` module in the CID Data Collection framework
