# What's new in Kiro User Activity Dashboard

## Kiro User Activity Dashboard v1.1.0

Reworks the CUR-backed subscription reporting from a user-centric view into a licence-centric one, and adds cost attribution for licences nobody is using. The `kiro_cur_view` dataset and the subscription KPIs existed in v1.0.0; what changes here is what they count, how they are scoped, and what the idle-licence table tells you.

### New

* Credit & Overage Tracking: `Users Below 25% of Plan` KPI, the low-utilization counterpart to the existing at-risk and overage KPIs. Intended for conversations with team managers rather than direct action, since low credit consumption is not the same as low productivity
* Cost attribution on the existing `Idle Kiro Licences` table: `Monthly Fee` and `Inactivity Cost`. The monthly fee is derived from what CUR actually charged, divided by the months billed, so tier changes, discounts, and private pricing are all reflected without maintaining a price list. Inactivity Cost is months idle multiplied by that fee, cumulative across the whole idle stretch rather than for the selected month alone
* Licence tenure on the same table: `Licence Since`, `Months Active`, and `Months Idle`. Months Active exists to prevent over-reacting, since somebody productive for a year who has been quiet for two months is a different case from a licence barely used since purchase
* `data_collection_database_name` as a resolved parameter, so `kiro_user_activity_view` reads from the discovered Data Collection database instead of assuming it matches the dashboard's Athena database

### Changed

* **Licence counting is per account and subscriber.** One licence is one `resource_id` within one `account_id`, so the same person subscribed in two accounts holds two licences, each independently idle and reclaimable. The KPIs previously counted distinct subscribers, which collapsed the two and hid the wasted spend
* **Subscription KPIs renamed to match what they now count**: `Total Active Kiro Users` and `Total Inactive Kiro Users` become `Active Kiro Licences` and `Idle Kiro Licences`. `Total Kiro Subscriptions` keeps its name but changes grain. Active + Idle = Total by construction
* **The date control is now `Billing period`**, scoped to both datasets, defaulting to the previous month. Kiro allocates and resets plan credits per calendar month, and subscription fee lines are dated either on the first of the billing month or spread daily through month end, so a partial window understates utilization and can drop a licence's billing rows entirely
* **Inactive now means "billed in the selected month, consumed no credits in it"**, computed from that month's rows alone, so a past month's answer does not drift as new data lands. The previous test was evaluated after the date filter, which made "inactive" mean "no credit usage inside whatever window you happen to be browsing" and moved the answer every time the date control changed
* `Kiro User Has Credits Usage` removed, replaced by `period_fees`, `period_credits`, and `inactive_in_period`
* `monthly_credits_per_message` replaces averaging the per-row `credits_per_message`, which understated the figure roughly 2.4x. The activity report writes a user's daily credit total on a single one of that user's model rows, so averaging divides a real ratio by the model-row count
* Per-model credit columns removed from the Model & Client pivot. Credits cannot be attributed to an individual model from this source for the same reason; the columns reported which row carried the total, placing 99.8% of credits on one model
* `Top 50 Users by Message Count` Top-N filter now ranks on summed messages rather than row count, which previously ranked users by how many models they had touched
* Per-user monthly pivots now carry an explicit month row, so figures cannot mix calendar months on one line
* Tier and utilization figures evaluate over the calendar month rather than per day
* Widget explanations moved from individual visual subtitles into per-sheet text boxes

### Fixed

* **`kiro_cur_view` double-counted `unblended_cost`.** Its email lookup used `SELECT DISTINCT userid, user_email`, which emits two rows for any subscriber that has ever reported two different email addresses, fanning out every CUR row it joined to. The lookup is now aggregated to exactly one row per subscriber
* `user_email` is now resolved per subscriber in `kiro_user_activity_view`. The source report does not always populate it, which split a single person into two identities wherever the dashboard grouped by user, and produced a blank entry in the User filter control
* `Last Activity` rendered as `yyyy-03-Th` instead of a date. Quick Suite display formats use moment.js tokens, which differ from the parse pattern used in the dataset
* Conditional formatting on plan utilization used `> 0.75` where the at-risk flag uses `>= 0.75`, so a user at exactly 75% was counted but not highlighted
* Stale column-width references that silently had no effect
* Account and User filter controls now also scope the subscription widgets

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
