# What's new in Kiro User Activity Dashboard

## Kiro User Activity Dashboard v1.1.1

Corrects plan-credit and utilization reporting for the Pro Plus and Pro Max tiers, which read as
0 in v1.0.0 and v1.1.0. Anyone with users on those tiers should update: the affected figures were
wrong rather than missing, and the tier recommendation derived from them was wrong with them.

Reported by Shingo Horisawa, who also supplied the real activity report files that identified the
cause.

### Fixed

* **Plan Credits read 0 for Pro Plus and Pro Max, and every figure derived from it was wrong.**
  The activity report writes the tier in screaming snake case (`PRO_PLUS`, `PRO_MAX`) while the
  tier ladder matched `proplus` and `promax`, so those tiers fell through to the 0 fallback.
  Single-word tiers such as `PRO` and `POWER` matched by accident, which is why only the two-word
  tiers were affected. Consequences, all now corrected:
  * `Plan Credits` and `Plan Utilization Percentage` read 0 for those licences
  * `Subscription Tier Recommendation` always read `Downgrade Candidate`, including for a Pro Max
    licence at 98% of its allowance
  * **`Users at Risk` silently under-counted**, because a licence with no known allowance
    computes 0% utilization and so fails the 75% test. Nothing on the sheet looked wrong
  * conditional formatting on utilization never fired for those tiers

  The tier is now normalized once into a dedicated `tier_key` column, lowercased with
  underscores, hyphens and spaces stripped, and every comparison matches on that. This accepts
  the report's `PRO_MAX`, the CUR usage type's `ProMax`, and the `ProMax` that kiro.dev documents,
  so it is robust to the spelling differing again between sources.
* **`Users at Risk` is now guarded against an unknown tier**, matching the guard
  `Users Below 25% of Plan` already had. A tier with no known allowance can no longer be
  silently excluded from the at-risk count.
* **An unrecognized tier now reports `Unknown Tier`** instead of `Downgrade Candidate`. A missing
  plan allowance is no longer able to masquerade as low utilization, which is what let this defect
  survive two releases behind a plausible-looking recommendation.
* **`pricing_unit` comparisons are case-normalized.** Not a live defect, but `Credits` is the only
  literal separating "the user consumed credits" from "the user was billed": had it stopped
  matching, every licence would have been reported as idle with a full Inactivity Cost against it.
* **A duplicate field identifier that had been reporting an error since v1.0.0.** The Overage
  Credits KPI and the Users in Overage KPI both used the identifier `kpi-overage-val` for
  different columns. QuickSight binds an identifier once, so `DescribeDashboardDefinition`
  returned a `COLUMN_NOT_FOUND` error for one of the two visuals on every deployment since the
  initial release. Both sheets rendered, which is why it went unnoticed.

* **Overage spend was invisible.** Overage reached the dashboard only as a credit *count* from the
  activity report, never as money, and no visual used the CUR cost column at all - so a licence
  running $200 of overage looked identical to one running none. Credit & Overage Tracking gains an
  **Overage Cost** KPI, the first figure on that sheet expressed in currency.
* **Fee versus consumption is now decided by the billing operation, not the pricing unit.** CUR
  records the same credit consumption under two different pricing units, and the second variant
  carries real usage. Those rows were counted as a subscription fee *and* excluded from
  consumption, so an affected licence showed an inflated Monthly Fee and a blank Last Activity -
  reading as "billed but never used" for someone who had used it. Confirmed against a real
  subscriber, whose first consumption record takes that form.
* **The subscription roster now counts every charge rather than only the recurring fee.**
  Roster membership and the monthly rate are different questions: the first needs to catch any
  charge, the second only the subscription fee. Serving both from one measure meant a charge under
  an unrecognised billing operation would have removed the licence from Total, Active and Idle
  entirely, with its cost figures reading zero. They are now separate measures.

* **Subscription Type now shows the tier for every tier, including new ones.** It enumerated four
  tiers and Pro Max was not among them, so a Pro Max licence rendered as "monthly-subscription" in
  the Idle Kiro Licences table - losing the one thing the column exists to show. The tier is now
  derived from the billing usage type, so a tier Kiro adds appears without a dashboard change. This
  also distinguishes the Individual product line from Enterprise, which was previously impossible.

### Notes

* No Athena view changed, so this update needs no `--recursive`. A SPICE refresh is required for
  the new `tier_key` column to materialize.
* The activity report now also carries a `Usage_Limit` column holding the plan allowance directly.
  It confirms the ladder's values are correct (2000 for Pro Plus, 5000 for Pro Max) and is the
  intended future source of truth, which would remove the hardcoded ladder entirely. It is not
  collected yet.

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
