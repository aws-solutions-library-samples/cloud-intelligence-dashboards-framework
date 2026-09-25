# What's new in Kiro User Activity Dashboard

## Kiro User Activity Dashboard v1.1.1

**Update if you have users on Pro Plus or Pro Max, or licences that changed tier mid-month.**
Figures for those licences were wrong rather than missing, and the recommendations derived
from them were wrong with them.

Reported by Shingo Horisawa, who a identified the root cause.

### Fixed

* **Plan Credits read 0 for Pro Plus and Pro Max.** The activity report writes the tier as
  `PRO_PLUS` / `PRO_MAX` while the tier ladder matched `proplus` / `promax`. Single-word tiers
  such as `PRO` matched by accident, which is why only the two-word tiers broke. This affected
  `Plan Credits`, `Plan Utilization %`, conditional formatting, and `Subscription Tier
  Recommendation`, which always read `Downgrade Candidate` - including for a Pro Max licence at
  98% of its allowance. **`Users at Risk` also silently under-counted**, because a licence with
  no known allowance computes 0% utilization and so fails the 75% test. The tier is now
  normalized once and matched everywhere on that, so it tolerates the spelling differing
  between sources again.
* **Licences in active use were reported idle, with an Inactivity Cost against them.** Activity
  was read from CUR, which does not record it reliably: in-plan credits are included in the
  subscription and cost nothing, so no billing row is emitted for them. Consumption now comes
  from the activity report, and `Last Activity` no longer reads blank for a licence being used
  daily.
* **A licence that changed tier mid-month appeared twice**, once per tier, with two
  contradictory recommendations and two different allowances. Tier, allowance and utilization
  now resolve once per licence-month, matching how Kiro bills a mid-month upgrade.
* **Inflated Monthly Fee and blank Last Activity for some licences.** Fee versus consumption is
  now decided by the billing operation rather than the pricing unit, because the same
  consumption arrives under two pricing units. Affected rows were counted as a fee *and*
  excluded from consumption, so a licence that had been used read as billed but never used.
* **`Subscription Type` showed "monthly-subscription" for Pro Max** in the Idle Kiro Licences
  table, losing the one thing the column exists to show. It is now derived from the billing
  usage type, so a tier Kiro adds later appears without a dashboard change.
* **An unrecognized tier now reports `Unknown Tier`** instead of `Downgrade Candidate`. A
  missing allowance masquerading as low utilization is what let the defect above survive two
  releases.
* **A duplicate field identifier present since v1.0.0.** Two KPIs shared one identifier for
  different columns, so `DescribeDashboardDefinition` returned `COLUMN_NOT_FOUND` on every
  deployment since the initial release. Both sheets rendered, which is why it went unnoticed.

### New

* **Overage Cost** KPI on Credit & Overage Tracking. Overage previously reached the dashboard
  only as a credit count, never as money, so a licence running $200 of overage looked identical
  to one running none.

### Upgrading

**An Athena view changed, so this update needs `--recursive`**, then a SPICE refresh of **both**
datasets (`kiro_user_activity` and `kiro_cur_view`). Verify with
`describe-dashboard-definition --query 'Errors'`, which should return `null` - a clean
`cid-cmd` run on its own is not proof.

## Kiro User Activity Dashboard v1.1.0

Reworks CUR-backed subscription reporting from a user-centric view into a licence-centric one,
and adds cost attribution for licences nobody is using.

### New

* Cost attribution on `Idle Kiro Licences`: `Monthly Fee` and `Inactivity Cost`. The fee comes
  from what CUR actually charged, so tier changes, discounts and private pricing are reflected
  without maintaining a price list. Inactivity Cost is cumulative across the whole idle
  stretch, not just the selected month
* Licence tenure on the same table: `Licence Since`, `Months Active`, `Months Idle`. Months
  Active exists to prevent over-reacting - someone productive for a year who has been quiet for
  two months is a different case from a licence barely used since purchase
* `Users Below 25% of Plan` KPI. Intended for conversations with managers rather than direct
  action, since low credit consumption is not the same as low productivity

### Changed

* **Licence counting is per account and subscriber.** The same person subscribed in two
  accounts holds two licences, each independently reclaimable. The KPIs previously counted
  distinct subscribers, collapsing the two and hiding the wasted spend
* **KPIs renamed to match what they count**: `Total Active/Inactive Kiro Users` become
  `Active/Idle Kiro Licences`. `Total Kiro Subscriptions` keeps its name but changes grain
* **The date control is now `Billing period`**, defaulting to the previous month and scoped to
  both datasets. Kiro resets plan credits per calendar month, and fee lines are dated either on
  the 1st or spread daily through month end, so a partial window understates utilization and
  can drop a licence's billing rows entirely
* **Idle now means "billed in the selected month, consumed no credits in it"**, computed from
  that month's rows alone, so a past month's answer does not drift as new data lands
* Tier and utilization figures now evaluate over the calendar month rather than per day
* Per-model credit columns removed from the Model & Client pivot, and per-row credit ratios
  replaced with monthly ones. The report writes a user's daily credit total on a single one of
  their model rows, so averaging understated the figure roughly 2.4x and the columns reported
  only which row carried the total
* `Top 50 Users by Message Count` now ranks on summed messages, not row count, which previously
  ranked users by how many models they had touched
* Widget explanations moved from visual subtitles into per-sheet text boxes

### Fixed

* **`kiro_cur_view` double-counted cost.** Its email lookup emitted two rows for any subscriber
  that had ever reported two email addresses, fanning out every CUR row it joined to
* `user_email` is now resolved per subscriber. The source does not always populate it, which
  split one person into two identities and produced a blank entry in the User control
* `Last Activity` rendered as `yyyy-03-Th` instead of a date
* Conditional formatting used `> 0.75` where the at-risk flag uses `>= 0.75`, so a user at
  exactly 75% was counted but not highlighted
* Account and User controls now also scope the subscription widgets

## Kiro User Activity Dashboard v1.0.0

* Initial release. Four sheets: Executive Summary, User Engagement, Credit & Overage Tracking,
  Client Type Breakdown
* KPIs for active users, messages, credits used and overage credits; daily trends by client
  type; credits by subscription tier; per-user activity and overage detail
* Subscription tier credit allowances built in (Free 50, Pro 1000, Pro+ 2000, Power 10000)
* Single SPICE dataset on a daily refresh, collected via the `kiro-user-activity` module in the
  CID Data Collection framework
