# What's new in the Service Quotas Dashboard

## v1.0.0
* Initial release.
* Overview: quotas, services, regions and accounts in scope; adjustable vs non-adjustable and global vs regional quotas; quota count by service (logarithmic axis, so services with a few dozen quotas stay visible next to those with thousands) and by region, one color per bar; quotas raised above the AWS default.
* Service Quota Utilization High Level Overview: one bar per quota, account and region (`service: quota name | quota code | account | region`), highest utilization first, on a fixed 0–100% scale with fixed colors (green below 20%, amber 20–80%, red 80% and above). Number of quotas to show: 250 by default, up to 1,000 (the most a QuickSight bar chart shows). Filters for minimum utilization and risk level.
* Adjusted Service Quotas Breakdown: quotas whose current value differs from the AWS default, with a Show control (Adjusted only by default, or All quotas).
* Quota Comparison: compare quota limits in four ways: two accounts region by region; one account against the AWS default (Raised / Default / Below default); two regions of one account (e.g. primary vs DR) with Region A and Region B; two accounts in different regions. Status per quota, highlighting of differences, Differences only / All, Service filter.
* When a quota is in the data for one side only, the other side shows "Not in scope" and Difference shows "–": the quota wasn't collected there, so its limit is unknown, not zero.
* About tab: how each view works, the comparison cases, every status and the fields.
