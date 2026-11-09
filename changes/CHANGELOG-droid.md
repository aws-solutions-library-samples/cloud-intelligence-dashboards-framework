# What's new in the DROID Dashboard
The DROID Dashboard provides comprehensive RDS instance analysis across multi-account AWS environments, helping teams track version compliance, backup coverage, end-of-support risks, and cost optimization opportunities.

## Key Features and Capabilities
1. **Version Compliance Tracking**: Real-time comparison of current engine versions against latest available versions with automatic upgrade path identification
2. **End-of-Support Detection**: Proactive alerts for databases approaching or past AWS end-of-support dates with timeline visualization
3. **Backup Coverage Monitoring**: Daily, 14-day, and 30-day backup compliance tracking with certificate backup status and cluster snapshot analysis
4. **Maintenance Action Tracking**: Centralized view of pending maintenance actions across all accounts and regions
5. **Aurora I/O Optimized Recommendations**: Cost analysis identifying clusters where switching to I/O Optimized configuration is estimated to reduce cost, based on projected dollar savings (eliminated I/O charges net of higher compute and storage)
6. **Security and Resiliency Insights**: Encryption status, Multi-AZ configuration, storage autoscaling, and certificate expiration tracking
7. **Multi-Account Cost Analysis**: RDS spending breakdown by account, region, engine, and instance type with processor type (Graviton vs x86) identification
8. **Dynamic Filtering**: Interactive parameter controls for collection date, account, region, engine, and custom tag-based filtering

## Technical Highlights
- **Multi-account Support**: Cross-account data collection via AssumeRole with organization-wide visibility
- **Automated Data Pipeline**: EventBridge-triggered Step Functions orchestrating Lambda → Crawler workflows
- **Efficient Storage**: Partitioned S3 data (account/region/date) with Glue crawlers for automatic schema updates
- **CID Framework Integration**: Seamless deployment via cid-cmd with standard CID patterns and naming conventions

## DROID Dashboard - v1.0.1
**Important:** Update with `cid-cmd update --dashboard-id droid --force --recursive` to get the `rds_aurora_io_optimized_view` fix.
* **RDS Cost and Usage**: Aurora I/O-Optimized recommendations now also work on CUR Legacy through the CUR 2.0 proxy view (the region is read from `product['region']` instead of `product_region_code`)
* **RDS Security and Resiliency**: DB Certificate Status shows `Not Collected` when the certificate expiry date is not available, instead of `Expired`
* **RDS Snapshot Backup**: "Average Snapshot Backup % for past 30 days" and "Snapshot Backup % by Account - Last 14 Days" now include the current day, so they show data right after a collection

## DROID Dashboard - v1.0.0
* Initial release
* 5 tabs: Inventory, Version and Maintenance, Security and Resiliency, Backup, Cost
* Plus an About tab with dashboard information and credits
* 90 visuals: 29 KPIs, 24 tables (including 5 pivot tables), 12 bar charts, 4 combo charts, 1 scatter plot, 4 pie charts, 1 tree map, 7 gauges, and 8 insights
* Supports Aurora (MySQL/PostgreSQL), RDS (MySQL, PostgreSQL, MariaDB, SQL Server, Oracle, Db2)
* Tested with 110+ instances across 3 accounts × 5 regions
