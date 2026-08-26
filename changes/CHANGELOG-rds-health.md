# What's new in the RDS Health Dashboard
The RDS Health Dashboard provides comprehensive RDS instance analysis across multi-account AWS environments, helping teams track version compliance, backup coverage, end-of-support risks, and cost optimization opportunities.

## Key Features and Capabilities
1. **Version Compliance Tracking**: Real-time comparison of current engine versions against latest available versions with automatic upgrade path identification
2. **End-of-Support Detection**: Proactive alerts for databases approaching or past AWS end-of-support dates with timeline visualization
3. **Backup Coverage Monitoring**: Daily and 7-day backup compliance tracking with certificate backup status and cluster snapshot analysis
4. **Maintenance Action Tracking**: Centralized view of pending maintenance actions across all accounts and regions
5. **Aurora I/O Optimized Recommendations**: Cost analysis identifying clusters where switching to I/O Optimized configuration saves money (>25% IO cost threshold)
6. **Security & Resiliency Insights**: Encryption status, Multi-AZ configuration, storage autoscaling, and certificate expiration tracking
7. **Multi-Account Cost Analysis**: RDS spending breakdown by account, region, engine, and instance type with processor type (Graviton vs x86) identification
8. **Dynamic Filtering**: Interactive parameter controls for date range, account, region, engine, and custom tag-based filtering

## Technical Highlights
- **Multi-account Support**: Cross-account data collection via AssumeRole with organization-wide visibility
- **Automated Data Pipeline**: EventBridge-triggered Step Functions orchestrating Lambda → Crawler workflows
- **Efficient Storage**: Partitioned S3 data (account/region/date) with Glue crawlers for automatic schema updates
- **CID Framework Integration**: Seamless deployment via cid-cmd with standard CID patterns and naming conventions

## RDS Health Dashboard - v1.0.0
* Initial release
* 5 tabs: Inventory, Version & Maintenance, Security & Resiliency, Backup, Cost
* 30 KPI visuals, 24 tables, 12 bar charts, 11 gauges, 10 insights, 4 pie charts
* Supports Aurora (MySQL/PostgreSQL), RDS (MySQL, PostgreSQL, MariaDB, SQL Server, Oracle)
* Tested with 110+ instances across 3 accounts × 5 regions
