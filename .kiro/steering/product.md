# Cloud Intelligence Dashboards (CUDOS Framework)

## Product Overview
The Cloud Intelligence Dashboards is an open-source framework that provides AWS customers with cost and usage insights through 20+ pre-built dashboards. It's designed to help organizations achieve financial accountability, optimize costs, track usage goals, and implement governance best practices.

## Key Components
- **Foundational Dashboards**: Core dashboards requiring only Cost and Usage Report (CUR)
- **Advanced Dashboards**: Enhanced dashboards requiring CID Data Collection and CUR
- **Additional Dashboards**: Specialized dashboards for specific use cases

## Target Users
- FinOps teams (primary owners)
- Executives
- Engineers
- Cost optimization specialists

## Architecture
Built on AWS services including S3, Athena, Glue, QuickSight, and Lambda. Supports both foundational architecture (CUR-only) and advanced architecture (with data collection from multiple AWS services).

## Deployment Methods
1. Command-line tool (`cid-cmd`)
2. CloudFormation templates (recommended)
3. Both support deployment in under 30 minutes