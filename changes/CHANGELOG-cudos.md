# What's new in CUDOS Dashboard

# CUDOS v5

# CUDOS - 5.6.1

* **Executive: Trends**: Fix sort order in Amortized Cost by Account visual. Excluded unused RI and SP costs in the current month.
* **Databases**: Excluded unused RI costs in the current month.

# CUDOS - 5.6

* **Executive: Trends**: Sheet 'MoM Trends' renamed to 'Trends'. Added control Trends Type which allows to switch between Monthly, Weekly and Daily Trends. Added control Trends Time Range which allows to adjust a time period when switching between Trend Types. Changed positioning of Product and Account Trends visuals to effectively present more than 3 trends periods. 
* **Databases**: Added 'Amazon ElastiCache Redis Migration to Valkey - Adoption Tracking and Potential Savings' section with visuals 'ElastiCache Redis vs Valkey: Cost Breakdown', 'ElastiCache Valkey: Achieved Cost Efficiency Gains vs Redis', 'Monthly Cost Savings Opportunities with Migration to Valkey' and 'Redis Cache Instances and Serverlless Cache: Cost Savings Opportunities with migration to Valkey' to track migration progress to Valkey.
* **Taxonomy Explorer**: 'TAGSplorer sheet complexly redesigned and renamed to Taxonomy Explorer. Now it allows to interactively explore cost and usage details across 2 Taxonomy Dimensions such as tags and account level mappings which added to the dashboard via new [Add Organizational Taxonomy](https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/add-org-taxonomy.html) capability. 
* **AI/ML**: Added 'Amazon Bedrock Model Unit Cost' and 'Amazon Bedrock Cost per Legal Entity' visuals . Updated Bedrock UsageType Group for more accurate usage type categorization. Added Model Name calculated field to group different model ids under the same name on 'Amazon Bedrock Daily Cost per Resource' visual.
* **Amazon S3 Summary**: Added 'Group By' control which allows to switch between S3 Operation and S3 UsageType Group in 'Total Cost per Bucket by Operation', 'Daily Cost Bucket Explorer' and 'Daily Storage Bucket Explorer' visuals.
* **Analytics**: Added new section AWS Glue with 'AWS Glue Spend per Account','AWS Glue Spend per UsageType Group', 'AWS Glue Spend per Usage Type' and 'AWS Glue Detailed Resource View'  visuals.
* **Analytics**: Added new section Amazon Elastic MapReduce (EMR) with 'EMR Spend per Account', 'EMR Spend Per UsageType Group', 'EMR Spend Per UsageType Sub Group', 'EMR Spend Per Processor Type' 'EMR Daily Cost per Usage Type Sub Group' and 'EMR Detailed Resource View (Top 20)' visuals.
* **Executive: RI/SP Summary**: Added Amazon ElastiCache, OpenSearch, Redshift, DynamoDB and MemoryDB services into 'Savings Option for Compute by Owner / Consumer' visual

# CUDOS - 5.5

* **DynamoDB**: Refactored visuals to improve user experience and simplify navigation. *'DynamoDB Accounts by Category'* visuals replaced with 'DynamoDB Cost per Account' and 'DynamoDB Cost per Usage Type Group'
* **DynamoDB**: New section *'DynamoDB Provisioned Capacity - Reservations Savings Estimation'* which allows to calculate estimated savings for Amazon DynamoDB reserved capacity based on custom commitment amount.
* **DynamoDB**: New section 'DynamoDB Provisioned Capacity - Reservation Coverage & Usage Monitoring' which allows monitor reserved capacity coverage per region | capacity type dimensions
* **DynamoDB**: New visual *'Infrequent Access Tables Cost and Cost Efficiency Gain/Loss vs Standard Storage'* which shows efficiency gains from Infrequent Access tables and also tables which could be migrated to Standard table class
* **Monitoring and Observability**: New section 'AWS Config Periodic Recording Savings Opportunities' which shows potential savings from migration to periodic configuration item recording
* **Monitoring and Observability**: New section *'Account and Regions without AWS Config'* which allows to identify account and regions with AWS service usage and where AWS Config is not enabled
* **Analytics**: Improved *'QuickSight Usage Type Group'* calculated field to accommodate the latest QuickSight pricing constructs
* **AI/ML**: Added Guardrails to the *'Bedrock UsageType Group'* calculated field
* **Databases**: Updated action filter on *'RI Coverage per region | engine | instance type or family'* to be applied to *'Top 20 instances'* allowing to focus on resources which belong to particular RI dimension
* **Compute**: Bug fix for *'EKS Extended Support Cost per Account'* visual. Added missing filter for Last 30 days.

 

# CUDOS - 5.4
* **[new tab] Security**: Introducing Security tab with cost and usage details for Security services. New tab includes visuals 'Security Spend per Service', 'Security Spend per Account' and respective detailed view sections for Amazon Cognito and Amazon GuardDuty
* **Security**: New Amazon Cognito section with visuals 'Amazon Cognito Spend and Projected Cost for M2M App Clients and Tokens', 'Amazon Cognito Spend and Projected Cost for M2M App Clients and Tokens per Account' and 'Amazon Cognito Detailed View'
* **Security**: New Amazon GuardDuty section with visuals 'Amazon GuardDuty Spend per UsageType', 'Amazon GuardDuty Spend per Account' and 'Accounts and Regions where Amazon GuardDuty is not enabled'
* **Databases**: Bugfix for Storage Type categorization on 'Daily Storage Cost'
* **DynamoDB**: Improved 'TOP 15 Candidates for Infrequent Access Tables Last 30 Days' visual to exclude tables after they migrated to Infrequent Access

# CUDOS - 5.3

* **AI/ML:** Added section 'Amazon Q' which provide insights into Amazon Q spend and usage details with visuals 'Amazon Q spend trend', 'Amazon Q Index usage trend', 'Amazon Q spend per Account', 'Amazon Q spend per Region', 'Amazon Q spend per Operation', 'Amazon Q Index Daily Unit Usage per Account'
* **AI/ML:** Added visual 'SageMaker ML instance Hourly Spend by Purchase Option'. 'SageMaker Spend and Unit Cost by Instance Type' changed to 'SageMaker Spend and Unit Cost by Instance Family' with ability to drill down to Instance Type
* **AI/ML:** Amazon Bedrock spend per UsageType Group now categorizes Bedrock spend by Image Generation, Input Tokes, Output Tokens, Model Units and Model Customization
* **Storage & Backup:** Sheet 'Storage' renamed to 'Storage & Backup'. Added section Backup with visuals to track cost and usage insights for backup storage and snapshots, namely 'Backup spend per Service', 'Backup spend per Account', 'Backup spend per Region', 'Backup spend per Usage Type', 'Daily Backup spend per Service', 'Daily Backup Spend per Resource' and 'Backup Spend Detailed Resource View'
* **Compute:** New section EC2 Compute Elasticity with visuals which show % of usage time per instance over last 30 days to identify instances and accounts which could be scheduled to stop when they are not needed, namely 'Average EC2 Instance Usage time % and Cost per Account', 'EC2 Hourly Cost', 'EC2 Instances Usage time % and Cost' and 'EC2 Instances Daily Usage in Hours'
* **Compute:** New visual 'Lambda Spend per Processor Architecture' to track adoption of Arm/Graviton 2 based Lambda functions.
* **Compute:** New EKS Extended Support section with visuals 'EKS Extended Support Cost', 'EKS Extended Support Cost per Account', 'EKS Clusters with Extended Support'
* **Databases:** New Section Databases Elasticity with visuals which show % of usage time per resource over last 30 days to identify instances and accounts which could be scheduled to stop when they are not needed, namely 'Databases Average Usage time % and Cost', 'Databases Hourly Usage', 'Databases Usage time % and Cost per Resource', 'Databases Instances Daily Usage in Hours'
* **Databases:** Added Amazon MemoryDB spend and usage to the existing visuals
* **Monitoring and Observability:** 'CloudTrail Usage Cost by Usage Type' now groups by usage type without region prefix to simplify CloudTrail cost tracking  

# CUDOS - 5.2

* **Databases:** New RDS Extended Support section with 'RDS Extended Support Cost', 'RDS Extended Support Cost per Account' and 'RDS Extended Support Cost per Resource' visuals
* **MoM Trends**: New control MoM Comparison Type which allows to switch between Calendar Month (default) and Normalized Month. With Normalized Month every month is normalized to amount of days in the prevois month for more accurate MoM comparison. Normalized Month = average daily cost for the month * amount of days in previous month
* **Data Transfer & Networking:** New Data Transfer Type NAT Gateway Data Processed which allows to see cost and usage for data processed by NAT Gateways.
* **Amazon S3:** Added tooltip with usage amount and pricing unit to 'Total Cost per Bucket by Operation' visual
* **Compute:** New visual 'EC2 Spot Savings per Account' 


# CUDOS - 5.1.1

**Changes**:
* **Databases:** Fix for Normalized Instance Hours not shown on RI Coverage and Daily Elasticity visuals.

# CUDOS - 5.1.0
**Important!** If you have CUDOS v5.0.0 deployed you can use [standard update process](https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/update-dashboards.html). If you have previous version deployed follow [this guide](https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/faq.html#how-to-update-to-cudos-v5-if-i-have-previous-version-of-cudos-installed) to deploy CUDOS v5. You can check version of your currently deployed dashboard on the About tab.

**Changes**
* **Executive: Billing Summary**: Added control to group Invoice and Amortized Spend by Payer Account, Charge Type Group and Service Category from [FOCUS](https://focus.finops.org/#specification) specification.
* **Executive: Billing Summary**: Added top level filter control by Charge Type Group.
* **Executive: MoM Trends**: Added top level filter control by Charge Type Group.
* **[new tab] Analytics**: Introducing Analytics tab with visuals for Amazon QuickSight: 'QuickSight Spend per Account', 'QuickSight Spend per Usage Type Group', 'QuickSight Author Users historical Usage and Cost', 'QuickSight Reader Users Sessions Usage and Cost','QuickSight SPICE historical Usage (GB) and Cost', 'QuickSight Users Detailed View' and others.
* **Amazon S3**: Added S3 Insights Explorer section which allows to focus on cost and usage of different S3 features and associated optimization opportunities such as 'Cross Region Replication Data Transfer', 'S3 Inventory', 'S3 Replication Time Control' and others.
* **Amazon S3**: Added 'Amazon S3 Total Cost by Region' visual. Adjusted position of 'Total Cost per Bucket by Operation (Top 20)' visual and added drill down to usage type.
* **Monitoring and Observability**: CloudWatch Usage Cost per Usage Type visual changed to show Usage Type Sub Group allowing to drill down to Usage Type. Added recommendation 'Consider using Infrequent Access log class at 50 percent lower per GB ingestion price'
* **Databases**: Added Usage and Usage Unit tooltips to the 'RI Coverage per region | engine | instance type or family' visual.
* **Storage**: Changed 'Volume Explorer: TOP 50 EBS Volume Details' filter from previous month to last 30 days.

# CUDOS - 5.0.0
**Important!** Learn more about CUDOS v5 and how to deploy in our [FAQ](https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/faq.html#cudos-v5-faqs) page.

**Changes**
* Re-designed dataset structure. All datasets used by CUDOS v5 use fast QuickSight SPICE storage which reduces time required to load visuals. CUDOS v5 is using 3 datasets:
    1. **summary_view** with historical data for last 7 months (by default) and with daily granularity for latest 3 months without resource details. See source code [here](https://github.com/aws-samples/aws-cudos-framework-deployment/blob/cudos_v5_cfn_changes/cid/builtin/core/data/queries/cid/summary_view_sp_ri.sql)
    2. **resource_view** with cost and usage details for every resource for last 30 days with daily granularity. See source code [here](https://github.com/aws-samples/aws-cudos-framework-deployment/blob/main/cid/builtin/core/data/queries/cudos/resource_view_sp_ri.sql)
    3. **hourly_view** with hourly granularity for last 30 days without resource level details. See source code [here](https://github.com/aws-samples/aws-cudos-framework-deployment/blob/cudos_v5_cfn_changes/cid/builtin/core/data/queries/cudos/hourly_view_sp_ri.sql)
    Datasets **customer_all**, **ec2_running_cost** and **savings_plans_eligible_spend** are not used by CUDOS v5. 
* **Executive: MoM Trends**: Introducing Service Category from [FOCUS](https://focus.finops.org/#specification) specification. Added visual '*Amortized Cost by Service Category*' with AWS services categorization according to FOCUS.
* **Executive: MoM Trends**: Added '*Top 10 resources daily spend*' and '*Top 20 resources Cost and Usage*' which show top resources for selected dimentions on any other visual on the 'MoM Trends' tab.
* **AI/ML**: Added Amazon Bedrock section with detailed visuals including Amazon Bedrock spend by Account, by Region, by Pricing Model, by UsageType Group and '*Bedrock Daily Cost per Resource (Model)*' and '*Bedrock Detailed Resource View*'.
* **AI/ML**: Improved Usage Type Group on *'SageMaker spend per Usage Type Group'* visual to include SageMaker Canvas, Batch Transform and Asynchronous Inference
* **Databases**: *'Daily Storage Cost'* visual now includes category for RDS Aurora I/O Optimized Storage. Improved date formatting to show day month and year in *'Daily Cost by Instance Family|Instance Type|Processor Type for All*' visual
* **All tabs**: Improved styling in the table and pivot visuals

# CUDOS v4 - Deprecated

## CUDOS - 4.79
* **Data Transfer and Networking**: Added 'Public IPv4 addresses' section with 'Public IPv4 Cost and Projection (Last 30 days)', 'Public IPv4 Cost and Projection per Account (Last 30 days)' and 'Public IPv4 ENIs and Elastic IPs used more than 1 hr (Last 30 days)' visuals allowing to estimate cost impact for [Public IPv4 charges effective from February 1, 2024](https://aws.amazon.com/blogs/aws/new-aws-public-ipv4-address-charge-public-ip-insights/) and monitor cost of idle Elastic IP addresses 
* **Compute**: Added section 'Amazon EC2 Spot Instances Savings' with best practices recommendations for Spot tools and usage optimization. Added visual 'EC2 Spot Savings Detailed View' which provides breakdown of Spot savings per platform, instance type, region and availability zone
* **Compute**: Fix for 'TOP 20 "ECS Fargate", "EKS Fargate" and "EKS Control Plane" cost' visual 
* **Compute**: Removed 'EC2 Coverage in Normalized Hours (Last 30 days)' as it duplicates details available in 'EC2 Compute Unit Cost and Normalized Hours by Purchase Option' visual excluding empty resource ids
* **Databases** and **Monitoring and Observability**: Added references to [Cloud Financial Framework](https://catalog.workshops.aws/awscff/en-US) guidances in respective recommendations sections

## CUDOS - 4.78.1
* **Executive: Billing Summary**: Renamed label for Total bar to Amortized cost on *'Total Savings and Discounts details'* visual

## CUDOS - 4.78.0
* **Executive: Billing Summary**: Added '*Total Savings and Discounts details*' and '*Total Savings and Discounts, %*' visuals showing SP, RI, Spot savings, Refunds, Credits and Discounts in one place. '*Discounts Previous Month*' visual now shows also SP, RI, Spot savings and renamed to '*Savings and Discounts Previous Month: RI SP Savings, Spot Savings, Credits, Refunds, Others*'
* **Messaging and Streaming** (formerly Message Brokers tab): Added visual '*Idle Amazon Kinesis Data Streams and Consumers*' which lists all idle Amazon Kinesis Data Streams and Consumers. Added breakdown by operation to '*TOP 20 Amazon Kinesis resources*' visual
* **Executive: RI/SP Summary**: Added SP Commitment per Hour and SP Unused Commitment per Hour columns to '*Reserved Instance & Savings Plan Tracker*'
* **Amazon DynamoDB**: '*Total DynamoDB Usage Cost Per Operation*' visual now includes RI covered capacity and shows breakdown by DynamoDB categories and renamed to '*DynamoDB Cost Per Category*'
* **Amazon S3**: '*Daily Storage Bucket Explorer*' and '*Daily Cost Bucket Explorer*' visuals switched from previous 3 month filters to last 90 days to capture data from current month
* **Data Transfer**: Daily visuals '*Data Transfer GB per Service*' and '*Data Transfer Daily GB per Operations*' switched from previous 3 month filters to last 90 days to capture data from current month. Added ability to drill down to operation and usage type for '*Data Transfer Costs per Type*' and '*Data Transfer GB per Service*' visuals

## CUDOS - 4.77.0

* OPTICS Explorer: Added 'Forecast Spend' visual allowing to forecast AWS spend by any dimension available in top level filters e.g. by particular Service, Operation, Region, Account etc.
* Amazon S3: Fix for storage calculation in 'Daily Storage Bucket Explorer' visual
* Data Transfer: Fix to exclude Direct Connect hourly charges from data transfer amount calculations
* Storage: Fix to filter out empty values in 'TOP 20 EBS Volumes Previous Month' and 'TOP 20 EBS Snapshots Previous Month'

## CUDOS - 4.76.1

* Storage : Fix for 'EBS Storage Unit Cost' visual not showing data
* Amazon S3: Added account tooltip to 'Total Cost per Bucket Previous Month by Operation' visual

## CUDOS - 4.76.0

* Databases and Compute: Added ability to switch between Instance Hours, Normalized Hours and Cost with the top level control 'Usage Unit’. Normalized Hours allows you to get more precise tracking of your usage taking into account size of the instances. Learn more about normalized hours in recommendations sections of each tab
* Databases: Service Select control changed to multi-select allowing to focus on multiple services at once
* Databases: New visuals 'RI Coverage per region | engine | instance type or family (for size flexible), 'Daily RI coverage' (with ability to set RI coverage goal for the reference), 'Total RI Coverage %'. These new visuals and also 'Database Daily Elasticity' and 'Daily Cost' visuals can be switched between Instance Hours, Normalized Hours and Cost with 'Usage Unit' control
* Databases: Added ability to switch between Instance Family, Instance Type and Processor type with 'Group By' control 
* Databases: 'Cost by Database Engines' visuals shows engines for all database services
* Compute: 'EC2 Compute Unit Cost', 'EC2 Coverage by Purchase Option', 'EC2 Hours by Platform', 'EC2 Coverage (Last 30 days)', 'EC2 Daily Compute Unit Cost by Purchase Option' visuals can be switched between nstance Hours, Normalized Hours and Cost with 'Usage Unit' top level control
* Compute: Added ability to switch between Instance Family, Instance Type and Processor type with 'Group By' control 
* Compute: New visual 'EC2 Daily Compute Unit Cost and Usage'
* Data Transfer: Update in Data Transfer types categorization logic to capture S3 to Direct Connect data transfer under 'Region to Direct Connect' category


## CUDOS - 4.75

* Databases: Added 'Daily Storage Cost' visual which shows detailed storage cost breakdown and can be used to track GP3 volumes adoption for Amazon RDS and OpenSearch 
* MoM Trends: Added Payer Account to 'AWS Marketplace spend detailed view'
* Data Transfer Summary: New visual 'Single-AZ VPC endpoints' which helps to asses reliability risks and consider to deploy production VPC endpoints in a several Availability Zones
* Data Transfer Summary: Data Transfer types categorization logic so you have more precise breakdown of different data transfer types


## CUDOS - 4.74

* MoM Trends: Added AWS Marketplace section with visuals 'AWS Marketplace spend by Legal Entity' and 'AWS Marketplace spend detailed view'
* MoM Trends: Added Control to filter by Legal Entity
* Monitoring and Observability: Merged visuals 'TOP 10 CloudWatch Resources Previous Month' and 'TOP 10 CloudWatch Log Resources Previous Month' into 'TOP 10 CloudWatch Resources' and changed time interval to last 30 days
* Monitoring and Observability: Added 'TOP 10 CloudWatch Resources Daily Cost' visual
* OPTICS Explorer: Added ability to filter by 'Billing Entity' and 'Legal Entity'
* Data Transfer Summary: S3 Transfer Acceleration added to Region to Internet data transfer type


## CUDOS - 4.73

* Compute Summary: 'Unused On-Demand Capacity Reservations Cost per Account' changed from monthly to daily aggregation for more granular visibility 
* Billing Summary: Drill down to description on 'Discounts: Credits, Refunds, Others' now grouped by discount type 
* End User Computing: Adjusted 'Cost Optimizer for Amazon WorkSpaces' solution name in Recommendations section

## CUDOS - 4.72

* Databases: Added ability to vizualise spend and usage for Amazon Neptune
* Compute Summary: Added insights and recommendations to AWS Lambda Summary section with the links to respective documentation
* RI/SP Summary: Added Cost line to ‘Avg. Hourly EC2 Cost by Pricing Model’ which allows to get total EC2 compute cost per hour


## CUDOS - 4.71

* Compute Summary: Changed  'On-Demand Capacity Reservations Usage Hours' visual to show data for Last 30 days instead of previous month so it displays current inventory and utilization of On-Demand Capacity Reservations
* Monitoring and Observability: Added CloudWatch Synthetics and CloudWatch Metrics Steams to 'CloudWatch Usage Cost per Usage Type Group' visual
* Monitoring and Observability: Added pricing unit and usage amount to 'TOP 10 CloudWatch Resources Previous Month' and 'TOP 10 CloudWatch Log Resources Previous Month' visuals


## CUDOS - 4.70

* Compute Summary: Added visuals 'Unused On-Demand Capacity Reservations Cost per Account' and 'On-Demand Capacity Reservations Usage Hours (Previous Month)'
* Compute Summary: Combined visuals 'EC2 Instance Type Running Hours Cost' and 'EC2 Family Running Hours Cost' into 'EC2 Running Hours Cost by Purchase Option' and added ability to switch between Instance Family and Instance Type with 'Group By' control 
* Compute Summary: Top 3 previous generation instances types Focus Area extended to top 5 added ability to switch between Instance Family and Instance Type with 'Group By' control 

## CUDOS - 4.69.1

* End User Computing: Added detailed description for Storage and Application Bundles in 'Workspaces spend per Bundle' and 'WorkSpaces Cost and Usage Top 20' vizuals

## CUDOS - 4.69

* Compute Summary: Added ability to choose service (ECS or EKS) for AWS Fargate visuals
* Compute Summary: 'TOP 20 Fargate Clusters' visual renamed to 'TOP 20 "ECS Fargate", "EKS Fargate" and "EKS Control Plane" cost' and shows corresponding Fargate and EKS Cluster Control plane charges
* End User Computing: Added 'Workspaces Count per Running Mode', 'Average Cost per WorkSpace','Average Usage for AutoStop WorkSpaces (hours)','Workspaces Count per Region','Workspaces spend per Bundle' and 'Average WorkSpaces cost per Region' visuals
* End User Computing: Added Bundle description to 'WorkSpaces Cost and Usage Top 20' and 'Workspaces AutoStop with monthly usage over 80 hours' visuals


## CUDOS - 4.68.1

* RI/SP Summary: Fix filters for "Amortised Spend by Purchase Option and Savings (Potential Savings Eligible Workloads Only)" vizual to exclude services which can't be covered by RIs or SPs 

## CUDOS - 4.68

* Databases: Replaced "Database Instance Daily Elasticity by Purchase Option" with "Database Instance Hours Daily Elasticity by Purchase Option" and added a unit metric to the visual 

## CUDOS - 4.67.3

* MoM Trends: Updated charge_type filter

## CUDOS - 4.67.2 

* Storage Summary: Filters applied to daily visuals changed to last 90 days fron last 3 months
* Payer and Account Name/ID controls now show only relevant values for respective control
* OPTICS Explorer: Relative dates filter instead of Start/End dates

## CUDOS - 4.67.1 

* Amazon S3: Minor calculation accuracy fix for "TOP 5 Accounts for Glacier Vault migration to Glacier Deep Archive Savings Opportunity" via "S3 Glacier Deep Archive Estimated Cost" calculated field

## CUDOS - 4.67

* Monitoring and Observability: New visual "TOP 10 CloudWatch Log Resources Previous Month"; Various improvements to actions on cloudwatch visuals; removed Operation from "TOP 10 CloudWatch Resources Previous Month" visual

## CUDOS - 4.66.3

* Monitoring and Observability: New recommendations

## CUDOS - 4.66.1

* MoM Trends: Removed billing entity field from visual "MoM Trends by Product Code (AWS Marketplace items use product_name)", control left as is

## CUDOS - 4.66

* MoM Trends: Additional visibility and control in to billing entity origin AWS vs AWS Marketplace

## CUDOS - 4.65

* Data Transfer: Amazon CloudFront is now a separate Data Transfer Type category; New multi-select list for Data Transfer Types

## CUDOS - 4.64.2

* MoM Trends: Updated filters on all MoM trends visuals to match edge cases for Refunds and Credits 

## CUDOS - 4.64.1

* Databases: Updated filters on all Database visuals to match edge cases for Refunds and Credits 

## CUDOS - 4.64

* Compute Summary: Excluded charges for Dedicated Hosts from EC2 instance family and instance types visuals
* Compute Summary: New visual ‘EC2 Spot Savings’
* End User Computing: Updated recommendations for Workspaces Cost Optimizer solution new capability to delete unused workspaces which will reduce costs for customers by terminating the workspaces which have not been used for a month

## CUDOS - 4.63.4

* Databases: Fixed a filter on "Database Instance Daily Elasticity by Purchase Option" to include OpenSearch Instances

## CUDOS - 4.63.3

* GameTech: Renamed "GameLift OnDemand Instance Types"(Monthly) to GameLift Costs by Instance Types; renamed "GameLift OnDemand Instance Types"(Daily) to "GameLift Instance Types Last 90 Days" and adjusted filters

## CUDOS - 4.63.2

* DynamoDB: Including replication costs now in “TOP 15 Candidates for IA Tables Previous Month”

## CUDOS - 4.63.1

* DynamoDB: New visual “TOP 15 Candidates for IA Tables Previous Month”
* DynamoDB: Renamed “Ondemand” to “Commited Throughput” on “DynamoDB Provisioned Capacity Hourly Usage by Purchase Option Last 30 Days” visual

## CUDOS - 4.62

* DynamoDB: “Total Reservations Cost Previous Month” renamed to “Total Reservations Savings Previous Month” and now shows reservation type breakdown
* DynamoDB: New recommendations for IA Table Class
* DynamoDB: Renamed “DynamoDB MoM Unused Cost” to “DynamoDB MoM Reservations Unused Cost” and added additional controls for more fine grained visualisation

## CUDOS - 4.61.3

* Storage Summary: Renaming "EBS Unit Cost" to "EBS Storage Unit Cost"

## CUDOS - 4.61.2

* RI/SP Summary: "Savings Option for Compute by Owner / Consumer Previous Month" now applies "Amazon Elastic Container Service filter" 

## CUDOS - 4.61.1

* DynamoDB: Renamed Visual "DynamoDB PayPerRequestThroughput Usage and Cost Last 3 Months" to "DynamoDB PayPerRequestThroughput(OnDemand) Usage and Cost Last 90 Days" and reapplied filters
 
## CUDOS - 4.61

* S3: New visual TOP 5 Accounts for Glacier Vault migration to Glacier Deep Archive Savings Opportunity 

## CUDOS - 4.60.5

* Databases: Fixed daily filters to last 90 days and updated visual names. 

## CUDOS - 4.60.4

* About: Updated the About tab. 

## CUDOS - 4.60.3

* DataTransfer: Added classification "Other" for all unclassified datatransfer. 

## CUDOS - 4.60.2

* Compute: EC2 Family Running Hours Cost Last 90 Days visual now has a focus area controll on the right to filter in or out diferent purchase options

## CUDOS - 4.60.1

* MoM Trends: Aligned charge type filters on all visuals 

## CUDOS - 4.60

* Databases: Elasticsearch renamed to OpenSearch
* End User Computing: WorkSpaces Autostop visuals filter on the last 30 days now, instead of the previous month
* DataTransfer: Resource level visuals filter on the last 30 days now, instead of the previous month
* MoM Trends: Added MoM difference in absolute values (MoM $), now its possible to sort by all columns

## CUDOS - 4.59.1

* DynamoDB: PITR recommendation now has a link to the AWS documentation. 

## CUDOS - 4.59

* Compute Summary: New visual "TOP 10 Accounts by EC2 CPU Credit Usage Cost" and related recommendation. 

## CUDOS - 4.58

* RI/SP Summary: Renamed Previous Month Savings → Previous Month Net Savings; Renamed Usage Saving by Pricing Model and Service → Usage Net Saving by Pricing Model and Service; Now these two visuals show NET savings with respect to unutilized savings, this affects customers with unitilized ri/sp.

## CUDOS - 4.57

* Sheet Visit Anonymous Statistics. We added this little icon 📊 to each sheet to better prioritise new visuals and features for CUDOS Dashboard based on the sheet popularity. No PII data is collected, we are querying frequency of the icon download from the CloudFront logs of the CloudFront distribution where the icon is stored. If you would like to opt-out of this anonymous statistics collection, please save your dashboard as analysis and remove the icon from the text blocks 

## CUDOS - 4.56.1

* RI/SP Summary: New recommendation on Budgets
* MoM Trends: New recommendation on AWS Cost Anomaly Detection
* Storage Sumary: Renamed "TOP EBS Spend Accounts Previous Month" to "TOP EBS Spend Accounts Last 3 Months"

## CUDOS - 4.56

* Compute/Media/Message Brokers: TOP Visuals for Lambda, Fargate Clusters, Kinesis, Elemental now have 20 and 5 distinct filters delivering better granularity in visualisation, and visualise last 30 days
* DynamoDB: TOP 15 Dynamo DB Tables Previous Month Usage Cost now visualises last 30 days, added account what the table belongs to and now has an action filter to Daily Cost per Table Previous Month
* DynamoDB: DynamoDB Period over period was missing a charge_type filter, fixed

## CUDOS - 4.55
* Data Transfer now includes only chargable DTO. Example: Excluding S3, ALB -> CloudFront, as it is not charged.

## CUDOS - 4.54
* Scale optimised for smaller screens

## CUDOS - 4.53.2
* S3: Daily Storage Bucket Explorer now visualises total bucket usage amount

## CUDOS - 4.53
* DynamoDB: Cost breakdown per account is now visualised in categories, OnDemand = PayPerRequest, Commited Throughput is CommitedThroughput that is not covered by Reservations

## CUDOS - 4.52
* Billing Summary: Discounts now show positive value; New visual showing discounts and their type MoM

## CUDOS - 4.51.2
* Storage Summary: Fixed a type on the EBS Explorer filter

## CUDOS - 4.51.1
* RI/SP Summary: Removed a filter on a few visuals that was breaking on filtering by account ids

## CUDOS - 4.51
* AI/ML: Textract and Rekognition visuals

## CUDOS - 4.50.1
* AI/ML: SageMaker Savings Plan recommendations

## CUDOS - 4.50
* Compute Summary: EC2 Running Hours by Platform Last 3 Month introducing OS details
* Compute Summary: TOP 20 Lambdas Previous Month Account in Tooltip

## CUDOS - 4.49
* Compute Summary: Renamed “EC2 Coverage MoM (Cost)” to EC2 Running Hours Cost by Purchase Option
* Storage Summary: TOP 20 EBS Volumes Previous Month EBS Volumes and Snapshots have dedicated visuals
* Data Transfer: VPC Endpoint Utilisation
* S3: Top 10 Buckets MoM now uses single visual
* GameTech and Media: Elemental MediaConvert Elasticity by purchase option

## CUDOS - 4.48.3
* Message Brokers: Kinesis best practice recommendations
* Databases: *6g instance type migration considerations
* GameTech & Media: +Tooltip over TOP 20 resource IDs

## CUDOS - 4.48
* Message Brokers: +Amazon MSK Visuals

## CUDOS - 4.47.1
* Billing Summary: TOP 10 accounts spend now includes Marketplace charges
