# What's new in TAO Dashboard

## TAO Dashboard v4.0.0:
**Important:** Update to this version requires [CID Data Collection](https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/data-collection.html) v3.13.0 or above. Manual data collection is no longer supported. Before updating TAO dashboard to v4 make sure you [deployed](https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/data-collection-deployment.html) or [updated](https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/data-collection-update.html) Data Collection stacks. Once Data Collection is deployed you can update TAO dashboard to v4 by running these commands in your CloudShell (recommended) or other terminal:

```
pip3 install --upgrade cid-cmd
cid-cmd update --dashboard-id ta-organizational-view --recursive
```

**New tab TA Explorer** Trusted Advisor Explorer section which allows to interactively explore all flagged resources across all checks and accounts in a single place moved to separate tab

**New tab TA Priority** New visuals which show recommendations created by your account team via [Trusted Advisor Priority](https://docs.aws.amazon.com/awssupport/latest/user/trusted-advisor-priority.html)

**Cost Optimization tab:** Native Trusted Advisor checks Low Utilization Amazon EC2 Instances, Underutilized Amazon EBS Volumes, Amazon RDS Idle DB Instances, AWS Lambda over-provisioned functions for memory size replaced by respective more accurate checks from Cost Optimization Hub. Read more details in the [blog post](https://aws.amazon.com/blogs/aws-cloud-financial-management/optimize-your-aws-spend-with-new-cost-savings-features-in-aws-trusted-advisor/)

**Security tab:** AWS CloudTrail Logging check is replaced by AWS CloudTrail Management Event Logging check with improved accuracy

## TAO Dashboard v3.1.1:
**All tabs:** bugfix to exclude flagged resources which don't appear in latest report per account, e.g. exclude fixed resources

## TAO Dashboard v3.1.0:
**Important:** Update to this version requires cid-cmd v0.2.27. Please update cid-cmd first before updating the dashboard. During the update you'd need to provide a path to s3 folder where your Trusted Advisor data is stored. QuickSight dataset ta-organizational-view will be updated, please make a copy if you've made any customizations to the dataset. To update run these commands in your CloudShell (recommended) or other terminal:

```
python3 -m ensurepip --upgrade
pip3 install --upgrade cid-cmd
cid-cmd update --dashboard-id ta-organizational-view --recursive
```

**Changes:**

**Fault Tolerance tab:** Added historical trends for all checks

**Summary tab:** Added Trusted Advisor Explorer section which allows to interactively explore all flagged resources across all checks and accounts in a single place 

**Bug fixes and improvements:**:
 * Historical trends visuals across all tabs now include not only actual checks to better represent trends. 
 * Bug fix for detailed view visuals across all tabs to exclude duplicates in some corner cases 



## TAO Dashboard v2.0.0:

**Important:** Update to this version requires cid-cmd v0.2.12. Please update cid-cmd first before updating the dashboard. During the update you'd need to provide a path to s3 folder where your Trusted Advisor data is stored. QuickSight dataset ta-organizational-view will be updated, please make a copy if you've made any customizations to the dataset. To update run these commands in your CloudShell (recommended) or other terminal:

```
python3 -m ensurepip --upgrade
pip3 install --upgrade cid-cmd
cid-cmd update --dashboard-id ta-organizational-view --recursive
```

**Changes:**

**Performance Improvements:** Added Athena view to limit amount of data loaded to QuickSight SPICE and extracted heavy calculation fields from dashboard to dataset level   
**New tab Security Hub Checks:** Added visuals with detailed view for Security Hub checks delivered to AWS Trusted Advisor:
* Security Hub Flagged resources by month
* Security Hub Flagged resources by Top Accounts
* Security Hub Flagged Resources

**New tab AWS Well Architected reviews:** Added visuals with detailed view for AWS Well Architected reviews details delivered to AWS Trusted Advisor:
* Identified High Risk Items by Workload
* Identified High Risk Items by Category
* Well-Architected Reviews Detailed View

**Fault Tolerance tab**: Added visuals for Amazon ElastiCache and MemoryDB Multi-AZ clusters which alert customers when they're running in a Single-AZ configuration in order to improve fault tolerance, and enhanced availability of their Redis clusters.

**Cost Optimization tab**: fixed total calculation for Cost Optimization checks

## TAO Dashboard v1.5:
Added Account and IsSuppressed controls to the Performance and Fault Tolerance tabs

## TAO Dashboard v1.4:
**New tab Performance:** Added visuals with detailed view for Trusted Advisor Performance checks:
* High Utilization Amazon EC2 Instances
* Large Number of EC2 Security Group Rules Applied to an Instance
* Large Number of Rules in an EC2 Security Group
* Amazon Route 53 Alias Resource Record Sets
* CloudFront Header Forwarding and Cache Hit Ratio
* CloudFront Content Delivery Optimization
* Amazon EBS Provisioned IOPS (SSD) Volume Attachment Configuration
* Overutilized Amazon EBS Magnetic Volumes
**New tab Fault Tolerance:** Added visuals with detailed view for Trusted Advisor Fault Tolerance checks
* Amazon EBS Snapshots
* Amazon RDS Backups
* Amazon RDS Multi-AZ
* Amazon EC2 Availability Zone Balance
* AWS Lambda VPC-enabled Functions without Multi-AZ Redundancy
* Auto Scaling Group Health Check
* Amazon S3 Bucket Logging
* Amazon S3 Bucket Versioning
* ELB Cross-Zone Load Balancing
* VPN Tunnel Redundancy

* Cost Optimization: Fix for estimated monthly savings displayed incorrectly for numbers above 1000
* Cost Optimization: Fix for duplicate instances on Low Utilization Amazon EC2 Instances visual when instances type changed

## TAO Dashboard v1.4:
* New tab Performance: Added visuals with detailed view for Trusted Advisor Performance checks:
  * High Utilization Amazon EC2 Instances
  * Large Number of EC2 Security Group Rules Applied to an Instance
  * Large Number of Rules in an EC2 Security Group
  * Amazon Route 53 Alias Resource Record Sets
  * CloudFront Header Forwarding and Cache Hit Ratio
  * CloudFront Content Delivery Optimization
  * Amazon EBS Provisioned IOPS (SSD) Volume Attachment Configuration
  * Overutilized Amazon EBS Magnetic Volumes
* New tab Fault Tolerance: Added visuals with detailed view for Trusted Advisor Fault Tolerance checks
  * Amazon EBS Snapshots
  * Amazon RDS Backups
  * Amazon RDS Multi-AZ
  * Amazon EC2 Availability Zone Balance
  * AWS Lambda VPC-enabled Functions without Multi-AZ Redundancy
  * Auto Scaling Group Health Check
  * Amazon S3 Bucket Logging
  * Amazon S3 Bucket Versioning
  * ELB Cross-Zone Load Balancing
  * VPN Tunnel Redundancy
* Cost Optimization: Fix for estimated monthly savings displayed incorrectly for numbers above 1000
* Cost Optimization: Fix for duplicate instances on Low Utilization Amazon EC2 Instances visual when instances type changed

## TAO Dashboard v1.2.1:
* Fix for duplicating Lambda Functions on Security and Cost Optimization sheets

## TAO Dashboard v1.2:
* Security tab new visuals:
  * AWS Lambda Functions Using Deprecated (or about to be deprecated) Runtimes
* Cost Optimization tab new visuals:
  * AWS Lambda Functions with High Error Rates
* Fix for items without checks with OK status in Cost Optimization resource view visuals

## TAO Dashboard v1.1:
* Security tab new visuals:
  * Multi-factor authentication on root account
  * AWS Cloudtrail logging
  * Amazon EBS public snapshots
  * Security groups - Unrestricted access
  * Amazon EBS and RDS public snapshots
  * Exposed access keys
  * Amazon CloudFront custom SSL certificates in the IAM certificate store

## TAO Dashboard v1.0
* Initial release
