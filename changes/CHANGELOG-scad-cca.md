# What's new in SCAD Containers Cost Allocation Dashboard

## SCAD Containers Cost Allocation Dashboard - v3.0.1

* Fixed an issue with GPU cost showing 0
* Added node_gpu column

## SCAD Containers Cost Allocation Dashboard - v3.0.0
* New features:
  * GPU cost support (Amazon EKS only), reflected as follows:
    * New KPI visual in the "Executive Summary" sheet
    * GPU cost is included in "Cost Metric" drop-down list in all sheets
    * GPU cost is included in the calculation of "Total Cost" cost metric in all sheets
    * GPU Cost column added to the pivot in "Workloads Explorer" sheet
  * AWS Fargate support (Amazon EKS and Amazon ECS), reflected as follows:
    * All data in the dashboard now includes Fargate pods/tasks, in any visual, unless filtered by the user 
    * Added "Launch Type" control to all sheets, includes the options "EC2" and "Fargate"
    * Added "Launch Type" option in the "Group By" control in the "Workloads Explorer" sheet
  * Added EC2 instance processor fields, reflected as follows:
    * New "Processor Family" filter and group-by option in the "Workloads Explorer" sheet (values examples: Graviton, AMD, Intel, Apple). Based on the value from the `product[physical_processor]` CUR column
    * New "Processor" filter and group-by option in the "Workloads Explorer" sheet, includes the exact processor name from the `product[physical_processor]` CUR column
    * New processor family coverage visual in the "Cluster Breakdown" sheet
* Bug fixes:
  * Fixed JOIN issue with daily and monthly CUR, which resulted in missing data
* Removals:
  * Removed the following KPI visuals from the "Executive Summary" sheet: "Total Clusters", "Total EC2 Instances", "Total EKS Pods", "Total ECS Tasks"

## SCAD Containers Cost Allocation Dashboard - v2.0.0
* Added support for AWS Split Cost Allocation Data (SCAD) for ECS:
  * All visuals now include SCAD ECS data (including AWS Batch on ECS), in addition to SCAD EKS data
  * The "EKS Breakdown" sheet has been renamed "Cluster Breakdown"
* CUR 2.0 support: The dashboard now defaults to CUR 2.0, and still supports legacy CUR
* The `scad_cca_summary_view` and `scad_cca_hourly_resource_view` Athena views have been converged to a single view.  
The time and data granularity levels remain the same, but now in a single view instead of 2 views
* The hourly and resource-level interactive visuals in the "Cluster Breakdown" sheet have been removed, and converged with the visuals in the "Workloads Explorer" tab, which now have hourly and resource-level data, in addition to the current granularity levels in these visuals

Notes:  
* For this version (v2.0.0), the minimum required `cid-cmd` version is 4.0.9
* Starting this version (v2.0.0), the `scad_cca_hourly_resource_view` QuickSight dataset and Athena view are no longer used by the dashboard.  
Following an upgrade to this version (v2.0.0), after verifying functionality of the dashboard, you can delete the `scad_cca_hourly_resource_view` QuickSight dataset and Athena view (in this order).  
They won't be deleted automatically, and will continue refreshing and incurring cost if you don't delete them.  
Deleting the `scad_cca_hourly_resource_view` QuickSight dataset and Athena view is not necessary on a new installation of the dashboard, only following an upgrade.  
Deleting a QuickSight dataset: https://docs.aws.amazon.com/quicksight/latest/user/delete-a-data-set.html  
Deleting an Athena view: https://docs.aws.amazon.com/athena/latest/ug/drop-view.html

## SCAD Containers Cost Allocation Dashboard - v1.0.0
* Added support to view Net Amortized Cost in "View Cost As" control in all sheets
* Removed "Exclude last 1 month" from all date range controls to prevent "No Data" (because Split Cost Allocation Data for EKS starts filling data only in current month)
* Fixed issue where all split cost and usage metrics were lower than they should be, for pods on EC2 instances that were running for less than a full hour
* Fixed aggregation issues for usage metrics in Athena views

## SCAD Containers Cost Allocation Dashboard - v0.0.1
* Initial release
