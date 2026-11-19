# What's new in the Sustainability Proxy and Carbon emissions data Dashboard

## Sustainability Proxy and Carbon emissions data Dashboard - v6.1.0

**Important:** This release changes views and datasets. Update with `cid-cmd update --dashboard-id sustainability-proxy-metrics --force --recursive`. During the update, select the cost allocation tags and cost categories to add to the datasets, then select which of them to add as dashboard filters and Group By options. When you are shown the difference for each `sus_*` view, choose **proceed and override**, otherwise the views keep the old tag column. Without a terminal, add `--on-drift override`. The workload tag selected in earlier versions is not carried over, so select it again.

**Important:** If you replaced the `sus_buss_kpi` view with your own business metrics, rename its `tagValue` column to `tags_json` and key your rows by the same `tags_json` value that the proxy views produce.

* **Compute Proxies, Storage Proxies, Data Transfer / Networking Proxies**: Group By now lists each tag and cost category you select during deployment, instead of a single 'Environment Tag' option. With no tags selected, Group By shows Linked Account only.
* **Compute Proxies, Storage Proxies, Data Transfer / Networking Proxies**: The tags and cost categories you select are also added as dashboard filters.
* **Compute Proxies, Storage Proxies, Data Transfer / Networking Proxies**: With Include Business KPI set to Yes, grouping by a tag or cost category scales usage by the business KPI of the matching workload.
* **Deployment**: Deploying with CloudFormation no longer fails with 'Please set parameter tag' ([#1428](https://github.com/aws-solutions-library-samples/cloud-intelligence-dashboards-framework/issues/1428)). The views use the same `tags_json` pattern as CUDOS, and a deployment without a terminal adds no tags.

## Sustainability Proxy and Carbon emissions data Dashboard - v6.0.1
* **Carbon Emissions**: Removed the `source_account_id` and `report_name` columns from the `sus_carbon_emissions` view and dataset.
* **Deployment**: Set the default of the workload tag parameter to `none`.

## Sustainability Proxy and Carbon emissions data Dashboard - v6.0.0
* **Carbon Emissions**: Added Scope 3 emissions and broke out Scope 1, Scope 2 and Scope 3 values, with location-based and market-based values for Scope 2 and Scope 3.
* **Carbon Emissions**: Fixed the sorting of the x axis.

## Sustainability Proxy and Carbon emissions data Dashboard - v5.0.0
* Added Location Based Metric (LBM) data from the Carbon Data export. 
* Fixed null account id issue when account map value not found.

## Sustainability Proxy and Carbon emissions data Dashboard - v4.0.0
* Removed option for using experimental-programmatic-access-ccft (Deprecated)
* Added dependency on the Carbon Data export.
* Updated datasets to gain data from the Carbon Data export.
