# What's new in Extended Support Cost Projection

## Extended Support Cost Projection - v4.0.4

**Important:** This version requires the data collection version 3.2.0+. Update to this version requires a forced update. Since this is a major version upgrade, the `cid-cmd` tool will ask to confirm a recursive update or not. Please make sure to confirm the recursive update by answering **yes** to continue the update process and have the new Elasticache dataset and Athena view deployed for the dashboard.

If you have modified the Extended Support Cost Projection dashboard visuals, these changes will be overridden when the dashboard is updated. Consider backing-up the existing dashboard by creating an analysis from it if you want to keep a reference to customised visuals so you can re-apply them after the update takes place.

To update run these commands in your CloudShell (recommended) or other terminal:

```
python3 -m ensurepip --upgrade
pip3 install --upgrade cid-cmd
cid-cmd update --dashboard-id extended-support-cost-projection --force --recursive
```

- Adjusting OpenSearch view query to resolve instance normalization factor based on CUR product_instance_type instead of the instance type retrieved from inventory data. The inventory data will continue to provide the different domains available.
- Added OpenSearch version 1.1 to release calendar.
- Added EKS Kubernetes versions 1.33 and 1.34 to release calendar.
- Adjusted Aurora MySQL 3 year 1 start date to 2028-05-01.

## Extended Support Cost Projection - v4.0.3

**Important:** This version requires the data collection version 3.2.0+. Update to this version requires a forced update. Since this is a major version upgrade, the `cid-cmd` tool will ask to confirm a recursive update or not. Please make sure to confirm the recursive update by answering **yes** to continue the update process and have the new Elasticache dataset and Athena view deployed for the dashboard.

If you have modified the Extended Support Cost Projection dashboard visuals, these changes will be overridden when the dashboard is updated. Consider backing-up the existing dashboard by creating an analysis from it if you want to keep a reference to customised visuals so you can re-apply them after the update takes place.

To update run these commands in your CloudShell (recommended) or other terminal:

```
python3 -m ensurepip --upgrade
pip3 install --upgrade cid-cmd
cid-cmd update --dashboard-id extended-support-cost-projection --force --recursive
```

- Adding Extended Support Elasticache sheet.

## Extended Support Cost Projection - v4.0.2

**Important:** This version requires the data collection version 3.2.0+. Update to this version requires a forced and recursive update.

If you have modified the Extended Support Cost Projection dashboard visuals, these changes will be overridden when the dashboard is updated. Consider backing-up the existing dashboard by creating an analysis from it if you want to keep a reference to customised visuals so you can re-apply them after the update takes place.

To update run these commands in your CloudShell (recommended) or other terminal:

```
python3 -m ensurepip --upgrade
pip3 install --upgrade cid-cmd
cid-cmd update --dashboard-id extended-support-cost-projection
```

- Fixing existing behaviour for engine, engine version and cluster version controls where selections are being retained across sheets, resulting in mixed values being displayed in controls. New individual parameters and controls are now defined for RDS engine and engine version, EKS cluster version, and OpenSearch engine version. Filters on each sheet are associated to their new corresponding parameters.

## Extended Support Cost Projection - v4.0.1

**Important:** This version requires the data collection version 3.2.0+. Update to this version requires a forced and recursive update.

If you have modified the Extended Support Cost Projection dashboard view queries, they will be overridden when the dashboard is updated. Consider backing-up the existing view queries if they contain custom changes you want to keep so you can re-apply them after the update takes place.

To update run these commands in your CloudShell (recommended) or other terminal:

```
python3 -m ensurepip --upgrade
pip3 install --upgrade cid-cmd
cid-cmd update --dashboard-id extended-support-cost-projection --force --recursive
```

- Adding/Adjusting RDS, EKS and OpenSearch release calendar dates.
- Removing year 3 start date fallback to year 1-2 start date for RDS engine versions that do not have year 3 start date. This aligns the information displayed for year 3 start date with the zero estimated cost change applied in version 4.0.0.


## Extended Support Cost Projection - v4.0.0

**Important:** This version requires the data collection version 3.2.0+. Update to this version requires a forced update. Since this is a major version upgrade, the `cid-cmd` tool will ask to confirm a recursive update or not. Please make sure to confirm the recursive update by answering **yes** to continue the update process and have the new OpenSearch dataset and Athena view deployed for the dashboard.

If you have modified the Extended Support Cost Projection dashboard view queries, they will be overridden when the dashboard is updated. Consider backing-up the existing view queries if they contain custom changes you want to keep so you can re-apply them after the update takes place.

To update run these commands in your CloudShell (recommended) or other terminal:

```
python3 -m ensurepip --upgrade
pip3 install --upgrade cid-cmd
cid-cmd update --dashboard-id extended-support-cost-projection --force
```

- Adding Extended Support OpenSearch sheet.
- Setting date range for cost estimation to be the last 30 days of usage. Estimated cost values are now always based on resource usage for the last 30 days.
- Adjusting RDS "Year 3" estimated cost to be zero when the database engine version extended support only lasts for 2 years.
- Adjusting visuals by adding labels that indicate the values and information displayed corresponds to the last 30 days.
- Removing date range filter control from all sheets.
- Adding new filter controls for engine, engine version and cluster version in the RDS, EKS and OpenSearch sheets.


## Extended Support Cost Projection - v3.1.0

**Important:** This version requires the data collection version 3.2.0+. Update to this version requires a forced and recursive update. If you have modified the Extended Support Cost Projection dashboard view queries, they will be overridden when the dashboard is updated. Consider backing-up the existing view queries if they contain custom changes you want to keep so you can re-apply them after the update takes place.

To update run these commands in your CloudShell (recommended) or other terminal:

```
python3 -m ensurepip --upgrade
pip3 install --upgrade cid-cmd
cid-cmd update --dashboard-id extended-support-cost-projection --force --recursive
```

## Extended Support Cost Projection - v3.0.0

**Important:** This version requires the data collection version 3.2.0+. Update to this version requires a forced and recursive update. If you have modified the Extended Support Cost Projection dashboard view queries, they will be overridden when the dashboard is updated. Consider backing-up the existing view queries if they contain custom changes you want to keep so you can re-apply them after the update takes place.

To update run these commands in your CloudShell (recommended) or other terminal:

```
python3 -m ensurepip --upgrade
pip3 install --upgrade cid-cmd
cid-cmd update --dashboard-id extended-support-cost-projection --force --recursive
```

- Adjusted RDS queries to resolve Aurora Serverless databases for versions 1 and 2.

## Extended Support Cost Projection - v2.0.0

**Important:** Update to this version requires a forced and recursive update. If you have modified the Extended Support Cost Projection dashboard view queries, they will be overridden when the dashboard is updated. Consider backing-up the existing view queries if they contain custom changes you want to keep so you can re-apply them after the update takes place.

To update run these commands in your CloudShell (recommended) or other terminal:

```
python3 -m ensurepip --upgrade
pip3 install --upgrade cid-cmd
cid-cmd update --dashboard-id extended-support-cost-projection --force --recursive
```

- Added Kubernetes version 1.30 to the calendar data in the EKS view definition.

## Extended Support Cost Projection - v1.2.0

**Important:** Update to this version requires a forced and recursive update. If you have modified the Extended Support Cost Projection dashboard view queries, they will be overridden when the dashboard is updated. Consider backing-up the existing view queries if they contain custom changes you want to keep so you can re-apply them after the update takes place.

To update run these commands in your CloudShell (recommended) or other terminal:

```
python3 -m ensurepip --upgrade
pip3 install --upgrade cid-cmd
cid-cmd update --dashboard-id extended-support-cost-projection --force --recursive
```

You will be presented with the option "proceed and override" to update the RDS and EKS views. Please choose this option to ensure changes to the underlying view queries are applied in your RDS and EKS Extended support views.

- Adding preprocessing for RDS engine versions to simplify release calendar maintenance. Major versions will be used normally except in cases where minor versions are targeted for the extended support.
- Added remaining dates for the release calendar.
- Simplified release calendar maintenance by using only major version (or major.minor for where a minor version is targeted).
- Added account names and IDs to breakdown tables.
- Added start date for RDS Extended Support Estimated Cost Breakdown visual
- Increased size of EKS Extended Support Estimated Cost Breakdown to shows more items without scrolling
- Added action filters for all visuals in the dashboard

## v1.1.0
* Includes EKS Extended Support Cost Projection sheet.
* Adjusted titles for visuals showing estimated costs. Estimates are based on resource usage over a given period of time.
* Adjusted names for calculated fields and parameters.


## v1.0.0
* Initial release
* Includes RDS Extended Support Cost Projection and About sheets.
