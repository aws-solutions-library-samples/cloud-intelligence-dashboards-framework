# What's new in the KPI Dashboard

## KPI - 2.1.0
* Fix for Billing Period and Account Name controls

## KPI - 2.1.0
* Removed link to deprecated feedback form

## KPI - 2.0.0
> [!IMPORTANT] 
> Update to this version requires cid-cmd v0.2.23. Please update cid-cmd first before updating the dashboard. During the update QuickSight datasets and Athena views will be updated, please make a copy if you've made any customizations. To update run these commands in your CloudShell (recommended) or other terminal:
```
python3 -m ensurepip --upgrade
pip3 install --upgrade cid-cmd
cid-cmd update --dashboard-id kpi_dashboard --recursive
```
> [!WARNING]
> You will be prompted to override KPI dashboard views and datasets with v2.0.0 versions. It's required to choose **proceed and override** for kpi_instance_all and kpi_tracker views and datasets while you can choose **keep existing** for others. Any customizations done to visuals for which you've selected **proceed and override** will be overwritten hence it's important to save copies of them in case you would like to re-implement them after update. You'll be able to see the diff of the changes before selecting an option.

Release notes:
* KPI Tracker: Added new KPI 'RDS Open Source Engines Coverage'
* Metrics Summary: Added RDS visual showing 'RDS Oracle Coverage', 'RDS SQL Server Coverage', 'RDS Open Source Engines Coverage', 'RDS Graviton Coverage'
* RDS: RDS Graviton coverage and savings estimations moved to the new RDS tab. Added visuals 'Top 10 Accounts Spend for Amazon RDS running on Graviton Processors', 'Top 10 Accounts Spend for Amazon RDS running on Other Processors' 
* RDS: Added section RDS Engines with Licensing Options with visuals 'Spend trend of RDS Engine Oracle, SQL Server by License Model', 'Potential Savings by migrating RDS Engine Oracle, SQL Server to Open Source engines', 'Top 10 Accounts Spend for RDS Engine Oracle, SQL Server', 'Coverage by Database Engines for Amazon Relational Database Service' and 'RDS Oracle, SQL Server  Instances and Potential Savings'

  

## KPI - 1.2.1
* Other Graviton: Fixed potential savings filter to show a correct monthly value.

## KPI - 1.2
* Metrics Summary: Added EC2 Unit Cost to EC2 section
* KPI S3 Storage All Athena view: Updated product_volume_type case when statement to like '%Intelligent%' for Intelligent Tiering to meet AIA formatting

## KPI - 1.1
* Calculated field correction for Normalized Usage Quantity in the summary_view and kpi_instance_all data sets. 
    ```bash
    ifelse(
    locate({instance_type}, '10xlarge')>0, 80*{usage_quantity},
    locate({instance_type}, '12xlarge')>0, 96*{usage_quantity},
    locate({instance_type}, '16xlarge')>0, 128*{usage_quantity},
    locate({instance_type}, '18xlarge')>0, 144*{usage_quantity},
    locate({instance_type}, '24xlarge')>0, 192*{usage_quantity},
    locate({instance_type}, '32xlarge')>0, 256*{usage_quantity}, 
    locate({instance_type}, '2xlarge')>0, 16*{usage_quantity},
    locate({instance_type}, '4xlarge')>0, 32*{usage_quantity},
    locate({instance_type}, '8xlarge')>0, 64*{usage_quantity},
    locate({instance_type}, '9xlarge')>0, 72*{usage_quantity},
    locate({instance_type}, 'nano')>0, .25*{usage_quantity},
    locate({instance_type}, 'micro')>0, 0.5*{usage_quantity},
    locate({instance_type}, 'small')>0, 1*{usage_quantity},
    locate({instance_type}, 'medium')>0, 2*{usage_quantity},
    locate({instance_type}, 'xlarge')>0, 8*{usage_quantity},
    locate({instance_type}, 'large')>0, 4*{usage_quantity},
    {usage_quantity})
    ```

## KPI - 1
* Launch of KPI Dashboard
