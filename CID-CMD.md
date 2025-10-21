# Command Tool CID-CMD

Cloud Intelligence Dashboards provides a python tool for installation and upgrade of Dashboards. This tool can also export dashboards to deployable artifacts including QuickSight Dashboards, Athena Views, Glue Tables etc.

CID is also provided in a form of CloudFormation templates. See detailed instructions in the [AWS Documentation](https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/deployment-in-global-regions.html) site.


## Before you start
1. :heavy_exclamation_mark: Complete the prerequisites for respective dashboard.
2. :heavy_exclamation_mark: [Specifying a Query Result Location Using a Workgroup](https://docs.aws.amazon.com/athena/latest/ug/querying.html#query-results-specify-location-workgroup)
3. :heavy_exclamation_mark: Make sure QuickSight [Enterprise edition](https://aws.amazon.com/premiumsupport/knowledge-center/quicksight-enterprise-account/) is activated.

## Command line tool cid-cmd

#### Demo: Deployment of Dashboards with cid-cmd tool

   [![asciicast](https://asciinema.org/a/467770.svg)](https://asciinema.org/a/467770)

#### Install

1. Launch [AWS CloudShell](https://console.aws.amazon.com/cloudshell/home) or your local shell

2. Install Python package cid-cmd
    ```bash
    pip3 install --upgrade cid-cmd
    ```

#### Dashboard Deployment

```bash
cid-cmd deploy
```


#### Update existing Dashboards
Update only Dashboard
```bash
cid-cmd update
```
Update dashboard and all dependencies (Datasets and Athena View). WARNING: this will override any customization of SQL files and Datasets.
```bash
cid-cmd update --force --recursive
```

#### Show Dashboard Status
Show dashboards status

```bash
cid-cmd status
```
[<img width="558" alt="status" src="https://github.com/aws-samples/aws-cudos-framework-deployment/assets/82834333/cae2015f-0f81-4593-80b3-c67ec1200fcd">](https://www.youtube.com/watch?v=ivr1MoGaApM)


####  Share QuickSight resources
```bash
cid-cmd share
```

#### Initialize Amazon QuickSight
One time action to initialize Amazon QuickSight Enterprise Edition.

```bash
cid-cmd init-qs
```

#### Initialize CUR
One time action to initialize Athena table and Crawler from s3 with CUR data. Currently only CUR1 supported.

```bash
cid-cmd init-cur
```

#### Create CUR proxy
There are 2 CUR formats that `cid-cmd` supports. CUR1 and CUR2 have slightly different fields structure. Each dashboard can be developed using CUR2 or CUR1, but on installation `cid-cmd` will deploy a special Athena View "CUR proxy". This View will play a role of compatibility layer between the CUR version used by Dashboard and available CUR. This View can be extended as new dashboard request CUR fields.

Please note that Cost Allocation Tags and Cost Categories are not present in CUR Proxy by default. You will need to add these fields by modifying CUR Proxy in Athena or using `cid-cmd` tool:

For CUR2 proxy (using CUR1 data as source)
```bash
cid-cmd create-cur-proxy -vv \
   --cur-version '2' \
   --cur-table-name 'mycur1' \
   --athena-workgroup 'primary' \
   --fields "resource_tags['user_cost_center'],resource_tags['user_owner']"
```

For CUR1 proxy (using CUR2 data as source)
```bash
cid-cmd create-cur-proxy -vv \
   --cur-version '1' \
   --cur-table-name 'mycur2' \
   --athena-workgroup 'primary' \
   --fields "resource_tags_user_cost_center,resource_tags_user_owner"
```


#### Delete Dashboard and all dependencies unused by other
Delete Dashboards and all dependencies unused by other CID-managed dashboards.(including QuickSight datasets, Athena views and tables)
```bash
cid-cmd delete
```

Delete Command Options:
```
 --dashboard-id TEXT QuickSight dashboard id
 --athena-database TEXT Athena database
```

#### Export
The command `export` lets you download or share a customized dashboard with another AWS Account. It takes the QuickSight Analysis as an input and generates all the assets needed to deploy your Analysis into another AWS Account. This command will generate a yaml file with a description of the Dashboard and all required Datasets. Also this command generates a QuickSight Template in the current AWS Account that can be used for Dashboard deployment in other accounts. The resource file can be used with all other cid commands. Both accounts must have relevant Athena Views and Tables.

Export from account A:
```
cid-cmd export
```

Deployment to account B:
```
cid-cmd deploy --resources ./mydashboard.yaml
```

#### See available commands and command line options
```
cid-cmd --help
```

#### CSV to Athena View
Generate and SQL file for Athena View for CSV file

```
cid-cmd csv2view --input my_mapping.csv --name my_mapping
```
This command generates a SQL file that you can execute. Please mind [Athena Service Limit for Query Size](https://docs.aws.amazon.com/athena/latest/ug/service-limits.html#service-limits-query-string-length).



## Troubleshooting and Support
If you experience unexpected behaviour of the `cid-cmd` script please run `cid-cmd` in debug mode:

```bash
cid-cmd -vv [command]
```
    
This will produce a log file in the same directory that were at the tile of launch of cid-cmd. 

:heavy_exclamation_mark:Inspect the produced debug log for any sensitive information and anonymize it.

We encourage you to open [new issue](https://github.com/aws-samples/aws-cudos-framework-deployment/issues/new) with description of the problem and attached debug log file.
