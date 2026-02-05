#!/bin/bats


account_id=$(aws sts get-caller-identity --query "Account" --output text )
database_name="${database_name:-cid_data_export}" # If variable not set or null, use default

@test "Install" {
  run cid-cmd -vv deploy  \
    --dashboard-id cost_intelligence_dashboard \
    --athena-database $database_name\
    --account-map-source dummy \
    --share-with-account \

  [ "$status" -eq 0 ]
}

@test "Views created" {
  run aws athena get-table-metadata \
    --catalog-name 'AwsDataCatalog'\
    --database-name $database_name \
    --table-name 'summary_view' \
 
 # FIXME: add
 #  compute_savings_plan_eligible_spend
 #  summary_view
 #  s3_view
 #  customer_all
 #  ec2_running_cost

  [ "$status" -eq 0 ]
}

@test "Dataset created" {
  run aws quicksight describe-data-set \
    --aws-account-id $account_id \
    --data-set-id d01a936f-2b8f-49dd-8f95-d9c7130c5e46

  [ "$status" -eq 0 ]
}

@test "Dashboard created" {
  run aws quicksight describe-dashboard \
    --aws-account-id $account_id \
    --dashboard-id cost_intelligence_dashboard

  [ "$status" -eq 0 ]
}

@test "Update works" {
  run cid-cmd -vv --yes update --force --recursive  \
    --dashboard-id cost_intelligence_dashboard \

  [ "$status" -eq 0 ]
  echo "$output" | grep 'Update completed'
}


@test "Delete runs" {
  run cid-cmd -vv --yes delete \
    --dashboard-id cost_intelligence_dashboard

  [ "$status" -eq 0 ]
}

@test "Dashboard is deleted" {
  run aws quicksight describe-dashboard \
    --aws-account-id $account_id \
    --dashboard-id cost_intelligence_dashboard

  [ "$status" -ne 0 ]
}

@test "Dataset is deleted" {
  run aws quicksight describe-data-set \
    --aws-account-id $account_id \
    --data-set-id d01a936f-2b8f-49dd-8f95-d9c7130c5e46

  [ "$status" -ne 0 ]
}

@test "View is deleted" {
  skip 
  run aws athena get-table-metadata \
    --catalog-name 'AwsDataCatalog'\
    --database-name $database_name \
    --table-name 'summary_view'

  [ "$status" -ne 0 ]
}