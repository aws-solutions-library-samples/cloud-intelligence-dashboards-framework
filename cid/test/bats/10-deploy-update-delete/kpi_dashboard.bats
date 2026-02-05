#!/bin/bats


account_id=$(aws sts get-caller-identity --query "Account" --output text )
database_name="${database_name:-id_data_export}" # If variable not set or null, use default

@test "Install" {
  run cid-cmd -vv deploy  \
    --dashboard-id kpi_dashboard \
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
 #  - kpi_ebs_storage_all
 #  - kpi_ebs_snap
 #  - kpi_instance_all
 #  - kpi_s3_storage_all
 #  - kpi_tracker
 #  - summary_view

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
    --dashboard-id kpi_dashboard

  [ "$status" -eq 0 ]
}

@test "Update works" {
  run cid-cmd -vv --yes update --force --recursive  \
    --dashboard-id kpi_dashboard \

  [ "$status" -eq 0 ]
  echo "$output" | grep 'Update completed'
}


@test "Delete runs" {
  run cid-cmd -vv --yes delete \
    --dashboard-id kpi_dashboard

  [ "$status" -eq 0 ]
}

@test "Dashboard is deleted" {
  run aws quicksight describe-dashboard \
    --aws-account-id $account_id \
    --dashboard-id kpi_dashboard

  [ "$status" -ne 0 ]
}

@test "Dataset is deleted" {
  skip "summary_view can be used by others"
  run aws quicksight describe-data-set \
    --aws-account-id $account_id \
    --data-set-id d01a936f-2b8f-49dd-8f95-d9c7130c5e46

  [ "$status" -ne 0 ]
}

@test "View is deleted" {
  skip "summary_view can be used by others"
  run aws athena get-table-metadata \
    --catalog-name 'AwsDataCatalog'\
    --database-name $database_name \
    --table-name 'summary_view'

  [ "$status" -ne 0 ]
}