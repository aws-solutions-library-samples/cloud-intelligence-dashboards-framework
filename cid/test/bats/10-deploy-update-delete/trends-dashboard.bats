#!/bin/bats


account_id=$(aws sts get-caller-identity --query "Account" --output text )
database_name="${database_name:-athenacurcfn_cur1}" # If variable not set or null, use default
quicksight_group="${quicksight_group:-cid-owners}" # If variable not set or null, use default
quicksight_datasource_id="${quicksight_datasource_id:-CID-CMD-Athena}" # If variable not set or null, use default

@test "Install" {
  run cid-cmd -vv deploy  \
    --dashboard-id trends-dashboard \
    --athena-database $database_name\
    --account-map-source dummy \
    --athena-workgroup primary\
    --quicksight-group $quicksight_group \
    --share-with-account \
    --quicksight-datasource-id $quicksight_datasource_id \

  [ "$status" -eq 0 ]
}

@test "Views created" {
  run aws athena get-table-metadata \
    --catalog-name 'AwsDataCatalog'\
    --database-name $database_name \
    --table-name 'monthly_anomaly_detection' \
 
 # FIXME: add
 # - daily-anomaly-detection
 # - monthly-bill-by-account
 # - monthly-anomaly-detection

  [ "$status" -eq 0 ]
}

@test "Dataset created" {
  run aws quicksight describe-data-set \
    --aws-account-id $account_id \
    --data-set-id 0f11c81d-536a-405f-8de0-d0dc247627ad

  [ "$status" -eq 0 ]
}

@test "Dashboard created" {
  run aws quicksight describe-dashboard \
    --aws-account-id $account_id \
    --dashboard-id trends-dashboard

  [ "$status" -eq 0 ]
}

@test "Update works" {
  run cid-cmd -vv --yes update --force --recursive  \
    --dashboard-id trends-dashboard \
    --athena-database $database_name\
    --athena-workgroup primary\
    --quicksight-group $quicksight_group \
    --quicksight-datasource-id $quicksight_datasource_id \

  [ "$status" -eq 0 ]
  echo "$output" | grep 'Update completed'
}


@test "Delete runs" {
  run cid-cmd -vv --yes delete \
    --athena-database $database_name\
    --athena-workgroup primary\
    --dashboard-id trends-dashboard

  [ "$status" -eq 0 ]
}

@test "Dashboard is deleted" {
  run aws quicksight describe-dashboard \
    --aws-account-id $account_id \
    --dashboard-id trends-dashboard

  [ "$status" -ne 0 ]
}

@test "Dataset is deleted" {
  run aws quicksight describe-data-set \
    --aws-account-id $account_id \
    --data-set-id 0f11c81d-536a-405f-8de0-d0dc247627ad

  [ "$status" -ne 0 ]
}

@test "View is deleted" {
  run aws athena get-table-metadata \
    --catalog-name 'AwsDataCatalog'\
    --database-name $database_name \
    --table-name 'monthly_anomaly_detection'

  [ "$status" -ne 0 ]
}