#!/bin/bats

account_id=$(aws sts get-caller-identity --query "Account" --output text )
quicksight_group="${quicksight_group:-cid-owners}" # If variable not set or null, use default
quicksight_datasource_id="${quicksight_datasource_id:-CID-CMD-Athena}" # If variable not set or null, use default

@test "Install" {
  run cid-cmd -vv deploy  \
    --dashboard-id ta-organizational-view \
    --athena-database 'optimization_data' \
    --athena-workgroup primary\
    --quicksight-group $quicksight_group \
    --share-with-account \
    --quicksight-datasource-id $quicksight_datasource_id \

    --view-ta-organizational-view-reports-s3FolderPath "s3://cid-data-$account_id/optics-data-collector/ta-data'"

  [ "$status" -eq 0 ]
}

@test "Views created" {
  run aws athena get-table-metadata \
    --catalog-name 'AwsDataCatalog'\
    --database-name 'optimization_data' \
    --table-name 'ta_organizational_view_reports'

  [ "$status" -eq 0 ]
}

@test "Dataset created" {
  run aws quicksight describe-data-set \
    --aws-account-id $account_id \
    --data-set-id ta-organizational-view

  [ "$status" -eq 0 ]
}

@test "Dashboard created" {
  run aws quicksight describe-dashboard \
    --aws-account-id $account_id \
    --dashboard-id ta-organizational-view

  [ "$status" -eq 0 ]
}

@test "Update works" {
  run cid-cmd -vv --yes update --force --recursive  \
    --dashboard-id ta-organizational-view \
    --athena-workgroup primary\
    --quicksight-group $quicksight_group \
    --quicksight-datasource-id $quicksight_datasource_id \
    --view-ta-organizational-view-reports-s3FolderPath "s3://cid-data-$account_id)/optics-data-collector/ta-data"

  [ "$status" -eq 0 ]
  echo "$output" | grep 'Update completed'
}

@test "Delete runs" {
  run cid-cmd -vv --yes delete \
    --dashboard-id ta-organizational-view

  [ "$status" -eq 0 ]
}

@test "Dashboard is deleted" {
  run aws quicksight describe-dashboard \
    --aws-account-id $account_id \
    --dashboard-id ta-organizational-view

  [ "$status" -ne 0 ]
}

@test "Dataset is deleted" {
  run aws quicksight describe-data-set \
    --aws-account-id $account_id \
    --data-set-id ta-organizational-view

  [ "$status" -ne 0 ]
}

@test "View is deleted" {
  run aws quicksight describe-dashboard \
    --aws-account-id $account_id \
    --dashboard-id ta-organizational-view

  [ "$status" -ne 0 ]
}