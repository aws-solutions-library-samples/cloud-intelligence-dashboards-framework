#!/bin/bats

account_id=$(aws sts get-caller-identity --query "Account" --output text )
database_name="${database_name:-optimization_data}" # If variable not set or null, use default
quicksight_group="${quicksight_group:-cid-owners}" # If variable not set or null, use default
quicksight_datasource_id="${quicksight_datasource_id:-CID-CMD-Athena}" # If variable not set or null, use default

@test "Install RDS Health Dashboard" {
  run cid-cmd -vv deploy  \
    --dashboard-id rds-health-dashboard \
    --athena-database $database_name\
    --athena-workgroup primary\
    --quicksight-group $quicksight_group \
    --share-with-account \
    --quicksight-datasource-id $quicksight_datasource_id \

  [ "$status" -eq 0 ]
}

@test "Views created - rds_analysis_daily_backup_view" {
  run aws athena get-table-metadata \
    --catalog-name 'AwsDataCatalog'\
    --database-name $database_name \
    --table-name 'rds_analysis_daily_backup_view'

  [ "$status" -eq 0 ]
}

@test "Views created - rds_analysis_dist_view" {
  run aws athena get-table-metadata \
    --catalog-name 'AwsDataCatalog'\
    --database-name $database_name \
    --table-name 'rds_analysis_dist_view'

  [ "$status" -eq 0 ]
}

@test "Views created - rds_analysis_endofsupport_view" {
  run aws athena get-table-metadata \
    --catalog-name 'AwsDataCatalog'\
    --database-name $database_name \
    --table-name 'rds_analysis_endofsupport_view'

  [ "$status" -eq 0 ]
}

@test "Views created - rds_analysis_maintenance_view" {
  run aws athena get-table-metadata \
    --catalog-name 'AwsDataCatalog'\
    --database-name $database_name \
    --table-name 'rds_analysis_maintenance_view'

  [ "$status" -eq 0 ]
}

@test "Views created - rds_analysis_tags_view" {
  run aws athena get-table-metadata \
    --catalog-name 'AwsDataCatalog'\
    --database-name $database_name \
    --table-name 'rds_analysis_tags_view'

  [ "$status" -eq 0 ]
}

@test "Views created - rds_aurora_io_optimization_view" {
  run aws athena get-table-metadata \
    --catalog-name 'AwsDataCatalog'\
    --database-name $database_name \
    --table-name 'rds_aurora_io_optimization_view'

  [ "$status" -eq 0 ]
}

@test "Views created - rds_cost_summary_view" {
  run aws athena get-table-metadata \
    --catalog-name 'AwsDataCatalog'\
    --database-name $database_name \
    --table-name 'rds_cost_summary_view'

  [ "$status" -eq 0 ]
}

@test "Dataset created - rds_analysis_daily_backup_dataset" {
  run aws quicksight describe-data-set \
    --aws-account-id $account_id \
    --data-set-id rds_analysis_daily_backup_dataset

  [ "$status" -eq 0 ]
}

@test "Dataset created - rds_analysis_dataset" {
  run aws quicksight describe-data-set \
    --aws-account-id $account_id \
    --data-set-id rds_analysis_dataset

  [ "$status" -eq 0 ]
}

@test "Dataset created - rds_analysis_endofsupport_dataset" {
  run aws quicksight describe-data-set \
    --aws-account-id $account_id \
    --data-set-id rds_analysis_endofsupport_dataset

  [ "$status" -eq 0 ]
}

@test "Dataset created - rds_analysis_maintenance_dataset" {
  run aws quicksight describe-data-set \
    --aws-account-id $account_id \
    --data-set-id rds_analysis_maintenance_dataset

  [ "$status" -eq 0 ]
}

@test "Dataset created - rds_analysis_tags_dataset" {
  run aws quicksight describe-data-set \
    --aws-account-id $account_id \
    --data-set-id rds_analysis_tags_dataset

  [ "$status" -eq 0 ]
}

@test "Dataset created - rds_aurora_io_optimized_dataset" {
  run aws quicksight describe-data-set \
    --aws-account-id $account_id \
    --data-set-id rds_aurora_io_optimized_dataset

  [ "$status" -eq 0 ]
}

@test "Dataset created - rds_cost_summary_view" {
  run aws quicksight describe-data-set \
    --aws-account-id $account_id \
    --data-set-id rds_cost_summary_view

  [ "$status" -eq 0 ]
}

@test "Dashboard created" {
  run aws quicksight describe-dashboard \
    --aws-account-id $account_id \
    --dashboard-id rds-health-dashboard

  [ "$status" -eq 0 ]
}

@test "Update works" {
  run cid-cmd -vv --yes update --force --recursive  \
    --dashboard-id rds-health-dashboard \
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
    --dashboard-id rds-health-dashboard

  [ "$status" -eq 0 ]
}

@test "Dashboard is deleted" {
  run aws quicksight describe-dashboard \
    --aws-account-id $account_id \
    --dashboard-id rds-health-dashboard

  [ "$status" -ne 0 ]
}

@test "Datasets are deleted" {
  skip "Datasets may be shared with other dashboards"
  run aws quicksight describe-data-set \
    --aws-account-id $account_id \
    --data-set-id rds_analysis_dataset

  [ "$status" -ne 0 ]
}

@test "Views are deleted" {
  skip "Views may be shared with other dashboards"
  run aws athena get-table-metadata \
    --catalog-name 'AwsDataCatalog'\
    --database-name $database_name \
    --table-name 'rds_analysis_dist_view'

  [ "$status" -ne 0 ]
}
